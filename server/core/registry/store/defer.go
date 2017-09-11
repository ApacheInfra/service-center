//Copyright 2017 Huawei Technologies Co., Ltd
//
//Licensed under the Apache License, Version 2.0 (the "License");
//you may not use this file except in compliance with the License.
//You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
//Unless required by applicable law or agreed to in writing, software
//distributed under the License is distributed on an "AS IS" BASIS,
//WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//See the License for the specific language governing permissions and
//limitations under the License.
package store

import (
	"encoding/json"
	pb "github.com/ServiceComb/service-center/server/core/proto"
	"github.com/ServiceComb/service-center/util"
	"github.com/coreos/etcd/mvcc/mvccpb"
	"sync"
	"time"
)

type DeferHandler interface {
	OnCondition(Cache, []*Event) bool
	HandleChan() <-chan *Event
}

type InstanceEventDeferHandler struct {
	enabled bool
	events  map[string]*Event
	ttls    map[string]int64
	mux     sync.RWMutex
	deferCh chan *Event
}

func (iedh *InstanceEventDeferHandler) needDefer(cache Cache, evts []*Event) bool {
	kvCache, ok := cache.(*KvCache)
	if !ok {
		return false
	}
	if len(evts)/kvCache.Size() < 0 {
		return false
	}

	for _, evt := range evts {
		if evt.Type != pb.EVT_DELETE {
			return false
		}
	}
	return true
}

func (iedh *InstanceEventDeferHandler) init() {
	if iedh.deferCh == nil {
		iedh.deferCh = make(chan *Event, event_block_size)
	}

	if iedh.events == nil {
		iedh.events = make(map[string]*Event, event_block_size)
		iedh.ttls = make(map[string]int64, event_block_size)
		util.Go(iedh.check)
	}
}

func (iedh *InstanceEventDeferHandler) OnCondition(cache Cache, evts []*Event) bool {
	iedh.mux.Lock()
	if !iedh.enabled && iedh.needDefer(cache, evts) {
		util.Logger().Warnf(nil, "self preservation is enabled, caught %d DELETE events", len(evts))
		iedh.enabled = true
	}

	if !iedh.enabled {
		iedh.mux.Unlock()
		return false
	}

	iedh.init()

	for _, evt := range evts {
		switch evt.Type {
		case pb.EVT_CREATE, pb.EVT_UPDATE:
			delete(iedh.events, evt.Key)
			delete(iedh.ttls, evt.Key)

			iedh.deferCh <- evt

			util.Logger().Debugf("recover key %s events", evt.Key)
		case pb.EVT_DELETE:
			kv, ok := evt.Object.(*mvccpb.KeyValue)
			if !ok {
				continue
			}
			var instance pb.MicroServiceInstance
			err := json.Unmarshal(kv.Value, &instance)
			if err != nil {
				util.Logger().Errorf(err, "unmarshal instance file failed, key is %s", evt.Key)
				continue
			}
			iedh.events[evt.Key] = evt
			iedh.ttls[evt.Key] = int64(instance.HealthCheck.Interval)
		}
	}
	iedh.mux.Unlock()
	return true
}

func (iedh *InstanceEventDeferHandler) check(stopCh <-chan struct{}) {
	defer util.RecoverAndReport()
	for {
		select {
		case <-stopCh:
			return
		case <-time.After(time.Second):
			iedh.mux.Lock()
			if len(iedh.ttls) == 0 {
				iedh.enabled = false
				iedh.mux.Unlock()

				util.Logger().Warnf(nil, "self preservation is stopped")
				continue
			}

			for key, ttl := range iedh.ttls {
				ttl--
				if ttl > 0 {
					iedh.ttls[key] = ttl
					continue
				}

				evt := iedh.events[key]
				delete(iedh.events, key)
				delete(iedh.ttls, key)

				iedh.deferCh <- evt

				util.Logger().Debugf("defer handle timed out, key is %s", evt.Key)
			}
			iedh.mux.Unlock()
		}
	}
}

func (iedh *InstanceEventDeferHandler) HandleChan() <-chan *Event {
	return iedh.deferCh
}
