/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package event

import (
	"encoding/json"
	"fmt"
	"github.com/apache/incubator-servicecomb-service-center/pkg/util"
	"github.com/apache/incubator-servicecomb-service-center/server/core"
	"github.com/apache/incubator-servicecomb-service-center/server/core/backend"
	"github.com/apache/incubator-servicecomb-service-center/server/core/backend/store"
	pb "github.com/apache/incubator-servicecomb-service-center/server/core/proto"
	"github.com/apache/incubator-servicecomb-service-center/server/infra/registry"
	"github.com/apache/incubator-servicecomb-service-center/server/mux"
	serviceUtil "github.com/apache/incubator-servicecomb-service-center/server/service/util"
	"github.com/coreos/etcd/mvcc/mvccpb"
	"golang.org/x/net/context"
	"time"
)

type DependencyEventHandler struct {
	signals *util.UniQueue
}

func (h *DependencyEventHandler) Type() store.StoreType {
	return store.DEPENDENCY_QUEUE
}

func (h *DependencyEventHandler) OnEvent(evt *store.KvEvent) {
	action := evt.Action
	if action != pb.EVT_CREATE && action != pb.EVT_UPDATE && action != pb.EVT_INIT {
		return
	}

	h.signals.Put(context.Background(), struct{}{})
}

func (h *DependencyEventHandler) loop() {
	util.Go(func(stopCh <-chan struct{}) {
		waitDelayIndex := 0
		waitDelay := []int{1, 1, 5, 10, 20, 30, 60}
		retry := func() {
			if waitDelayIndex >= len(waitDelay) {
				waitDelayIndex = 0
			}
			<-time.After(time.Duration(waitDelay[waitDelayIndex]) * time.Second)
			waitDelayIndex++

			h.signals.Put(context.Background(), struct{}{})
		}
		for {
			select {
			case <-stopCh:
				return
			case <-h.signals.Chan():
				lock, err := mux.Try(mux.DEP_QUEUE_LOCK)
				if err != nil {
					util.Logger().Errorf(err, "try to lock %s failed", mux.DEP_QUEUE_LOCK)
					retry()
					continue
				}

				if lock == nil {
					continue
				}

				err = h.Handle()
				lock.Unlock()
				if err != nil {
					util.Logger().Errorf(err, "handle dependency event failed")
					retry()
					continue
				}
			case <-time.After(10 * time.Second):
				key := core.GetServiceDependencyQueueRootKey("")
				resp, _ := store.Store().DependencyQueue().Search(context.Background(),
					registry.WithStrKey(key),
					registry.WithPrefix(),
					registry.WithCountOnly(),
					registry.WithCacheOnly())
				if resp != nil && resp.Count > 0 {
					util.Logger().Infof("wait for dependency event timed out(10s) and found %d items still in queue",
						resp.Count)
					h.signals.Put(context.Background(), struct{}{})
				}
			}
		}
	})
}

type DependencyEventHandlerResource struct {
	dep    *pb.ConsumerDependency
	kv     *mvccpb.KeyValue
	domainProject  string
}

func NewDependencyEventHandlerResource(dep *pb.ConsumerDependency, kv *mvccpb.KeyValue, domainProject string) *DependencyEventHandlerResource {
	return &DependencyEventHandlerResource{
		dep,
		kv,
		domainProject,
	}
}

func (h *DependencyEventHandler) Handle() error {
	key := core.GetServiceDependencyQueueRootKey("")
	resp, err := store.Store().DependencyQueue().Search(context.Background(),
		registry.WithStrKey(key),
		registry.WithPrefix())
	if err != nil {
		return err
	}

	// maintain dependency rules.
	l := len(resp.Kvs)
	if l == 0 {
		return nil
	}

	lenKvs := len(resp.Kvs)
	resourcesMap := make(map[string][]*DependencyEventHandlerResource, lenKvs)

	ctx := context.Background()
	for _, kv := range resp.Kvs {
		r := &pb.ConsumerDependency{}
		consumerId, domainProject, data := pb.GetInfoFromDependencyQueueKV(kv)

		err := json.Unmarshal(data, r)
		if err != nil {
			util.Logger().Errorf(err, "maintain dependency failed, unmarshal failed, consumer %s dependency: %s",
				consumerId, util.BytesToStringWithNoCopy(data))

			if err = h.removeKV(ctx, kv); err != nil {
				return err
			}
			continue
		}

		lockKey := serviceUtil.NewDependencyLockKey(domainProject, r.Consumer.Environment)
		res := NewDependencyEventHandlerResource(r, kv, domainProject)
		resources := resourcesMap[lockKey]
		if _, ok := resourcesMap[lockKey]; !ok {
			resources = make([]*DependencyEventHandlerResource, 0, lenKvs)
		}
		resources = append(resources, res)
		resourcesMap[lockKey] = resources
	}

	dependencyRuleHandleResults := make(chan error, len(resourcesMap))
	for lockKey, resources := range resourcesMap {
		go func(lockKey string, resources []*DependencyEventHandlerResource){
			err := h.dependencyRuleHandle(ctx, lockKey, resources)
			dependencyRuleHandleResults <- err
		}(lockKey, resources)
	}
	var lastErr error
	finishedCount := 0
	for err := range dependencyRuleHandleResults {
		finishedCount++
		if err != nil {
			lastErr = err
		}
		if finishedCount == len(resourcesMap) {
			close(dependencyRuleHandleResults)
		}
	}
	return lastErr
}

func (h *DependencyEventHandler)dependencyRuleHandle(ctx context.Context, lockKey string, resources []*DependencyEventHandlerResource) error{
	lock, err := serviceUtil.DependencyLock(lockKey)
	if err != nil {
		util.Logger().Errorf(err, "create dependency rule locker failed")
		return err
	}
	defer lock.Unlock()
	for _, res := range resources {
		r := res.dep
		consumerFlag := util.StringJoin([]string{r.Consumer.AppId, r.Consumer.ServiceName, r.Consumer.Version}, "/")

		domainProject := res.domainProject
		consumerInfo := pb.DependenciesToKeys([]*pb.MicroServiceKey{r.Consumer}, domainProject)[0]
		providersInfo := pb.DependenciesToKeys(r.Providers, domainProject)

		consumerId, err := serviceUtil.GetServiceId(ctx, consumerInfo)
		if err != nil {
			util.Logger().Errorf(err, "modify dependency rule failed, override: %t, consumer %s", r.Override, consumerFlag)
			return fmt.Errorf("get consumer %s id failed, override: %t, %s", consumerFlag, r.Override, err.Error())
		}
		if len(consumerId) == 0 {
			util.Logger().Errorf(nil, "maintain dependency failed, override: %t, consumer %s does not exist",
				r.Override, consumerFlag)

			if err = h.removeKV(ctx, res.kv); err != nil {
				util.Logger().Errorf(err, "remove dependency rule failed, override: %t, consumer %s", r.Override, consumerFlag)
				return err
			}
			continue
		}

		var dep serviceUtil.Dependency
		dep.DomainProject = domainProject
		dep.Consumer = consumerInfo
		dep.ProvidersRule = providersInfo
		dep.ConsumerId = consumerId
		if r.Override {
			err = serviceUtil.CreateDependencyRule(ctx, &dep)
		} else {
			err = serviceUtil.AddDependencyRule(ctx, &dep)
		}

		if err != nil {
			util.Logger().Errorf(err, "modify dependency rule failed, override: %t, consumer %s", r.Override, consumerFlag)
			return fmt.Errorf("override: %t, consumer is %s, %s", r.Override, consumerFlag, err.Error())
		}

		if err = h.removeKV(ctx, res.kv); err != nil {
			util.Logger().Errorf(err, "remove dependency rule failed, override: %t, consumer %s", r.Override, consumerFlag)
			return err
		}

		util.Logger().Infof("maintain dependency %v successfully, override: %t", r, r.Override)
	}
	return nil
}

func (h *DependencyEventHandler) removeKV(ctx context.Context, kv *mvccpb.KeyValue) error {
	dresp, err := backend.Registry().TxnWithCmp(ctx, []registry.PluginOp{registry.OpDel(registry.WithKey(kv.Key))},
		[]registry.CompareOp{registry.OpCmp(registry.CmpVer(kv.Key), registry.CMP_EQUAL, kv.Version)},
		nil)
	if err != nil {
		return fmt.Errorf("can not remove the dependency %s request, %s", util.BytesToStringWithNoCopy(kv.Key), err.Error())
	}
	if !dresp.Succeeded {
		util.Logger().Infof("the dependency %s request is changed", util.BytesToStringWithNoCopy(kv.Key))
	}
	return nil
}

func NewDependencyEventHandler() *DependencyEventHandler {
	h := &DependencyEventHandler{
		signals: util.NewUniQueue(),
	}
	h.loop()
	return h
}
