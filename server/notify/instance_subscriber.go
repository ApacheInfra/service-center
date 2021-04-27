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

package notify

import (
	"context"
	"time"

	"github.com/apache/servicecomb-service-center/pkg/gopool"
	"github.com/apache/servicecomb-service-center/pkg/log"
	"github.com/apache/servicecomb-service-center/pkg/notify"
	simple "github.com/apache/servicecomb-service-center/pkg/time"
	pb "github.com/go-chassis/cari/discovery"
)

const (
	AddJobTimeout  = 1 * time.Second
	EventQueueSize = 5000
)

var INSTANCE = notify.RegisterType("INSTANCE", EventQueueSize)

// 状态变化推送
type InstanceEvent struct {
	notify.Event
	Revision int64
	Response *pb.WatchInstanceResponse
}

type InstanceEvents struct {
	notify.Event
	Revision int64
	Response []*pb.WatchInstanceResponse
}

type InstanceEventListWatcher struct {
	notify.Subscriber
	needMerge    bool
	Job          chan *InstanceEvent
	MergedJob    chan *InstanceEvents
	ListRevision int64
	ListFunc     func() (results []*pb.WatchInstanceResponse, rev int64)
	listCh       chan struct{}
}

func (w *InstanceEventListWatcher) SetError(err error) {
	w.Subscriber.SetError(err)
	// 触发清理job
	e := w.Service().Publish(notify.NewErrEvent(w))
	if e != nil {
		log.Error("", e)
	}
}

func (w *InstanceEventListWatcher) OnAccept() {
	if w.Err() != nil {
		return
	}
	log.Debugf("accepted by notify service, %s watcher %s %s", w.Type(), w.Group(), w.Subject())
	gopool.Go(w.listAndPublishJobs)
}

func (w *InstanceEventListWatcher) listAndPublishJobs(_ context.Context) {
	defer close(w.listCh)
	if w.ListFunc == nil {
		return
	}
	results, rev := w.ListFunc()
	w.ListRevision = rev
	for _, response := range results {
		w.sendMessage(NewInstanceEvent(w.Group(), w.Subject(), w.ListRevision, response))
	}
}

//被通知
func (w *InstanceEventListWatcher) OnMessage(job notify.Event) {
	if w.Err() != nil {
		return
	}

	select {
	case <-w.listCh:
	default:
		timer := time.NewTimer(w.Timeout())
		select {
		case <-w.listCh:
			timer.Stop()
		case <-timer.C:
			log.Errorf(nil,
				"the %s listwatcher %s %s is not ready[over %s], send the event %v",
				w.Type(), w.Group(), w.Subject(), w.Timeout(), job)
		}
	}

	w.sendMessage(job)
}

func (w *InstanceEventListWatcher) sendMessage(j interface{}) {
	if w.needMerge {
		w.sendMergedMessages(j)
		return
	}
	w.sendSingleMessage(j)
}

func (w *InstanceEventListWatcher) sendSingleMessage(j interface{}) {
	defer log.Recover()
	job, ok := j.(*InstanceEvent)
	if !ok {
		return
	}
	if job.Revision >= 0 && job.Revision <= w.ListRevision {
		log.Warnf("unexpected notify %s job is coming in, watcher %s %s, job is %v, current revision is %v",
			w.Type(), w.Group(), w.Subject(), job, w.ListRevision)
		return
	}
	select {
	case w.Job <- job:
	default:
		timer := time.NewTimer(w.Timeout())
		select {
		case w.Job <- job:
			timer.Stop()
		case <-timer.C:
			log.Errorf(nil,
				"the %s watcher %s %s event queue is full[over %s], drop the event %v",
				w.Type(), w.Group(), w.Subject(), w.Timeout(), job)
		}
	}
}

func (w *InstanceEventListWatcher) sendMergedMessages(j interface{}) {
	defer log.Recover()
	job, ok := j.(*InstanceEvents)
	if !ok {
		return
	}
	if job.Revision >= 0 && job.Revision <= w.ListRevision {
		log.Warnf("unexpected notify %s merge ob is coming in, watcher %s %s, job is %v, current revision is %v",
			w.Type(), w.Group(), w.Subject(), job, w.ListRevision)
		return
	}
	select {
	case w.MergedJob <- job:
	default:
		timer := time.NewTimer(w.Timeout())
		select {
		case w.MergedJob <- job:
			timer.Stop()
		case <-timer.C:
			log.Errorf(nil,
				"the %s watcher %s %s event queue is full[over %s], drop the event %v",
				w.Type(), w.Group(), w.Subject(), w.Timeout(), job)
		}
	}
}

func (w *InstanceEventListWatcher) Timeout() time.Duration {
	return AddJobTimeout
}

func (w *InstanceEventListWatcher) Close() {
	close(w.Job)
}

func NewInstanceEvent(serviceID, domainProject string, rev int64, response *pb.WatchInstanceResponse) *InstanceEvent {
	return &InstanceEvent{
		Event:    notify.NewEvent(INSTANCE, domainProject, serviceID),
		Revision: rev,
		Response: response,
	}
}

func NewInstanceEvents(serviceID, domainProject string, rev int64, response []*pb.WatchInstanceResponse) *InstanceEvents {
	return &InstanceEvents{
		Event:    notify.NewEvent(INSTANCE, domainProject, serviceID),
		Revision: rev,
		Response: response,
	}
}

func NewInstanceEventWithTime(serviceID, domainProject string, rev int64, createAt simple.Time, response *pb.WatchInstanceResponse) *InstanceEvent {
	return &InstanceEvent{
		Event:    notify.NewEventWithTime(INSTANCE, domainProject, serviceID, createAt),
		Revision: rev,
		Response: response,
	}
}

func NewInstanceEventListWatcher(serviceID, domainProject string,
	listFunc func() (results []*pb.WatchInstanceResponse, rev int64)) *InstanceEventListWatcher {
	watcher := &InstanceEventListWatcher{
		Subscriber: notify.NewSubscriber(INSTANCE, domainProject, serviceID),
		Job:        make(chan *InstanceEvent, INSTANCE.QueueSize()),
		MergedJob:  make(chan *InstanceEvents, INSTANCE.QueueSize()),
		ListFunc:   listFunc,
		listCh:     make(chan struct{}),
	}
	return watcher
}
