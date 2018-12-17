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
	"github.com/apache/servicecomb-service-center/pkg/log"
	"github.com/apache/servicecomb-service-center/pkg/util"
	apt "github.com/apache/servicecomb-service-center/server/core"
	"github.com/apache/servicecomb-service-center/server/core/backend"
	pb "github.com/apache/servicecomb-service-center/server/core/proto"
	"github.com/apache/servicecomb-service-center/server/notify"
	"github.com/apache/servicecomb-service-center/server/plugin/pkg/discovery"
	"github.com/apache/servicecomb-service-center/server/service/cache"
	"github.com/apache/servicecomb-service-center/server/service/metrics"
	serviceUtil "github.com/apache/servicecomb-service-center/server/service/util"
	"golang.org/x/net/context"
	"strings"
)

type InstanceEventHandler struct {
}

func (h *InstanceEventHandler) Type() discovery.Type {
	return backend.INSTANCE
}

func (h *InstanceEventHandler) OnEvent(evt discovery.KvEvent) {
	action := evt.Type
	providerId, providerInstanceId, domainProject := apt.GetInfoFromInstKV(evt.KV.Key)
	idx := strings.Index(domainProject, "/")
	domainName := domainProject[:idx]
	switch action {
	case pb.EVT_INIT:
		metrics.ReportInstances(domainName, 1)
		return
	case pb.EVT_CREATE:
		metrics.ReportInstances(domainName, 1)
	case pb.EVT_DELETE:
		metrics.ReportInstances(domainName, -1)
		if !apt.IsDefaultDomainProject(domainProject) {
			projectName := domainProject[idx+1:]
			serviceUtil.RemandInstanceQuota(
				util.SetDomainProject(context.Background(), domainName, projectName))
		}
	}

	if notify.NotifyCenter().Closed() {
		log.Warnf("caught [%s] instance[%s/%s] event, but notify service is closed",
			action, providerId, providerInstanceId)
		return
	}

	// 查询服务版本信息
	ctx := context.WithValue(context.WithValue(context.Background(),
		serviceUtil.CTX_CACHEONLY, "1"),
		serviceUtil.CTX_GLOBAL, "1")
	ms, err := serviceUtil.GetService(ctx, domainProject, providerId)
	if ms == nil {
		log.Errorf(err, "caught [%s] instance[%s/%s] event, get cached provider's file failed",
			action, providerId, providerInstanceId)
		return
	}

	log.Infof("caught [%s] service[%s][%s/%s/%s/%s] instance[%s] event",
		action, providerId, ms.Environment, ms.AppId, ms.ServiceName, ms.Version, providerInstanceId)

	// 查询所有consumer
	consumerIds, _, err := serviceUtil.GetAllConsumerIds(ctx, domainProject, ms)
	if err != nil {
		log.Errorf(err, "get service[%s][%s/%s/%s/%s]'s consumerIds failed",
			providerId, ms.Environment, ms.AppId, ms.ServiceName, ms.Version)
		return
	}

	PublishInstanceEvent(domainProject, action, pb.MicroServiceToKey(domainProject, ms),
		evt.KV.Value.(*pb.MicroServiceInstance), evt.Revision, consumerIds)
}

func NewInstanceEventHandler() *InstanceEventHandler {
	return &InstanceEventHandler{}
}

func PublishInstanceEvent(domainProject string, action pb.EventType, serviceKey *pb.MicroServiceKey, instance *pb.MicroServiceInstance, rev int64, subscribers []string) {
	defer cache.FindInstances.Remove(serviceKey)

	if len(subscribers) == 0 {
		return
	}

	response := &pb.WatchInstanceResponse{
		Response: pb.CreateResponse(pb.Response_SUCCESS, "Watch instance successfully."),
		Action:   string(action),
		Key:      serviceKey,
		Instance: instance,
	}
	for _, consumerId := range subscribers {
		// TODO add超时怎么处理？
		job := notify.NewInstanceEvent(consumerId, apt.GetInstanceRootKey(domainProject)+"/", rev, response)
		notify.NotifyCenter().Publish(job)
	}
}
