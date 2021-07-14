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

package core

import (
	"context"
	"strings"

	"github.com/apache/servicecomb-service-center/datasource"
	"github.com/apache/servicecomb-service-center/pkg/proto"
	"github.com/apache/servicecomb-service-center/pkg/util"
	"github.com/apache/servicecomb-service-center/server/config"
	"github.com/apache/servicecomb-service-center/version"
	"github.com/go-chassis/cari/discovery"
)

var (
	ServiceAPI proto.ServiceCtrlServer
	Service    = &discovery.MicroService{}
	Instance   = &discovery.MicroServiceInstance{}
)

const (
	RegistryServiceName  = "SERVICECENTER"
	RegistryServiceAlias = "SERVICECENTER"

	RegistryDefaultLeaseRenewalInterval int32 = 30
	RegistryDefaultLeaseRetryTimes      int32 = 3

	CtxScSelf util.CtxKey = "_sc_self"
)

func InitRegistration() {
	Service = &discovery.MicroService{
		Environment: discovery.ENV_PROD,
		AppId:       datasource.RegistryAppID,
		ServiceName: RegistryServiceName,
		Alias:       RegistryServiceAlias,
		Version:     version.Ver().Version,
		Status:      discovery.MS_UP,
		Level:       "BACK",
		Schemas: []string{
			"servicecenter.grpc.api.ServiceCtrl",
			"servicecenter.grpc.api.ServiceInstanceCtrl",
		},
		Properties: map[string]string{
			discovery.PropAllowCrossApp: "true",
		},
	}
	if config.GetProfile().IsDev() {
		Service.Environment = discovery.ENV_DEV
	}

	Instance = &discovery.MicroServiceInstance{
		Status: discovery.MSI_UP,
		HealthCheck: &discovery.HealthCheck{
			Mode:     discovery.CHECK_BY_HEARTBEAT,
			Interval: RegistryDefaultLeaseRenewalInterval,
			Times:    RegistryDefaultLeaseRetryTimes,
		},
	}
}

func AddDefaultContextValue(ctx context.Context) context.Context {
	return util.WithNoCache(util.SetContext(util.SetDomainProject(ctx,
		datasource.RegistryDomain, datasource.RegistryProject),
		CtxScSelf, true))
}

func RegisterGlobalServices() {
	for _, s := range strings.Split(config.GetRegistry().GlobalVisible, ",") {
		if len(s) > 0 {
			datasource.RegisterGlobalService(s)
		}
	}
	datasource.RegisterGlobalService(Service.ServiceName)
}

func IsSCInstance(ctx context.Context) bool {
	b, _ := ctx.Value(CtxScSelf).(bool)
	return b
}

func GetExistenceRequest() *discovery.GetExistenceRequest {
	return &discovery.GetExistenceRequest{
		Type:        discovery.ExistenceMicroservice,
		Environment: Service.Environment,
		AppId:       Service.AppId,
		ServiceName: Service.ServiceName,
		Version:     Service.Version,
	}
}

func GetServiceRequest(serviceID string) *discovery.GetServiceRequest {
	return &discovery.GetServiceRequest{
		ServiceId: serviceID,
	}
}

func CreateServiceRequest() *discovery.CreateServiceRequest {
	return &discovery.CreateServiceRequest{
		Service: Service,
	}
}

func RegisterInstanceRequest() *discovery.RegisterInstanceRequest {
	return &discovery.RegisterInstanceRequest{
		Instance: Instance,
	}
}

func UnregisterInstanceRequest() *discovery.UnregisterInstanceRequest {
	return &discovery.UnregisterInstanceRequest{
		ServiceId:  Instance.ServiceId,
		InstanceId: Instance.InstanceId,
	}
}

func HeartbeatRequest() *discovery.HeartbeatRequest {
	return &discovery.HeartbeatRequest{
		ServiceId:  Instance.ServiceId,
		InstanceId: Instance.InstanceId,
	}
}
