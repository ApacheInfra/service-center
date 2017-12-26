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
	"github.com/apache/incubator-servicecomb-service-center/pkg/util"
	pb "github.com/apache/incubator-servicecomb-service-center/server/core/proto"
	"github.com/apache/incubator-servicecomb-service-center/version"
	"golang.org/x/net/context"
)

var Service *pb.MicroService
var Instance *pb.MicroServiceInstance

const (
	REGISTRY_DOMAIN  = "default"
	REGISTRY_PROJECT = "default"

	REGISTRY_APP_ID       = "default"
	REGISTRY_SERVICE_NAME = "SERVICECENTER"

	REGISTRY_DEFAULT_LEASE_RENEWALINTERVAL int32 = 30
	REGISTRY_DEFAULT_LEASE_RETRYTIMES      int32 = 3

	IS_SC_SELF = "sc_self"
)

func init() {
	Service = &pb.MicroService{
		Environment: pb.ENV_PROD,
		AppId:       REGISTRY_APP_ID,
		ServiceName: REGISTRY_SERVICE_NAME,
		Version:     version.Ver().Version,
		Status:      pb.MS_UP,
		Level:       "BACK",
		Schemas: []string{
			"servicecenter.grpc.api.ServiceCtrl",
			"servicecenter.grpc.api.ServiceInstanceCtrl",
		},
		Properties: map[string]string{
			pb.PROP_ALLOW_CROSS_APP: "true",
		},
	}
	if version.Ver().RunMode == "dev" {
		Service.Environment = pb.ENV_DEV
	}

	Instance = &pb.MicroServiceInstance{
		Status: pb.MSI_UP,
		HealthCheck: &pb.HealthCheck{
			Mode:     pb.CHECK_BY_HEARTBEAT,
			Interval: REGISTRY_DEFAULT_LEASE_RENEWALINTERVAL,
			Times:    REGISTRY_DEFAULT_LEASE_RETRYTIMES,
		},
	}
}

func AddDefaultContextValue(ctx context.Context) context.Context {
	ctx = util.SetContext(ctx, "domain", REGISTRY_DOMAIN)
	ctx = util.SetContext(ctx, "project", REGISTRY_PROJECT)
	ctx = util.SetContext(ctx, IS_SC_SELF, true)
	return ctx
}

func IsDefaultDomainProject(domainProject string) bool {
	return domainProject == util.StringJoin([]string{REGISTRY_DOMAIN, REGISTRY_PROJECT}, "/")
}

func IsShared(key *pb.MicroServiceKey) bool {
	if !IsDefaultDomainProject(key.Tenant) {
		return false
	}
	return key.AppId == Service.AppId
}

func IsSCKey(key *pb.MicroServiceKey) bool {
	if !IsShared(key) {
		return false
	}
	return key.ServiceName == Service.ServiceName
}

func IsSCInstance(ctx context.Context) bool {
	if ctx.Value(IS_SC_SELF) != nil && ctx.Value(IS_SC_SELF).(bool) {
		return true
	}
	return false
}

func GetExistenceRequest() *pb.GetExistenceRequest {
	return &pb.GetExistenceRequest{
		Type:        pb.EXISTENCE_MS,
		Environment: Service.Environment,
		AppId:       Service.AppId,
		ServiceName: Service.ServiceName,
		Version:     Service.Version,
	}
}

func GetServiceRequest(serviceId string) *pb.GetServiceRequest {
	return &pb.GetServiceRequest{
		ServiceId: serviceId,
	}
}

func CreateServiceRequest() *pb.CreateServiceRequest {
	return &pb.CreateServiceRequest{
		Service: Service,
	}
}

func RegisterInstanceRequest(hostName string, endpoints []string) *pb.RegisterInstanceRequest {
	Instance.HostName = hostName
	Instance.Endpoints = endpoints
	return &pb.RegisterInstanceRequest{
		Instance: Instance,
	}
}

func UnregisterInstanceRequest() *pb.UnregisterInstanceRequest {
	return &pb.UnregisterInstanceRequest{
		ServiceId:  Instance.ServiceId,
		InstanceId: Instance.InstanceId,
	}
}

func HeartbeatRequest() *pb.HeartbeatRequest {
	return &pb.HeartbeatRequest{
		ServiceId:  Instance.ServiceId,
		InstanceId: Instance.InstanceId,
	}
}

func GetRegionAndAvailableZone(in *pb.DataCenterInfo) (region string, availableZone string) {
	if in == nil {
		return "", ""
	}
	region = in.Region
	availableZone = in.AvailableZone
	return
}
