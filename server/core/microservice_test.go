// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package core

import (
	"context"
	"github.com/apache/servicecomb-service-center/datasource"
	"testing"

	"github.com/apache/servicecomb-service-center/server/config"
	"github.com/go-chassis/cari/discovery"
)

func init() {
	config.Init()
}

func TestPrepareSelfRegistration(t *testing.T) {
	config.Server.Environment = "dev"
	InitRegistration()
	if Service == nil || Service.Environment != "development" {
		t.Fatalf("TestPrepareSelfRegistration faild, %v", Service)
	}

	config.Server.Environment = "prod"
	InitRegistration()
	if Service == nil || Service.AppId != "default" || Service.ServiceName != "SERVICECENTER" ||
		Service.Environment != "production" || Service.Properties["allowCrossApp"] != "true" {
		t.Fatalf("TestPrepareSelfRegistration faild, %v", Service)
	}

	if Instance == nil || Instance.Status != "UP" {
		t.Fatalf("TestPrepareSelfRegistration faild, %v", Instance)
	}

	if IsSCInstance(context.Background()) {
		t.Fatalf("TestPrepareSelfRegistration faild")
	}

	exist := GetExistenceRequest()
	if exist == nil || exist.Environment != "production" || exist.ServiceName != "SERVICECENTER" ||
		exist.AppId != "default" {
		t.Fatalf("TestPrepareSelfRegistration faild, %v", exist)
	}
}

func TestSetSharedMode(t *testing.T) {
	RegisterGlobalServices()
	if datasource.IsGlobal(&discovery.MicroServiceKey{}) {
		t.Fatalf("TestSetSharedMode failed")
	}
	if datasource.IsGlobal(&discovery.MicroServiceKey{Tenant: "default"}) {
		t.Fatalf("TestSetSharedMode failed")
	}
	if datasource.IsGlobal(&discovery.MicroServiceKey{Tenant: "default/default"}) {
		t.Fatalf("TestSetSharedMode failed")
	}
	if datasource.IsGlobal(&discovery.MicroServiceKey{Tenant: "default/default", AppId: "default"}) {
		t.Fatalf("TestSetSharedMode failed")
	}

	config.Server.Config.GlobalVisible = "shared"
	RegisterGlobalServices()
	if datasource.IsGlobal(&discovery.MicroServiceKey{Tenant: "default/default", AppId: "default", ServiceName: "no-shared"}) {
		t.Fatalf("TestSetSharedMode failed")
	}
	if !datasource.IsGlobal(&discovery.MicroServiceKey{Tenant: "default/default", AppId: "default", ServiceName: "shared"}) {
		t.Fatalf("TestSetSharedMode failed")
	}
	if !datasource.IsGlobal(&discovery.MicroServiceKey{Tenant: "default/default", AppId: "default", Alias: "shared"}) {
		t.Fatalf("TestSetSharedMode failed")
	}
}
