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

//Package test prepare service center required module before UT
package test

import (
	"time"

	_ "github.com/apache/servicecomb-service-center/server/init"
	"github.com/apache/servicecomb-service-center/server/service/disco"

	_ "github.com/apache/servicecomb-service-center/server/bootstrap"

	"github.com/apache/servicecomb-service-center/datasource"
	"github.com/apache/servicecomb-service-center/server/core"
	"github.com/go-chassis/go-archaius"
)

func init() {
	t := archaius.Get("TEST_MODE")
	if t == nil {
		t = "etcd"
	}
	archaius.Set("rbac.releaseLockAfter", "3s")
	if t == "etcd" {
		archaius.Set("registry.cache.mode", 0)
		archaius.Set("discovery.kind", "etcd")
		archaius.Set("registry.kind", "etcd")
	} else {
		archaius.Set("registry.heartbeat.kind", "checker")
	}
	datasource.Init(datasource.Options{
		Kind:                datasource.Kind(t.(string)),
		ReleaseAccountAfter: 3 * time.Second,
	})
	core.ServiceAPI = disco.AssembleResources()
}
