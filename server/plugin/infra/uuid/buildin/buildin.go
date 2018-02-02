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

package buildin

import (
	"github.com/apache/incubator-servicecomb-service-center/pkg/util"
	mgr "github.com/apache/incubator-servicecomb-service-center/server/plugin"
)

func init() {
	mgr.RegisterPlugin(mgr.Plugin{mgr.UUID, "buildin", New})
}

func New() mgr.PluginInstance {
	return &BuildinUUID{}
}

type BuildinUUID struct {
}

func (du *BuildinUUID) GetServiceId() string {
	df, ok := mgr.DynamicPluginFunc(mgr.UUID, "GetServiceId").(func() string)
	if ok {
		return df()
	}
	return util.GenerateUuid()
}

func (du *BuildinUUID) GetInstanceId() string {
	df, ok := mgr.DynamicPluginFunc(mgr.UUID, "GetInstanceId").(func() string)
	if ok {
		return df()
	}
	return util.GenerateUuid()
}
