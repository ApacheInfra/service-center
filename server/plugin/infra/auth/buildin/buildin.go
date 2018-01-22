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
	"github.com/apache/incubator-servicecomb-service-center/pkg/plugin"
	"github.com/apache/incubator-servicecomb-service-center/pkg/util"
	mgr "github.com/apache/incubator-servicecomb-service-center/server/plugin"
	"net/http"
)

var authFunc func(r *http.Request) error

func init() {
	mgr.RegisterPlugin(mgr.Plugin{mgr.AUTH, "buildin", New})
}

func New() mgr.PluginInstance {
	return &BuildInAuth{}
}

type BuildInAuth struct {
}

func (ba *BuildInAuth) Identify(r *http.Request) error {
	return nil
}

func findAuthFunc(funcName string) func(r *http.Request) error {
	ff, err := plugin.FindFunc(mgr.AUTH.String(), funcName)
	if err != nil {
		return nil
	}
	f, ok := ff.(func(*http.Request) error)
	if !ok {
		util.Logger().Warnf(nil, "unexpected function '%s' format found in plugin 'auth'.", funcName)
		return nil
	}
	return f
}
