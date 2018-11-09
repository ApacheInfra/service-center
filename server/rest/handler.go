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

package rest

import (
	roa "github.com/apache/servicecomb-service-center/pkg/rest"
	"github.com/apache/servicecomb-service-center/pkg/util"
	"github.com/apache/servicecomb-service-center/server/interceptor"
	"net/http"
	"time"
)

const CTX_START_TIMESTAMP = "x-start-timestamp"

func init() {
	// api
	RegisterServerHandler("/", &ServerHandler{})
}

type ServerHandler struct {
}

func (s *ServerHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	util.SetRequestContext(r, CTX_START_TIMESTAMP, time.Now())

	err := interceptor.InvokeInterceptors(w, r)
	if err != nil {
		return
	}

	roa.GetRouter().ServeHTTP(w, r)

	// CAUTION: There will be cause a concurrent problem,
	// if here get/set the HTTP request headers.
}
