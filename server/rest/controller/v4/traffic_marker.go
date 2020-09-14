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

package v4

import (
	"net/http"

	"github.com/apache/servicecomb-service-center/pkg/rest"
)

type Governance struct {
}

func (t *Governance) Create(w http.ResponseWriter, req *http.Request) {

}
func (t *Governance) List(w http.ResponseWriter, req *http.Request) {

}
func (t *Governance) URLPatterns() []rest.Route {
	return []rest.Route{
		//servicecomb.trafficMarker.{name}
		//servicecomb.rateLimiting.{name}
		{Method: http.MethodPost, Path: "/v4/:project/governance/:customPolicyKind", Func: t.Create},
		{Method: http.MethodGet, Path: "/v4/:project/governance/:customPolicyKind/", Func: t.List},
	}
}
