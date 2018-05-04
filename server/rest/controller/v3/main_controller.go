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
package v3

import (
	"encoding/json"
	"github.com/apache/incubator-servicecomb-service-center/pkg/rest"
	pb "github.com/apache/incubator-servicecomb-service-center/server/core/proto"
	"github.com/apache/incubator-servicecomb-service-center/server/rest/controller"
	"github.com/apache/incubator-servicecomb-service-center/server/rest/controller/v4"
	"github.com/apache/incubator-servicecomb-service-center/version"
	"net/http"
)

var (
	versionJsonCache []byte
	versionResp      *pb.Response
)

const API_VERSION = "3.0.0"

func init() {
	result := v4.Result{
		VersionSet: version.Ver(),
		ApiVersion: API_VERSION,
	}
	versionJsonCache, _ = json.Marshal(result)
	versionResp = pb.CreateResponse(pb.Response_SUCCESS, "get version successfully")
}

type MainService struct {
	v4.MainService
}

func (this *MainService) URLPatterns() []rest.Route {
	return []rest.Route{
		{rest.HTTP_METHOD_GET, "/version", this.GetVersion},
		{rest.HTTP_METHOD_GET, "/health", this.ClusterHealth},
	}
}

func (this *MainService) GetVersion(w http.ResponseWriter, r *http.Request) {
	controller.WriteBytes(w, versionResp, versionJsonCache)
}
