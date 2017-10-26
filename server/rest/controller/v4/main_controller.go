//Copyright 2017 Huawei Technologies Co., Ltd
//
//Licensed under the Apache License, Version 2.0 (the "License");
//you may not use this file except in compliance with the License.
//You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
//Unless required by applicable law or agreed to in writing, software
//distributed under the License is distributed on an "AS IS" BASIS,
//WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//See the License for the specific language governing permissions and
//limitations under the License.
package v4

import (
	"encoding/json"
	"github.com/ServiceComb/service-center/pkg/rest"
	"github.com/ServiceComb/service-center/pkg/util"
	"github.com/ServiceComb/service-center/server/core"
	"github.com/ServiceComb/service-center/server/rest/controller"
	"github.com/ServiceComb/service-center/version"
	"github.com/astaxie/beego"
	"net/http"
)

const API_VERSION = "4.0.0"

var RunMode string

type Result struct {
	version.VersionSet
	ApiVersion string `json:"apiVersion"`
	RunMode    string `json:"runMode"`
}

type MainService struct {
	//
}

func init() {
	RunMode = beego.AppConfig.DefaultString("runmode", "prod")
}

func (this *MainService) URLPatterns() []rest.Route {
	return []rest.Route{
		{rest.HTTP_METHOD_GET, "/v4/:domain/registry/version", this.GetVersion},
		{rest.HTTP_METHOD_GET, "/v4/:domain/registry/health", this.ClusterHealth},
	}
}

func (this *MainService) ClusterHealth(w http.ResponseWriter, r *http.Request) {
	resp, err := core.InstanceAPI.ClusterHealth(r.Context())
	if err != nil {
		util.Logger().Error("health check failed", err)
		controller.WriteText(http.StatusInternalServerError, "health check failed", w)
		return
	}

	respInternal := resp.Response
	resp.Response = nil
	controller.WriteJsonResponse(respInternal, resp, err, w)
}

func (this *MainService) GetVersion(w http.ResponseWriter, r *http.Request) {
	result := Result{
		version.Ver(),
		API_VERSION,
		RunMode,
	}
	resultJSON, _ := json.Marshal(result)
	w.Header().Set("Content-Type", "application/json;charset=utf-8")
	w.Write(resultJSON)
}
