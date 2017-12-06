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
	pb "github.com/ServiceComb/service-center/server/core/proto"
	scerr "github.com/ServiceComb/service-center/server/error"
	"github.com/ServiceComb/service-center/server/rest/controller"
	"io/ioutil"
	"net/http"
)

type DependencyService struct {
	//
}

func (this *DependencyService) URLPatterns() []rest.Route {
	return []rest.Route{
		{rest.HTTP_METHOD_POST, "/v4/:domain/registry/dependencies", this.AddDependenciesForMicroServices},
		{rest.HTTP_METHOD_PUT, "/v4/:domain/registry/dependencies", this.CreateDependenciesForMicroServices},
		{rest.HTTP_METHOD_GET, "/v4/:domain/registry/microservices/:consumerId/providers", this.GetConProDependencies},
		{rest.HTTP_METHOD_GET, "/v4/:domain/registry/microservices/:providerId/consumers", this.GetProConDependencies},
	}
}

func (this *DependencyService) AddDependenciesForMicroServices(w http.ResponseWriter, r *http.Request) {
	requestBody, err := ioutil.ReadAll(r.Body)
	if err != nil {
		util.Logger().Error("body err", err)
		controller.WriteError(w, scerr.ErrInvalidParams, err.Error())
		return
	}
	request := &pb.AddDependenciesRequest{}
	err = json.Unmarshal(requestBody, request)
	if err != nil {
		util.Logger().Error("Invalid json", err)
		controller.WriteError(w, scerr.ErrInvalidParams, err.Error())
		return
	}

	resp, err := core.ServiceAPI.AddDependenciesForMicroServices(r.Context(), request)
	controller.WriteResponse(w, resp.Response, nil)
}

func (this *DependencyService) CreateDependenciesForMicroServices(w http.ResponseWriter, r *http.Request) {
	requestBody, err := ioutil.ReadAll(r.Body)
	if err != nil {
		util.Logger().Error("body err", err)
		controller.WriteError(w, scerr.ErrInvalidParams, err.Error())
		return
	}
	request := &pb.CreateDependenciesRequest{}
	err = json.Unmarshal(requestBody, request)
	if err != nil {
		util.Logger().Error("Invalid json", err)
		controller.WriteError(w, scerr.ErrInvalidParams, err.Error())
		return
	}

	resp, err := core.ServiceAPI.CreateDependenciesForMicroServices(r.Context(), request)
	controller.WriteResponse(w, resp.Response, nil)
}

func (this *DependencyService) GetConProDependencies(w http.ResponseWriter, r *http.Request) {
	request := &pb.GetDependenciesRequest{
		ServiceId: r.URL.Query().Get(":consumerId"),
	}
	resp, _ := core.ServiceAPI.GetConsumerDependencies(r.Context(), request)
	respInternal := resp.Response
	resp.Response = nil
	controller.WriteResponse(w, respInternal, resp)
}

func (this *DependencyService) GetProConDependencies(w http.ResponseWriter, r *http.Request) {
	request := &pb.GetDependenciesRequest{
		ServiceId: r.URL.Query().Get(":providerId"),
	}
	resp, _ := core.ServiceAPI.GetProviderDependencies(r.Context(), request)
	respInternal := resp.Response
	resp.Response = nil
	controller.WriteResponse(w, respInternal, resp)
}
