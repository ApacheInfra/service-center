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
	"encoding/json"
	"github.com/apache/incubator-servicecomb-service-center/pkg/log"
	"github.com/apache/incubator-servicecomb-service-center/pkg/rest"
	"github.com/apache/incubator-servicecomb-service-center/server/core"
	pb "github.com/apache/incubator-servicecomb-service-center/server/core/proto"
	scerr "github.com/apache/incubator-servicecomb-service-center/server/error"
	"github.com/apache/incubator-servicecomb-service-center/server/rest/controller"
	"io/ioutil"
	"net/http"
	"strings"
)

type TagService struct {
	//
}

func (this *TagService) URLPatterns() []rest.Route {
	return []rest.Route{
		{rest.HTTP_METHOD_POST, "/v4/:project/registry/microservices/:serviceId/tags", this.AddTags},
		{rest.HTTP_METHOD_PUT, "/v4/:project/registry/microservices/:serviceId/tags/:key", this.UpdateTag},
		{rest.HTTP_METHOD_GET, "/v4/:project/registry/microservices/:serviceId/tags", this.GetTags},
		{rest.HTTP_METHOD_DELETE, "/v4/:project/registry/microservices/:serviceId/tags/:key", this.DeleteTags},
	}
}

func (this *TagService) AddTags(w http.ResponseWriter, r *http.Request) {
	message, err := ioutil.ReadAll(r.Body)
	if err != nil {
		log.Error("body err", err)
		controller.WriteError(w, scerr.ErrInvalidParams, err.Error())
		return
	}
	var tags map[string]map[string]string
	err = json.Unmarshal(message, &tags)
	if err != nil {
		log.Error("Unmarshal error", err)
		controller.WriteError(w, scerr.ErrInvalidParams, err.Error())
		return
	}

	resp, err := core.ServiceAPI.AddTags(r.Context(), &pb.AddServiceTagsRequest{
		ServiceId: r.URL.Query().Get(":serviceId"),
		Tags:      tags["tags"],
	})
	controller.WriteResponse(w, resp.Response, nil)
}

func (this *TagService) UpdateTag(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()
	resp, _ := core.ServiceAPI.UpdateTag(r.Context(), &pb.UpdateServiceTagRequest{
		ServiceId: query.Get(":serviceId"),
		Key:       query.Get(":key"),
		Value:     query.Get("value"),
	})
	controller.WriteResponse(w, resp.Response, nil)
}

func (this *TagService) GetTags(w http.ResponseWriter, r *http.Request) {
	resp, _ := core.ServiceAPI.GetTags(r.Context(), &pb.GetServiceTagsRequest{
		ServiceId: r.URL.Query().Get(":serviceId"),
	})
	respInternal := resp.Response
	resp.Response = nil
	controller.WriteResponse(w, respInternal, resp)
}

func (this *TagService) DeleteTags(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()
	keys := query.Get(":key")
	ids := strings.Split(keys, ",")

	resp, _ := core.ServiceAPI.DeleteTags(r.Context(), &pb.DeleteServiceTagsRequest{
		ServiceId: query.Get(":serviceId"),
		Keys:      ids,
	})
	controller.WriteResponse(w, resp.Response, nil)
}
