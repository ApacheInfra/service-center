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
	"github.com/ServiceComb/service-center/pkg/rest"
	"github.com/ServiceComb/service-center/pkg/util"
	"github.com/ServiceComb/service-center/server/core"
	pb "github.com/ServiceComb/service-center/server/core/proto"
	scerr "github.com/ServiceComb/service-center/server/error"
	"github.com/ServiceComb/service-center/server/rest/controller"
	"io/ioutil"
	"net/http"
	"strings"
)

type RuleService struct {
	//
}

func (this *RuleService) URLPatterns() []rest.Route {
	return []rest.Route{
		{rest.HTTP_METHOD_POST, "/v4/:project/registry/microservices/:serviceId/rules", this.AddRule},
		{rest.HTTP_METHOD_GET, "/v4/:project/registry/microservices/:serviceId/rules", this.GetRules},
		{rest.HTTP_METHOD_PUT, "/v4/:project/registry/microservices/:serviceId/rules/:rule_id", this.UpdateRule},
		{rest.HTTP_METHOD_DELETE, "/v4/:project/registry/microservices/:serviceId/rules/:rule_id", this.DeleteRule},
	}
}
func (this *RuleService) AddRule(w http.ResponseWriter, r *http.Request) {
	message, err := ioutil.ReadAll(r.Body)
	if err != nil {
		util.Logger().Error("bory err", err)
		controller.WriteError(w, scerr.ErrInvalidParams, err.Error())
		return
	}
	rule := map[string][]*pb.AddOrUpdateServiceRule{}
	err = json.Unmarshal(message, &rule)
	if err != nil {
		util.Logger().Errorf(err, "Unmarshal error")
		controller.WriteError(w, scerr.ErrInvalidParams, err.Error())
		return
	}

	resp, err := core.ServiceAPI.AddRule(r.Context(), &pb.AddServiceRulesRequest{
		ServiceId: r.URL.Query().Get(":serviceId"),
		Rules:     rule["rules"],
	})
	respInternal := resp.Response
	resp.Response = nil
	controller.WriteResponse(w, respInternal, resp)
}

func (this *RuleService) DeleteRule(w http.ResponseWriter, r *http.Request) {
	rule_id := r.URL.Query().Get(":rule_id")
	ids := strings.Split(rule_id, ",")

	resp, _ := core.ServiceAPI.DeleteRule(r.Context(), &pb.DeleteServiceRulesRequest{
		ServiceId: r.URL.Query().Get(":serviceId"),
		RuleIds:   ids,
	})
	controller.WriteResponse(w, resp.Response, nil)
}

func (this *RuleService) UpdateRule(w http.ResponseWriter, r *http.Request) {
	message, err := ioutil.ReadAll(r.Body)
	if err != nil {
		util.Logger().Error("body err", err)
		controller.WriteError(w, scerr.ErrInvalidParams, err.Error())
		return
	}

	rule := pb.AddOrUpdateServiceRule{}
	err = json.Unmarshal(message, &rule)
	if err != nil {
		util.Logger().Error("Unmarshal error", err)
		controller.WriteError(w, scerr.ErrInvalidParams, err.Error())
		return
	}
	resp, err := core.ServiceAPI.UpdateRule(r.Context(), &pb.UpdateServiceRuleRequest{
		ServiceId: r.URL.Query().Get(":serviceId"),
		RuleId:    r.URL.Query().Get(":rule_id"),
		Rule:      &rule,
	})
	controller.WriteResponse(w, resp.Response, nil)
}

func (this *RuleService) GetRules(w http.ResponseWriter, r *http.Request) {
	resp, _ := core.ServiceAPI.GetRule(r.Context(), &pb.GetServiceRulesRequest{
		ServiceId: r.URL.Query().Get(":serviceId"),
	})
	respInternal := resp.Response
	resp.Response = nil
	controller.WriteResponse(w, respInternal, resp)
}
