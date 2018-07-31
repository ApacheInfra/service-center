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
package admin

import (
	"github.com/apache/incubator-servicecomb-service-center/pkg/util"
	"github.com/apache/incubator-servicecomb-service-center/server/admin/model"
	"github.com/apache/incubator-servicecomb-service-center/server/core"
	"github.com/apache/incubator-servicecomb-service-center/server/core/backend"
	pb "github.com/apache/incubator-servicecomb-service-center/server/core/proto"
	scerr "github.com/apache/incubator-servicecomb-service-center/server/error"
	"golang.org/x/net/context"
)

var AdminServiceAPI = &AdminService{}

type AdminService struct {
}

func (service *AdminService) Dump(ctx context.Context, in *model.DumpRequest) (*model.DumpResponse, error) {

	domainProject := util.ParseDomainProject(ctx)
	var cache model.Cache

	if !core.IsDefaultDomainProject(domainProject) {
		return &model.DumpResponse{
			Response: pb.CreateResponse(scerr.ErrForbidden, "Required admin permission"),
		}, nil
	}

	service.dumpAll(ctx, &cache)

	return &model.DumpResponse{
		Response: pb.CreateResponse(pb.Response_SUCCESS, "Admin dump successfully"),
		Cache:    &cache,
	}, nil
}

func (service *AdminService) dumpAll(ctx context.Context, cache *model.Cache) {
	util.NewGo(ctx, util.PoolConfigure().Workers(2)).
		Do(func(_ context.Context) { setValue(backend.Store().Service(), &cache.Microservices) }).
		Do(func(_ context.Context) { setValue(backend.Store().ServiceIndex(), &cache.Indexes) }).
		Do(func(_ context.Context) { setValue(backend.Store().ServiceAlias(), &cache.Aliases) }).
		Do(func(_ context.Context) { setValue(backend.Store().ServiceTag(), &cache.Tags) }).
		Do(func(_ context.Context) { setValue(backend.Store().Rule(), &cache.Rules) }).
		Do(func(_ context.Context) { setValue(backend.Store().DependencyRule(), &cache.DependencyRules) }).
		Do(func(_ context.Context) { setValue(backend.Store().SchemaSummary(), &cache.Summaries) }).
		Do(func(_ context.Context) { setValue(backend.Store().Instance(), &cache.Instances) }).
		Done()
}

func setValue(e backend.Entity, setter model.Setter) {
	e.Cache().ForEach(func(k string, kv *backend.KeyValue) (next bool) {
		setter.SetValue(&model.KV{Key: k, Rev: kv.ModRevision, Value: kv.Value})
		return true
	})
}
