/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except request compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to request writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cache

import (
	"context"
	"strings"

	"github.com/apache/servicecomb-service-center/datasource"
	"github.com/apache/servicecomb-service-center/datasource/mongo/client/model"
	"github.com/apache/servicecomb-service-center/datasource/mongo/sd"
	"github.com/apache/servicecomb-service-center/pkg/util"
	"github.com/go-chassis/cari/discovery"
)

func GetProviderServiceOfDeps(provider *discovery.MicroService) (*discovery.MicroServiceDependency, bool) {
	res := sd.Store().Dep().Cache().GetValue(genDepserivceKey(datasource.Provider, provider))
	deps, ok := transCacheToDep(res)
	if !ok {
		return nil, false
	}
	return deps[0], true
}

func transCacheToDep(cache []interface{}) ([]*discovery.MicroServiceDependency, bool) {
	res := make([]*discovery.MicroServiceDependency, 0, len(cache))
	for _, v := range cache {
		t, ok := v.(model.DependencyRule)
		if !ok {
			return nil, false
		}
		res = append(res, t.Dep)
	}
	if len(res) == 0 {
		return nil, false
	}
	return res, true
}

func genDepserivceKey(ruleType string, service *discovery.MicroService) string {
	return strings.Join([]string{ruleType, service.AppId, service.ServiceName, service.Version}, datasource.Split)
}

func GetRulesByServiceID(ctx context.Context, serviceID string) ([]*model.Rule, bool) {
	index := genServiceIDIndex(ctx, serviceID)
	cacheRes := sd.Store().Rule().Cache().GetValue(index)
	return transCacheToRules(cacheRes)
}

func GetRulesByServiceIDAcrossDomain(ctx context.Context, serviceID string) ([]*model.Rule, bool) {
	index := genServiceIDIndexAcrossDomain(ctx, serviceID)
	cacheRes := sd.Store().Rule().Cache().GetValue(index)
	return transCacheToRules(cacheRes)
}

func GetServiceRulesByServiceID(ctx context.Context, serviceID string) ([]*discovery.ServiceRule, bool) {
	index := genServiceIDIndex(ctx, serviceID)
	cacheRes := sd.Store().Rule().Cache().GetValue(index)
	return transCacheToServiceRules(cacheRes)
}

func GetRulesByRuleID(ctx context.Context, serviceID string, ruleID string) ([]*model.Rule, bool) {
	index := genRuleIDIndex(ctx, serviceID, ruleID)
	cacheRes := sd.Store().Rule().Cache().GetValue(index)
	return transCacheToRules(cacheRes)
}

func transCacheToRules(cacheRules []interface{}) ([]*model.Rule, bool) {
	res := make([]*model.Rule, 0, len(cacheRules))
	for _, v := range cacheRules {
		t, ok := v.(model.Rule)
		if !ok {
			return nil, false
		}
		res = append(res, &model.Rule{
			Domain:    t.Domain,
			Project:   t.Project,
			ServiceID: t.ServiceID,
			Rule:      t.Rule,
		})
	}
	if len(res) == 0 {
		return nil, false
	}
	return res, true
}

func transCacheToServiceRules(cacheRules []interface{}) ([]*discovery.ServiceRule, bool) {
	res := make([]*discovery.ServiceRule, 0, len(cacheRules))
	for _, v := range cacheRules {
		t, ok := v.(model.Rule)
		if !ok {
			return nil, false
		}
		res = append(res, t.Rule)
	}
	if len(res) == 0 {
		return nil, false
	}
	return res, true
}

func genRuleIDIndex(ctx context.Context, serviceID string, ruleID string) string {
	return strings.Join([]string{util.ParseDomain(ctx), util.ParseProject(ctx), serviceID, ruleID}, datasource.Split)
}

func GetServiceByID(ctx context.Context, serviceID string) (*model.Service, bool) {
	index := genServiceIDIndex(ctx, serviceID)
	cacheRes := sd.Store().Service().Cache().GetValue(index)

	if len(cacheRes) == 0 {
		return nil, false
	}

	res, ok := transCacheToService(cacheRes)
	if !ok {
		return nil, false
	}
	return res[0], true
}

func GetServiceID(ctx context.Context, key *discovery.MicroServiceKey) (serviceID string, exist bool) {
	cacheIndex := genServiceKeyIndex(ctx, key)
	res := sd.Store().Service().Cache().GetValue(cacheIndex)
	cacheService, ok := transCacheToService(res)
	if !ok {
		return
	}
	return cacheService[0].Service.ServiceId, true
}

func GetServiceByIDAcrossDomain(ctx context.Context, serviceID string) (*model.Service, bool) {
	index := genServiceIDIndexAcrossDomain(ctx, serviceID)
	cacheRes := sd.Store().Service().Cache().GetValue(index)

	if len(cacheRes) == 0 {
		return nil, false
	}

	res, ok := transCacheToService(cacheRes)
	if !ok {
		return nil, false
	}

	return res[0], true
}

func GetServicesByDomainProject(domainProject string) (service []*model.Service, exist bool) {
	services := make([]*model.Service, 0)
	sd.Store().Service().Cache().GetValue(domainProject)
	if len(services) == 0 {
		return services, false
	}
	return services, true
}

func GetMicroServicesByDomainProject(domainProject string) (service []*discovery.MicroService, exist bool) {
	services, exist := GetServicesByDomainProject(domainProject)
	if !exist || len(services) == 0 {
		return nil, false
	}
	ms := make([]*discovery.MicroService, len(services))
	for i, s := range services {
		ms[i] = s.Service
	}
	return ms, true
}

func transCacheToService(services []interface{}) ([]*model.Service, bool) {
	res := make([]*model.Service, 0, len(services))
	for _, v := range services {
		t, ok := v.(model.Service)
		if !ok {
			return nil, false
		}
		res = append(res, &model.Service{
			Domain:  t.Domain,
			Project: t.Project,
			Tags:    t.Tags,
			Service: t.Service,
		})
	}
	if len(res) == 0 {
		return nil, false
	}
	return res, true
}

func genServiceIDIndexAcrossDomain(ctx context.Context, serviceID string) string {
	return strings.Join([]string{util.ParseTargetDomainProject(ctx), serviceID}, datasource.Split)
}

func genServiceIDIndex(ctx context.Context, serviceID string) string {
	return strings.Join([]string{util.ParseDomainProject(ctx), serviceID}, datasource.Split)
}

func genServiceKeyIndex(ctx context.Context, key *discovery.MicroServiceKey) string {
	return strings.Join([]string{util.ParseDomain(ctx), util.ParseProject(ctx), key.AppId, key.ServiceName, key.Version}, datasource.Split)
}

func GetMicroServiceInstancesByID(ctx context.Context, serviceID string) ([]*discovery.MicroServiceInstance, bool) {
	index := genServiceIDIndex(ctx, serviceID)
	cacheInstances := sd.Store().Instance().Cache().GetValue(index)
	insts, ok := transCacheToMicroInsts(cacheInstances)
	if !ok {
		return nil, false
	}
	return insts, true
}

func CountInstances(ctx context.Context, serviceID string) (int, bool) {
	index := genServiceIDIndex(ctx, serviceID)
	cacheInstances := sd.Store().Instance().Cache().GetValue(index)
	if len(cacheInstances) == 0 {
		return 0, false
	}
	return len(cacheInstances), true
}

func GetInstance(ctx context.Context, serviceID string, instanceID string) (*model.Instance, bool) {
	index := generateInstanceIDIndex(util.ParseDomainProject(ctx), serviceID, instanceID)
	cacheInstance := sd.Store().Instance().Cache().GetValue(index)
	insts, ok := transCacheToInsts(cacheInstance)
	if !ok {
		return nil, false
	}
	return insts[0], true
}

func GetInstances(ctx context.Context) ([]*model.Instance, bool) {
	index := util.ParseDomainProject(ctx)
	cacheInstance := sd.Store().Instance().Cache().GetValue(index)
	insts, ok := transCacheToInsts(cacheInstance)
	if !ok {
		return nil, false
	}
	return insts, true
}

func transCacheToMicroInsts(cache []interface{}) ([]*discovery.MicroServiceInstance, bool) {
	res := make([]*discovery.MicroServiceInstance, 0, len(cache))
	for _, iter := range cache {
		inst, ok := iter.(model.Instance)
		if !ok {
			return nil, false
		}
		res = append(res, inst.Instance)
	}
	if len(res) == 0 {
		return nil, false
	}
	return res, true
}

func transCacheToInsts(cache []interface{}) ([]*model.Instance, bool) {
	res := make([]*model.Instance, 0, len(cache))
	for _, iter := range cache {
		inst, ok := iter.(model.Instance)
		if !ok {
			return nil, false
		}
		res = append(res, &inst)
	}
	if len(res) == 0 {
		return nil, false
	}
	return res, true
}

func generateInstanceIDIndex(domainProject string, serviceID string, instanceID string) string {
	return util.StringJoin([]string{
		domainProject,
		serviceID,
		instanceID,
	}, datasource.Split)
}

func GetSchema(ctx context.Context, serviceID string, schemaID string) (*model.Schema, bool) {
	index := generateSchemaIDIndex(util.ParseDomainProject(ctx), serviceID, schemaID)
	cacheSchema := sd.Store().Schema().Cache().GetValue(index)
	schemas, ok := transCacheToSchemas(cacheSchema)
	if !ok {
		return nil, false
	}
	return schemas[0], true
}

func GetSchemas(ctx context.Context, serviceID string) ([]*discovery.Schema, bool) {
	index := genServiceIDIndex(ctx, serviceID)
	cacheSchema := sd.Store().Schema().Cache().GetValue(index)
	schema, ok := transCacheToMicroSchemas(cacheSchema)
	if !ok {
		return nil, false
	}
	return schema, true
}

func transCacheToSchemas(cache []interface{}) ([]*model.Schema, bool) {
	res := make([]*model.Schema, 0, len(cache))
	for _, iter := range cache {
		schema, ok := iter.(model.Schema)
		if !ok {
			return nil, false
		}
		res = append(res, &schema)
	}
	if len(res) == 0 {
		return nil, false
	}
	return res, true
}

func transCacheToMicroSchemas(cache []interface{}) ([]*discovery.Schema, bool) {
	res := make([]*discovery.Schema, 0, len(cache))
	for _, iter := range cache {
		schema, ok := iter.(model.Schema)
		if !ok {
			return nil, false
		}
		msSchema := &discovery.Schema{
			Schema:   schema.Schema,
			SchemaId: schema.SchemaID,
			Summary:  schema.SchemaSummary,
		}
		res = append(res, msSchema)
	}
	if len(res) == 0 {
		return nil, false
	}
	return res, true
}

func generateSchemaIDIndex(domainProject string, serviceID string, schemaID string) string {
	return util.StringJoin([]string{
		domainProject,
		serviceID,
		schemaID,
	}, datasource.Split)
}
