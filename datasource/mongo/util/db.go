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

package util

import (
	"context"

	"github.com/go-chassis/cari/rbac"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/x/bsonx"

	"github.com/apache/servicecomb-service-center/datasource"
	"github.com/apache/servicecomb-service-center/datasource/mongo/client/model"
	"github.com/apache/servicecomb-service-center/pkg/util"
)

type Option func(filter bson.M)

func Domain(domain string) Option {
	return func(filter bson.M) {
		filter[model.ColumnDomain] = domain
	}
}

func Project(project string) Option {
	return func(filter bson.M) {
		filter[model.ColumnProject] = project
	}
}

func AccountName(name interface{}) Option {
	return func(filter bson.M) {
		filter[model.ColumnAccountName] = name
	}
}

func Password(password string) Option {
	return func(filter bson.M) {
		filter[model.ColumnPassword] = password
	}
}

func Roles(roles []string) Option {
	return func(filter bson.M) {
		filter[model.ColumnRoles] = roles
	}
}

func TokenExpirationTime(tokenExpirationTime string) Option {
	return func(filter bson.M) {
		filter[model.ColumnTokenExpirationTime] = tokenExpirationTime
	}
}

func CurrentPassword(password string) Option {
	return func(filter bson.M) {
		filter[model.ColumnCurrentPassword] = password
	}
}

func Status(status string) Option {
	return func(filter bson.M) {
		filter[model.ColumnStatus] = status
	}
}

func ID(id string) Option {
	return func(filter bson.M) {
		filter[model.ColumnID] = id
	}
}

func RoleName(name string) Option {
	return func(filter bson.M) {
		filter[model.ColumnRoleName] = name
	}
}

func Perms(perms []*rbac.Permission) Option {
	return func(filter bson.M) {
		filter[model.ColumnPerms] = perms
	}
}

func AccountUpdateTime(dt interface{}) Option {
	return func(filter bson.M) {
		filter[model.ColumnAccountUpdateTime] = dt
	}
}

func RoleUpdateTime(dt interface{}) Option {
	return func(filter bson.M) {
		filter[model.ColumnRoleUpdateTime] = dt
	}
}

func AccountLockKey(key interface{}) Option {
	return func(filter bson.M) {
		filter[model.ColumnAccountLockKey] = key
	}
}

func AccountLockStatus(status interface{}) Option {
	return func(filter bson.M) {
		filter[model.ColumnAccountLockStatus] = status
	}
}

func AccountLockReleaseAt(releaseAt interface{}) Option {
	return func(filter bson.M) {
		filter[model.ColumnAccountLockReleaseAt] = releaseAt
	}
}

func In(data interface{}) Option {
	return func(filter bson.M) {
		filter["$in"] = data
	}
}

func NotIn(data interface{}) Option {
	return func(filter bson.M) {
		filter["$nin"] = data
	}
}

func Set(data interface{}) Option {
	return func(filter bson.M) {
		filter["$set"] = data
	}
}

func Nor(options ...Option) Option {
	return func(filter bson.M) {
		filter["$nor"] = bson.A{NewFilter(options...)}
	}
}

func Or(options ...Option) Option {
	return func(filter bson.M) {
		var conditions bson.A
		for _, option := range options {
			conditions = append(conditions, NewFilter(option))
		}
		filter["$or"] = conditions
	}
}

func NewFilter(options ...Option) bson.M {
	filter := bson.M{}
	for _, option := range options {
		option(filter)
	}
	return filter
}

func NewDomainProjectFilter(domain string, project string, options ...func(filter bson.M)) bson.M {
	filter := bson.M{
		model.ColumnDomain:  domain,
		model.ColumnProject: project,
	}
	for _, option := range options {
		option(filter)
	}
	return filter
}

func NewBasicFilter(ctx context.Context, options ...func(filter bson.M)) bson.M {
	domain := util.ParseDomain(ctx)
	project := util.ParseProject(ctx)

	filter := bson.M{
		model.ColumnDomain:  domain,
		model.ColumnProject: project,
	}
	for _, option := range options {
		option(filter)
	}
	return filter
}

func InstanceServiceID(serviceID interface{}) Option {
	return func(filter bson.M) {
		filter[ConnectWithDot([]string{model.ColumnInstance, model.ColumnServiceID})] = serviceID
	}
}

func InstanceInstanceID(instanceID string) Option {
	return func(filter bson.M) {
		filter[ConnectWithDot([]string{model.ColumnInstance, model.ColumnInstanceID})] = instanceID
	}
}

// ServiceServiceID serviceID can be string or bson.M
func ServiceServiceID(serviceID interface{}) Option {
	return func(filter bson.M) {
		filter[ConnectWithDot([]string{model.ColumnService, model.ColumnServiceID})] = serviceID
	}
}

func ServiceEnv(env string) Option {
	return func(filter bson.M) {
		filter[ConnectWithDot([]string{model.ColumnService, model.ColumnEnv})] = env
	}
}

func ServiceAppID(appID string) Option {
	return func(filter bson.M) {
		filter[ConnectWithDot([]string{model.ColumnService, model.ColumnAppID})] = appID
	}
}

func ServiceModTime(modTime string) Option {
	return func(filter bson.M) {
		filter[ConnectWithDot([]string{model.ColumnService, model.ColumnModTime})] = modTime
	}
}

func ServiceProperty(property map[string]string) Option {
	return func(filter bson.M) {
		filter[ConnectWithDot([]string{model.ColumnService, model.ColumnProperty})] = property
	}
}

func ServiceServiceName(serviceName interface{}) Option {
	return func(filter bson.M) {
		filter[ConnectWithDot([]string{model.ColumnService, model.ColumnServiceName})] = serviceName
	}
}

func ServiceID(serviceID string) Option {
	return func(filter bson.M) {
		filter[model.ColumnServiceID] = serviceID
	}
}

func ServiceAlias(alias string) Option {
	return func(filter bson.M) {
		filter[ConnectWithDot([]string{model.ColumnService, model.ColumnAlias})] = alias
	}
}

func ServiceSchemas(schemas []string) Option {
	return func(filter bson.M) {
		filter[ConnectWithDot([]string{model.ColumnService, model.ColumnSchemas})] = schemas
	}
}

func ServiceVersion(version interface{}) Option {
	return func(filter bson.M) {
		filter[ConnectWithDot([]string{model.ColumnService, model.ColumnVersion})] = version
	}
}

func ServiceType(serviceType string) Option {
	return func(filter bson.M) {
		filter[model.ColumnServiceType] = serviceType
	}
}

func ServiceKeyTenant(tenant string) Option {
	return func(filter bson.M) {
		filter[ConnectWithDot([]string{model.ColumnServiceKey, model.ColumnTenant})] = tenant
	}
}

func ServiceKeyAppID(appID string) Option {
	return func(filter bson.M) {
		filter[ConnectWithDot([]string{model.ColumnServiceKey, model.ColumnAppID})] = appID
	}
}

func ServiceKeyServiceName(serviceName string) Option {
	return func(filter bson.M) {
		filter[ConnectWithDot([]string{model.ColumnServiceKey, model.ColumnServiceName})] = serviceName
	}
}

func ServiceKeyServiceEnv(env string) Option {
	return func(filter bson.M) {
		filter[ConnectWithDot([]string{model.ColumnServiceKey, model.ColumnEnv})] = env
	}
}

func ServiceKeyServiceVersion(version string) Option {
	return func(filter bson.M) {
		filter[ConnectWithDot([]string{model.ColumnServiceKey, model.ColumnVersion})] = version
	}
}

func Schema(schema string) Option {
	return func(filter bson.M) {
		filter[model.ColumnSchema] = schema
	}
}

func SchemaID(schemaID string) Option {
	return func(filter bson.M) {
		filter[model.ColumnSchemaID] = schemaID
	}
}

func RuleAttribute(attribute string) Option {
	return func(filter bson.M) {
		filter[ConnectWithDot([]string{model.ColumnRule, model.ColumnAttribute})] = attribute
	}
}

func RuleRuleID(ruleID string) Option {
	return func(filter bson.M) {
		filter[ConnectWithDot([]string{model.ColumnRule, model.ColumnRuleID})] = ruleID
	}
}

func RuleRuleType(ruleType string) Option {
	return func(filter bson.M) {
		filter[ConnectWithDot([]string{model.ColumnRule, model.ColumnRuleType})] = ruleType
	}
}

func RulePattern(pattern string) Option {
	return func(filter bson.M) {
		filter[ConnectWithDot([]string{model.ColumnRule, model.ColumnPattern})] = pattern
	}
}

func RuleDescription(description string) Option {
	return func(filter bson.M) {
		filter[ConnectWithDot([]string{model.ColumnRule, model.ColumnDescription})] = description
	}
}

func RuleModTime(modTime string) Option {
	return func(filter bson.M) {
		filter[ConnectWithDot([]string{model.ColumnRule, model.ColumnModTime})] = modTime
	}
}

func SchemaSummary(schemaSummary string) Option {
	return func(filter bson.M) {
		filter[model.ColumnSchemaSummary] = schemaSummary
	}
}

func Tags(tags map[string]string) Option {
	return func(filter bson.M) {
		filter[model.ColumnTag] = tags
	}
}

func InstanceModTime(modTime string) Option {
	return func(filter bson.M) {
		filter[ConnectWithDot([]string{model.ColumnService, model.ColumnModTime})] = modTime
	}
}

func InstanceStatus(status string) Option {
	return func(filter bson.M) {
		filter[ConnectWithDot([]string{model.ColumnInstance, model.ColumnStatus})] = status
	}
}

func InstanceProperties(properties map[string]string) Option {
	return func(filter bson.M) {
		filter[ConnectWithDot([]string{model.ColumnInstance, model.ColumnProperty})] = properties
	}
}

func BuildIndexDoc(keys ...string) mongo.IndexModel {
	keysDoc := bsonx.Doc{}
	for _, key := range keys {
		keysDoc = keysDoc.Append(key, bsonx.Int32(1))
	}
	index := mongo.IndexModel{
		Keys: keysDoc,
	}
	return index
}

func NotGlobal() Option {
	var names []string
	for name := range datasource.GlobalServiceNames {
		names = append(names, name)
	}
	inFilter := NewFilter(In(names))
	return Nor(
		Domain(datasource.RegistryDomain),
		Project(datasource.RegistryProject),
		ServiceAppID(datasource.RegistryAppID),
		ServiceServiceName(inFilter),
	)
}

func Global() Option {
	var names []string
	for name := range datasource.GlobalServiceNames {
		names = append(names, name)
	}
	inFilter := NewFilter(In(names))
	options := []Option{
		Domain(datasource.RegistryDomain),
		Project(datasource.RegistryProject),
		ServiceAppID(datasource.RegistryAppID),
		ServiceServiceName(inFilter),
	}
	return func(filter bson.M) {
		for _, option := range options {
			option(filter)
		}
	}
}
