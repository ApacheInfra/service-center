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

package quota

import (
	"context"

	"github.com/apache/servicecomb-service-center/datasource"
	"github.com/go-chassis/cari/discovery"
)

func ServiceUsage(ctx context.Context, request *discovery.GetServiceCountRequest) (int64, error) {
	resp, err := datasource.GetMetadataManager().GetServiceCount(ctx, request)
	if err != nil {
		return 0, err
	}
	return resp.Count, nil
}

func InstanceUsage(ctx context.Context, request *discovery.GetServiceCountRequest) (int64, error) {
	resp, err := datasource.GetMetadataManager().GetInstanceCount(ctx, request)
	if err != nil {
		return 0, err
	}
	return resp.Count, nil
}

func RuleUsage(ctx context.Context, serviceID string) (int64, error) {
	resp, err := datasource.GetMetadataManager().GetRules(ctx, &discovery.GetServiceRulesRequest{
		ServiceId: serviceID,
	})
	if err != nil {
		return 0, err
	}
	return int64(len(resp.Rules)), nil
}

func SchemaUsage(ctx context.Context, serviceID string) (int64, error) {
	resp, err := datasource.GetMetadataManager().GetAllSchemas(ctx, &discovery.GetAllSchemaRequest{
		ServiceId:  serviceID,
		WithSchema: false,
	})
	if err != nil {
		return 0, err
	}
	return int64(len(resp.Schemas)), nil
}

func RoleUsage(ctx context.Context) (int64, error) {
	_, used, err := datasource.GetRoleManager().ListRole(ctx)
	if err != nil {
		return 0, err
	}
	return used, nil
}

func AccountUsage(ctx context.Context) (int64, error) {
	_, used, err := datasource.GetAccountManager().ListAccount(ctx)
	if err != nil {
		return 0, err
	}
	return used, nil
}
