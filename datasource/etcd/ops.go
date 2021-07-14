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

package etcd

import (
	"context"
	"strings"

	"github.com/apache/servicecomb-service-center/datasource"
	"github.com/apache/servicecomb-service-center/datasource/etcd/path"
	serviceUtil "github.com/apache/servicecomb-service-center/datasource/etcd/util"
	pb "github.com/go-chassis/cari/discovery"
)

func (ds *MetadataManager) GetServiceCount(ctx context.Context, request *pb.GetServiceCountRequest) (*pb.GetServiceCountResponse, error) {
	domainProject := request.Domain
	if request.Project != "" {
		domainProject += path.SPLIT + request.Project
	}
	all, err := serviceUtil.GetOneDomainProjectServiceCount(ctx, domainProject)
	if err != nil {
		return nil, err
	}
	global, err := ds.getGlobalServiceCount(ctx, domainProject)
	if err != nil {
		return nil, err
	}
	return &pb.GetServiceCountResponse{
		Response: pb.CreateResponse(pb.ResponseSuccess, "Get service count by domain project successfully"),
		Count:    all - global,
	}, nil
}

func (ds *MetadataManager) getGlobalServiceCount(ctx context.Context, domainProject string) (int64, error) {
	if strings.Index(datasource.RegistryDomainProject+datasource.SPLIT, domainProject+datasource.SPLIT) != 0 {
		return 0, nil
	}
	global, err := serviceUtil.GetGlobalServiceCount(ctx)
	if err != nil {
		return 0, err
	}
	return global, nil
}

func (ds *MetadataManager) GetInstanceCount(ctx context.Context, request *pb.GetServiceCountRequest) (*pb.GetServiceCountResponse, error) {
	domainProject := request.Domain
	if request.Project != "" {
		domainProject += path.SPLIT + request.Project
	}
	all, err := serviceUtil.GetOneDomainProjectInstanceCount(ctx, domainProject)
	if err != nil {
		return nil, err
	}
	global, err := ds.getGlobalInstanceCount(ctx, domainProject)
	if err != nil {
		return nil, err
	}
	return &pb.GetServiceCountResponse{
		Response: pb.CreateResponse(pb.ResponseSuccess, "Get instance count by domain/project successfully"),
		Count:    all - global,
	}, nil
}

func (ds *MetadataManager) getGlobalInstanceCount(ctx context.Context, domainProject string) (int64, error) {
	if strings.Index(datasource.RegistryDomainProject+path.SPLIT, domainProject+path.SPLIT) != 0 {
		return 0, nil
	}
	global, err := serviceUtil.GetGlobalInstanceCount(ctx)
	if err != nil {
		return 0, err
	}
	return global, nil
}
