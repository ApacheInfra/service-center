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

package mongo

import (
	"context"

	"github.com/apache/servicecomb-service-center/datasource/mongo/client/dao"
	mutil "github.com/apache/servicecomb-service-center/datasource/mongo/util"
	pb "github.com/go-chassis/cari/discovery"
	"go.mongodb.org/mongo-driver/bson"
)

func (ds *MetadataManager) GetServiceCount(ctx context.Context, request *pb.GetServiceCountRequest) (
	*pb.GetServiceCountResponse, error) {
	options := []mutil.Option{mutil.NotGlobal(), mutil.Domain(request.Domain)}
	if request.Project != "" {
		options = append(options, mutil.Project(request.Project))
	}
	count, err := dao.CountService(ctx, mutil.NewFilter(options...))
	if err != nil {
		return nil, err
	}
	return &pb.GetServiceCountResponse{
		Response: pb.CreateResponse(pb.ResponseSuccess, "Get instance count by domain/project successfully"),
		Count:    count,
	}, nil
}

func (ds *MetadataManager) GetInstanceCount(ctx context.Context, request *pb.GetServiceCountRequest) (
	*pb.GetServiceCountResponse, error) {
	inFilter, err := ds.getNotGlobalServiceFilter(ctx)
	if err != nil {
		return nil, err
	}
	options := []mutil.Option{mutil.Domain(request.Domain)}
	if request.Project != "" {
		options = append(options, mutil.Project(request.Project))
	}
	options = append(options, mutil.InstanceServiceID(inFilter))
	count, err := dao.CountInstance(ctx, mutil.NewFilter(options...))
	if err != nil {
		return nil, err
	}
	return &pb.GetServiceCountResponse{
		Response: pb.CreateResponse(pb.ResponseSuccess, "Get instance count by domain/project successfully"),
		Count:    count,
	}, nil
}

func (ds *MetadataManager) getNotGlobalServiceFilter(ctx context.Context) (bson.M, error) {
	serviceIDs := make([]string, 0)
	services, err := dao.GetServices(ctx, mutil.NewFilter(mutil.Global()))
	if err != nil {
		return nil, err
	}
	for _, service := range services {
		serviceIDs = append(serviceIDs, service.Service.ServiceId)
	}
	return mutil.NewFilter(mutil.NotIn(serviceIDs)), nil
}
