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

package mongo

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/go-chassis/cari/discovery"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/apache/servicecomb-service-center/datasource"
	"github.com/apache/servicecomb-service-center/datasource/mongo/client"
	"github.com/apache/servicecomb-service-center/datasource/mongo/heartbeat"
	"github.com/apache/servicecomb-service-center/pkg/gopool"
	"github.com/apache/servicecomb-service-center/pkg/log"
	"github.com/apache/servicecomb-service-center/pkg/util"
	apt "github.com/apache/servicecomb-service-center/server/core"
	"github.com/apache/servicecomb-service-center/server/plugin/uuid"
)

func (ds *DataSource) RegisterService(ctx context.Context, request *discovery.CreateServiceRequest) (
	*discovery.CreateServiceResponse, error) {
	service := request.Service

	domain := util.ParseDomain(ctx)
	project := util.ParseProject(ctx)
	//todo add quota check
	requestServiceID := service.ServiceId

	if len(requestServiceID) == 0 {
		ctx = util.SetContext(ctx, uuid.ContextKey, util.StringJoin([]string{domain, project, service.Environment, service.AppId, service.ServiceName, service.Alias, service.Version}, "/"))
		service.ServiceId = uuid.Generator().GetServiceID(ctx)
	}
	// the service unique index in table is (serviceId,serviceEnv,serviceAppid,servicename,serviceAlias,serviceVersion)
	existID, err := ServiceExistID(ctx, service.ServiceId)
	if err != nil {
		return &discovery.CreateServiceResponse{
			Response: discovery.CreateResponse(discovery.ErrInternal, "Check service exist failed"),
		}, err
	}
	exist, err := ServiceExist(ctx, &discovery.MicroServiceKey{
		Environment: service.Environment,
		AppId:       service.AppId,
		ServiceName: service.ServiceName,
		Alias:       service.Alias,
		Version:     service.Version,
	})
	if err != nil {
		return &discovery.CreateServiceResponse{
			Response: discovery.CreateResponse(discovery.ErrInternal, "Check service exist failed"),
		}, err
	}
	if existID || exist {
		return &discovery.CreateServiceResponse{
			Response: discovery.CreateResponse(discovery.ErrServiceAlreadyExists, "ServiceID conflict or found the same service."),
		}, nil
	}
	insertRes, err := client.GetMongoClient().Insert(ctx, CollectionService, &Service{Domain: domain, Project: project, ServiceInfo: service})
	if err != nil {
		if client.IsDuplicateKey(err) {
			return &discovery.CreateServiceResponse{
				Response: discovery.CreateResponse(discovery.ErrServiceAlreadyExists, "ServiceID or ServiceInfo conflict."),
			}, nil
		}
		return &discovery.CreateServiceResponse{
			Response: discovery.CreateResponse(discovery.ErrInternal, "Register service failed."),
		}, err
	}

	remoteIP := util.GetIPFromContext(ctx)
	log.Info(fmt.Sprintf("create micro-service[%s][%s] successfully,operator: %s",
		service.ServiceId, insertRes.InsertedID, remoteIP))

	return &discovery.CreateServiceResponse{
		Response:  discovery.CreateResponse(discovery.ResponseSuccess, "Register service successfully"),
		ServiceId: service.ServiceId,
	}, nil
}

func (ds *DataSource) GetServices(ctx context.Context, request *discovery.GetServicesRequest) (
	*discovery.GetServicesResponse, error) {

	domain := util.ParseDomain(ctx)
	project := util.ParseProject(ctx)

	filter := bson.M{ColumnDomain: domain, ColumnProject: project}

	services, err := GetServices(ctx, filter)
	if err != nil {
		return &discovery.GetServicesResponse{
			Response: discovery.CreateResponse(discovery.ErrInternal, "get services data failed."),
		}, nil
	}

	return &discovery.GetServicesResponse{
		Response: discovery.CreateResponse(discovery.ResponseSuccess, "Get all services successfully."),
		Services: services,
	}, nil
}

func (ds *DataSource) GetApplications(ctx context.Context, request *discovery.GetAppsRequest) (*discovery.GetAppsResponse, error) {
	domain := util.ParseDomain(ctx)
	project := util.ParseProject(ctx)

	filter := bson.M{
		ColumnDomain:  domain,
		ColumnProject: project,
		StringBuilder([]string{ColumnServiceInfo, ColumnEnv}): request.Environment}

	services, err := GetServices(ctx, filter)
	if err != nil {
		return &discovery.GetAppsResponse{
			Response: discovery.CreateResponse(discovery.ErrInternal, "get services data failed."),
		}, nil
	}
	l := len(services)
	if l == 0 {
		return &discovery.GetAppsResponse{
			Response: discovery.CreateResponse(discovery.ErrInternal, "get services data failed."),
		}, nil
	}
	apps := make([]string, 0, l)
	hash := make(map[string]struct{}, l)
	for _, svc := range services {
		if !request.WithShared && apt.IsGlobal(discovery.MicroServiceToKey(util.ParseDomainProject(ctx), svc)) {
			continue
		}
		if _, ok := hash[svc.AppId]; ok {
			continue
		}
		hash[svc.AppId] = struct{}{}
		apps = append(apps, svc.AppId)
	}
	return &discovery.GetAppsResponse{
		Response: discovery.CreateResponse(discovery.ResponseSuccess, "Get all applications successfully."),
		AppIds:   apps,
	}, nil
}

func (ds *DataSource) GetService(ctx context.Context, request *discovery.GetServiceRequest) (
	*discovery.GetServiceResponse, error) {
	svc, err := GetService(ctx, GeneratorServiceFilter(ctx, request.ServiceId))
	if err != nil {
		log.Error(fmt.Sprintf("failed to get single service %s from mongo", request.ServiceId), err)
		return &discovery.GetServiceResponse{
			Response: discovery.CreateResponse(discovery.ErrInternal, "get service data from mongodb failed."),
		}, err
	}
	if svc != nil {
		return &discovery.GetServiceResponse{
			Response: discovery.CreateResponse(discovery.ResponseSuccess, "Get service successfully."),
			Service:  svc.ServiceInfo,
		}, nil
	}
	return &discovery.GetServiceResponse{
		Response: discovery.CreateResponse(discovery.ErrServiceNotExists, "Service not exist."),
	}, nil
}

func (ds *DataSource) ExistServiceByID(ctx context.Context, request *discovery.GetExistenceByIDRequest) (*discovery.GetExistenceByIDResponse, error) {

	exist, err := ServiceExistID(ctx, request.ServiceId)
	if err != nil {
		return &discovery.GetExistenceByIDResponse{
			Response: discovery.CreateResponse(discovery.ErrInternal, "Check service exist failed."),
			Exist:    false,
		}, err
	}

	return &discovery.GetExistenceByIDResponse{
		Response: discovery.CreateResponse(discovery.ResponseSuccess, "Check ExistService successfully."),
		Exist:    exist,
	}, nil
}

func (ds *DataSource) ExistService(ctx context.Context, request *discovery.GetExistenceRequest) (*discovery.GetExistenceResponse, error) {
	serviceKey := &discovery.MicroServiceKey{
		Environment: request.Environment,
		AppId:       request.AppId,
		ServiceName: request.ServiceName,
		Alias:       request.ServiceName,
		Version:     request.Version,
	}
	//todo add verison match.
	services, err := GetServices(ctx, GeneratorServiceNameFilter(ctx, serviceKey))
	if err != nil {
		return &discovery.GetExistenceResponse{
			Response: discovery.CreateResponse(discovery.ErrInternal, err.Error()),
		}, err
	}
	if len(services) != 0 {
		return &discovery.GetExistenceResponse{
			Response:  discovery.CreateResponse(discovery.ResponseSuccess, "get service id successfully."),
			ServiceId: services[0].ServiceId,
		}, nil
	}
	services, err = GetServices(ctx, GeneratorServiceAliasFilter(ctx, serviceKey))
	if err != nil {
		return &discovery.GetExistenceResponse{
			Response: discovery.CreateResponse(discovery.ErrInternal, err.Error()),
		}, err
	}
	if len(services) != 0 {
		return &discovery.GetExistenceResponse{
			Response:  discovery.CreateResponse(discovery.ResponseSuccess, "get service id successfully."),
			ServiceId: services[0].ServiceId,
		}, nil
	}
	return &discovery.GetExistenceResponse{
		Response: discovery.CreateResponse(discovery.ErrServiceNotExists, "Service does not exist"),
	}, nil
}

func (ds *DataSource) UnregisterService(ctx context.Context, request *discovery.DeleteServiceRequest) (*discovery.DeleteServiceResponse, error) {
	exist, err := ServiceExistID(ctx, request.ServiceId)
	if err != nil {
		return &discovery.DeleteServiceResponse{
			Response: discovery.CreateResponse(discovery.ErrInternal, "Delete service failed,failed to get service."),
		}, err
	}
	if !exist {
		return &discovery.DeleteServiceResponse{
			Response: discovery.CreateResponse(discovery.ErrServiceNotExists, "Delete service failed,service not exist."),
		}, nil
	}
	session, err := client.GetMongoClient().StartSession(ctx)
	if err != nil {
		return &discovery.DeleteServiceResponse{
			Response: discovery.CreateResponse(discovery.ErrInternal, "DelService failed to create session."),
		}, err
	}
	if err = session.StartTransaction(); err != nil {
		return &discovery.DeleteServiceResponse{
			Response: discovery.CreateResponse(discovery.ErrInternal, "DelService failed to start session."),
		}, err
	}
	defer session.EndSession(ctx)
	//todo delete instance,tags,schemas...
	res, err := DelServicePri(ctx, request.ServiceId, request.Force)
	if err != nil {
		errAbort := session.AbortTransaction(ctx)
		if errAbort != nil {
			return &discovery.DeleteServiceResponse{
				Response: discovery.CreateResponse(discovery.ErrInternal, "Txn delete service abort failed."),
			}, errAbort
		}
		return &discovery.DeleteServiceResponse{
			Response: discovery.CreateResponse(discovery.ErrInternal, "Delete service failed"),
		}, err
	}
	errCommit := session.CommitTransaction(ctx)
	if errCommit != nil {
		return &discovery.DeleteServiceResponse{
			Response: discovery.CreateResponse(discovery.ErrInternal, "Txn delete service commit failed."),
		}, errCommit
	}
	return &discovery.DeleteServiceResponse{
		Response: res,
	}, nil
}

func DelServicePri(ctx context.Context, serviceID string, force bool) (*discovery.Response, error) {
	remoteIP := util.GetIPFromContext(ctx)
	title := "delete"
	if force {
		title = "force delete"
	}

	if serviceID == apt.Service.ServiceId {
		log.Error(fmt.Sprintf("%s micro-service %s failed, operator: %s", title, serviceID, remoteIP), ErrNotAllowDeleteSC)
		return discovery.CreateResponse(discovery.ErrInvalidParams, ErrNotAllowDeleteSC.Error()), nil
	}
	microservice, err := GetService(ctx, GeneratorServiceFilter(ctx, serviceID))
	if err != nil {
		log.Error(fmt.Sprintf("%s micro-service %s failed, get service file failed, operator: %s",
			title, serviceID, remoteIP), err)
		return discovery.CreateResponse(discovery.ErrInternal, err.Error()), err
	}
	if microservice == nil {
		log.Error(fmt.Sprintf("%s micro-service %s failed, service does not exist, operator: %s",
			title, serviceID, remoteIP), err)
		return discovery.CreateResponse(discovery.ErrServiceNotExists, "Service does not exist."), nil
	}
	// 强制删除，则与该服务相关的信息删除，非强制删除： 如果作为该被依赖（作为provider，提供服务,且不是只存在自依赖）或者存在实例，则不能删除
	if !force {
		log.Info("force delete,should del instance...")
		//todo wait for dep interface
	}
	filter := GeneratorServiceFilter(ctx, serviceID)
	//todo del instances
	tables := []string{CollectionService, CollectionSchema, CollectionRule}
	for _, col := range tables {
		_, err := client.GetMongoClient().Delete(ctx, col, filter)
		if err != nil {
			return discovery.CreateResponse(discovery.ErrInternal, err.Error()), err
		}
	}
	return discovery.CreateResponse(discovery.ResponseSuccess, "Unregister service successfully."), nil

}

func (ds *DataSource) UpdateService(ctx context.Context, request *discovery.UpdateServicePropsRequest) (
	*discovery.UpdateServicePropsResponse, error) {

	exist, err := ServiceExistID(ctx, request.ServiceId)
	if err != nil {
		return &discovery.UpdateServicePropsResponse{
			Response: discovery.CreateResponse(discovery.ErrInternal, "UpdateService failed,failed to get service."),
		}, err
	}
	if !exist {
		return &discovery.UpdateServicePropsResponse{
			Response: discovery.CreateResponse(discovery.ErrServiceNotExists, "UpdateService failed,service not exist."),
		}, nil
	}

	updateData := bson.M{
		"$set": bson.M{
			StringBuilder([]string{ColumnServiceInfo, ColumnModTime}):  strconv.FormatInt(time.Now().Unix(), 10),
			StringBuilder([]string{ColumnServiceInfo, ColumnProperty}): request.Properties}}
	err = UpdateService(ctx, GeneratorServiceFilter(ctx, request.ServiceId), updateData)
	if err != nil {
		log.Error(fmt.Sprintf("update service %s properties failed, update mongo failed", request.ServiceId), err)
		return &discovery.UpdateServicePropsResponse{
			Response: discovery.CreateResponse(discovery.ErrUnavailableBackend, "Update doc in mongo failed."),
		}, nil
	}
	return &discovery.UpdateServicePropsResponse{
		Response: discovery.CreateResponse(discovery.ResponseSuccess, "Update service successfully."),
	}, nil
}

func (ds *DataSource) GetDeleteServiceFunc(ctx context.Context, serviceID string, force bool,
	serviceRespChan chan<- *discovery.DelServicesRspInfo) func(context.Context) {
	return func(_ context.Context) {}
}

func (ds *DataSource) GetServiceDetail(ctx context.Context, request *discovery.GetServiceRequest) (
	*discovery.GetServiceDetailResponse, error) {
	mgSvc, err := GetService(ctx, GeneratorServiceFilter(ctx, request.ServiceId))
	if err != nil {
		return &discovery.GetServiceDetailResponse{
			Response: discovery.CreateResponse(discovery.ErrInternal, err.Error()),
		}, err
	}
	if mgSvc == nil {
		return &discovery.GetServiceDetailResponse{
			Response: discovery.CreateResponse(discovery.ErrServiceNotExists, "Service does not exist."),
		}, nil
	}
	svc := mgSvc.ServiceInfo
	versions, err := GetServicesVersions(ctx, bson.M{})
	if err != nil {
		log.Error(fmt.Sprintf("get service %s %s %s all versions failed", svc.Environment, svc.AppId, svc.ServiceName), err)
		return &discovery.GetServiceDetailResponse{
			Response: discovery.CreateResponse(discovery.ErrInternal, err.Error()),
		}, err
	}
	options := []string{"tags", "rules", "instances", "schemas", "dependencies"}
	serviceInfo, err := getServiceDetailUtil(ctx, mgSvc, false, options)
	if err != nil {
		return &discovery.GetServiceDetailResponse{
			Response: discovery.CreateResponse(discovery.ErrInternal, err.Error()),
		}, err
	}
	serviceInfo.MicroService = svc
	serviceInfo.MicroServiceVersions = versions
	return &discovery.GetServiceDetailResponse{
		Response: discovery.CreateResponse(discovery.ResponseSuccess, "Get service successfully"),
		Service:  serviceInfo,
	}, nil

}

func (ds *DataSource) GetServicesInfo(ctx context.Context, request *discovery.GetServicesInfoRequest) (
	*discovery.GetServicesInfoResponse, error) {
	optionMap := make(map[string]struct{}, len(request.Options))
	for _, opt := range request.Options {
		optionMap[opt] = struct{}{}
	}

	options := make([]string, 0, len(optionMap))
	if _, ok := optionMap["all"]; ok {
		optionMap["statistics"] = struct{}{}
		options = []string{"tags", "rules", "instances", "schemas", "dependencies"}
	} else {
		for opt := range optionMap {
			options = append(options, opt)
		}
	}
	//todo add get statistics info
	services, err := GetMongoServices(ctx, bson.M{})
	if err != nil {
		log.Error("get all services by domain failed", err)
		return &discovery.GetServicesInfoResponse{
			Response: discovery.CreateResponse(discovery.ErrInternal, err.Error()),
		}, err
	}
	allServiceDetails := make([]*discovery.ServiceDetail, 0, len(services))
	domainProject := util.ParseDomainProject(ctx)
	for _, mgSvc := range services {
		if !request.WithShared && apt.IsGlobal(discovery.MicroServiceToKey(domainProject, mgSvc.ServiceInfo)) {
			continue
		}
		if len(request.AppId) > 0 {
			if request.AppId != mgSvc.ServiceInfo.AppId {
				continue
			}
			if len(request.ServiceName) > 0 && request.ServiceName != mgSvc.ServiceInfo.ServiceName {
				continue
			}
		}

		serviceDetail, err := getServiceDetailUtil(ctx, mgSvc, request.CountOnly, options)
		if err != nil {
			return &discovery.GetServicesInfoResponse{
				Response: discovery.CreateResponse(discovery.ErrInternal, err.Error()),
			}, err
		}
		serviceDetail.MicroService = mgSvc.ServiceInfo
		allServiceDetails = append(allServiceDetails, serviceDetail)
	}

	return &discovery.GetServicesInfoResponse{
		Response:          discovery.CreateResponse(discovery.ResponseSuccess, "Get services info successfully."),
		AllServicesDetail: allServiceDetails,
		Statistics:        nil,
	}, nil
}

func (ds *DataSource) AddTags(ctx context.Context, request *discovery.AddServiceTagsRequest) (*discovery.AddServiceTagsResponse, error) {
	service, err := GetService(ctx, GeneratorServiceFilter(ctx, request.ServiceId))
	if err != nil {
		log.Error(fmt.Sprintf("failed to add tags for service %s for get service failed", request.ServiceId), err)
		return &discovery.AddServiceTagsResponse{
			Response: discovery.CreateResponse(discovery.ErrInternal, "Failed to check service exist"),
		}, nil
	}
	if service == nil {
		return &discovery.AddServiceTagsResponse{Response: discovery.CreateResponse(discovery.ErrServiceNotExists, "Service not exist")}, nil
	}
	//todo add quto check
	dataTags := service.Tags
	tags := request.Tags
	for key, value := range dataTags {
		if _, ok := tags[key]; ok {
			continue
		}
		tags[key] = value
	}
	err = UpdateService(ctx, GeneratorServiceFilter(ctx, request.ServiceId), bson.M{"$set": bson.M{ColumnTag: tags}})
	if err != nil {
		log.Error(fmt.Sprintf("update service %s tags failed.", request.ServiceId), err)
		return &discovery.AddServiceTagsResponse{
			Response: discovery.CreateResponse(discovery.ErrInternal, err.Error()),
		}, nil
	}
	return &discovery.AddServiceTagsResponse{
		Response: discovery.CreateResponse(discovery.ResponseSuccess, "Add service tags successfully."),
	}, nil
}

func (ds *DataSource) GetTags(ctx context.Context, request *discovery.GetServiceTagsRequest) (*discovery.GetServiceTagsResponse, error) {
	svc, err := GetService(ctx, GeneratorServiceFilter(ctx, request.ServiceId))
	if err != nil {
		log.Error(fmt.Sprintf("failed to get service %s tags", request.ServiceId), err)
		return &discovery.GetServiceTagsResponse{
			Response: discovery.CreateResponse(discovery.ErrInternal, err.Error()),
		}, nil
	}
	if svc == nil {
		return &discovery.GetServiceTagsResponse{
			Response: discovery.CreateResponse(discovery.ErrServiceNotExists, "Service does not exist"),
		}, nil
	}
	return &discovery.GetServiceTagsResponse{
		Response: discovery.CreateResponse(discovery.ResponseSuccess, "Get service tags successfully."),
		Tags:     svc.Tags,
	}, nil
}

func (ds *DataSource) UpdateTag(ctx context.Context, request *discovery.UpdateServiceTagRequest) (*discovery.UpdateServiceTagResponse, error) {
	svc, err := GetService(ctx, GeneratorServiceFilter(ctx, request.ServiceId))
	if err != nil {
		log.Error(fmt.Sprintf("failed to get %s tags", request.ServiceId), err)
		return &discovery.UpdateServiceTagResponse{
			Response: discovery.CreateResponse(discovery.ErrInternal, err.Error()),
		}, nil
	}
	if svc == nil {
		return &discovery.UpdateServiceTagResponse{
			Response: discovery.CreateResponse(discovery.ErrServiceNotExists, "Service does not exist"),
		}, nil
	}
	dataTags := svc.Tags
	if len(dataTags) > 0 {
		if _, ok := dataTags[request.Key]; !ok {
			return &discovery.UpdateServiceTagResponse{
				Response: discovery.CreateResponse(discovery.ErrTagNotExists, "Tag does not exist"),
			}, nil
		}
	}
	newTags := make(map[string]string, len(dataTags))
	for k, v := range dataTags {
		newTags[k] = v
	}
	newTags[request.Key] = request.Value

	err = UpdateService(ctx, GeneratorServiceFilter(ctx, request.ServiceId), bson.M{"$set": bson.M{ColumnTag: newTags}})
	if err != nil {
		log.Error(fmt.Sprintf("update service %s tags failed", request.ServiceId), err)
		return &discovery.UpdateServiceTagResponse{
			Response: discovery.CreateResponse(discovery.ErrInternal, err.Error()),
		}, nil
	}
	return &discovery.UpdateServiceTagResponse{
		Response: discovery.CreateResponse(discovery.ResponseSuccess, "Update service tag success."),
	}, nil
}

func (ds *DataSource) DeleteTags(ctx context.Context, request *discovery.DeleteServiceTagsRequest) (*discovery.DeleteServiceTagsResponse, error) {
	svc, err := GetService(ctx, GeneratorServiceFilter(ctx, request.ServiceId))
	if err != nil {
		log.Error(fmt.Sprintf("failed to get service %s tags", request.ServiceId), err)
		return &discovery.DeleteServiceTagsResponse{
			Response: discovery.CreateResponse(discovery.ErrInternal, err.Error()),
		}, nil
	}
	if svc == nil {
		return &discovery.DeleteServiceTagsResponse{
			Response: discovery.CreateResponse(discovery.ErrServiceNotExists, "Service does not exist"),
		}, nil
	}
	dataTags := svc.Tags
	newTags := make(map[string]string, len(dataTags))
	for k, v := range dataTags {
		newTags[k] = v
	}
	if len(dataTags) > 0 {
		for _, key := range request.Keys {
			if _, ok := dataTags[key]; !ok {
				return &discovery.DeleteServiceTagsResponse{
					Response: discovery.CreateResponse(discovery.ErrTagNotExists, "Tag does not exist"),
				}, nil
			}
			delete(newTags, key)
		}
	}
	err = UpdateService(ctx, GeneratorServiceFilter(ctx, request.ServiceId), bson.M{"$set": bson.M{ColumnTag: newTags}})
	if err != nil {
		log.Error(fmt.Sprintf("delete service %s tags failed", request.ServiceId), err)
		return &discovery.DeleteServiceTagsResponse{
			Response: discovery.CreateResponse(discovery.ErrInternal, err.Error()),
		}, nil
	}
	return &discovery.DeleteServiceTagsResponse{
		Response: discovery.CreateResponse(discovery.ResponseSuccess, "Update service tag success."),
	}, nil
}

func (ds *DataSource) GetSchema(ctx context.Context, request *discovery.GetSchemaRequest) (*discovery.GetSchemaResponse, error) {
	exist, err := ServiceExistID(ctx, request.ServiceId)
	if err != nil {
		return &discovery.GetSchemaResponse{
			Response: discovery.CreateResponse(discovery.ErrInternal, "GetSchema failed to check service exist."),
		}, nil
	}
	if !exist {
		return &discovery.GetSchemaResponse{
			Response: discovery.CreateResponse(discovery.ErrServiceNotExists, "GetSchema service does not exist."),
		}, nil
	}
	Schema, err := GetSchema(ctx, GeneratorSchemaFilter(ctx, request.ServiceId, request.SchemaId))
	if err != nil {
		return &discovery.GetSchemaResponse{
			Response: discovery.CreateResponse(discovery.ErrInternal, "GetSchema failed from mongodb."),
		}, nil
	}
	return &discovery.GetSchemaResponse{
		Response:      discovery.CreateResponse(discovery.ResponseSuccess, "Get schema info successfully."),
		Schema:        Schema.SchemaInfo,
		SchemaSummary: Schema.SchemaSummary,
	}, nil
}

func (ds *DataSource) GetAllSchemas(ctx context.Context, request *discovery.GetAllSchemaRequest) (*discovery.GetAllSchemaResponse, error) {
	exist, err := ServiceExistID(ctx, request.ServiceId)
	if err != nil {
		return &discovery.GetAllSchemaResponse{
			Response: discovery.CreateResponse(discovery.ErrServiceNotExists, "GetAllSchemas failed for get service failed"),
		}, nil
	}
	if !exist {
		return &discovery.GetAllSchemaResponse{
			Response: discovery.CreateResponse(discovery.ErrServiceNotExists, "GetAllSchemas failed for service not exist"),
		}, nil
	}

	schemas, err := GetSchemas(ctx, GeneratorServiceFilter(ctx, request.ServiceId))
	if err != nil {
		return &discovery.GetAllSchemaResponse{
			Response: discovery.CreateResponse(discovery.ErrInternal, "GetAllSchemas failed for get schemas failed"),
		}, nil
	}
	return &discovery.GetAllSchemaResponse{
		Response: discovery.CreateResponse(discovery.ResponseSuccess, "Get all schema info successfully."),
		Schemas:  schemas,
	}, nil
}

func (ds *DataSource) ExistSchema(ctx context.Context, request *discovery.GetExistenceRequest) (*discovery.GetExistenceResponse, error) {
	exist, err := ServiceExistID(ctx, request.ServiceId)
	if err != nil {
		return &discovery.GetExistenceResponse{
			Response: discovery.CreateResponse(discovery.ErrServiceNotExists, "ExistSchema failed for get service failed"),
		}, nil
	}
	if !exist {
		return &discovery.GetExistenceResponse{
			Response: discovery.CreateResponse(discovery.ErrServiceNotExists, "ExistSchema failed for service not exist"),
		}, nil
	}
	Schema, err := GetSchema(ctx, GeneratorSchemaFilter(ctx, request.ServiceId, request.SchemaId))
	if err != nil {
		return &discovery.GetExistenceResponse{
			Response: discovery.CreateResponse(discovery.ErrInternal, "ExistSchema failed for get schema failed."),
		}, nil
	}
	if Schema == nil {
		return &discovery.GetExistenceResponse{
			Response: discovery.CreateResponse(discovery.ErrSchemaNotExists, "ExistSchema failed for schema not exist."),
		}, nil
	}
	return &discovery.GetExistenceResponse{
		Response:  discovery.CreateResponse(discovery.ResponseSuccess, "Schema exist."),
		Summary:   Schema.SchemaSummary,
		SchemaId:  Schema.SchemaID,
		ServiceId: Schema.ServiceID,
	}, nil
}

func (ds *DataSource) DeleteSchema(ctx context.Context, request *discovery.DeleteSchemaRequest) (*discovery.DeleteSchemaResponse, error) {
	exist, err := ServiceExistID(ctx, request.ServiceId)
	if err != nil {
		return &discovery.DeleteSchemaResponse{
			Response: discovery.CreateResponse(discovery.ErrServiceNotExists, "DeleteSchema failed for get service failed."),
		}, nil
	}
	if !exist {
		return &discovery.DeleteSchemaResponse{
			Response: discovery.CreateResponse(discovery.ErrServiceNotExists, "DeleteSchema failed for service not exist."),
		}, nil
	}
	filter := GeneratorSchemaFilter(ctx, request.ServiceId, request.SchemaId)
	_, err = client.GetMongoClient().Delete(ctx, CollectionSchema, filter)
	if err != nil {
		return &discovery.DeleteSchemaResponse{
			Response: discovery.CreateResponse(discovery.ErrUnavailableBackend, "DeleteSchema failed for delete schema failed."),
		}, nil
	}
	return &discovery.DeleteSchemaResponse{
		Response: discovery.CreateResponse(discovery.ResponseSuccess, "Delete schema info successfully."),
	}, nil
}

func (ds *DataSource) ModifySchema(ctx context.Context, request *discovery.ModifySchemaRequest) (*discovery.ModifySchemaResponse, error) {
	remoteIP := util.GetIPFromContext(ctx)
	serviceID := request.ServiceId
	schemaID := request.SchemaId
	schema := discovery.Schema{
		SchemaId: request.SchemaId,
		Summary:  request.Summary,
		Schema:   request.Schema,
	}
	session, err := client.GetMongoClient().StartSession(ctx)
	if err != nil {
		return &discovery.ModifySchemaResponse{
			Response: discovery.CreateResponse(discovery.ErrInternal, "ModifySchema failed to create session."),
		}, err
	}
	if err = session.StartTransaction(); err != nil {
		return &discovery.ModifySchemaResponse{
			Response: discovery.CreateResponse(discovery.ErrInternal, "ModifySchema failed to start session."),
		}, err
	}
	defer session.EndSession(ctx)
	err = ds.modifySchema(ctx, request.ServiceId, &schema)
	if err != nil {
		log.Error(fmt.Sprintf("modify schema %s %s failed, operator %s", serviceID, schemaID, remoteIP), err)
		errAbort := session.AbortTransaction(ctx)
		if errAbort != nil {
			return &discovery.ModifySchemaResponse{
				Response: discovery.CreateResponse(discovery.ErrInternal, "Txn ModifySchema Abort failed."),
			}, errAbort
		}
		return &discovery.ModifySchemaResponse{
			Response: discovery.CreateResponse(discovery.ErrInternal, "Txn ModifySchema failed."),
		}, err
	}
	err = session.CommitTransaction(ctx)
	if err != nil {
		return &discovery.ModifySchemaResponse{
			Response: discovery.CreateResponse(discovery.ErrInternal, "Txn ModifySchema CommitTransaction failed."),
		}, err
	}
	log.Info(fmt.Sprintf("modify schema[%s/%s] successfully, operator: %s", serviceID, schemaID, remoteIP))
	return &discovery.ModifySchemaResponse{
		Response: discovery.CreateResponse(discovery.ResponseSuccess, "modify schema info success."),
	}, nil
}

func (ds *DataSource) ModifySchemas(ctx context.Context, request *discovery.ModifySchemasRequest) (*discovery.ModifySchemasResponse, error) {
	svc, err := GetService(ctx, GeneratorServiceFilter(ctx, request.ServiceId))
	if err != nil {
		return &discovery.ModifySchemasResponse{Response: discovery.CreateResponse(discovery.ErrInternal, err.Error())}, err
	}
	if svc == nil {
		return &discovery.ModifySchemasResponse{Response: discovery.CreateResponse(discovery.ErrServiceNotExists, "Service not exist")}, nil
	}
	session, err := client.GetMongoClient().StartSession(ctx)
	if err != nil {
		return &discovery.ModifySchemasResponse{
			Response: discovery.CreateResponse(discovery.ErrInternal, "ModifySchemas failed to start session"),
		}, err
	}
	if err = session.StartTransaction(); err != nil {
		return &discovery.ModifySchemasResponse{
			Response: discovery.CreateResponse(discovery.ErrInternal, "ModifySchemas failed to start session"),
		}, err
	}
	defer session.EndSession(ctx)
	err = ds.modifySchemas(ctx, svc.ServiceInfo, request.Schemas)
	if err != nil {
		errAbort := session.AbortTransaction(ctx)
		if errAbort != nil {
			return &discovery.ModifySchemasResponse{
				Response: discovery.CreateResponse(discovery.ErrInternal, "Txn ModifySchemas Abort failed."),
			}, errAbort
		}
		return &discovery.ModifySchemasResponse{Response: discovery.CreateResponse(discovery.ErrInternal, err.Error())}, err
	}
	err = session.CommitTransaction(ctx)
	if err != nil {
		return &discovery.ModifySchemasResponse{
			Response: discovery.CreateResponse(discovery.ErrInternal, "Txn ModifySchemas CommitTransaction failed."),
		}, err
	}
	return &discovery.ModifySchemasResponse{
		Response: discovery.CreateResponse(discovery.ResponseSuccess, "modify schemas info success"),
	}, nil

}

func (ds *DataSource) modifySchema(ctx context.Context, serviceID string, schema *discovery.Schema) *discovery.Error {
	remoteIP := util.GetIPFromContext(ctx)
	svc, err := GetService(ctx, GeneratorServiceFilter(ctx, serviceID))
	if err != nil {
		return discovery.NewError(discovery.ErrInternal, err.Error())
	}
	if svc == nil {
		return discovery.NewError(discovery.ErrServiceNotExists, "service does not exist.")
	}
	microservice := svc.ServiceInfo
	var isExist bool
	for _, sid := range microservice.Schemas {
		if sid == schema.SchemaId {
			isExist = true
			break
		}
	}
	var newSchemas []string
	if !ds.isSchemaEditable(microservice) {
		if len(microservice.Schemas) != 0 && !isExist {
			return discovery.NewError(discovery.ErrUndefinedSchemaID, "non-existent schemaID can't be added request "+discovery.ENV_PROD)
		}
		respSchema, err := GetSchema(ctx, GeneratorSchemaFilter(ctx, serviceID, schema.SchemaId))
		if err != nil {
			return discovery.NewError(discovery.ErrUnavailableBackend, err.Error())
		}
		if schema != nil {
			if len(schema.Summary) == 0 {
				log.Error(fmt.Sprintf("modify schema %s %s failed, get schema summary failed, operator: %s",
					serviceID, schema.SchemaId, remoteIP), err)
				return discovery.NewError(discovery.ErrUnavailableBackend, err.Error())
			}
			if len(respSchema.SchemaSummary) != 0 {
				log.Error(fmt.Sprintf("mode, schema %s %s already exist, can not be changed, operator: %s",
					serviceID, schema.SchemaId, remoteIP), err)
				return discovery.NewError(discovery.ErrModifySchemaNotAllow, "schema already exist, can not be changed request "+discovery.ENV_PROD)
			}
		}
		if len(microservice.Schemas) == 0 {
			copy(newSchemas, microservice.Schemas)
			newSchemas = append(newSchemas, schema.SchemaId)
		}
	} else {
		if !isExist {
			copy(newSchemas, microservice.Schemas)
			newSchemas = append(newSchemas, schema.SchemaId)
		}
	}
	if len(newSchemas) != len(microservice.Schemas) {

		updateData := bson.M{StringBuilder([]string{ColumnServiceInfo, ColumnSchemas}): newSchemas}
		err := UpdateService(ctx, GeneratorServiceFilter(ctx, serviceID), bson.M{"$set": updateData})
		if err != nil {
			return discovery.NewError(discovery.ErrInternal, err.Error())
		}
	}
	newSchema := bson.M{"$set": bson.M{ColumnSchemaInfo: schema.Schema, ColumnSchemaSummary: schema.Summary}}
	err = UpdateSchema(ctx, GeneratorSchemaFilter(ctx, serviceID, schema.SchemaId), newSchema, options.FindOneAndUpdate().SetUpsert(true))
	if err != nil {
		return discovery.NewError(discovery.ErrInternal, err.Error())
	}
	return nil
}

func (ds *DataSource) modifySchemas(ctx context.Context, service *discovery.MicroService, schemas []*discovery.Schema) *discovery.Error {
	remoteIP := util.GetIPFromContext(ctx)
	serviceID := service.ServiceId
	schemasFromDatabase, err := GetSchemas(ctx, GeneratorServiceFilter(ctx, serviceID))
	if err != nil {
		log.Error(fmt.Sprintf("modify service %s schemas failed, get schemas failed, operator: %s", serviceID, remoteIP), err)
		return discovery.NewError(discovery.ErrUnavailableBackend, err.Error())
	}
	needUpdateSchemas, needAddSchemas, needDeleteSchemas, nonExistSchemaIds :=
		datasource.SchemasAnalysis(schemas, schemasFromDatabase, service.Schemas)
	if !ds.isSchemaEditable(service) {
		if len(service.Schemas) == 0 {
			//todo add quota check
			updateData := bson.M{StringBuilder([]string{ColumnServiceInfo, ColumnSchemas}): nonExistSchemaIds}
			err := UpdateService(ctx, GeneratorServiceFilter(ctx, serviceID), bson.M{"$set": updateData})
			if err != nil {
				log.Error(fmt.Sprintf("modify service %s schemas failed, update service.Schemas failed, operator: %s",
					serviceID, remoteIP), err)
				return discovery.NewError(discovery.ErrInternal, err.Error())
			}
		} else {
			if len(nonExistSchemaIds) != 0 {
				errInfo := fmt.Errorf("non-existent schemaIDs %v", nonExistSchemaIds)
				log.Error(fmt.Sprintf("modify service %s schemas failed, operator: %s", serviceID, remoteIP), err)
				return discovery.NewError(discovery.ErrUndefinedSchemaID, errInfo.Error())
			}
			for _, needUpdateSchema := range needUpdateSchemas {
				exist, err := SchemaExist(ctx, serviceID, needUpdateSchema.SchemaId)
				if err != nil {
					return discovery.NewError(discovery.ErrInternal, err.Error())
				}
				if !exist {
					err := UpdateSchema(ctx, GeneratorSchemaFilter(ctx, serviceID, needUpdateSchema.SchemaId), bson.M{"$set": bson.M{ColumnSchemaInfo: needUpdateSchema.Schema, ColumnSchemaSummary: needUpdateSchema.Summary}}, options.FindOneAndUpdate().SetUpsert(true))
					if err != nil {
						return discovery.NewError(discovery.ErrInternal, err.Error())
					}
				} else {
					log.Warn(fmt.Sprintf("schema[%s/%s] and it's summary already exist, skip to update, operator: %s",
						serviceID, needUpdateSchema.SchemaId, remoteIP))
				}
			}
		}

		for _, schema := range needAddSchemas {
			log.Info(fmt.Sprintf("add new schema[%s/%s], operator: %s", serviceID, schema.SchemaId, remoteIP))
			err := UpdateSchema(ctx, GeneratorSchemaFilter(ctx, serviceID, schema.SchemaId), bson.M{"$set": bson.M{ColumnSchemaInfo: schema.Schema, ColumnSchemaSummary: schema.Summary}}, options.FindOneAndUpdate().SetUpsert(true))
			if err != nil {
				return discovery.NewError(discovery.ErrInternal, err.Error())
			}
		}
	} else {

		var schemaIDs []string
		for _, schema := range needAddSchemas {
			log.Info(fmt.Sprintf("add new schema[%s/%s], operator: %s", serviceID, schema.SchemaId, remoteIP))
			err := UpdateSchema(ctx, GeneratorSchemaFilter(ctx, serviceID, schema.SchemaId), bson.M{"$set": bson.M{ColumnSchemaInfo: schema.Schema, ColumnSchemaSummary: schema.Summary}}, options.FindOneAndUpdate().SetUpsert(true))
			if err != nil {
				return discovery.NewError(discovery.ErrInternal, err.Error())
			}
			schemaIDs = append(schemaIDs, schema.SchemaId)
		}

		for _, schema := range needUpdateSchemas {
			log.Info(fmt.Sprintf("update schema[%s/%s], operator: %s", serviceID, schema.SchemaId, remoteIP))
			err := UpdateSchema(ctx, GeneratorSchemaFilter(ctx, serviceID, schema.SchemaId), bson.M{"$set": bson.M{ColumnSchemaInfo: schema.Schema, ColumnSchemaSummary: schema.Summary}}, options.FindOneAndUpdate().SetUpsert(true))
			if err != nil {
				return discovery.NewError(discovery.ErrInternal, err.Error())
			}
			schemaIDs = append(schemaIDs, schema.SchemaId)
		}

		for _, schema := range needDeleteSchemas {
			log.Info(fmt.Sprintf("delete non-existent schema[%s/%s], operator: %s", serviceID, schema.SchemaId, remoteIP))
			err = DeleteSchema(ctx, GeneratorSchemaFilter(ctx, serviceID, schema.SchemaId))
			if err != nil {
				return discovery.NewError(discovery.ErrInternal, err.Error())
			}
		}

		updateData := bson.M{StringBuilder([]string{ColumnServiceInfo, ColumnSchemas}): schemaIDs}
		err := UpdateService(ctx, GeneratorServiceFilter(ctx, serviceID), bson.M{"$set": updateData})
		if err != nil {
			log.Error(fmt.Sprintf("modify service %s schemas failed, update service.Schemas failed, operator: %s", serviceID, remoteIP), err)
			return discovery.NewError(discovery.ErrInternal, err.Error())
		}
	}
	return nil
}

func (ds *DataSource) AddRule(ctx context.Context, request *discovery.AddServiceRulesRequest) (*discovery.AddServiceRulesResponse, error) {
	exist, err := ServiceExistID(ctx, request.ServiceId)
	if err != nil {
		log.Error(fmt.Sprintf("failed to add rules for service %s for get service failed", request.ServiceId), err)
		return &discovery.AddServiceRulesResponse{
			Response: discovery.CreateResponse(discovery.ErrInternal, "Failed to check service exist"),
		}, nil
	}
	if !exist {
		return &discovery.AddServiceRulesResponse{Response: discovery.CreateResponse(discovery.ErrServiceNotExists, "Service does not exist")}, nil
	}
	//todo add quota check
	rules, err := GetRules(ctx, request.ServiceId)
	if err != nil {
		return &discovery.AddServiceRulesResponse{
			Response: discovery.CreateResponse(discovery.ErrInternal, err.Error()),
		}, err
	}
	var ruleType string
	if len(rules) != 0 {
		ruleType = rules[0].RuleType
	}
	ruleIDs := make([]string, 0, len(request.Rules))
	for _, rule := range request.Rules {
		if len(ruleType) == 0 {
			ruleType = rule.RuleType
		} else if ruleType != rule.RuleType {
			return &discovery.AddServiceRulesResponse{
				Response: discovery.CreateResponse(discovery.ErrBlackAndWhiteRule, "Service can only contain one rule type,Black or white."),
			}, nil
		}
		//the rule unique index is (serviceid,attribute,pattern)
		exist, err := RuleExist(ctx, GeneratorRuleAttFilter(ctx, request.ServiceId, rule.Attribute, rule.Pattern))
		if err != nil {
			return &discovery.AddServiceRulesResponse{
				Response: discovery.CreateResponse(discovery.ErrUnavailableBackend, "Can not check rule if exist."),
			}, nil
		}
		if exist {
			continue
		}
		timestamp := strconv.FormatInt(time.Now().Unix(), 10)
		ruleAdd := &Rule{
			Domain:    util.ParseDomain(ctx),
			Project:   util.ParseProject(ctx),
			ServiceID: request.ServiceId,
			RuleInfo: &discovery.ServiceRule{
				RuleId:       util.GenerateUUID(),
				RuleType:     rule.RuleType,
				Attribute:    rule.Attribute,
				Pattern:      rule.Pattern,
				Description:  rule.Description,
				Timestamp:    timestamp,
				ModTimestamp: timestamp,
			},
		}
		ruleIDs = append(ruleIDs, ruleAdd.RuleInfo.RuleId)
		_, err = client.GetMongoClient().Insert(ctx, CollectionRule, ruleAdd)
		if err != nil {
			return &discovery.AddServiceRulesResponse{
				Response: discovery.CreateResponse(discovery.ErrInternal, err.Error()),
			}, err
		}
	}
	return &discovery.AddServiceRulesResponse{
		Response: discovery.CreateResponse(discovery.ResponseSuccess, "Add service rules successfully."),
		RuleIds:  ruleIDs,
	}, nil
}

func (ds *DataSource) GetRules(ctx context.Context, request *discovery.GetServiceRulesRequest) (
	*discovery.GetServiceRulesResponse, error) {
	exist, err := ServiceExistID(ctx, request.ServiceId)
	if err != nil {
		return &discovery.GetServiceRulesResponse{
			Response: discovery.CreateResponse(discovery.ErrServiceNotExists, "GetRules failed for get service failed."),
		}, nil
	}
	if !exist {
		return &discovery.GetServiceRulesResponse{
			Response: discovery.CreateResponse(discovery.ErrServiceNotExists, "GetRules failed for service not exist."),
		}, nil
	}
	rules, err := GetRules(ctx, request.ServiceId)
	if err != nil {
		return &discovery.GetServiceRulesResponse{
			Response: discovery.CreateResponse(discovery.ErrInternal, err.Error()),
		}, nil
	}
	return &discovery.GetServiceRulesResponse{
		Response: discovery.CreateResponse(discovery.ResponseSuccess, "Get service rules successfully."),
		Rules:    rules,
	}, nil
}

func (ds *DataSource) DeleteRule(ctx context.Context, request *discovery.DeleteServiceRulesRequest) (
	*discovery.DeleteServiceRulesResponse, error) {
	exist, err := ServiceExistID(ctx, request.ServiceId)
	if err != nil {
		log.Error(fmt.Sprintf("failed to add tags for service %s for get service failed", request.ServiceId), err)
		return &discovery.DeleteServiceRulesResponse{
			Response: discovery.CreateResponse(discovery.ErrInternal, "Failed to check service exist"),
		}, err
	}
	if !exist {
		return &discovery.DeleteServiceRulesResponse{Response: discovery.CreateResponse(discovery.ErrServiceNotExists, "Service not exist")}, nil
	}
	for _, ruleID := range request.RuleIds {
		exist, err := RuleExist(ctx, GeneratorRuleFilter(ctx, request.ServiceId, ruleID))
		if err != nil {
			return &discovery.DeleteServiceRulesResponse{
				Response: discovery.CreateResponse(discovery.ErrInternal, err.Error()),
			}, nil
		}
		if !exist {
			return &discovery.DeleteServiceRulesResponse{
				Response: discovery.CreateResponse(discovery.ErrRuleNotExists, "This rule does not exist."),
			}, nil
		}
	}

	return &discovery.DeleteServiceRulesResponse{
		Response: discovery.CreateResponse(discovery.ResponseSuccess, "Delete service rules successfully."),
	}, nil
}

func (ds *DataSource) UpdateRule(ctx context.Context, request *discovery.UpdateServiceRuleRequest) (
	*discovery.UpdateServiceRuleResponse, error) {
	exist, err := ServiceExistID(ctx, request.ServiceId)
	if err != nil {
		return &discovery.UpdateServiceRuleResponse{
			Response: discovery.CreateResponse(discovery.ErrServiceNotExists, "UpdateRule failed for get service failed."),
		}, nil
	}
	if !exist {
		return &discovery.UpdateServiceRuleResponse{
			Response: discovery.CreateResponse(discovery.ErrServiceNotExists, "UpdateRule failed for service not exist."),
		}, nil
	}
	rules, err := GetRules(ctx, request.ServiceId)
	if err != nil {
		return &discovery.UpdateServiceRuleResponse{
			Response: discovery.CreateResponse(discovery.ErrUnavailableBackend, "UpdateRule failed for get rule."),
		}, nil
	}
	if len(rules) >= 1 && rules[0].RuleType != request.Rule.RuleType {
		return &discovery.UpdateServiceRuleResponse{
			Response: discovery.CreateResponse(discovery.ErrModifyRuleNotAllow, "Exist multiple rules, can not change rule type. Rule type is ."+rules[0].RuleType),
		}, nil
	}
	exist, err = RuleExist(ctx, GeneratorRuleFilter(ctx, request.ServiceId, request.RuleId))
	if err != nil {
		return &discovery.UpdateServiceRuleResponse{
			Response: discovery.CreateResponse(discovery.ErrInternal, err.Error()),
		}, nil
	}
	if !exist {
		return &discovery.UpdateServiceRuleResponse{
			Response: discovery.CreateResponse(discovery.ErrRuleNotExists, "This rule does not exist."),
		}, nil
	}

	newRule := bson.M{
		StringBuilder([]string{ColumnRuleInfo, ColumnRuleType}):    request.Rule.RuleType,
		StringBuilder([]string{ColumnRuleInfo, ColumnPattern}):     request.Rule.Pattern,
		StringBuilder([]string{ColumnRuleInfo, ColumnAttribute}):   request.Rule.Attribute,
		StringBuilder([]string{ColumnRuleInfo, ColumnDescription}): request.Rule.Description,
		StringBuilder([]string{ColumnRuleInfo, ColumnModTime}):     strconv.FormatInt(time.Now().Unix(), 10)}

	err = UpdateRule(ctx, GeneratorRuleFilter(ctx, request.ServiceId, request.RuleId), bson.M{"$set": newRule})
	if err != nil {
		return &discovery.UpdateServiceRuleResponse{
			Response: discovery.CreateResponse(discovery.ErrInternal, err.Error()),
		}, err
	}
	return &discovery.UpdateServiceRuleResponse{
		Response: discovery.CreateResponse(discovery.ResponseSuccess, "Update service rules succesfully."),
	}, nil
}

func (ds *DataSource) isSchemaEditable(service *discovery.MicroService) bool {
	return (len(service.Environment) != 0 && service.Environment != discovery.ENV_PROD) || ds.SchemaEditable
}

func ServiceExist(ctx context.Context, service *discovery.MicroServiceKey) (bool, error) {
	filter := GeneratorServiceNameFilter(ctx, service)
	return client.GetMongoClient().DocExist(ctx, CollectionService, filter)
}

func ServiceExistID(ctx context.Context, serviceID string) (bool, error) {
	filter := GeneratorServiceFilter(ctx, serviceID)
	return client.GetMongoClient().DocExist(ctx, CollectionService, filter)
}

func GetService(ctx context.Context, filter bson.M) (*Service, error) {
	findRes, err := client.GetMongoClient().FindOne(ctx, CollectionService, filter)
	if err != nil {
		return nil, err
	}
	var svc *Service
	if findRes.Err() != nil {
		//not get any service,not db err
		return nil, nil
	}
	err = findRes.Decode(&svc)
	if err != nil {
		return nil, err
	}
	return svc, nil
}

func GetServices(ctx context.Context, filter bson.M) ([]*discovery.MicroService, error) {
	res, err := client.GetMongoClient().Find(ctx, CollectionService, filter)
	if err != nil {
		return nil, err
	}
	var services []*discovery.MicroService
	for res.Next(ctx) {
		var tmp Service
		err := res.Decode(&tmp)
		if err != nil {
			return nil, err
		}
		services = append(services, tmp.ServiceInfo)
	}
	return services, nil
}

func GetMongoServices(ctx context.Context, filter bson.M) ([]*Service, error) {
	res, err := client.GetMongoClient().Find(ctx, CollectionService, filter)
	if err != nil {
		return nil, err
	}
	var services []*Service
	for res.Next(ctx) {
		var tmp *Service
		err := res.Decode(&tmp)
		if err != nil {
			return nil, err
		}
		services = append(services, tmp)
	}
	return services, nil
}

func GetServicesVersions(ctx context.Context, filter interface{}) ([]string, error) {
	res, err := client.GetMongoClient().Find(ctx, CollectionService, filter)
	if err != nil {
		return nil, nil
	}
	var versions []string
	for res.Next(ctx) {
		var tmp string
		err := res.Decode(&tmp)
		if err != nil {
			return nil, err
		}
		versions = append(versions, tmp)
	}
	return versions, nil
}

func getServiceDetailUtil(ctx context.Context, mgs *Service, countOnly bool, options []string) (*discovery.ServiceDetail, error) {
	serviceDetail := new(discovery.ServiceDetail)
	if countOnly {
		serviceDetail.Statics = new(discovery.Statistics)
	}
	for _, opt := range options {
		expr := opt
		switch expr {
		case "tags":
			serviceDetail.Tags = mgs.Tags
		case "rules":
			rules, err := GetRules(ctx, mgs.ServiceInfo.ServiceId)
			if err != nil {
				log.Error(fmt.Sprintf("get service %s's all rules failed", mgs.ServiceInfo.ServiceId), err)
				return nil, err
			}
			for _, rule := range rules {
				rule.Timestamp = rule.ModTimestamp
			}
			serviceDetail.Rules = rules
		case "instances":
			//todo wait instance interface
		case "schemas":
			schemas, err := GetSchemas(ctx, GeneratorServiceFilter(ctx, mgs.ServiceInfo.ServiceId))
			if err != nil {
				log.Error(fmt.Sprintf("get service %s's all schemas failed", mgs.ServiceInfo.ServiceId), err)
				return nil, err
			}
			serviceDetail.SchemaInfos = schemas
		case "dependencies":
			//todo wait dependencied interface
		case "":
			continue
		default:
			log.Info(fmt.Sprintf("request option %s is invalid", opt))
		}
	}
	return serviceDetail, nil
}

func UpdateService(ctx context.Context, filter interface{}, m bson.M) error {
	return client.GetMongoClient().DocUpdate(ctx, CollectionService, filter, m)
}

func GetRules(ctx context.Context, serviceID string) ([]*discovery.ServiceRule, error) {
	domain := util.ParseDomain(ctx)
	project := util.ParseProject(ctx)
	filter := bson.M{ColumnDomain: domain, ColumnProject: project, ColumnServiceID: serviceID}

	ruleRes, err := client.GetMongoClient().Find(ctx, CollectionRule, filter)
	if err != nil {
		return nil, err
	}
	var rules []*discovery.ServiceRule
	for ruleRes.Next(ctx) {
		var tmpRule *Rule
		err := ruleRes.Decode(&tmpRule)
		if err != nil {
			return nil, err
		}
		rules = append(rules, tmpRule.RuleInfo)
	}
	return rules, nil
}

func UpdateRule(ctx context.Context, filter interface{}, m bson.M) error {
	return client.GetMongoClient().DocUpdate(ctx, CollectionRule, filter, m)
}

func UpdateSchema(ctx context.Context, filter interface{}, m bson.M, opts ...*options.FindOneAndUpdateOptions) error {
	return client.GetMongoClient().DocUpdate(ctx, CollectionSchema, filter, m, opts...)
}

func DeleteSchema(ctx context.Context, filter interface{}) error {
	res, err := client.GetMongoClient().DocDelete(ctx, CollectionSchema, filter)
	if err != nil {
		return err
	}
	if !res {
		return ErrDeleteSchemaFailed
	}
	return nil
}

func RuleExist(ctx context.Context, filter bson.M) (bool, error) {
	return client.GetMongoClient().DocExist(ctx, CollectionRule, filter)
}

func GeneratorServiceFilter(ctx context.Context, serviceID string) bson.M {
	domain := util.ParseDomain(ctx)
	project := util.ParseProject(ctx)

	return bson.M{
		ColumnDomain:  domain,
		ColumnProject: project,
		StringBuilder([]string{ColumnServiceInfo, ColumnServiceID}): serviceID}
}

func GeneratorServiceNameFilter(ctx context.Context, service *discovery.MicroServiceKey) bson.M {
	domain := util.ParseDomain(ctx)
	project := util.ParseProject(ctx)

	return bson.M{
		ColumnDomain:  domain,
		ColumnProject: project,
		StringBuilder([]string{ColumnServiceInfo, ColumnEnv}):         service.Environment,
		StringBuilder([]string{ColumnServiceInfo, ColumnAppID}):       service.AppId,
		StringBuilder([]string{ColumnServiceInfo, ColumnServiceName}): service.ServiceName,
		StringBuilder([]string{ColumnServiceInfo, ColumnVersion}):     service.Version}
}

func GeneratorServiceAliasFilter(ctx context.Context, service *discovery.MicroServiceKey) bson.M {
	domain := util.ParseDomain(ctx)
	project := util.ParseProject(ctx)

	return bson.M{
		ColumnDomain:  domain,
		ColumnProject: project,
		StringBuilder([]string{ColumnServiceInfo, ColumnEnv}):     service.Environment,
		StringBuilder([]string{ColumnServiceInfo, ColumnAppID}):   service.AppId,
		StringBuilder([]string{ColumnServiceInfo, ColumnAlias}):   service.Alias,
		StringBuilder([]string{ColumnServiceInfo, ColumnVersion}): service.Version}
}

func GeneratorRuleAttFilter(ctx context.Context, serviceID, attribute, pattern string) bson.M {
	return bson.M{
		ColumnServiceID: serviceID,
		StringBuilder([]string{ColumnRuleInfo, ColumnAttribute}): attribute,
		StringBuilder([]string{ColumnRuleInfo, ColumnPattern}):   pattern}
}

func GeneratorSchemaFilter(ctx context.Context, serviceID, schemaID string) bson.M {
	domain := util.ParseDomain(ctx)
	project := util.ParseProject(ctx)

	return bson.M{ColumnDomain: domain, ColumnProject: project, ColumnServiceID: serviceID, ColumnSchemaID: schemaID}
}

func GeneratorRuleFilter(ctx context.Context, serviceID, ruleID string) bson.M {
	domain := util.ParseDomain(ctx)
	project := util.ParseProject(ctx)

	return bson.M{
		ColumnDomain:    domain,
		ColumnProject:   project,
		ColumnServiceID: serviceID,
		StringBuilder([]string{ColumnRuleInfo, ColumnRuleID}): ruleID}
}

func GetSchemas(ctx context.Context, filter bson.M) ([]*discovery.Schema, error) {
	getRes, err := client.GetMongoClient().Find(ctx, CollectionSchema, filter)
	if err != nil {
		return nil, err
	}
	var schemas []*discovery.Schema
	for getRes.Next(ctx) {
		var tmp *Schema
		err = getRes.Decode(&tmp)
		if err != nil {
			return nil, err
		}
		schemas = append(schemas, &discovery.Schema{
			SchemaId: tmp.SchemaID,
			Summary:  tmp.SchemaSummary,
			Schema:   tmp.SchemaInfo,
		})
	}
	return schemas, nil
}

func GetSchema(ctx context.Context, filter bson.M) (*Schema, error) {
	findRes, err := client.GetMongoClient().FindOne(ctx, CollectionSchema, filter)
	if err != nil {
		return nil, err
	}
	var schema *Schema
	err = findRes.Decode(&schema)
	if err != nil {
		return nil, err
	}
	return schema, nil
}

func SchemaExist(ctx context.Context, serviceID, schemaID string) (bool, error) {
	num, err := client.GetMongoClient().Count(ctx, CollectionSchema, GeneratorSchemaFilter(ctx, serviceID, schemaID))
	if err != nil {
		return false, err
	}
	return num != 0, nil
}

// Instance management
func (ds *DataSource) RegisterInstance(ctx context.Context, request *discovery.RegisterInstanceRequest) (*discovery.RegisterInstanceResponse, error) {
	remoteIP := util.GetIPFromContext(ctx)
	instance := request.Instance

	// 允许自定义 id
	if len(instance.InstanceId) > 0 {
		resp, err := ds.Heartbeat(ctx, &discovery.HeartbeatRequest{
			InstanceId: instance.InstanceId,
			ServiceId:  instance.ServiceId,
		})
		if err != nil || resp == nil {
			log.Error(fmt.Sprintf("register service %s's instance failed, endpoints %s, host '%s', operator %s",
				instance.ServiceId, instance.Endpoints, instance.HostName, remoteIP), err)
			return &discovery.RegisterInstanceResponse{
				Response: discovery.CreateResponse(discovery.ErrInternal, err.Error()),
			}, nil
		}
		switch resp.Response.GetCode() {
		case discovery.ResponseSuccess:
			log.Info(fmt.Sprintf("register instance successful, reuse instance[%s/%s], operator %s",
				instance.ServiceId, instance.InstanceId, remoteIP))
			return &discovery.RegisterInstanceResponse{
				Response:   resp.Response,
				InstanceId: instance.InstanceId,
			}, nil
		case discovery.ErrInstanceNotExists:
			// register a new one
			return registryInstance(ctx, request)
		default:
			log.Error(fmt.Sprintf("register instance failed, reuse instance %s %s, operator %s",
				instance.ServiceId, instance.InstanceId, remoteIP), err)
			return &discovery.RegisterInstanceResponse{
				Response: resp.Response,
			}, err
		}
	}

	if err := preProcessRegisterInstance(ctx, instance); err != nil {
		log.Error(fmt.Sprintf("register service %s instance failed, endpoints %s, host %s operator %s",
			instance.ServiceId, instance.Endpoints, instance.HostName, remoteIP), err)
		return &discovery.RegisterInstanceResponse{
			Response: discovery.CreateResponseWithSCErr(err),
		}, nil
	}
	return registryInstance(ctx, request)
}

// GetInstances returns instances under the current domain
func (ds *DataSource) GetInstance(ctx context.Context, request *discovery.GetOneInstanceRequest) (*discovery.GetOneInstanceResponse, error) {
	service := &Service{}
	var err error
	var serviceIDs []string
	if len(request.ConsumerServiceId) > 0 {
		filter := GeneratorServiceFilter(ctx, request.ConsumerServiceId)
		service, err = GetService(ctx, filter)
		if err != nil {
			log.Error(fmt.Sprintf(" get consumer failed, consumer %s find provider instance %s",
				request.ConsumerServiceId, request.ProviderInstanceId), err)
			return &discovery.GetOneInstanceResponse{
				Response: discovery.CreateResponse(discovery.ErrInternal, err.Error()),
			}, err
		}
		if service == nil {
			log.Error(fmt.Sprintf("consumer does not exist, consumer %s find provider instance %s %s",
				request.ConsumerServiceId, request.ProviderServiceId, request.ProviderInstanceId), err)
			return &discovery.GetOneInstanceResponse{
				Response: discovery.CreateResponse(discovery.ErrServiceNotExists,
					fmt.Sprintf("Consumer[%s] does not exist.", request.ConsumerServiceId)),
			}, nil
		}
	}

	filter := GeneratorServiceFilter(ctx, request.ProviderServiceId)
	provider, err := GetService(ctx, filter)
	if err != nil {
		log.Error(fmt.Sprintf("get provider failed, consumer %s find provider instance %s %s",
			request.ConsumerServiceId, request.ProviderServiceId, request.ProviderInstanceId), err)
		return &discovery.GetOneInstanceResponse{
			Response: discovery.CreateResponse(discovery.ErrInternal, err.Error()),
		}, err
	}
	if provider == nil {
		log.Error(fmt.Sprintf("provider does not exist, consumer %s find provider instance %s %s",
			request.ConsumerServiceId, request.ProviderServiceId, request.ProviderInstanceId), err)
		return &discovery.GetOneInstanceResponse{
			Response: discovery.CreateResponse(discovery.ErrServiceNotExists,
				fmt.Sprintf("Provider[%s] does not exist.", request.ProviderServiceId)),
		}, nil
	}
	findFlag := fmt.Sprintf("consumer[%s][%s/%s/%s/%s] find provider[%s][%s/%s/%s/%s] instance[%s]",
		request.ConsumerServiceId, service.ServiceInfo.Environment, service.ServiceInfo.AppId, service.ServiceInfo.ServiceName, service.ServiceInfo.Version,
		provider.ServiceInfo.ServiceId, provider.ServiceInfo.Environment, provider.ServiceInfo.AppId, provider.ServiceInfo.ServiceName, provider.ServiceInfo.Version,
		request.ProviderInstanceId)

	domainProject := util.ParseDomainProject(ctx)
	services, err := findServices(ctx, discovery.MicroServiceToKey(domainProject, provider.ServiceInfo))
	if err != nil {
		log.Error(fmt.Sprintf("get instance failed %s", findFlag), err)
		return &discovery.GetOneInstanceResponse{
			Response: discovery.CreateResponse(discovery.ErrInternal, err.Error()),
		}, err
	}
	if services != nil {
		serviceIDs = filterServiceIDs(ctx, request.ConsumerServiceId, request.Tags, services)
	}
	if services == nil || len(serviceIDs) == 0 {
		mes := fmt.Errorf("%s failed, provider does not exist", findFlag)
		log.Error("get instance failed", mes)
		return &discovery.GetOneInstanceResponse{
			Response: discovery.CreateResponse(discovery.ErrServiceNotExists, mes.Error()),
		}, nil
	}
	instances, err := instancesFilter(ctx, serviceIDs)
	if len(instances) == 0 {
		mes := fmt.Errorf("%s failed, provider instance does not exist", findFlag)
		log.Error("get instance failed", err)
		return &discovery.GetOneInstanceResponse{
			Response: discovery.CreateResponse(discovery.ErrInstanceNotExists, mes.Error()),
		}, nil
	}
	return &discovery.GetOneInstanceResponse{
		Response: discovery.CreateResponse(discovery.ResponseSuccess, "Get instance successfully."),
		Instance: instances[0],
	}, nil
}

func (ds *DataSource) GetInstances(ctx context.Context, request *discovery.GetInstancesRequest) (*discovery.GetInstancesResponse, error) {
	domainProject := util.ParseDomainProject(ctx)
	service := &Service{}
	var err error
	var serviceIDs []string

	if len(request.ConsumerServiceId) > 0 {
		filter := GeneratorServiceFilter(ctx, request.ConsumerServiceId)
		service, err = GetService(ctx, filter)
		if err != nil {
			log.Error(fmt.Sprintf("get consumer failed, consumer %s find provider %s instances",
				request.ConsumerServiceId, request.ProviderServiceId), err)
			return &discovery.GetInstancesResponse{
				Response: discovery.CreateResponse(discovery.ErrInternal, err.Error()),
			}, err
		}
		if service == nil {
			log.Error(fmt.Sprintf("consumer does not exist, consumer %s find provider %s instances",
				request.ConsumerServiceId, request.ProviderServiceId), err)
			return &discovery.GetInstancesResponse{
				Response: discovery.CreateResponse(discovery.ErrServiceNotExists,
					fmt.Sprintf("Consumer[%s] does not exist.", request.ConsumerServiceId)),
			}, nil
		}
	}

	filter := GeneratorServiceFilter(ctx, request.ProviderServiceId)
	provider, err := GetService(ctx, filter)
	if err != nil {
		log.Error(fmt.Sprintf("get provider failed, consumer %s find provider instances %s",
			request.ConsumerServiceId, request.ProviderServiceId), err)
		return &discovery.GetInstancesResponse{
			Response: discovery.CreateResponse(discovery.ErrInternal, err.Error()),
		}, err
	}
	if provider == nil {
		log.Error(fmt.Sprintf("provider does not exist, consumer %s find provider %s  instances",
			request.ConsumerServiceId, request.ProviderServiceId), err)
		return &discovery.GetInstancesResponse{
			Response: discovery.CreateResponse(discovery.ErrServiceNotExists,
				fmt.Sprintf("provider[%s] does not exist.", request.ProviderServiceId)),
		}, nil
	}

	findFlag := fmt.Sprintf("consumer[%s][%s/%s/%s/%s] find provider[%s][%s/%s/%s/%s] instances",
		request.ConsumerServiceId, service.ServiceInfo.Environment, service.ServiceInfo.AppId, service.ServiceInfo.ServiceName, service.ServiceInfo.Version,
		provider.ServiceInfo.ServiceId, provider.ServiceInfo.Environment, provider.ServiceInfo.AppId, provider.ServiceInfo.ServiceName, provider.ServiceInfo.Version)

	services, err := findServices(ctx, discovery.MicroServiceToKey(domainProject, provider.ServiceInfo))
	if err != nil {
		log.Error(fmt.Sprintf("get instances failed %s", findFlag), err)
		return &discovery.GetInstancesResponse{
			Response: discovery.CreateResponse(discovery.ErrInternal, err.Error()),
		}, err
	}
	if services != nil {
		serviceIDs = filterServiceIDs(ctx, request.ConsumerServiceId, request.Tags, services)
	}
	if services == nil || len(serviceIDs) == 0 {
		mes := fmt.Errorf("%s failed, provider does not exist", findFlag)
		log.Error("get instances failed", mes)
		return &discovery.GetInstancesResponse{
			Response: discovery.CreateResponse(discovery.ErrServiceNotExists, mes.Error()),
		}, nil
	}
	instances, err := instancesFilter(ctx, serviceIDs)
	if err != nil {
		log.Error(fmt.Sprintf("get instances failed %s", findFlag), err)
		return &discovery.GetInstancesResponse{
			Response: discovery.CreateResponse(discovery.ErrInternal, err.Error()),
		}, err
	}
	return &discovery.GetInstancesResponse{
		Response:  discovery.CreateResponse(discovery.ResponseSuccess, "Query service instances successfully."),
		Instances: instances,
	}, nil
}

// GetProviderInstances returns instances under the specified domain
func (ds *DataSource) GetProviderInstances(ctx context.Context, request *discovery.GetProviderInstancesRequest) (instances []*discovery.MicroServiceInstance, rev string, err error) {
	domain := util.ParseDomain(ctx)
	project := util.ParseProject(ctx)
	filter := bson.M{
		ColumnDomain:  domain,
		ColumnProject: project,
		StringBuilder([]string{ColumnInstanceInfo, ColumnServiceID}): request.ProviderServiceId}

	findRes, err := client.GetMongoClient().Find(ctx, CollectionInstance, filter)
	if err != nil {
		return
	}

	for findRes.Next(ctx) {
		var mongoInstance Instance
		err := findRes.Decode(&mongoInstance)
		if err == nil {
			instances = append(instances, mongoInstance.InstanceInfo)
		}
	}

	return instances, "", nil
}

func (ds *DataSource) GetAllInstances(ctx context.Context, request *discovery.GetAllInstancesRequest) (*discovery.GetAllInstancesResponse, error) {

	domain := util.ParseDomain(ctx)
	project := util.ParseProject(ctx)

	filter := bson.M{ColumnDomain: domain, ColumnProject: project}

	findRes, err := client.GetMongoClient().Find(ctx, CollectionInstance, filter)
	if err != nil {
		return nil, err
	}
	resp := &discovery.GetAllInstancesResponse{
		Response: discovery.CreateResponse(discovery.ResponseSuccess, "Get all instances successfully"),
	}

	for findRes.Next(ctx) {
		var instance Instance
		err := findRes.Decode(&instance)
		if err != nil {
			return &discovery.GetAllInstancesResponse{
				Response: discovery.CreateResponse(discovery.ErrInternal, err.Error()),
			}, err
		}
		resp.Instances = append(resp.Instances, instance.InstanceInfo)
	}

	return resp, nil
}

func (ds *DataSource) BatchGetProviderInstances(ctx context.Context, request *discovery.BatchGetInstancesRequest) (instances []*discovery.MicroServiceInstance, rev string, err error) {
	if request == nil || len(request.ServiceIds) == 0 {
		return nil, "", ErrInvalidParamBatchGetInstancesRequest
	}

	domain := util.ParseDomain(ctx)
	project := util.ParseProject(ctx)

	for _, providerServiceID := range request.ServiceIds {
		filter := bson.M{
			ColumnDomain:  domain,
			ColumnProject: project,
			StringBuilder([]string{ColumnInstanceInfo, ColumnServiceID}): providerServiceID}
		findRes, err := client.GetMongoClient().Find(ctx, CollectionInstance, filter)
		if err != nil {
			return instances, "", nil
		}

		for findRes.Next(ctx) {
			var mongoInstance Instance
			err := findRes.Decode(&mongoInstance)
			if err == nil {
				instances = append(instances, mongoInstance.InstanceInfo)
			}
		}
	}

	return instances, "", nil
}

// FindInstances returns instances under the specified domain
func (ds *DataSource) FindInstances(ctx context.Context, request *discovery.FindInstancesRequest) (*discovery.FindInstancesResponse, error) {
	provider := &discovery.MicroServiceKey{
		Tenant:      util.ParseTargetDomainProject(ctx),
		Environment: request.Environment,
		AppId:       request.AppId,
		ServiceName: request.ServiceName,
		Alias:       request.ServiceName,
		Version:     request.VersionRule,
	}

	if apt.IsGlobal(provider) {
		return ds.findSharedServiceInstance(ctx, request, provider)
	}

	return ds.findInstance(ctx, request, provider)
}

func (ds *DataSource) UpdateInstanceStatus(ctx context.Context, request *discovery.UpdateInstanceStatusRequest) (*discovery.UpdateInstanceStatusResponse, error) {
	updateStatusFlag := util.StringJoin([]string{request.ServiceId, request.InstanceId, request.Status}, "/")

	// todo finish get instance
	instance, err := GetInstance(ctx, request.ServiceId, request.InstanceId)
	if err != nil {
		log.Error(fmt.Sprintf("update instance %s status failed", updateStatusFlag), err)
		return &discovery.UpdateInstanceStatusResponse{
			Response: discovery.CreateResponse(discovery.ErrInternal, err.Error()),
		}, err
	}
	if instance == nil {
		log.Error(fmt.Sprintf("update instance %s status failed, instance does not exist", updateStatusFlag), err)
		return &discovery.UpdateInstanceStatusResponse{
			Response: discovery.CreateResponse(discovery.ErrInstanceNotExists, "Service instance does not exist."),
		}, nil
	}

	copyInstanceRef := *instance
	copyInstanceRef.InstanceInfo.Status = request.Status

	if err := UpdateInstanceS(ctx, copyInstanceRef.InstanceInfo); err != nil {
		log.Error(fmt.Sprintf("update instance %s status failed", updateStatusFlag), err)
		resp := &discovery.UpdateInstanceStatusResponse{
			Response: discovery.CreateResponseWithSCErr(err),
		}
		if err.InternalError() {
			return resp, err
		}
		return resp, nil
	}

	log.Info(fmt.Sprintf("update instance[%s] status successfully", updateStatusFlag))
	return &discovery.UpdateInstanceStatusResponse{
		Response: discovery.CreateResponse(discovery.ResponseSuccess, "Update service instance status successfully."),
	}, nil
}

func (ds *DataSource) UpdateInstanceProperties(ctx context.Context, request *discovery.UpdateInstancePropsRequest) (*discovery.UpdateInstancePropsResponse, error) {
	instanceFlag := util.StringJoin([]string{request.ServiceId, request.InstanceId}, "/")

	instance, err := GetInstance(ctx, request.ServiceId, request.InstanceId)
	if err != nil {
		log.Error(fmt.Sprintf("update instance %s properties failed", instanceFlag), err)
		return &discovery.UpdateInstancePropsResponse{
			Response: discovery.CreateResponse(discovery.ErrInternal, err.Error()),
		}, err
	}
	if instance == nil {
		log.Error(fmt.Sprintf("update instance %s properties failed, instance does not exist", instanceFlag), err)
		return &discovery.UpdateInstancePropsResponse{
			Response: discovery.CreateResponse(discovery.ErrInstanceNotExists, "Service instance does not exist."),
		}, nil
	}

	copyInstanceRef := *instance
	copyInstanceRef.InstanceInfo.Properties = request.Properties

	// todo finish update instance
	if err := UpdateInstanceP(ctx, copyInstanceRef.InstanceInfo); err != nil {
		log.Error(fmt.Sprintf("update instance %s properties failed", instanceFlag), err)
		resp := &discovery.UpdateInstancePropsResponse{
			Response: discovery.CreateResponseWithSCErr(err),
		}
		if err.InternalError() {
			return resp, err
		}
		return resp, nil
	}

	log.Info(fmt.Sprintf("update instance[%s] properties successfully", instanceFlag))
	return &discovery.UpdateInstancePropsResponse{
		Response: discovery.CreateResponse(discovery.ResponseSuccess, "Update service instance properties successfully."),
	}, nil
}

func (ds *DataSource) UnregisterInstance(ctx context.Context, request *discovery.UnregisterInstanceRequest) (*discovery.UnregisterInstanceResponse, error) {
	remoteIP := util.GetIPFromContext(ctx)
	serviceID := request.ServiceId
	instanceID := request.InstanceId

	instanceFlag := util.StringJoin([]string{serviceID, instanceID}, "/")

	domain := util.ParseDomain(ctx)
	project := util.ParseProject(ctx)
	filter := bson.M{
		ColumnDomain:  domain,
		ColumnProject: project,
		StringBuilder([]string{ColumnInstanceInfo, ColumnServiceID}):  serviceID,
		StringBuilder([]string{ColumnInstanceInfo, ColumnInstanceID}): instanceID}
	result, err := client.GetMongoClient().Delete(ctx, CollectionInstance, filter)
	if err != nil || result.DeletedCount == 0 {
		log.Error(fmt.Sprintf("unregister instance failed, instance %s, operator %s revoke instance failed", instanceFlag, remoteIP), err)
		return &discovery.UnregisterInstanceResponse{
			Response: discovery.CreateResponse(discovery.ErrInternal, "delete instance failed"),
		}, err
	}

	log.Info(fmt.Sprintf("unregister instance[%s], operator %s", instanceFlag, remoteIP))
	return &discovery.UnregisterInstanceResponse{
		Response: discovery.CreateResponse(discovery.ResponseSuccess, "Unregister service instance successfully."),
	}, nil
}

func (ds *DataSource) Heartbeat(ctx context.Context, request *discovery.HeartbeatRequest) (*discovery.HeartbeatResponse, error) {
	remoteIP := util.GetIPFromContext(ctx)
	instanceFlag := util.StringJoin([]string{request.ServiceId, request.InstanceId}, "/")
	err := KeepAliveLease(ctx, request)
	if err != nil {
		log.Error(fmt.Sprintf("heartbeat failed, instance %s operator %s", instanceFlag, remoteIP), err)
		resp := &discovery.HeartbeatResponse{
			Response: discovery.CreateResponseWithSCErr(err),
		}
		if err.InternalError() {
			return resp, err
		}
		return resp, nil
	}
	return &discovery.HeartbeatResponse{
		Response: discovery.CreateResponse(discovery.ResponseSuccess,
			"Update service instance heartbeat successfully."),
	}, nil
}

func (ds *DataSource) HeartbeatSet(ctx context.Context, request *discovery.HeartbeatSetRequest) (*discovery.HeartbeatSetResponse, error) {
	domainProject := util.ParseDomainProject(ctx)

	heartBeatCount := len(request.Instances)
	existFlag := make(map[string]bool, heartBeatCount)
	instancesHbRst := make(chan *discovery.InstanceHbRst, heartBeatCount)
	noMultiCounter := 0

	for _, heartbeatElement := range request.Instances {
		if _, ok := existFlag[heartbeatElement.ServiceId+heartbeatElement.InstanceId]; ok {
			log.Warn(fmt.Sprintf("instance[%s/%s] is duplicate request heartbeat set",
				heartbeatElement.ServiceId, heartbeatElement.InstanceId))
			continue
		} else {
			existFlag[heartbeatElement.ServiceId+heartbeatElement.InstanceId] = true
			noMultiCounter++
		}
		gopool.Go(getHeartbeatFunc(ctx, domainProject, instancesHbRst, heartbeatElement))
	}

	count := 0
	successFlag := false
	failFlag := false
	instanceHbRstArr := make([]*discovery.InstanceHbRst, 0, heartBeatCount)

	for hbRst := range instancesHbRst {
		count++
		if len(hbRst.ErrMessage) != 0 {
			failFlag = true
		} else {
			successFlag = true
		}
		instanceHbRstArr = append(instanceHbRstArr, hbRst)
		if count == noMultiCounter {
			close(instancesHbRst)
		}
	}

	if !failFlag && successFlag {
		log.Info(fmt.Sprintf("batch update heartbeats[%d] successfully", count))
		return &discovery.HeartbeatSetResponse{
			Response:  discovery.CreateResponse(discovery.ResponseSuccess, "Heartbeat set successfully."),
			Instances: instanceHbRstArr,
		}, nil
	}

	log.Info(fmt.Sprintf("batch update heartbeats failed %v", request.Instances))
	return &discovery.HeartbeatSetResponse{
		Response:  discovery.CreateResponse(discovery.ErrInstanceNotExists, "Heartbeat set failed."),
		Instances: instanceHbRstArr,
	}, nil
}

func (ds *DataSource) BatchFind(ctx context.Context, request *discovery.BatchFindInstancesRequest) (*discovery.BatchFindInstancesResponse, error) {
	response := &discovery.BatchFindInstancesResponse{
		Response: discovery.CreateResponse(discovery.ResponseSuccess, "Batch query service instances successfully."),
	}

	var err error

	response.Services, err = ds.batchFindServices(ctx, request)
	if err != nil {
		return &discovery.BatchFindInstancesResponse{
			Response: discovery.CreateResponse(discovery.ErrInternal, err.Error()),
		}, err
	}

	response.Instances, err = ds.batchFindInstances(ctx, request)
	if err != nil {
		return &discovery.BatchFindInstancesResponse{
			Response: discovery.CreateResponse(discovery.ErrInternal, err.Error()),
		}, err
	}

	return response, nil
}

func registryInstance(ctx context.Context, request *discovery.RegisterInstanceRequest) (*discovery.RegisterInstanceResponse, error) {
	domain := util.ParseDomain(ctx)
	project := util.ParseProject(ctx)
	remoteIP := util.GetIPFromContext(ctx)
	instance := request.Instance
	instanceID := instance.InstanceId
	data := &Instance{
		Domain:       domain,
		Project:      project,
		RefreshTime:  time.Now(),
		InstanceInfo: instance,
	}

	instanceFlag := fmt.Sprintf("endpoints %v, host '%s', serviceID %s",
		instance.Endpoints, instance.HostName, instance.ServiceId)

	insertRes, err := client.GetMongoClient().Insert(ctx, CollectionInstance, data)
	if err != nil {
		log.Error(fmt.Sprintf("register instance failed %s instanceID %s operator %s", instanceFlag, instanceID, remoteIP), err)
		return &discovery.RegisterInstanceResponse{
			Response: discovery.CreateResponse(discovery.ErrUnavailableBackend, err.Error()),
		}, err
	}

	log.Info(fmt.Sprintf("register instance %s, instanceID %s, operator %s",
		instanceFlag, insertRes.InsertedID, remoteIP))
	return &discovery.RegisterInstanceResponse{
		Response:   discovery.CreateResponse(discovery.ResponseSuccess, "Register service instance successfully."),
		InstanceId: instanceID,
	}, nil
}

func (ds *DataSource) findSharedServiceInstance(ctx context.Context, request *discovery.FindInstancesRequest, provider *discovery.MicroServiceKey) (*discovery.FindInstancesResponse, error) {
	var err error
	// it means the shared micro-services must be the same env with SC.
	provider.Environment = apt.Service.Environment
	findFlag := fmt.Sprintf("find shared provider[%s/%s/%s/%s]", provider.Environment, provider.AppId, provider.ServiceName, provider.Version)
	services, err := findServices(ctx, provider)
	if err != nil {
		log.Error(fmt.Sprintf("find shared service instance failed %s", findFlag), err)
		return &discovery.FindInstancesResponse{
			Response: discovery.CreateResponse(discovery.ErrInternal, err.Error()),
		}, err
	}
	if services == nil {
		mes := fmt.Errorf("%s failed, provider does not exist", findFlag)
		log.Error("find shared service instance failed", mes)
		return &discovery.FindInstancesResponse{
			Response: discovery.CreateResponse(discovery.ErrServiceNotExists, mes.Error()),
		}, nil
	}
	serviceIDs := filterServiceIDs(ctx, request.ConsumerServiceId, request.Tags, services)
	if len(serviceIDs) == 0 {
		return &discovery.FindInstancesResponse{
			Response:  discovery.CreateResponse(discovery.ResponseSuccess, "Query service instances successfully."),
			Instances: nil,
		}, nil
	}
	instances, err := instancesFilter(ctx, serviceIDs)
	if err != nil {
		log.Error(fmt.Sprintf("find shared service instance failed %s", findFlag), err)
		return &discovery.FindInstancesResponse{
			Response: discovery.CreateResponse(discovery.ErrInternal, err.Error()),
		}, err
	}
	return &discovery.FindInstancesResponse{
		Response:  discovery.CreateResponse(discovery.ResponseSuccess, "Query service instances successfully."),
		Instances: instances,
	}, nil
}

func (ds *DataSource) findInstance(ctx context.Context, request *discovery.FindInstancesRequest, provider *discovery.MicroServiceKey) (*discovery.FindInstancesResponse, error) {
	var err error
	domainProject := util.ParseDomainProject(ctx)
	service := &Service{ServiceInfo: &discovery.MicroService{Environment: request.Environment}}
	if len(request.ConsumerServiceId) > 0 {
		filter := GeneratorServiceFilter(ctx, request.ConsumerServiceId)
		service, err = GetService(ctx, filter)
		if err != nil {
			log.Error(fmt.Sprintf("get consumer failed, consumer %s find provider %s/%s/%s/%s",
				request.ConsumerServiceId, request.Environment, request.AppId, request.ServiceName, request.VersionRule), err)
			return &discovery.FindInstancesResponse{
				Response: discovery.CreateResponse(discovery.ErrInternal, err.Error()),
			}, err
		}
		if service == nil {
			log.Error(fmt.Sprintf("consumer does not exist, consumer %s find provider %s/%s/%s/%s",
				request.ConsumerServiceId, request.Environment, request.AppId, request.ServiceName, request.VersionRule), err)
			return &discovery.FindInstancesResponse{
				Response: discovery.CreateResponse(discovery.ErrServiceNotExists,
					fmt.Sprintf("Consumer[%s] does not exist.", request.ConsumerServiceId)),
			}, nil
		}
		provider.Environment = service.ServiceInfo.Environment
	}

	// provider is not a shared micro-service,
	// only allow shared micro-service instances found request different domains.
	ctx = util.SetTargetDomainProject(ctx, util.ParseDomain(ctx), util.ParseProject(ctx))
	provider.Tenant = util.ParseTargetDomainProject(ctx)

	findFlag := fmt.Sprintf("Consumer[%s][%s/%s/%s/%s] find provider[%s/%s/%s/%s]",
		request.ConsumerServiceId, service.ServiceInfo.Environment, service.ServiceInfo.AppId, service.ServiceInfo.ServiceName, service.ServiceInfo.Version,
		provider.Environment, provider.AppId, provider.ServiceName, provider.Version)
	services, err := findServices(ctx, provider)
	if err != nil {
		log.Error(fmt.Sprintf("find instance failed %s", findFlag), err)
		return &discovery.FindInstancesResponse{
			Response: discovery.CreateResponse(discovery.ErrInternal, err.Error()),
		}, err
	}
	if services == nil {
		mes := fmt.Errorf("%s failed, provider does not exist", findFlag)
		log.Error("find instance failed", mes)
		return &discovery.FindInstancesResponse{
			Response: discovery.CreateResponse(discovery.ErrServiceNotExists, mes.Error()),
		}, nil
	}
	serviceIDs := filterServiceIDs(ctx, request.ConsumerServiceId, request.Tags, services)
	if len(serviceIDs) == 0 {
		return &discovery.FindInstancesResponse{
			Response:  discovery.CreateResponse(discovery.ResponseSuccess, "Query service instances successfully."),
			Instances: nil,
		}, nil
	}
	instances, err := instancesFilter(ctx, serviceIDs)
	if err != nil {
		log.Error(fmt.Sprintf("find instance failed %s", findFlag), err)
		return &discovery.FindInstancesResponse{
			Response: discovery.CreateResponse(discovery.ErrInternal, err.Error()),
		}, err
	}
	// add dependency queue
	if len(request.ConsumerServiceId) > 0 &&
		len(serviceIDs) > 0 {
		provider, err = ds.reshapeProviderKey(ctx, provider, serviceIDs[0])
		if err != nil {
			return nil, err
		}
		if provider != nil {
			err = AddServiceVersionRule(ctx, domainProject, service.ServiceInfo, provider)
		} else {
			mes := fmt.Errorf("%s failed, provider does not exist", findFlag)
			log.Error("add service version rule failed", mes)
			return &discovery.FindInstancesResponse{
				Response: discovery.CreateResponse(discovery.ErrServiceNotExists, mes.Error()),
			}, nil
		}
		if err != nil {
			log.Error(fmt.Sprintf("add service version rule failed %s", findFlag), err)
			return &discovery.FindInstancesResponse{
				Response: discovery.CreateResponse(discovery.ErrInternal, err.Error()),
			}, err
		}
	}

	return &discovery.FindInstancesResponse{
		Response:  discovery.CreateResponse(discovery.ResponseSuccess, "Query service instances successfully."),
		Instances: instances,
	}, nil
}

func (ds *DataSource) reshapeProviderKey(ctx context.Context, provider *discovery.MicroServiceKey, providerID string) (
	*discovery.MicroServiceKey, error) {
	//维护version的规则,service name 可能是别名，所以重新获取
	filter := GeneratorServiceFilter(ctx, providerID)
	providerService, err := GetService(ctx, filter)
	if providerService == nil {
		return nil, err
	}

	versionRule := provider.Version
	provider = discovery.MicroServiceToKey(provider.Tenant, providerService.ServiceInfo)
	provider.Version = versionRule
	return provider, nil
}

func AddServiceVersionRule(ctx context.Context, domainProject string, consumer *discovery.MicroService, provider *discovery.MicroServiceKey) error {
	return nil
}

func GetInstance(ctx context.Context, serviceID string, instanceID string) (*Instance, error) {
	domain := util.ParseDomain(ctx)
	project := util.ParseProject(ctx)
	filter := bson.M{
		ColumnDomain:  domain,
		ColumnProject: project,
		StringBuilder([]string{ColumnInstanceInfo, ColumnServiceID}):  serviceID,
		StringBuilder([]string{ColumnInstanceInfo, ColumnInstanceID}): instanceID}
	findRes, err := client.GetMongoClient().FindOne(ctx, CollectionInstance, filter)
	if err != nil {
		return nil, err
	}
	var instance *Instance
	if findRes.Err() != nil {
		//not get any service,not db err
		return nil, nil
	}
	err = findRes.Decode(&instance)
	if err != nil {
		return nil, err
	}
	return instance, nil
}

func UpdateInstanceS(ctx context.Context, instance *discovery.MicroServiceInstance) *discovery.Error {
	domain := util.ParseDomain(ctx)
	project := util.ParseProject(ctx)
	filter := bson.M{
		ColumnDomain:  domain,
		ColumnProject: project,
		StringBuilder([]string{ColumnInstanceInfo, ColumnServiceID}):  instance.ServiceId,
		StringBuilder([]string{ColumnInstanceInfo, ColumnInstanceID}): instance.InstanceId}
	_, err := client.GetMongoClient().Update(ctx, CollectionInstance, filter, bson.M{"$set": bson.M{"instance.motTimestamp": strconv.FormatInt(time.Now().Unix(), 10), "instance.status": instance.Status}})
	if err != nil {
		return discovery.NewError(discovery.ErrUnavailableBackend, err.Error())
	}
	return nil
}

func UpdateInstanceP(ctx context.Context, instance *discovery.MicroServiceInstance) *discovery.Error {
	domain := util.ParseDomain(ctx)
	project := util.ParseProject(ctx)
	filter := bson.M{
		ColumnDomain:  domain,
		ColumnProject: project,
		StringBuilder([]string{ColumnInstanceInfo, ColumnServiceID}):  instance.ServiceId,
		StringBuilder([]string{ColumnInstanceInfo, ColumnInstanceID}): instance.InstanceId}
	_, err := client.GetMongoClient().Update(ctx, CollectionInstance, filter, bson.M{"$set": bson.M{"instance.motTimestamp": strconv.FormatInt(time.Now().Unix(), 10), "instance.properties": instance.Properties}})
	if err != nil {
		return discovery.NewError(discovery.ErrUnavailableBackend, err.Error())
	}
	return nil
}

func KeepAliveLease(ctx context.Context, request *discovery.HeartbeatRequest) *discovery.Error {
	_, err := heartbeat.Instance().Heartbeat(ctx, request)
	if err != nil {
		return discovery.NewError(discovery.ErrInstanceNotExists, err.Error())
	}
	return nil
}

func getHeartbeatFunc(ctx context.Context, domainProject string, instancesHbRst chan<- *discovery.InstanceHbRst, element *discovery.HeartbeatSetElement) func(context.Context) {
	return func(_ context.Context) {
		hbRst := &discovery.InstanceHbRst{
			ServiceId:  element.ServiceId,
			InstanceId: element.InstanceId,
			ErrMessage: "",
		}

		req := &discovery.HeartbeatRequest{
			InstanceId: element.InstanceId,
			ServiceId:  element.ServiceId,
		}

		err := KeepAliveLease(ctx, req)
		if err != nil {
			hbRst.ErrMessage = err.Error()
			log.Error(fmt.Sprintf("heartbeat set failed %s %s", element.ServiceId, element.InstanceId), err)
		}
		instancesHbRst <- hbRst
	}
}

func (ds *DataSource) batchFindServices(ctx context.Context, request *discovery.BatchFindInstancesRequest) (
	*discovery.BatchFindResult, error) {
	if len(request.Services) == 0 {
		return nil, nil
	}
	cloneCtx := util.CloneContext(ctx)

	services := &discovery.BatchFindResult{}
	failedResult := make(map[int32]*discovery.FindFailedResult)
	for index, key := range request.Services {
		findCtx := util.SetContext(cloneCtx, util.CtxRequestRevision, key.Rev)
		resp, err := ds.FindInstances(findCtx, &discovery.FindInstancesRequest{
			ConsumerServiceId: request.ConsumerServiceId,
			AppId:             key.Service.AppId,
			ServiceName:       key.Service.ServiceName,
			VersionRule:       key.Service.Version,
			Environment:       key.Service.Environment,
		})
		if err != nil {
			return nil, err
		}
		failed, ok := failedResult[resp.Response.GetCode()]
		AppendFindResponse(findCtx, int64(index), resp.Response, resp.Instances,
			&services.Updated, &services.NotModified, &failed)
		if !ok && failed != nil {
			failedResult[resp.Response.GetCode()] = failed
		}
	}
	for _, result := range failedResult {
		services.Failed = append(services.Failed, result)
	}
	return services, nil
}

func (ds *DataSource) batchFindInstances(ctx context.Context, request *discovery.BatchFindInstancesRequest) (*discovery.BatchFindResult, error) {
	if len(request.Instances) == 0 {
		return nil, nil
	}
	cloneCtx := util.CloneContext(ctx)
	// can not find the shared provider instances
	cloneCtx = util.SetTargetDomainProject(cloneCtx, util.ParseDomain(ctx), util.ParseProject(ctx))

	instances := &discovery.BatchFindResult{}
	failedResult := make(map[int32]*discovery.FindFailedResult)
	for index, key := range request.Instances {
		getCtx := util.SetContext(cloneCtx, util.CtxRequestRevision, key.Rev)
		resp, err := ds.GetInstance(getCtx, &discovery.GetOneInstanceRequest{
			ConsumerServiceId:  request.ConsumerServiceId,
			ProviderServiceId:  key.Instance.ServiceId,
			ProviderInstanceId: key.Instance.InstanceId,
		})
		if err != nil {
			return nil, err
		}
		failed, ok := failedResult[resp.Response.GetCode()]
		AppendFindResponse(getCtx, int64(index), resp.Response, []*discovery.MicroServiceInstance{resp.Instance},
			&instances.Updated, &instances.NotModified, &failed)
		if !ok && failed != nil {
			failedResult[resp.Response.GetCode()] = failed
		}
	}
	for _, result := range failedResult {
		instances.Failed = append(instances.Failed, result)
	}
	return instances, nil
}

func AppendFindResponse(ctx context.Context, index int64, resp *discovery.Response, instances []*discovery.MicroServiceInstance,
	updatedResult *[]*discovery.FindResult, notModifiedResult *[]int64, failedResult **discovery.FindFailedResult) {
	if code := resp.GetCode(); code != discovery.ResponseSuccess {
		if *failedResult == nil {
			*failedResult = &discovery.FindFailedResult{
				Error: discovery.NewError(code, resp.GetMessage()),
			}
		}
		(*failedResult).Indexes = append((*failedResult).Indexes, index)
		return
	}
	iv, _ := ctx.Value(util.CtxRequestRevision).(string)
	ov, _ := ctx.Value(util.CtxResponseRevision).(string)
	if len(iv) > 0 && iv == ov {
		*notModifiedResult = append(*notModifiedResult, index)
		return
	}
	*updatedResult = append(*updatedResult, &discovery.FindResult{
		Index:     index,
		Instances: instances,
		Rev:       ov,
	})
}

func preProcessRegisterInstance(ctx context.Context, instance *discovery.MicroServiceInstance) *discovery.Error {
	if len(instance.Status) == 0 {
		instance.Status = discovery.MSI_UP
	}

	if len(instance.InstanceId) == 0 {
		instance.InstanceId = uuid.Generator().GetInstanceID(ctx)
	}

	instance.Timestamp = strconv.FormatInt(time.Now().Unix(), 10)
	instance.ModTimestamp = instance.Timestamp

	// 这里应该根据租约计时
	renewalInterval := apt.RegistryDefaultLeaseRenewalinterval
	retryTimes := apt.RegistryDefaultLeaseRetrytimes
	if instance.HealthCheck == nil {
		instance.HealthCheck = &discovery.HealthCheck{
			Mode:     discovery.CHECK_BY_HEARTBEAT,
			Interval: renewalInterval,
			Times:    retryTimes,
		}
	} else {
		// Health check对象仅用于呈现服务健康检查逻辑，如果CHECK_BY_PLATFORM类型，表明由sidecar代发心跳，实例120s超时
		switch instance.HealthCheck.Mode {
		case discovery.CHECK_BY_HEARTBEAT:
			d := instance.HealthCheck.Interval * (instance.HealthCheck.Times + 1)
			if d <= 0 {
				return discovery.NewError(discovery.ErrInvalidParams, "invalid 'healthCheck' settings in request body.")
			}
		case discovery.CHECK_BY_PLATFORM:
			// 默认120s
			instance.HealthCheck.Interval = renewalInterval
			instance.HealthCheck.Times = retryTimes
		}
	}

	filter := GeneratorServiceFilter(ctx, instance.ServiceId)
	microservice, err := GetService(ctx, filter)
	if microservice == nil || err != nil {
		return discovery.NewError(discovery.ErrServiceNotExists, "invalid 'serviceID' in request body.")
	}
	instance.Version = microservice.ServiceInfo.Version
	return nil
}

func findServices(ctx context.Context, key *discovery.MicroServiceKey) ([]*Service, error) {
	tenant := strings.Split(key.Tenant, "/")
	if len(tenant) != 2 {
		return nil, errors.New("invalid 'domain' or 'project'")
	}
	rangeIdx := strings.Index(key.Version, "-")
	switch {
	case key.Version == "latest":
		filter := bson.M{
			ColumnDomain:  tenant[0],
			ColumnProject: tenant[1],
			StringBuilder([]string{ColumnServiceInfo, ColumnEnv}):         key.Environment,
			StringBuilder([]string{ColumnServiceInfo, ColumnAppID}):       key.AppId,
			StringBuilder([]string{ColumnServiceInfo, ColumnServiceName}): key.ServiceName,
		}
		return latestServicesFilter(ctx, filter)
	case len(key.Version) > 0 && key.Version[len(key.Version)-1:] == "+":
		start := key.Version[:len(key.Version)-1]
		filter := bson.M{
			ColumnDomain:  tenant[0],
			ColumnProject: tenant[1],
			StringBuilder([]string{ColumnServiceInfo, ColumnEnv}):         key.Environment,
			StringBuilder([]string{ColumnServiceInfo, ColumnAppID}):       key.AppId,
			StringBuilder([]string{ColumnServiceInfo, ColumnServiceName}): key.ServiceName,
			StringBuilder([]string{ColumnServiceInfo, ColumnVersion}):     bson.M{"$gte": start}}
		return servicesFilter(ctx, filter)
	case rangeIdx > 0:
		start := key.Version[:rangeIdx]
		end := key.Version[rangeIdx+1:]
		filter := bson.M{
			ColumnDomain:  tenant[0],
			ColumnProject: tenant[1],
			StringBuilder([]string{ColumnServiceInfo, ColumnEnv}):         key.Environment,
			StringBuilder([]string{ColumnServiceInfo, ColumnAppID}):       key.AppId,
			StringBuilder([]string{ColumnServiceInfo, ColumnServiceName}): key.ServiceName,
			StringBuilder([]string{ColumnServiceInfo, ColumnVersion}):     bson.M{"$gte": start, "$lte": end}}
		return servicesFilter(ctx, filter)
	default:
		filter := bson.M{
			ColumnDomain:  tenant[0],
			ColumnProject: tenant[1],
			StringBuilder([]string{ColumnServiceInfo, ColumnEnv}):         key.Environment,
			StringBuilder([]string{ColumnServiceInfo, ColumnAppID}):       key.AppId,
			StringBuilder([]string{ColumnServiceInfo, ColumnServiceName}): key.ServiceName,
			StringBuilder([]string{ColumnServiceInfo, ColumnVersion}):     key.Version}
		return servicesFilter(ctx, filter)
	}
}

func instancesFilter(ctx context.Context, serviceIDs []string) ([]*discovery.MicroServiceInstance, error) {
	resp, err := client.GetMongoClient().Find(ctx, CollectionInstance, bson.M{StringBuilder([]string{ColumnInstanceInfo, ColumnServiceID}): bson.M{"$in": serviceIDs}}, &options.FindOptions{
		Sort: bson.M{StringBuilder([]string{ColumnInstanceInfo, ColumnVersion}): -1}})
	if err != nil {
		return nil, err
	}
	if resp == nil {
		return nil, errors.New("no related instances were found")
	}
	var instances []*discovery.MicroServiceInstance
	for resp.Next(ctx) {
		var instance Instance
		err := resp.Decode(&instance)
		if err != nil {
			return nil, err
		}
		instances = append(instances, instance.InstanceInfo)
	}
	return instances, nil
}

func filterServiceIDs(ctx context.Context, consumerID string, tags []string, services []*Service) []string {
	var filterService []*Service
	var serviceIDs []string
	filterService = tagsFilter(services, tags)
	filterService = accessibleFilter(ctx, consumerID, filterService)
	for _, service := range filterService {
		serviceIDs = append(serviceIDs, service.ServiceInfo.ServiceId)
	}
	return serviceIDs
}

func tagsFilter(services []*Service, tags []string) []*Service {
	var newServices []*Service
	for _, service := range services {
		index := 0
		for ; index < len(tags); index++ {
			if _, ok := service.Tags[tags[index]]; !ok {
				break
			}
		}
		if index == len(tags) {
			newServices = append(newServices, service)
		}
	}
	return newServices
}

func accessibleFilter(ctx context.Context, consumerID string, services []*Service) []*Service {
	var newServices []*Service
	for _, service := range services {
		if err := accessible(ctx, consumerID, service.ServiceInfo.ServiceId); err != nil {
			findFlag := fmt.Sprintf("consumer '%s' find provider %s/%s/%s", consumerID,
				service.ServiceInfo.AppId, service.ServiceInfo.ServiceName, service.ServiceInfo.Version)
			log.Error(fmt.Sprintf("accessible filter failed, %s", findFlag), err)
			continue
		}
		newServices = append(newServices, service)
	}
	return newServices
}

func servicesFilter(ctx context.Context, filter bson.M) ([]*Service, error) {
	resp, err := client.GetMongoClient().Find(ctx, CollectionService, filter)
	if err != nil {
		return nil, err
	}
	if resp == nil {
		return nil, errors.New("no related services were found")
	}
	var services []*Service
	for resp.Next(ctx) {
		var service Service
		err := resp.Decode(&service)
		if err != nil {
			log.Error("type conversion error", err)
			return nil, err
		}
		services = append(services, &service)
	}
	return services, nil
}

func latestServicesFilter(ctx context.Context, filter bson.M) ([]*Service, error) {
	resp, err := client.GetMongoClient().Find(ctx, CollectionService, filter, &options.FindOptions{
		Sort: bson.M{StringBuilder([]string{ColumnServiceInfo, ColumnVersion}): -1}})
	if err != nil {
		return nil, err
	}
	if resp == nil {
		return nil, errors.New("no related services were found")
	}
	var services []*Service
	for resp.Next(ctx) {
		var service Service
		err := resp.Decode(&service)
		if err != nil {
			log.Error("type conversion error", err)
			return nil, err
		}
		services = append(services, &service)
		if services != nil {
			return services, nil
		}
	}
	return services, nil
}

func getTags(ctx context.Context, domain string, project string, serviceID string) (tags map[string]string, err error) {
	filter := bson.M{
		ColumnDomain:    domain,
		ColumnProject:   project,
		ColumnServiceID: serviceID,
	}
	result, err := client.GetMongoClient().FindOne(ctx, CollectionService, filter)
	if err != nil {
		return nil, err
	}
	if result.Err() != nil {
		return nil, result.Err()
	}
	var service Service
	err = result.Decode(&service)
	if err != nil {
		log.Error("type conversion error", err)
		return nil, err
	}
	return service.Tags, nil
}

func getService(ctx context.Context, domain string, project string, serviceID string) (*Service, error) {
	filter := bson.M{
		ColumnDomain:  domain,
		ColumnProject: project,
		StringBuilder([]string{ColumnServiceInfo, ColumnServiceID}): serviceID,
	}
	result, err := client.GetMongoClient().FindOne(ctx, CollectionService, filter)
	if err != nil {
		return nil, err
	}
	if result.Err() != nil {
		return nil, result.Err()
	}
	var svc Service
	err = result.Decode(&svc)
	if err != nil {
		return nil, err
	}
	return &svc, nil
}

func accessible(ctx context.Context, consumerID string, providerID string) *discovery.Error {
	if len(consumerID) == 0 {
		return nil
	}

	consumerDomain, consumerProject := util.ParseDomain(ctx), util.ParseProject(ctx)
	providerDomain, providerProject := util.ParseTargetDomain(ctx), util.ParseTargetProject(ctx)

	consumerService, err := getService(ctx, consumerDomain, consumerProject, consumerID)
	if err != nil {
		return discovery.NewError(discovery.ErrInternal, fmt.Sprintf("an error occurred in query consumer(%s)", err.Error()))
	}
	if consumerService == nil {
		return discovery.NewError(discovery.ErrServiceNotExists, "consumer serviceID is invalid")
	}

	// 跨应用权限
	providerService, err := getService(ctx, providerDomain, providerProject, providerID)
	if err != nil {
		return discovery.NewError(discovery.ErrInternal, fmt.Sprintf("an error occurred in query provider(%s)", err.Error()))
	}
	if providerService == nil {
		return discovery.NewError(discovery.ErrServiceNotExists, "provider serviceID is invalid")
	}
	err = allowAcrossDimension(ctx, providerService, consumerService)
	if err != nil {
		return discovery.NewError(discovery.ErrPermissionDeny, err.Error())
	}

	// 黑白名单
	rules, err := getRulesUtil(ctx, providerDomain, providerProject, providerID)
	if err != nil {
		return discovery.NewError(discovery.ErrInternal, fmt.Sprintf("an error occurred in query provider rules(%s)", err.Error()))
	}

	if len(rules) == 0 {
		return nil
	}

	validateTags, err := getTags(ctx, consumerDomain, consumerProject, consumerService.ServiceInfo.ServiceId)
	if err != nil {
		return discovery.NewError(discovery.ErrInternal, fmt.Sprintf("an error occurred in query consumer tags(%s)", err.Error()))
	}
	return matchRules(rules, consumerService.ServiceInfo, validateTags)
}

func matchRules(rulesOfProvider []*Rule, consumer *discovery.MicroService, tagsOfConsumer map[string]string) *discovery.Error {
	if consumer == nil {
		return discovery.NewError(discovery.ErrInvalidParams, "consumer is nil")
	}

	if len(rulesOfProvider) <= 0 {
		return nil
	}
	if rulesOfProvider[0].RuleInfo.RuleType == "WHITE" {
		return patternWhiteList(rulesOfProvider, tagsOfConsumer, consumer)
	}
	return patternBlackList(rulesOfProvider, tagsOfConsumer, consumer)
}

func parsePattern(v reflect.Value, rule *discovery.ServiceRule, tagsOfConsumer map[string]string, consumerID string) (string, *discovery.Error) {
	if strings.HasPrefix(rule.Attribute, "tag_") {
		key := rule.Attribute[4:]
		value := tagsOfConsumer[key]
		if len(value) == 0 {
			log.Info(fmt.Sprintf("can not find service[%s] tag[%s]", consumerID, key))
		}
		return value, nil
	}
	key := v.FieldByName(rule.Attribute)
	if !key.IsValid() {
		log.Error(fmt.Sprintf("can not find service[%s] field[%s], ruleID is %s",
			consumerID, rule.Attribute, rule.RuleId), nil)
		return "", discovery.NewError(discovery.ErrInternal, fmt.Sprintf("can not find field '%s'", rule.Attribute))
	}
	return key.String(), nil

}

func patternWhiteList(rulesOfProvider []*Rule, tagsOfConsumer map[string]string, consumer *discovery.MicroService) *discovery.Error {
	v := reflect.Indirect(reflect.ValueOf(consumer))
	consumerID := consumer.ServiceId
	for _, rule := range rulesOfProvider {
		value, err := parsePattern(v, rule.RuleInfo, tagsOfConsumer, consumerID)
		if err != nil {
			return err
		}
		if len(value) == 0 {
			continue
		}

		match, _ := regexp.MatchString(rule.RuleInfo.Pattern, value)
		if match {
			log.Info(fmt.Sprintf("consumer[%s][%s/%s/%s/%s] match white list, rule.Pattern is %s, value is %s",
				consumerID, consumer.Environment, consumer.AppId, consumer.ServiceName, consumer.Version,
				rule.RuleInfo.Pattern, value))
			return nil
		}
	}
	return discovery.NewError(discovery.ErrPermissionDeny, "not found in white list")
}

func patternBlackList(rulesOfProvider []*Rule, tagsOfConsumer map[string]string, consumer *discovery.MicroService) *discovery.Error {
	v := reflect.Indirect(reflect.ValueOf(consumer))
	consumerID := consumer.ServiceId
	for _, rule := range rulesOfProvider {
		var value string
		value, err := parsePattern(v, rule.RuleInfo, tagsOfConsumer, consumerID)
		if err != nil {
			return err
		}
		if len(value) == 0 {
			continue
		}

		match, _ := regexp.MatchString(rule.RuleInfo.Pattern, value)
		if match {
			log.Warn(fmt.Sprintf("no permission to access, consumer[%s][%s/%s/%s/%s] match black list, rule.Pattern is %s, value is %s",
				consumerID, consumer.Environment, consumer.AppId, consumer.ServiceName, consumer.Version,
				rule.RuleInfo.Pattern, value))
			return discovery.NewError(discovery.ErrPermissionDeny, "found in black list")
		}
	}
	return nil
}

func getRulesUtil(ctx context.Context, domain string, project string, serviceID string) ([]*Rule, error) {
	filter := bson.M{
		ColumnDomain:    domain,
		ColumnProject:   project,
		ColumnServiceID: serviceID,
	}
	resp, err := client.GetMongoClient().Find(ctx, CollectionRule, filter)
	if err != nil {
		return nil, err
	}
	if resp.Err() != nil {
		return nil, resp.Err()
	}
	var rules []*Rule
	for resp.Next(ctx) {
		var rule *Rule
		err := resp.Decode(rule)
		if err != nil {
			log.Error("type conversion error", err)
			return nil, err
		}
		rules = append(rules, rule)
	}
	return rules, nil
}

func allowAcrossDimension(ctx context.Context, providerService *Service, consumerService *Service) error {
	if providerService.ServiceInfo.AppId != consumerService.ServiceInfo.AppId {
		if len(providerService.ServiceInfo.Properties) == 0 {
			return fmt.Errorf("not allow across app access")
		}

		if allowCrossApp, ok := providerService.ServiceInfo.Properties[discovery.PropAllowCrossApp]; !ok || strings.ToLower(allowCrossApp) != "true" {
			return fmt.Errorf("not allow across app access")
		}
	}
	if !apt.IsGlobal(discovery.MicroServiceToKey(util.ParseTargetDomainProject(ctx), providerService.ServiceInfo)) &&
		providerService.ServiceInfo.Environment != consumerService.ServiceInfo.Environment {
		return fmt.Errorf("not allow across environment access")
	}
	return nil
}
