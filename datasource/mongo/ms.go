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
	"crypto/sha1"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/go-chassis/cari/discovery"
	"github.com/go-chassis/cari/pkg/errsvc"
	"github.com/jinzhu/copier"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/apache/servicecomb-service-center/datasource"
	"github.com/apache/servicecomb-service-center/datasource/cache"
	"github.com/apache/servicecomb-service-center/datasource/mongo/client"
	"github.com/apache/servicecomb-service-center/datasource/mongo/client/dao"
	"github.com/apache/servicecomb-service-center/datasource/mongo/client/model"
	"github.com/apache/servicecomb-service-center/datasource/mongo/heartbeat"
	mutil "github.com/apache/servicecomb-service-center/datasource/mongo/util"
	"github.com/apache/servicecomb-service-center/pkg/gopool"
	"github.com/apache/servicecomb-service-center/pkg/log"
	"github.com/apache/servicecomb-service-center/pkg/util"
	apt "github.com/apache/servicecomb-service-center/server/core"
	"github.com/apache/servicecomb-service-center/server/plugin/quota"
	"github.com/apache/servicecomb-service-center/server/plugin/uuid"
)

const baseTen = 10

var (
	ErrUndefinedSchemaID    = discovery.NewError(discovery.ErrUndefinedSchemaID, datasource.ErrUndefinedSchemaID.Error())
	ErrModifySchemaNotAllow = discovery.NewError(discovery.ErrModifySchemaNotAllow, datasource.ErrModifySchemaNotAllow.Error())
)

type MetadataManager struct {
	// SchemaNotEditable determines whether schema modification is not allowed
	SchemaNotEditable bool
	// InstanceTTL options
	InstanceTTL int64
}

func (ds *MetadataManager) RegisterService(ctx context.Context, request *discovery.CreateServiceRequest) (*discovery.CreateServiceResponse, error) {
	service := request.Service
	remoteIP := util.GetIPFromContext(ctx)
	domain := util.ParseDomain(ctx)
	project := util.ParseProject(ctx)
	serviceFlag := util.StringJoin([]string{
		service.Environment, service.AppId, service.ServiceName, service.Version}, "/")
	requestServiceID := service.ServiceId

	if len(requestServiceID) == 0 {
		ctx = util.SetContext(ctx, uuid.ContextKey, util.StringJoin([]string{domain, project, service.Environment, service.AppId, service.ServiceName, service.Alias, service.Version}, "/"))
		service.ServiceId = uuid.Generator().GetServiceID(ctx)
	}
	service.Timestamp = strconv.FormatInt(time.Now().Unix(), 10)
	service.ModTimestamp = service.Timestamp
	// the service unique index in table is (serviceId/serviceEnv,serviceAppid,servicename,serviceVersion)
	if len(service.Alias) != 0 {
		serviceID, err := GetServiceID(ctx, &discovery.MicroServiceKey{
			Environment: service.Environment,
			AppId:       service.AppId,
			ServiceName: service.ServiceName,
			Version:     service.Version,
			Alias:       service.Alias,
		})
		if err != nil && !errors.Is(err, datasource.ErrNoData) {
			return &discovery.CreateServiceResponse{
				Response: discovery.CreateResponse(discovery.ErrUnavailableBackend, err.Error()),
			}, err
		}
		if len(serviceID) != 0 {
			if len(requestServiceID) != 0 && requestServiceID != serviceID {
				log.Warn(fmt.Sprintf("create micro-service[%s] failed, service already exists, operator: %s",
					serviceFlag, remoteIP))
				return &discovery.CreateServiceResponse{
					Response: discovery.CreateResponse(discovery.ErrServiceAlreadyExists,
						"ServiceID conflict or found the same service with different id."),
				}, nil
			}
			return &discovery.CreateServiceResponse{
				Response:  discovery.CreateResponse(discovery.ResponseSuccess, "register service successfully"),
				ServiceId: serviceID,
			}, nil
		}
	}
	insertRes, err := client.GetMongoClient().Insert(ctx, model.CollectionService, &model.Service{Domain: domain, Project: project, Service: service})
	if err != nil {
		if client.IsDuplicateKey(err) {
			serviceIDInner, err := GetServiceID(ctx, &discovery.MicroServiceKey{
				Environment: service.Environment,
				AppId:       service.AppId,
				ServiceName: service.ServiceName,
				Version:     service.Version,
			})
			if err != nil && !errors.Is(err, datasource.ErrNoData) {
				return &discovery.CreateServiceResponse{
					Response: discovery.CreateResponse(discovery.ErrUnavailableBackend, err.Error()),
				}, err
			}
			// serviceid conflict with the service in the database
			if len(requestServiceID) != 0 && serviceIDInner != requestServiceID {
				return &discovery.CreateServiceResponse{
					Response: discovery.CreateResponse(discovery.ErrServiceAlreadyExists,
						"ServiceID conflict or found the same service with different id."),
				}, nil
			}
			return &discovery.CreateServiceResponse{
				Response:  discovery.CreateResponse(discovery.ResponseSuccess, "register service successfully"),
				ServiceId: serviceIDInner,
			}, nil
		}
		log.Error(fmt.Sprintf("create micro-service[%s] failed, service already exists, operator: %s",
			serviceFlag, remoteIP), err)
		return &discovery.CreateServiceResponse{
			Response: discovery.CreateResponse(discovery.ErrInternal, err.Error()),
		}, err
	}

	log.Info(fmt.Sprintf("create micro-service[%s][%s] successfully,operator: %s", service.ServiceId, insertRes.InsertedID, remoteIP))

	return &discovery.CreateServiceResponse{
		Response:  discovery.CreateResponse(discovery.ResponseSuccess, "Register service successfully"),
		ServiceId: service.ServiceId,
	}, nil
}

func (ds *MetadataManager) GetServices(ctx context.Context, request *discovery.GetServicesRequest) (*discovery.GetServicesResponse, error) {
	domain := util.ParseDomain(ctx)
	project := util.ParseProject(ctx)

	filter := bson.M{model.ColumnDomain: domain, model.ColumnProject: project}

	services, err := dao.GetMicroServices(ctx, filter)
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

func (ds *MetadataManager) GetApplications(ctx context.Context, request *discovery.GetAppsRequest) (*discovery.GetAppsResponse, error) {
	domain := util.ParseDomain(ctx)
	project := util.ParseProject(ctx)

	filter := bson.M{
		model.ColumnDomain:  domain,
		model.ColumnProject: project,
		mutil.ConnectWithDot([]string{model.ColumnService, model.ColumnEnv}): request.Environment}

	services, err := dao.GetMicroServices(ctx, filter)
	if err != nil {
		return &discovery.GetAppsResponse{
			Response: discovery.CreateResponse(discovery.ErrInternal, "get services data failed."),
		}, nil
	}
	l := len(services)
	if l == 0 {
		return &discovery.GetAppsResponse{
			Response: discovery.CreateResponse(discovery.ResponseSuccess, "Get all applications successfully."),
		}, nil
	}
	apps := make([]string, 0, l)
	hash := make(map[string]struct{}, l)
	for _, svc := range services {
		if !request.WithShared && datasource.IsGlobal(discovery.MicroServiceToKey(util.ParseDomainProject(ctx), svc)) {
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

func (ds *MetadataManager) GetService(ctx context.Context, request *discovery.GetServiceRequest) (*discovery.GetServiceResponse, error) {
	svc, ok := cache.GetServiceByID(ctx, request.ServiceId)
	if !ok {
		var err error
		svc, err = dao.GetServiceByID(ctx, request.ServiceId)
		if err != nil {
			if errors.Is(err, datasource.ErrNoData) {
				log.Debug(fmt.Sprintf("service %s not exist in db", request.ServiceId))
				return &discovery.GetServiceResponse{
					Response: discovery.CreateResponse(discovery.ErrServiceNotExists, "Service not exist."),
				}, nil
			}
			log.Error(fmt.Sprintf("failed to get single service %s from mongo", request.ServiceId), err)
			return &discovery.GetServiceResponse{
				Response: discovery.CreateResponse(discovery.ErrInternal, "get service data from mongodb failed."),
			}, err
		}
	}
	return &discovery.GetServiceResponse{
		Response: discovery.CreateResponse(discovery.ResponseSuccess, "Get service successfully."),
		Service:  svc.Service,
	}, nil
}

func (ds *MetadataManager) ExistServiceByID(ctx context.Context, request *discovery.GetExistenceByIDRequest) (*discovery.GetExistenceByIDResponse, error) {
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

func (ds *MetadataManager) ExistService(ctx context.Context, request *discovery.GetExistenceRequest) (*discovery.GetExistenceResponse, error) {
	domainProject := util.ParseDomainProject(ctx)
	serviceFlag := util.StringJoin([]string{
		request.Environment, request.AppId, request.ServiceName, request.Version}, "/")

	ids, exist, err := FindServiceIds(ctx, request.Version, &discovery.MicroServiceKey{
		Environment: request.Environment,
		AppId:       request.AppId,
		ServiceName: request.ServiceName,
		Alias:       request.ServiceName,
		Version:     request.Version,
		Tenant:      domainProject,
	})
	if err != nil {
		log.Error(fmt.Sprintf("micro-service[%s] exist failed, find serviceIDs failed", serviceFlag), err)
		return &discovery.GetExistenceResponse{
			Response: discovery.CreateResponse(discovery.ErrInternal, err.Error()),
		}, err
	}
	if !exist {
		log.Info(fmt.Sprintf("micro-service[%s] exist failed, service does not exist", serviceFlag))
		return &discovery.GetExistenceResponse{
			Response: discovery.CreateResponse(discovery.ErrServiceNotExists, serviceFlag+" does not exist."),
		}, nil
	}
	if len(ids) == 0 {
		log.Info(fmt.Sprintf("micro-service[%s] exist failed, version mismatch", serviceFlag))
		return &discovery.GetExistenceResponse{
			Response: discovery.CreateResponse(discovery.ErrServiceVersionNotExists, serviceFlag+" version mismatch."),
		}, nil
	}
	return &discovery.GetExistenceResponse{
		Response:  discovery.CreateResponse(discovery.ResponseSuccess, "get service id successfully."),
		ServiceId: ids[0], // 约定多个时，取较新版本
	}, nil
}

func (ds *MetadataManager) UnregisterService(ctx context.Context, request *discovery.DeleteServiceRequest) (*discovery.DeleteServiceResponse, error) {
	res, err := ds.DelServicePri(ctx, request.ServiceId, request.Force)
	if err != nil {
		return &discovery.DeleteServiceResponse{
			Response: discovery.CreateResponse(discovery.ErrInternal, "Delete service failed"),
		}, err
	}
	return &discovery.DeleteServiceResponse{
		Response: res,
	}, nil
}

func (ds *MetadataManager) DelServicePri(ctx context.Context, serviceID string, force bool) (*discovery.Response, error) {
	remoteIP := util.GetIPFromContext(ctx)
	title := "delete"
	if force {
		title = "force delete"
	}

	if serviceID == apt.Service.ServiceId {
		log.Error(fmt.Sprintf("%s micro-service %s failed, operator: %s", title, serviceID, remoteIP), mutil.ErrNotAllowDeleteSC)
		return discovery.CreateResponse(discovery.ErrInvalidParams, mutil.ErrNotAllowDeleteSC.Error()), nil
	}
	filter := mutil.NewBasicFilter(ctx, mutil.ServiceServiceID(serviceID))
	microservice, err := dao.GetService(ctx, filter)
	if err != nil {
		if errors.Is(err, datasource.ErrNoData) {
			log.Debug(fmt.Sprintf("%s micro-service %s failed, service does not exist, operator: %s",
				title, serviceID, remoteIP))
			return discovery.CreateResponse(discovery.ErrServiceNotExists, "Service does not exist."), nil
		}
		log.Error(fmt.Sprintf("%s micro-service %s failed, get service file failed, operator: %s",
			title, serviceID, remoteIP), err)
		return discovery.CreateResponse(discovery.ErrInternal, err.Error()), err
	}
	// 强制删除，则与该服务相关的信息删除，非强制删除： 如果作为该被依赖（作为provider，提供服务,且不是只存在自依赖）或者存在实例，则不能删除
	if !force {
		dr := NewProviderDependencyRelation(ctx, util.ParseDomainProject(ctx), microservice.Service)
		services, err := dr.GetDependencyConsumerIds()
		if err != nil {
			log.Error(fmt.Sprintf("delete micro-service[%s] failed, get service dependency failed, operator: %s",
				serviceID, remoteIP), err)
			return discovery.CreateResponse(discovery.ErrInternal, err.Error()), err
		}
		if l := len(services); l > 1 || (l == 1 && services[0] != serviceID) {
			log.Error(fmt.Sprintf("delete micro-service[%s] failed, other services[%d] depend on it, operator: %s",
				serviceID, l, remoteIP), err)
			return discovery.CreateResponse(discovery.ErrDependedOnConsumer, "Can not delete this service, other service rely it."), err
		}
		//todo wait for dep interface
		instancesExist, err := client.GetMongoClient().DocExist(ctx, model.CollectionInstance, bson.M{mutil.ConnectWithDot([]string{model.ColumnInstance, model.ColumnServiceID}): serviceID})
		if err != nil {
			log.Error(fmt.Sprintf("delete micro-service[%s] failed, get instances number failed, operator: %s",
				serviceID, remoteIP), err)
			return discovery.CreateResponse(discovery.ErrUnavailableBackend, err.Error()), err
		}
		if instancesExist {
			log.Error(fmt.Sprintf("delete micro-service[%s] failed, service deployed instances, operator: %s",
				serviceID, remoteIP), nil)
			return discovery.CreateResponse(discovery.ErrDeployedInstance, "Can not delete the service deployed instance(s)."), err
		}

	}

	schemaOps := client.MongoOperation{Table: model.CollectionSchema, Models: []mongo.WriteModel{mongo.NewDeleteManyModel().SetFilter(bson.M{model.ColumnServiceID: serviceID})}}
	rulesOps := client.MongoOperation{Table: model.CollectionRule, Models: []mongo.WriteModel{mongo.NewDeleteManyModel().SetFilter(bson.M{model.ColumnServiceID: serviceID})}}
	instanceOps := client.MongoOperation{Table: model.CollectionInstance, Models: []mongo.WriteModel{mongo.NewDeleteManyModel().SetFilter(bson.M{mutil.ConnectWithDot([]string{model.ColumnInstance, model.ColumnServiceID}): serviceID})}}
	serviceOps := client.MongoOperation{Table: model.CollectionService, Models: []mongo.WriteModel{mongo.NewDeleteOneModel().SetFilter(bson.M{mutil.ConnectWithDot([]string{model.ColumnService, model.ColumnServiceID}): serviceID})}}

	err = client.GetMongoClient().MultiTableBatchUpdate(ctx, []client.MongoOperation{schemaOps, rulesOps, instanceOps, serviceOps})
	if err != nil {
		log.Error(fmt.Sprintf("micro-service[%s] failed, operator: %s", serviceID, remoteIP), err)
		return discovery.CreateResponse(discovery.ErrUnavailableBackend, err.Error()), err
	}

	domainProject := util.ToDomainProject(microservice.Domain, microservice.Project)
	serviceKey := &discovery.MicroServiceKey{
		Tenant:      domainProject,
		Environment: microservice.Service.Environment,
		AppId:       microservice.Service.AppId,
		ServiceName: microservice.Service.ServiceName,
		Version:     microservice.Service.Version,
		Alias:       microservice.Service.Alias,
	}

	err = DeleteDependencyForDeleteService(domainProject, serviceID, serviceKey)
	if err != nil {
		log.Error(fmt.Sprintf("micro-service[%s] failed, operator: %s", serviceID, remoteIP), err)
		return discovery.CreateResponse(discovery.ErrUnavailableBackend, err.Error()), err
	}

	return discovery.CreateResponse(discovery.ResponseSuccess, "Unregister service successfully."), nil
}

func (ds *MetadataManager) UpdateService(ctx context.Context, request *discovery.UpdateServicePropsRequest) (*discovery.UpdateServicePropsResponse, error) {
	filter := mutil.NewBasicFilter(ctx, mutil.ServiceServiceID(request.ServiceId))
	setFilter := mutil.NewFilter(
		mutil.ServiceModTime(strconv.FormatInt(time.Now().Unix(), baseTen)),
		mutil.ServiceProperty(request.Properties),
	)
	updateFilter := mutil.NewFilter(
		mutil.Set(setFilter),
	)
	err := dao.UpdateService(ctx, filter, updateFilter)
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

func (ds *MetadataManager) GetDeleteServiceFunc(ctx context.Context, serviceID string, force bool, serviceRespChan chan<- *discovery.DelServicesRspInfo) func(context.Context) {
	return func(_ context.Context) {
		serviceRst := &discovery.DelServicesRspInfo{
			ServiceId:  serviceID,
			ErrMessage: "",
		}
		resp, err := ds.DelServicePri(ctx, serviceID, force)
		if err != nil {
			serviceRst.ErrMessage = err.Error()
		} else if resp.GetCode() != discovery.ResponseSuccess {
			serviceRst.ErrMessage = resp.GetMessage()
		}

		serviceRespChan <- serviceRst
	}
}

func (ds *MetadataManager) GetServiceDetail(ctx context.Context, request *discovery.GetServiceRequest) (*discovery.GetServiceDetailResponse, error) {
	filter := mutil.NewBasicFilter(ctx, mutil.ServiceServiceID(request.ServiceId))
	mgSvc, err := dao.GetService(ctx, filter)
	if err != nil {
		if errors.Is(err, datasource.ErrNoData) {
			return &discovery.GetServiceDetailResponse{
				Response: discovery.CreateResponse(discovery.ErrServiceNotExists, "Service does not exist."),
			}, nil
		}
		return &discovery.GetServiceDetailResponse{
			Response: discovery.CreateResponse(discovery.ErrInternal, err.Error()),
		}, err
	}
	svc := mgSvc.Service
	key := &discovery.MicroServiceKey{
		Environment: svc.Environment,
		AppId:       svc.AppId,
		ServiceName: svc.ServiceName,
	}
	filter = mutil.NewBasicFilter(ctx,
		mutil.ServiceEnv(key.Environment),
		mutil.ServiceAppID(key.AppId),
		mutil.ServiceServiceName(key.ServiceName),
	)
	versions, err := dao.GetServicesVersions(ctx, filter)
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

func (ds *MetadataManager) GetServicesInfo(ctx context.Context, request *discovery.GetServicesInfoRequest) (*discovery.GetServicesInfoResponse, error) {
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
	var st *discovery.Statistics
	if _, ok := optionMap["statistics"]; ok {
		var err error
		st, err = statistics(ctx, request.WithShared)
		if err != nil {
			return &discovery.GetServicesInfoResponse{
				Response: discovery.CreateResponse(discovery.ErrInternal, err.Error()),
			}, err
		}
		if len(optionMap) == 1 {
			return &discovery.GetServicesInfoResponse{
				Response:   discovery.CreateResponse(discovery.ResponseSuccess, "Statistics successfully."),
				Statistics: st,
			}, nil
		}
	}
	filters := ds.filterServices(ctx, request)
	services, err := dao.GetServices(ctx, filters)
	if err != nil {
		log.Error("get all services by domain failed", err)
		return &discovery.GetServicesInfoResponse{
			Response: discovery.CreateResponse(discovery.ErrInternal, err.Error()),
		}, err
	}
	allServiceDetails := make([]*discovery.ServiceDetail, 0, len(services))
	domainProject := util.ParseDomainProject(ctx)
	for _, mgSvc := range services {
		if !request.WithShared && datasource.IsGlobal(discovery.MicroServiceToKey(domainProject, mgSvc.Service)) {
			continue
		}

		serviceDetail, err := getServiceDetailUtil(ctx, mgSvc, request.CountOnly, options)
		if err != nil {
			return &discovery.GetServicesInfoResponse{
				Response: discovery.CreateResponse(discovery.ErrInternal, err.Error()),
			}, err
		}
		serviceDetail.MicroService = mgSvc.Service
		tmpServiceDetail := &discovery.ServiceDetail{}
		err = copier.CopyWithOption(tmpServiceDetail, serviceDetail, copier.Option{DeepCopy: true})
		if err != nil {
			return &discovery.GetServicesInfoResponse{
				Response: discovery.CreateResponse(discovery.ErrInternal, err.Error()),
			}, err
		}
		tmpServiceDetail.MicroService.Properties = nil
		tmpServiceDetail.MicroService.Schemas = nil
		instances := tmpServiceDetail.Instances
		for _, instance := range instances {
			instance.Properties = nil
		}
		allServiceDetails = append(allServiceDetails, tmpServiceDetail)
	}

	return &discovery.GetServicesInfoResponse{
		Response:          discovery.CreateResponse(discovery.ResponseSuccess, "Get services info successfully."),
		AllServicesDetail: allServiceDetails,
		Statistics:        st,
	}, nil
}

func (ds *MetadataManager) filterServices(ctx context.Context, request *discovery.GetServicesInfoRequest) bson.M {
	var opts []func(filter bson.M)

	if len(request.Environment) > 0 {
		opts = append(opts, mutil.ServiceEnv(request.Environment))
	}
	if len(request.AppId) > 0 {
		opts = append(opts, mutil.ServiceAppID(request.AppId))
	}
	if len(request.ServiceName) > 0 {
		opts = append(opts, mutil.ServiceServiceName(request.ServiceName))
	}
	return mutil.NewBasicFilter(ctx, opts...)
}

func (ds *MetadataManager) GetServicesStatistics(ctx context.Context, request *discovery.GetServicesRequest) (
	*discovery.GetServicesInfoStatisticsResponse, error) {
	ctx = util.WithCacheOnly(ctx)
	var st *discovery.Statistics
	var err error
	st, err = statistics(ctx, false)
	if err != nil {
		return &discovery.GetServicesInfoStatisticsResponse{
			Response: discovery.CreateResponse(discovery.ErrInternal, err.Error()),
		}, err
	}
	return &discovery.GetServicesInfoStatisticsResponse{
		Response:   discovery.CreateResponse(discovery.ResponseSuccess, "Get services statistics successfully."),
		Statistics: st,
	}, nil
}

func (ds *MetadataManager) AddTags(ctx context.Context, request *discovery.AddServiceTagsRequest) (*discovery.AddServiceTagsResponse, error) {
	filter := mutil.NewBasicFilter(ctx, mutil.ServiceServiceID(request.ServiceId))
	setFilter := mutil.NewFilter(
		mutil.Tags(request.Tags),
	)
	updateFilter := mutil.NewFilter(
		mutil.Set(setFilter),
	)
	err := dao.UpdateService(ctx, filter, updateFilter)
	if err == nil {
		return &discovery.AddServiceTagsResponse{
			Response: discovery.CreateResponse(discovery.ResponseSuccess, "Add service tags successfully."),
		}, nil
	}
	log.Error(fmt.Sprintf("update service %s tags failed.", request.ServiceId), err)
	if err == client.ErrNoDocuments {
		return &discovery.AddServiceTagsResponse{
			Response: discovery.CreateResponse(discovery.ErrServiceNotExists, err.Error()),
		}, nil
	}
	return &discovery.AddServiceTagsResponse{
		Response: discovery.CreateResponse(discovery.ErrInternal, err.Error()),
	}, nil
}

func (ds *MetadataManager) GetTags(ctx context.Context, request *discovery.GetServiceTagsRequest) (*discovery.GetServiceTagsResponse, error) {
	filter := mutil.NewBasicFilter(ctx, mutil.ServiceServiceID(request.ServiceId))
	svc, err := dao.GetService(ctx, filter)
	if err != nil {
		if errors.Is(err, datasource.ErrNoData) {
			log.Debug(fmt.Sprintf("service %s not exist in db", request.ServiceId))
			return &discovery.GetServiceTagsResponse{
				Response: discovery.CreateResponse(discovery.ErrServiceNotExists, "Service does not exist"),
			}, nil
		}
		log.Error(fmt.Sprintf("failed to get service %s tags", request.ServiceId), err)
		return &discovery.GetServiceTagsResponse{
			Response: discovery.CreateResponse(discovery.ErrInternal, err.Error()),
		}, nil
	}
	return &discovery.GetServiceTagsResponse{
		Response: discovery.CreateResponse(discovery.ResponseSuccess, "Get service tags successfully."),
		Tags:     svc.Tags,
	}, nil
}

func (ds *MetadataManager) UpdateTag(ctx context.Context, request *discovery.UpdateServiceTagRequest) (*discovery.UpdateServiceTagResponse, error) {
	filter := mutil.NewBasicFilter(ctx, mutil.ServiceServiceID(request.ServiceId))
	svc, err := dao.GetService(ctx, filter)
	if err != nil {
		if errors.Is(err, datasource.ErrNoData) {
			log.Debug(fmt.Sprintf("service %s not exist in db", request.ServiceId))
			return &discovery.UpdateServiceTagResponse{
				Response: discovery.CreateResponse(discovery.ErrServiceNotExists, "Service does not exist"),
			}, nil
		}
		log.Error(fmt.Sprintf("failed to get %s tags", request.ServiceId), err)
		return &discovery.UpdateServiceTagResponse{
			Response: discovery.CreateResponse(discovery.ErrInternal, err.Error()),
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
	setFilter := mutil.NewFilter(
		mutil.Tags(newTags),
	)
	updateFilter := mutil.NewFilter(
		mutil.Set(setFilter),
	)
	err = dao.UpdateService(ctx, filter, updateFilter)
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

func (ds *MetadataManager) DeleteTags(ctx context.Context, request *discovery.DeleteServiceTagsRequest) (*discovery.DeleteServiceTagsResponse, error) {
	filter := mutil.NewBasicFilter(ctx, mutil.ServiceServiceID(request.ServiceId))
	svc, err := dao.GetService(ctx, filter)
	if err != nil {
		if errors.Is(err, datasource.ErrNoData) {
			log.Debug(fmt.Sprintf("service %s not exist in db", request.ServiceId))
			return &discovery.DeleteServiceTagsResponse{
				Response: discovery.CreateResponse(discovery.ErrServiceNotExists, "Service does not exist"),
			}, nil
		}
		log.Error(fmt.Sprintf("failed to get service %s tags", request.ServiceId), err)
		return &discovery.DeleteServiceTagsResponse{
			Response: discovery.CreateResponse(discovery.ErrInternal, err.Error()),
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
	setFilter := mutil.NewFilter(
		mutil.Tags(newTags),
	)
	updateFilter := mutil.NewFilter(
		mutil.Set(setFilter),
	)
	err = dao.UpdateService(ctx, filter, updateFilter)
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

func (ds *MetadataManager) GetSchema(ctx context.Context, request *discovery.GetSchemaRequest) (*discovery.GetSchemaResponse, error) {
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
	filter := mutil.NewBasicFilter(ctx, mutil.ServiceID(request.ServiceId), mutil.SchemaID(request.SchemaId))
	schema, err := dao.GetSchema(ctx, filter)
	if err != nil {
		return &discovery.GetSchemaResponse{
			Response: discovery.CreateResponse(discovery.ErrInternal, "GetSchema failed from mongodb."),
		}, nil
	}
	if schema == nil {
		return &discovery.GetSchemaResponse{
			Response: discovery.CreateResponse(discovery.ErrSchemaNotExists, "Do not have this schema info."),
		}, nil
	}
	return &discovery.GetSchemaResponse{
		Response:      discovery.CreateResponse(discovery.ResponseSuccess, "Get schema info successfully."),
		Schema:        schema.Schema,
		SchemaSummary: schema.SchemaSummary,
	}, nil
}

func (ds *MetadataManager) GetAllSchemas(ctx context.Context, request *discovery.GetAllSchemaRequest) (*discovery.GetAllSchemaResponse, error) {
	domain := util.ParseDomain(ctx)
	project := util.ParseProject(ctx)
	filter := mutil.NewDomainProjectFilter(domain, project, mutil.ServiceServiceID(request.ServiceId))
	svc, err := dao.GetService(ctx, filter)
	if err != nil {
		if errors.Is(err, datasource.ErrNoData) {
			log.Debug(fmt.Sprintf("service %s not exist in db", request.ServiceId))
			return &discovery.GetAllSchemaResponse{
				Response: discovery.CreateResponse(discovery.ErrServiceNotExists, "GetAllSchemas failed for service not exist"),
			}, nil
		}
		log.Error(fmt.Sprintf("get service[%s] all schemas failed, get service failed", request.ServiceId), err)
		return &discovery.GetAllSchemaResponse{
			Response: discovery.CreateResponse(discovery.ErrInternal, err.Error()),
		}, err
	}
	schemasList := svc.Service.Schemas
	if len(schemasList) == 0 {
		return &discovery.GetAllSchemaResponse{
			Response: discovery.CreateResponse(discovery.ResponseSuccess, "Do not have this schema info."),
			Schemas:  []*discovery.Schema{},
		}, nil
	}
	schemas := make([]*discovery.Schema, 0, len(schemasList))
	for _, schemaID := range schemasList {
		tempSchema := &discovery.Schema{}
		tempSchema.SchemaId = schemaID
		filter := mutil.NewDomainProjectFilter(domain, project, mutil.ServiceID(request.ServiceId), mutil.SchemaID(schemaID))
		schema, err := dao.GetSchema(ctx, filter)
		if err != nil {
			return &discovery.GetAllSchemaResponse{
				Response: discovery.CreateResponse(discovery.ErrInternal, err.Error()),
			}, err
		}
		if schema == nil {
			schemas = append(schemas, tempSchema)
			continue
		}
		tempSchema.Summary = schema.SchemaSummary
		if request.WithSchema {
			tempSchema.Schema = schema.Schema
		}
		schemas = append(schemas, tempSchema)
	}
	return &discovery.GetAllSchemaResponse{
		Response: discovery.CreateResponse(discovery.ResponseSuccess, "Get all schema info successfully."),
		Schemas:  schemas,
	}, nil
}

func (ds *MetadataManager) ExistSchema(ctx context.Context, request *discovery.GetExistenceRequest) (*discovery.GetExistenceResponse, error) {
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
	filter := mutil.NewBasicFilter(ctx, mutil.ServiceID(request.ServiceId), mutil.SchemaID(request.SchemaId))
	Schema, err := dao.GetSchema(ctx, filter)
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

func (ds *MetadataManager) DeleteSchema(ctx context.Context, request *discovery.DeleteSchemaRequest) (*discovery.DeleteSchemaResponse, error) {
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
	filter := mutil.NewBasicFilter(ctx, mutil.ServiceID(request.ServiceId), mutil.SchemaID(request.SchemaId))
	res, err := client.GetMongoClient().DocDelete(ctx, model.CollectionSchema, filter)
	if err != nil {
		return &discovery.DeleteSchemaResponse{
			Response: discovery.CreateResponse(discovery.ErrUnavailableBackend, "DeleteSchema failed for delete schema failed."),
		}, err
	}
	if !res {
		return &discovery.DeleteSchemaResponse{
			Response: discovery.CreateResponse(discovery.ErrSchemaNotExists, "DeleteSchema failed for schema not exist."),
		}, nil
	}
	return &discovery.DeleteSchemaResponse{
		Response: discovery.CreateResponse(discovery.ResponseSuccess, "Delete schema info successfully."),
	}, nil
}

func (ds *MetadataManager) ModifySchema(ctx context.Context, request *discovery.ModifySchemaRequest) (*discovery.ModifySchemaResponse, error) {
	remoteIP := util.GetIPFromContext(ctx)
	serviceID := request.ServiceId
	schemaID := request.SchemaId
	schema := discovery.Schema{
		SchemaId: request.SchemaId,
		Summary:  request.Summary,
		Schema:   request.Schema,
	}
	err := ds.modifySchema(ctx, request.ServiceId, &schema)
	if err != nil {
		log.Error(fmt.Sprintf("modify schema[%s/%s] failed, operator: %s", serviceID, schemaID, remoteIP), err)
		resp := &discovery.ModifySchemaResponse{
			Response: discovery.CreateResponseWithSCErr(err),
		}
		if err.InternalError() {
			return resp, err
		}
		return resp, nil
	}
	log.Info(fmt.Sprintf("modify schema[%s/%s] successfully, operator: %s", serviceID, schemaID, remoteIP))
	return &discovery.ModifySchemaResponse{
		Response: discovery.CreateResponse(discovery.ResponseSuccess, "modify schema info success."),
	}, nil
}

func (ds *MetadataManager) ModifySchemas(ctx context.Context, request *discovery.ModifySchemasRequest) (*discovery.ModifySchemasResponse, error) {
	filter := mutil.NewBasicFilter(ctx, mutil.ServiceServiceID(request.ServiceId))
	svc, err := dao.GetService(ctx, filter)
	if err != nil {
		if errors.Is(err, datasource.ErrNoData) {
			return &discovery.ModifySchemasResponse{Response: discovery.CreateResponse(discovery.ErrServiceNotExists, "Service not exist")}, nil
		}
		return &discovery.ModifySchemasResponse{Response: discovery.CreateResponse(discovery.ErrInternal, err.Error())}, err
	}
	respErr := ds.modifySchemas(ctx, svc.Service, request.Schemas)
	if respErr != nil {
		resp := &discovery.ModifySchemasResponse{
			Response: discovery.CreateResponseWithSCErr(respErr),
		}
		if respErr.InternalError() {
			return resp, err
		}
		return resp, nil
	}
	return &discovery.ModifySchemasResponse{
		Response: discovery.CreateResponse(discovery.ResponseSuccess, "modify schemas info success"),
	}, nil

}

func (ds *MetadataManager) modifySchemas(ctx context.Context, service *discovery.MicroService, schemas []*discovery.Schema) *errsvc.Error {
	domain := util.ParseDomain(ctx)
	project := util.ParseProject(ctx)
	remoteIP := util.GetIPFromContext(ctx)
	serviceID := service.ServiceId
	filter := mutil.NewFilter(mutil.ServiceID(serviceID))
	schemasFromDatabase, err := dao.GetSchemas(ctx, filter)
	if err != nil {
		log.Error(fmt.Sprintf("modify service %s schemas failed, get schemas failed, operator: %s", serviceID, remoteIP), err)
		return discovery.NewError(discovery.ErrUnavailableBackend, err.Error())
	}

	needUpdateSchemas, needAddSchemas, needDeleteSchemas, nonExistSchemaIds :=
		datasource.SchemasAnalysis(schemas, schemasFromDatabase, service.Schemas)

	var schemasOps []mongo.WriteModel
	var serviceOps []mongo.WriteModel
	if !ds.isSchemaEditable() {
		if len(service.Schemas) == 0 {
			res := quota.NewApplyQuotaResource(quota.TypeSchema, util.ParseDomainProject(ctx), serviceID, int64(len(nonExistSchemaIds)))
			errQuota := quota.Apply(ctx, res)
			if errQuota != nil {
				log.Error(fmt.Sprintf("modify service[%s] schemas failed, operator: %s", serviceID, remoteIP), errQuota)
				return errQuota
			}
			filter = mutil.NewDomainProjectFilter(domain, project, mutil.ServiceServiceID(serviceID))
			setFilter := mutil.NewFilter(mutil.ServiceSchemas(nonExistSchemaIds))
			updateFilter := mutil.NewFilter(mutil.Set(setFilter))
			serviceOps = append(serviceOps, mongo.NewUpdateOneModel().SetUpdate(updateFilter).SetFilter(filter))
		} else {
			if len(nonExistSchemaIds) != 0 {
				errInfo := fmt.Errorf("non-existent schemaIDs %v", nonExistSchemaIds)
				log.Error(fmt.Sprintf("modify service %s schemas failed, operator: %s", serviceID, remoteIP), err)
				return discovery.NewError(discovery.ErrUndefinedSchemaID, errInfo.Error())
			}
			for _, needUpdateSchema := range needUpdateSchemas {
				exist, err := dao.SchemaSummaryExist(ctx, serviceID, needUpdateSchema.SchemaId)
				if err != nil {
					return discovery.NewError(discovery.ErrInternal, err.Error())
				}
				if !exist {
					filter = mutil.NewDomainProjectFilter(domain, project, mutil.ServiceID(serviceID), mutil.SchemaID(needUpdateSchema.SchemaId))
					setFilter := mutil.NewFilter(
						mutil.Schema(needUpdateSchema.Schema),
						mutil.SchemaSummary(needUpdateSchema.Summary),
					)
					updateFilter := mutil.NewFilter(
						mutil.Set(setFilter),
					)
					schemasOps = append(schemasOps, mongo.NewUpdateOneModel().SetFilter(filter).SetUpdate(updateFilter))
				} else {
					log.Warn(fmt.Sprintf("schema[%s/%s] and it's summary already exist, skip to update, operator: %s",
						serviceID, needUpdateSchema.SchemaId, remoteIP))
				}
			}
		}

		for _, schema := range needAddSchemas {
			log.Info(fmt.Sprintf("add new schema[%s/%s], operator: %s", serviceID, schema.SchemaId, remoteIP))
			schemasOps = append(schemasOps, mongo.NewInsertOneModel().SetDocument(&model.Schema{
				Domain:        domain,
				Project:       project,
				ServiceID:     serviceID,
				SchemaID:      schema.SchemaId,
				Schema:        schema.Schema,
				SchemaSummary: schema.Summary,
			}))
		}
	} else {
		quotaSize := len(needAddSchemas) - len(needDeleteSchemas)
		if quotaSize > 0 {
			res := quota.NewApplyQuotaResource(quota.TypeSchema, util.ParseDomainProject(ctx), serviceID, int64(quotaSize))
			err := quota.Apply(ctx, res)
			if err != nil {
				log.Error(fmt.Sprintf("modify service[%s] schemas failed, operator: %s", serviceID, remoteIP), err)
				return err
			}
		}
		var schemaIDs []string
		for _, schema := range needAddSchemas {
			log.Info(fmt.Sprintf("add new schema[%s/%s], operator: %s", serviceID, schema.SchemaId, remoteIP))
			schemasOps = append(schemasOps, mongo.NewInsertOneModel().SetDocument(&model.Schema{
				Domain:        domain,
				Project:       project,
				ServiceID:     serviceID,
				SchemaID:      schema.SchemaId,
				Schema:        schema.Schema,
				SchemaSummary: schema.Summary,
			}))
			schemaIDs = append(schemaIDs, schema.SchemaId)
		}

		for _, schema := range needUpdateSchemas {
			log.Info(fmt.Sprintf("update schema[%s/%s], operator: %s", serviceID, schema.SchemaId, remoteIP))
			filter = mutil.NewDomainProjectFilter(domain, project, mutil.ServiceID(serviceID), mutil.SchemaID(schema.SchemaId))
			setFilter := mutil.NewFilter(
				mutil.Schema(schema.Schema),
				mutil.SchemaSummary(schema.Summary),
			)
			updateFilter := mutil.NewFilter(
				mutil.Set(setFilter),
			)
			schemasOps = append(schemasOps, mongo.NewUpdateOneModel().SetFilter(filter).SetUpdate(updateFilter))
			schemaIDs = append(schemaIDs, schema.SchemaId)
		}

		for _, schema := range needDeleteSchemas {
			log.Info(fmt.Sprintf("delete non-existent schema[%s/%s], operator: %s", serviceID, schema.SchemaId, remoteIP))
			filter := mutil.NewDomainProjectFilter(domain, project, mutil.ServiceID(serviceID), mutil.SchemaID(schema.SchemaId))
			schemasOps = append(schemasOps, mongo.NewDeleteOneModel().SetFilter(filter))
		}
		filter := mutil.NewDomainProjectFilter(domain, project, mutil.ServiceServiceID(serviceID))
		setFilter := mutil.NewFilter(mutil.ServiceSchemas(schemaIDs))
		updateFilter := mutil.NewFilter(mutil.Set(setFilter))
		serviceOps = append(serviceOps, mongo.NewUpdateOneModel().SetUpdate(updateFilter).SetFilter(filter))
	}
	if len(schemasOps) > 0 {
		_, err = client.GetMongoClient().BatchUpdate(ctx, model.CollectionSchema, schemasOps)
		if err != nil {
			return discovery.NewError(discovery.ErrInternal, err.Error())
		}
	}
	if len(serviceOps) > 0 {
		_, err = client.GetMongoClient().BatchUpdate(ctx, model.CollectionService, serviceOps)
		if err != nil {
			return discovery.NewError(discovery.ErrInternal, err.Error())
		}
	}
	return nil
}

// modifySchema will be modified in the following cases
// 1.service have no relation --> update the schema && update the service
// 2.service is editable && service have relation with the schema --> update the shema
// 3.service is editable && service have no relation with the schema --> update the schema && update the service
// 4.service can't edit && service have relation with the schema && schema summary not exist --> update the schema
func (ds *MetadataManager) modifySchema(ctx context.Context, serviceID string, schema *discovery.Schema) *errsvc.Error {
	domain := util.ParseDomain(ctx)
	project := util.ParseProject(ctx)
	filter := mutil.NewDomainProjectFilter(domain, project, mutil.ServiceServiceID(serviceID))
	remoteIP := util.GetIPFromContext(ctx)
	svc, err := dao.GetService(ctx, filter)
	if err != nil {
		if errors.Is(err, datasource.ErrNoData) {
			return discovery.NewError(discovery.ErrServiceNotExists, "Service does not exist.")
		}
		return discovery.NewError(discovery.ErrInternal, err.Error())
	}
	microservice := svc.Service
	var isExist bool
	for _, sid := range microservice.Schemas {
		if sid == schema.SchemaId {
			isExist = true
			break
		}
	}
	var newSchemas []string
	if !ds.isSchemaEditable() {
		if len(microservice.Schemas) != 0 && !isExist {
			return ErrUndefinedSchemaID
		}
		filter = mutil.NewDomainProjectFilter(domain, project, mutil.ServiceID(serviceID), mutil.SchemaID(schema.SchemaId))
		respSchema, err := dao.GetSchema(ctx, filter)
		if err != nil {
			return discovery.NewError(discovery.ErrUnavailableBackend, err.Error())
		}
		if respSchema != nil {
			if len(schema.Summary) == 0 {
				log.Error(fmt.Sprintf("modify schema %s %s failed, get schema summary failed, operator: %s",
					serviceID, schema.SchemaId, remoteIP), err)
				return ErrModifySchemaNotAllow
			}
			if len(respSchema.SchemaSummary) != 0 {
				log.Error(fmt.Sprintf("mode, schema %s %s already exist, can not be changed, operator: %s",
					serviceID, schema.SchemaId, remoteIP), err)
				return ErrModifySchemaNotAllow
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
	if len(newSchemas) != 0 {
		filter = mutil.NewDomainProjectFilter(domain, project, mutil.ServiceServiceID(serviceID))
		setFilter := mutil.NewFilter(
			mutil.ServiceSchemas(newSchemas),
		)
		updateFilter := mutil.NewFilter(
			mutil.Set(setFilter),
		)
		err = dao.UpdateService(ctx, filter, updateFilter)
		if err != nil {
			return discovery.NewError(discovery.ErrInternal, err.Error())
		}
	}
	filter = mutil.NewDomainProjectFilter(domain, project, mutil.ServiceID(serviceID), mutil.SchemaID(schema.SchemaId))
	setFilter := mutil.NewFilter(
		mutil.Schema(schema.Schema),
		mutil.SchemaSummary(schema.Summary),
	)
	updateFilter := mutil.NewFilter(mutil.Set(setFilter))
	err = dao.UpdateSchema(ctx, filter, updateFilter, options.FindOneAndUpdate().SetUpsert(true))
	if err != nil {
		return discovery.NewError(discovery.ErrInternal, err.Error())
	}
	return nil
}

func (ds *MetadataManager) AddRule(ctx context.Context, request *discovery.AddServiceRulesRequest) (*discovery.AddServiceRulesResponse, error) {
	remoteIP := util.GetIPFromContext(ctx)
	domain := util.ParseDomain(ctx)
	project := util.ParseProject(ctx)
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
	res := quota.NewApplyQuotaResource(quota.TypeRule, util.ParseDomainProject(ctx), request.ServiceId, int64(len(request.Rules)))
	errQuota := quota.Apply(ctx, res)
	if errQuota != nil {
		log.Error(fmt.Sprintf("add service[%s] rule failed, operator: %s", request.ServiceId, remoteIP), errQuota)
		response := &discovery.AddServiceRulesResponse{
			Response: discovery.CreateResponseWithSCErr(errQuota),
		}
		if errQuota.InternalError() {
			return response, errQuota
		}
		return response, nil
	}
	filter := mutil.NewDomainProjectFilter(domain, project, mutil.ServiceID(request.ServiceId))
	rules, err := dao.GetServiceRules(ctx, filter)
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
		filter = mutil.NewFilter(
			mutil.ServiceID(request.ServiceId),
			mutil.RuleAttribute(rule.Attribute),
			mutil.RulePattern(rule.Pattern),
		)
		exist, err := dao.RuleExist(ctx, filter)
		if err != nil {
			return &discovery.AddServiceRulesResponse{
				Response: discovery.CreateResponse(discovery.ErrUnavailableBackend, "Can not check rule if exist."),
			}, nil
		}
		if exist {
			continue
		}
		timestamp := strconv.FormatInt(time.Now().Unix(), 10)
		ruleAdd := &model.Rule{
			Domain:    util.ParseDomain(ctx),
			Project:   util.ParseProject(ctx),
			ServiceID: request.ServiceId,
			Rule: &discovery.ServiceRule{
				RuleId:       util.GenerateUUID(),
				RuleType:     rule.RuleType,
				Attribute:    rule.Attribute,
				Pattern:      rule.Pattern,
				Description:  rule.Description,
				Timestamp:    timestamp,
				ModTimestamp: timestamp,
			},
		}
		ruleIDs = append(ruleIDs, ruleAdd.Rule.RuleId)
		_, err = client.GetMongoClient().Insert(ctx, model.CollectionRule, ruleAdd)
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

func (ds *MetadataManager) GetRules(ctx context.Context, request *discovery.GetServiceRulesRequest) (*discovery.GetServiceRulesResponse, error) {
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
	filter := mutil.NewBasicFilter(ctx, mutil.ServiceID(request.ServiceId))
	rules, err := dao.GetServiceRules(ctx, filter)
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

func (ds *MetadataManager) DeleteRule(ctx context.Context, request *discovery.DeleteServiceRulesRequest) (*discovery.DeleteServiceRulesResponse, error) {
	exist, err := ServiceExistID(ctx, request.ServiceId)
	domain := util.ParseDomain(ctx)
	project := util.ParseProject(ctx)
	if err != nil {
		log.Error(fmt.Sprintf("failed to add tags for service %s for get service failed", request.ServiceId), err)
		return &discovery.DeleteServiceRulesResponse{
			Response: discovery.CreateResponse(discovery.ErrInternal, "Failed to check service exist"),
		}, err
	}
	if !exist {
		return &discovery.DeleteServiceRulesResponse{Response: discovery.CreateResponse(discovery.ErrServiceNotExists, "Service not exist")}, nil
	}
	var delRules []mongo.WriteModel
	for _, ruleID := range request.RuleIds {
		filter := mutil.NewDomainProjectFilter(domain, project, mutil.ServiceID(request.ServiceId), mutil.RuleRuleID(ruleID))
		exist, err := dao.RuleExist(ctx, filter)
		if err != nil {
			return &discovery.DeleteServiceRulesResponse{
				Response: discovery.CreateResponse(discovery.ErrInternal, err.Error()),
			}, err
		}
		if !exist {
			return &discovery.DeleteServiceRulesResponse{
				Response: discovery.CreateResponse(discovery.ErrRuleNotExists, "This rule does not exist."),
			}, nil
		}
		filter = mutil.NewDomainProjectFilter(domain, project, mutil.ServiceID(request.ServiceId), mutil.RuleRuleID(ruleID))
		delRules = append(delRules, mongo.NewDeleteOneModel().SetFilter(filter))
	}
	if len(delRules) > 0 {
		_, err := client.GetMongoClient().BatchDelete(ctx, model.CollectionRule, delRules)
		if err != nil {
			return &discovery.DeleteServiceRulesResponse{
				Response: discovery.CreateResponse(discovery.ErrInternal, err.Error()),
			}, err
		}
	}
	return &discovery.DeleteServiceRulesResponse{
		Response: discovery.CreateResponse(discovery.ResponseSuccess, "Delete service rules successfully."),
	}, nil
}

func (ds *MetadataManager) UpdateRule(ctx context.Context, request *discovery.UpdateServiceRuleRequest) (*discovery.UpdateServiceRuleResponse, error) {
	domain := util.ParseDomain(ctx)
	project := util.ParseProject(ctx)
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
	filter := mutil.NewDomainProjectFilter(domain, project, mutil.ServiceID(request.ServiceId))
	rules, err := dao.GetServiceRules(ctx, filter)
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
	filter = mutil.NewDomainProjectFilter(domain, project, mutil.ServiceID(request.ServiceId), mutil.RuleRuleID(request.RuleId))
	exist, err = dao.RuleExist(ctx, filter)
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
	filter = mutil.NewDomainProjectFilter(domain, project, mutil.ServiceID(request.ServiceId), mutil.RuleRuleID(request.RuleId))
	setFilter := mutil.NewFilter(
		mutil.RuleRuleType(request.Rule.RuleType),
		mutil.RulePattern(request.Rule.Pattern),
		mutil.RuleAttribute(request.Rule.Attribute),
		mutil.RuleDescription(request.Rule.Description),
		mutil.RuleModTime(strconv.FormatInt(time.Now().Unix(), baseTen)),
	)
	updateFilter := mutil.NewFilter(mutil.Set(setFilter))
	err = dao.UpdateRule(ctx, filter, updateFilter)
	if err != nil {
		return &discovery.UpdateServiceRuleResponse{
			Response: discovery.CreateResponse(discovery.ErrInternal, err.Error()),
		}, err
	}
	return &discovery.UpdateServiceRuleResponse{
		Response: discovery.CreateResponse(discovery.ResponseSuccess, "Update service rules succesfully."),
	}, nil
}

func (ds *MetadataManager) isSchemaEditable() bool {
	return !ds.SchemaNotEditable
}

func ServiceExistID(ctx context.Context, serviceID string) (bool, error) {
	filter := mutil.NewBasicFilter(ctx, mutil.ServiceServiceID(serviceID))
	return client.GetMongoClient().DocExist(ctx, model.CollectionService, filter)
}

func getServiceDetailUtil(ctx context.Context, mgs *model.Service, countOnly bool, options []string) (*discovery.ServiceDetail, error) {
	serviceDetail := new(discovery.ServiceDetail)
	serviceID := mgs.Service.ServiceId
	domainProject := util.ParseDomainProject(ctx)
	domain := util.ParseDomain(ctx)
	project := util.ParseProject(ctx)
	if countOnly {
		serviceDetail.Statics = new(discovery.Statistics)
	}
	for _, opt := range options {
		expr := opt
		switch expr {
		case "tags":
			serviceDetail.Tags = mgs.Tags
		case "rules":
			filter := mutil.NewDomainProjectFilter(domain, project, mutil.ServiceID(mgs.Service.ServiceId))
			rules, err := dao.GetServiceRules(ctx, filter)
			if err != nil {
				log.Error(fmt.Sprintf("get service %s's all rules failed", mgs.Service.ServiceId), err)
				return nil, err
			}
			for _, rule := range rules {
				rule.Timestamp = rule.ModTimestamp
			}
			serviceDetail.Rules = rules
		case "instances":
			if countOnly {
				filter := mutil.NewDomainProjectFilter(domain, project, mutil.InstanceServiceID(serviceID))
				instanceCount, err := dao.CountInstance(ctx, filter)
				if err != nil {
					log.Error(fmt.Sprintf("get number of service [%s]'s instances failed", serviceID), err)
					return nil, err
				}
				serviceDetail.Statics.Instances = &discovery.StInstance{
					Count: instanceCount,
				}
				continue
			}
			filter := mutil.NewDomainProjectFilter(domain, project, mutil.InstanceServiceID(serviceID))
			instances, err := dao.GetMicroServiceInstances(ctx, filter)
			if err != nil {
				log.Error(fmt.Sprintf("get service[%s]'s all instances failed", serviceID), err)
				return nil, err
			}
			serviceDetail.Instances = instances
		case "schemas":
			filter := mutil.NewDomainProjectFilter(domain, project, mutil.ServiceID(serviceID))
			schemas, err := dao.GetSchemas(ctx, filter)
			if err != nil {
				log.Error(fmt.Sprintf("get service %s's all schemas failed", mgs.Service.ServiceId), err)
				return nil, err
			}
			serviceDetail.SchemaInfos = schemas
		case "dependencies":
			service := mgs.Service
			dr := NewDependencyRelation(ctx, domainProject, service, service)
			consumers, err := dr.GetDependencyConsumers(WithoutSelfDependency(), WithSameDomainProject())
			if err != nil {
				log.Error(fmt.Sprintf("get service[%s][%s/%s/%s/%s]'s all consumers failed",
					service.ServiceId, service.Environment, service.AppId, service.ServiceName, service.Version), err)
			}
			providers, err := dr.GetDependencyProviders(WithoutSelfDependency(), WithSameDomainProject())
			if err != nil {
				log.Error(fmt.Sprintf("get service[%s][%s/%s/%s/%s]'s all providers failed",
					service.ServiceId, service.Environment, service.AppId, service.ServiceName, service.Version), err)
				return nil, err
			}
			serviceDetail.Consumers = consumers
			serviceDetail.Providers = providers
		case "":
			continue
		default:
			log.Info(fmt.Sprintf("request option %s is invalid", opt))
		}
	}
	return serviceDetail, nil
}

// Instance management
func (ds *MetadataManager) RegisterInstance(ctx context.Context,
	request *discovery.RegisterInstanceRequest) (*discovery.RegisterInstanceResponse, error) {

	isCustomID := true

	if len(request.Instance.InstanceId) == 0 {
		isCustomID = false
		request.Instance.InstanceId = uuid.Generator().GetInstanceID(ctx)
	}

	// if queueSize is more than 0 and channel is not full, then do fast register instance
	if fastRegConfig.QueueSize > 0 && len(GetFastRegisterInstanceService().InstEventCh) < fastRegConfig.QueueSize {
		// fast register, just add instance to channel and batch register them later
		event := &InstanceRegisterEvent{ctx, request, isCustomID, 0}
		GetFastRegisterInstanceService().AddEvent(event)

		return &discovery.RegisterInstanceResponse{
			Response:   discovery.CreateResponse(discovery.ResponseSuccess, "Register service instance successfully."),
			InstanceId: request.Instance.InstanceId,
		}, nil
	}

	return RegisterInstanceSingle(ctx, request, isCustomID)
}

func RegisterInstanceSingle(ctx context.Context, request *discovery.RegisterInstanceRequest,
	isUserDefinedID bool) (*discovery.RegisterInstanceResponse, error) {

	resp, needRegister, err := preProcessRegister(ctx, request.Instance, isUserDefinedID)

	if err != nil || !needRegister {
		log.Error("pre process instance err, or instance already existed", err)
		return resp, err
	}

	return registryInstance(ctx, request)
}

func RegisterInstanceBatch(ctx context.Context, events []*InstanceRegisterEvent) (*discovery.RegisterInstanceResponse, error) {
	instances := make([]interface{}, len(events))

	for i, event := range events {
		eventCtx := event.Ctx
		instance := event.Request.Instance

		resp, needRegister, err := preProcessRegister(eventCtx, instance, event.isCustomID)

		if err != nil || !needRegister {
			log.Error("pre process instance err, or instance existed", err)
			return resp, err
		}

		domain := util.ParseDomain(eventCtx)
		project := util.ParseProject(eventCtx)

		data := model.Instance{
			Domain:      domain,
			Project:     project,
			RefreshTime: time.Now(),
			Instance:    instance,
		}
		instances[i] = data
	}

	return registryInstances(ctx, instances)
}

func preProcessRegister(ctx context.Context, instance *discovery.MicroServiceInstance,
	isUserDefinedID bool) (*discovery.RegisterInstanceResponse, bool, error) {
	remoteIP := util.GetIPFromContext(ctx)

	// 允许自定义 id
	if isUserDefinedID {
		resp, err := datasource.GetMetadataManager().Heartbeat(ctx, &discovery.HeartbeatRequest{
			InstanceId: instance.InstanceId,
			ServiceId:  instance.ServiceId,
		})
		if err != nil || resp == nil {
			log.Error(fmt.Sprintf("register service %s's instance failed, endpoints %s, host '%s', operator %s",
				instance.ServiceId, instance.Endpoints, instance.HostName, remoteIP), err)
			return &discovery.RegisterInstanceResponse{
				Response: discovery.CreateResponse(discovery.ErrInternal, err.Error()),
			}, false, nil
		}
		switch resp.Response.GetCode() {
		case discovery.ResponseSuccess:
			log.Info(fmt.Sprintf("register instance successful, reuse instance[%s/%s], operator %s",
				instance.ServiceId, instance.InstanceId, remoteIP))
			return &discovery.RegisterInstanceResponse{
				Response:   resp.Response,
				InstanceId: instance.InstanceId,
			}, false, nil
		case discovery.ErrInstanceNotExists:
			//register a new one
		default:
			log.Error(fmt.Sprintf("register instance failed, reuse instance %s %s, operator %s",
				instance.ServiceId, instance.InstanceId, remoteIP), err)
			return &discovery.RegisterInstanceResponse{
				Response: resp.Response,
			}, false, err
		}

	}

	if err := preProcessRegisterInstance(ctx, instance); err != nil {
		log.Error(fmt.Sprintf("register service %s instance failed, endpoints %s, host %s operator %s",
			instance.ServiceId, instance.Endpoints, instance.HostName, remoteIP), err)
		return &discovery.RegisterInstanceResponse{
			Response: discovery.CreateResponseWithSCErr(err),
		}, false, nil
	}

	return &discovery.RegisterInstanceResponse{
		Response:   discovery.CreateResponse(discovery.ResponseSuccess, "process success"),
		InstanceId: instance.InstanceId,
	}, true, nil
}

func (ds *MetadataManager) ExistInstanceByID(ctx context.Context, request *discovery.MicroServiceInstanceKey) (*discovery.GetExistenceByIDResponse, error) {
	exist, _ := dao.ExistInstance(ctx, request.ServiceId, request.InstanceId)
	if !exist {
		return &discovery.GetExistenceByIDResponse{
			Response: discovery.CreateResponse(discovery.ErrInstanceNotExists, "Check instance exist failed."),
			Exist:    false,
		}, datasource.ErrInstanceNotExists
	}

	return &discovery.GetExistenceByIDResponse{
		Response: discovery.CreateResponse(discovery.ResponseSuccess, "Check service exists successfully."),
		Exist:    exist,
	}, nil
}

// GetInstance returns instance under the current domain
func (ds *MetadataManager) GetInstance(ctx context.Context, request *discovery.GetOneInstanceRequest) (*discovery.GetOneInstanceResponse, error) {
	var service *model.Service
	var err error
	var serviceIDs []string
	domain := util.ParseDomain(ctx)
	project := util.ParseProject(ctx)
	if len(request.ConsumerServiceId) > 0 {
		filter := mutil.NewDomainProjectFilter(domain, project, mutil.ServiceServiceID(request.ConsumerServiceId))
		service, err = dao.GetService(ctx, filter)
		if err != nil {
			if errors.Is(err, datasource.ErrNoData) {
				log.Debug(fmt.Sprintf("consumer does not exist, consumer %s find provider instance %s %s",
					request.ConsumerServiceId, request.ProviderServiceId, request.ProviderInstanceId))
				return &discovery.GetOneInstanceResponse{
					Response: discovery.CreateResponse(discovery.ErrServiceNotExists,
						fmt.Sprintf("Consumer[%s] does not exist.", request.ConsumerServiceId)),
				}, nil
			}
			log.Error(fmt.Sprintf(" get consumer failed, consumer %s find provider instance %s",
				request.ConsumerServiceId, request.ProviderInstanceId), err)
			return &discovery.GetOneInstanceResponse{
				Response: discovery.CreateResponse(discovery.ErrInternal, err.Error()),
			}, err
		}
	}
	filter := mutil.NewDomainProjectFilter(domain, project, mutil.ServiceServiceID(request.ProviderServiceId))
	provider, err := dao.GetService(ctx, filter)
	if err != nil {
		if errors.Is(err, datasource.ErrNoData) {
			log.Debug(fmt.Sprintf("provider does not exist, consumer %s find provider instance %s %s",
				request.ConsumerServiceId, request.ProviderServiceId, request.ProviderInstanceId))
			return &discovery.GetOneInstanceResponse{
				Response: discovery.CreateResponse(discovery.ErrServiceNotExists,
					fmt.Sprintf("Provider[%s] does not exist.", request.ProviderServiceId)),
			}, nil
		}
		log.Error(fmt.Sprintf("get provider failed, consumer %s find provider instance %s %s",
			request.ConsumerServiceId, request.ProviderServiceId, request.ProviderInstanceId), err)
		return &discovery.GetOneInstanceResponse{
			Response: discovery.CreateResponse(discovery.ErrInternal, err.Error()),
		}, err
	}
	findFlag := func() string {
		return fmt.Sprintf("consumer[%s][%s/%s/%s/%s] find provider[%s][%s/%s/%s/%s] instance[%s]",
			request.ConsumerServiceId, service.Service.Environment, service.Service.AppId, service.Service.ServiceName, service.Service.Version,
			provider.Service.ServiceId, provider.Service.Environment, provider.Service.AppId, provider.Service.ServiceName, provider.Service.Version,
			request.ProviderInstanceId)
	}

	domainProject := util.ParseDomainProject(ctx)
	services, err := filterServices(ctx, discovery.MicroServiceToKey(domainProject, provider.Service))
	if err != nil {
		log.Error(fmt.Sprintf("get instance failed %s", findFlag()), err)
		return &discovery.GetOneInstanceResponse{
			Response: discovery.CreateResponse(discovery.ErrInternal, err.Error()),
		}, err
	}
	rev, _ := ctx.Value(util.CtxRequestRevision).(string)
	serviceIDs = filterServiceIDs(ctx, request.ConsumerServiceId, request.Tags, services)
	if len(serviceIDs) == 0 {
		mes := fmt.Errorf("%s failed, provider instance does not exist", findFlag())
		log.Error("query service failed", mes)
		return &discovery.GetOneInstanceResponse{
			Response: discovery.CreateResponse(discovery.ErrInstanceNotExists, mes.Error()),
		}, nil
	}
	instances, ok := cache.GetMicroServiceInstancesByID(request.ProviderServiceId)
	if !ok {
		instances, err = dao.GetMicroServiceInstancesByID(ctx, request.ProviderServiceId)
		if err != nil {
			log.Error(fmt.Sprintf("get instance failed %s", findFlag()), err)
			return &discovery.GetOneInstanceResponse{
				Response: discovery.CreateResponse(discovery.ErrInternal, err.Error()),
			}, err
		}
	}

	instance := instances[0]
	// use explicit instanceId to query
	if len(request.ProviderInstanceId) != 0 {
		isExist := false
		for _, ins := range instances {
			if ins.InstanceId == request.ProviderInstanceId {
				instance = ins
				isExist = true
				break
			}
		}
		if !isExist {
			mes := fmt.Errorf("%s failed, provider instance does not exist", findFlag())
			log.Error("get instance failed", mes)
			return &discovery.GetOneInstanceResponse{
				Response: discovery.CreateResponse(discovery.ErrInstanceNotExists, mes.Error()),
			}, nil
		}
	}

	newRev, _ := formatRevision(request.ConsumerServiceId, instances)
	if rev == newRev {
		instance = nil // for gRPC
	}
	// TODO support gRPC output context
	_ = util.WithResponseRev(ctx, newRev)

	return &discovery.GetOneInstanceResponse{
		Response: discovery.CreateResponse(discovery.ResponseSuccess, "Get instance successfully."),
		Instance: instance,
	}, nil
}

func (ds *MetadataManager) GetInstances(ctx context.Context, request *discovery.GetInstancesRequest) (*discovery.GetInstancesResponse, error) {
	service := &model.Service{}
	var err error

	if len(request.ConsumerServiceId) > 0 {
		var exist bool
		service, exist = cache.GetServiceByID(ctx, request.ConsumerServiceId)
		if !exist {
			service, err = dao.GetServiceByID(ctx, request.ConsumerServiceId)
			if err != nil {
				if errors.Is(err, datasource.ErrNoData) {
					log.Debug(fmt.Sprintf("consumer does not exist, consumer %s find provider %s instances",
						request.ConsumerServiceId, request.ProviderServiceId))
					return &discovery.GetInstancesResponse{
						Response: discovery.CreateResponse(discovery.ErrServiceNotExists,
							fmt.Sprintf("Consumer[%s] does not exist.", request.ConsumerServiceId)),
					}, nil
				}
				log.Error(fmt.Sprintf("get consumer failed, consumer %s find provider %s instances",
					request.ConsumerServiceId, request.ProviderServiceId), err)
				return &discovery.GetInstancesResponse{
					Response: discovery.CreateResponse(discovery.ErrInternal, err.Error()),
				}, err
			}
		}
	}

	provider, ok := cache.GetServiceByID(ctx, request.ProviderServiceId)
	if !ok {
		provider, err = dao.GetServiceByID(ctx, request.ProviderServiceId)
		if err != nil {
			if errors.Is(err, datasource.ErrNoData) {
				log.Debug(fmt.Sprintf("provider does not exist, consumer %s find provider %s  instances",
					request.ConsumerServiceId, request.ProviderServiceId))
				return &discovery.GetInstancesResponse{
					Response: discovery.CreateResponse(discovery.ErrServiceNotExists,
						fmt.Sprintf("provider[%s] does not exist.", request.ProviderServiceId)),
				}, nil
			}
			log.Error(fmt.Sprintf("get provider failed, consumer %s find provider instances %s",
				request.ConsumerServiceId, request.ProviderServiceId), err)
			return &discovery.GetInstancesResponse{
				Response: discovery.CreateResponse(discovery.ErrInternal, err.Error()),
			}, err
		}
	}

	findFlag := func() string {
		return fmt.Sprintf("consumer[%s][%s/%s/%s/%s] find provider[%s][%s/%s/%s/%s] instances",
			request.ConsumerServiceId, service.Service.Environment, service.Service.AppId, service.Service.ServiceName, service.Service.Version,
			provider.Service.ServiceId, provider.Service.Environment, provider.Service.AppId, provider.Service.ServiceName, provider.Service.Version)
	}

	rev, _ := ctx.Value(util.CtxRequestRevision).(string)
	serviceIDs := filterServiceIDs(ctx, request.ConsumerServiceId, request.Tags, []*model.Service{provider})
	if len(serviceIDs) == 0 {
		mes := fmt.Errorf("%s failed, provider does not exist", findFlag())
		log.Error("query service failed", mes)
		return &discovery.GetInstancesResponse{
			Response: discovery.CreateResponse(discovery.ErrServiceNotExists, mes.Error()),
		}, nil
	}
	instances, ok := cache.GetMicroServiceInstancesByID(request.ProviderServiceId)
	if !ok {
		instances, err = dao.GetMicroServiceInstancesByID(ctx, request.ProviderServiceId)
		if err != nil {
			log.Error(fmt.Sprintf("get instances failed %s", findFlag()), err)
			return &discovery.GetInstancesResponse{
				Response: discovery.CreateResponse(discovery.ErrInternal, err.Error()),
			}, err
		}
	}
	newRev, _ := formatRevision(request.ConsumerServiceId, instances)
	if rev == newRev {
		instances = nil // for gRPC
	}
	_ = util.WithResponseRev(ctx, newRev)
	return &discovery.GetInstancesResponse{
		Response:  discovery.CreateResponse(discovery.ResponseSuccess, "Query service instances successfully."),
		Instances: instances,
	}, nil
}

// GetProviderInstances returns instances under the specified domain
func (ds *MetadataManager) GetProviderInstances(ctx context.Context, request *discovery.GetProviderInstancesRequest) (instances []*discovery.MicroServiceInstance, rev string, err error) {
	filter := mutil.NewBasicFilter(ctx, mutil.InstanceServiceID(request.ProviderServiceId))
	instances, err = dao.GetMicroServiceInstances(ctx, filter)
	if err != nil {
		return
	}
	return instances, "", nil
}

func (ds *MetadataManager) GetAllInstances(ctx context.Context, request *discovery.GetAllInstancesRequest) (*discovery.GetAllInstancesResponse, error) {
	filter := mutil.NewBasicFilter(ctx)
	findRes, err := client.GetMongoClient().Find(ctx, model.CollectionInstance, filter)
	if err != nil {
		return nil, err
	}
	resp := &discovery.GetAllInstancesResponse{
		Response: discovery.CreateResponse(discovery.ResponseSuccess, "Get all instances successfully"),
	}

	for findRes.Next(ctx) {
		var instance model.Instance
		err := findRes.Decode(&instance)
		if err != nil {
			return &discovery.GetAllInstancesResponse{
				Response: discovery.CreateResponse(discovery.ErrInternal, err.Error()),
			}, err
		}
		resp.Instances = append(resp.Instances, instance.Instance)
	}

	return resp, nil
}

func (ds *MetadataManager) BatchGetProviderInstances(ctx context.Context, request *discovery.BatchGetInstancesRequest) (instances []*discovery.MicroServiceInstance, rev string, err error) {
	if request == nil || len(request.ServiceIds) == 0 {
		return nil, "", mutil.ErrInvalidParam
	}
	domain := util.ParseDomain(ctx)
	project := util.ParseProject(ctx)

	for _, providerServiceID := range request.ServiceIds {
		filter := mutil.NewDomainProjectFilter(domain, project, mutil.InstanceServiceID(providerServiceID))
		findRes, err := client.GetMongoClient().Find(ctx, model.CollectionInstance, filter)
		if err != nil {
			return instances, "", nil
		}

		for findRes.Next(ctx) {
			var mongoInstance model.Instance
			err := findRes.Decode(&mongoInstance)
			if err == nil {
				instances = append(instances, mongoInstance.Instance)
			}
		}
	}

	return instances, "", nil
}

// FindInstances returns instances under the specified domain
func (ds *MetadataManager) FindInstances(ctx context.Context, request *discovery.FindInstancesRequest) (*discovery.FindInstancesResponse, error) {
	provider := &discovery.MicroServiceKey{
		Tenant:      util.ParseTargetDomainProject(ctx),
		Environment: request.Environment,
		AppId:       request.AppId,
		ServiceName: request.ServiceName,
		Alias:       request.Alias,
		Version:     request.VersionRule,
	}
	rev, ok := ctx.Value(util.CtxRequestRevision).(string)
	if !ok {
		err := errors.New("rev request context is not type string")
		log.Error("", err)
		return &discovery.FindInstancesResponse{
			Response: discovery.CreateResponse(discovery.ErrInternal, err.Error()),
		}, err
	}

	if datasource.IsGlobal(provider) {
		return ds.findSharedServiceInstance(ctx, request, provider, rev)
	}

	return ds.findInstance(ctx, request, provider, rev)
}

func (ds *MetadataManager) UpdateInstanceStatus(ctx context.Context, request *discovery.UpdateInstanceStatusRequest) (*discovery.UpdateInstanceStatusResponse, error) {
	updateStatusFlag := util.StringJoin([]string{request.ServiceId, request.InstanceId, request.Status}, "/")

	// todo finish get instance
	filter := mutil.NewBasicFilter(ctx, mutil.InstanceServiceID(request.ServiceId), mutil.InstanceInstanceID(request.InstanceId))
	instance, err := dao.GetInstance(ctx, filter)
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
	copyInstanceRef.Instance.Status = request.Status
	setFilter := mutil.NewFilter(
		mutil.InstanceModTime(strconv.FormatInt(time.Now().Unix(), baseTen)),
		mutil.InstanceStatus(copyInstanceRef.Instance.Status),
	)
	updateFilter := mutil.NewFilter(mutil.Set(setFilter))
	if err := dao.UpdateInstance(ctx, filter, updateFilter); err != nil {
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

func (ds *MetadataManager) UpdateInstanceProperties(ctx context.Context, request *discovery.UpdateInstancePropsRequest) (*discovery.UpdateInstancePropsResponse, error) {
	instanceFlag := util.StringJoin([]string{request.ServiceId, request.InstanceId}, "/")
	domain := util.ParseDomain(ctx)
	project := util.ParseProject(ctx)
	filter := mutil.NewDomainProjectFilter(domain, project, mutil.InstanceServiceID(request.ServiceId), mutil.InstanceInstanceID(request.InstanceId))
	instance, err := dao.GetInstance(ctx, filter)
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
	copyInstanceRef.Instance.Properties = request.Properties

	// todo finish update instance
	filter = mutil.NewDomainProjectFilter(domain, project, mutil.InstanceServiceID(request.ServiceId), mutil.InstanceInstanceID(request.InstanceId))
	setFilter := mutil.NewFilter(
		mutil.InstanceModTime(strconv.FormatInt(time.Now().Unix(), baseTen)),
		mutil.InstanceProperties(copyInstanceRef.Instance.Properties),
	)
	updateFilter := mutil.NewFilter(mutil.Set(setFilter))
	if err := dao.UpdateInstance(ctx, filter, updateFilter); err != nil {
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

func (ds *MetadataManager) UnregisterInstance(ctx context.Context, request *discovery.UnregisterInstanceRequest) (*discovery.UnregisterInstanceResponse, error) {
	remoteIP := util.GetIPFromContext(ctx)
	serviceID := request.ServiceId
	instanceID := request.InstanceId

	instanceFlag := util.StringJoin([]string{serviceID, instanceID}, "/")

	filter := mutil.NewBasicFilter(ctx, mutil.InstanceServiceID(serviceID), mutil.InstanceInstanceID(instanceID))
	result, err := client.GetMongoClient().Delete(ctx, model.CollectionInstance, filter)
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

func (ds *MetadataManager) Heartbeat(ctx context.Context, request *discovery.HeartbeatRequest) (*discovery.HeartbeatResponse, error) {
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

func (ds *MetadataManager) HeartbeatSet(ctx context.Context, request *discovery.HeartbeatSetRequest) (*discovery.HeartbeatSetResponse, error) {
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

func (ds *MetadataManager) BatchFind(ctx context.Context, request *discovery.BatchFindInstancesRequest) (*discovery.BatchFindInstancesResponse, error) {
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
	ttl := instance.HealthCheck.Interval * (instance.HealthCheck.Times + 1)

	instanceFlag := fmt.Sprintf("ttl %ds, endpoints %v, host '%s', serviceID %s",
		ttl, instance.Endpoints, instance.HostName, instance.ServiceId)

	instanceID := instance.InstanceId
	data := &model.Instance{
		Domain:      domain,
		Project:     project,
		RefreshTime: time.Now(),
		Instance:    instance,
	}

	insertRes, err := client.GetMongoClient().Insert(ctx, model.CollectionInstance, data)
	if err != nil {
		if client.IsDuplicateKey(err) {
			return &discovery.RegisterInstanceResponse{
				Response:   discovery.CreateResponse(discovery.ResponseSuccess, "Register service instance successfully."),
				InstanceId: instanceID,
			}, nil
		}
		log.Error(fmt.Sprintf("register instance failed %s instanceID %s operator %s", instanceFlag, instanceID, remoteIP), err)
		return &discovery.RegisterInstanceResponse{
			Response: discovery.CreateResponse(discovery.ErrUnavailableBackend, err.Error()),
		}, err
	}

	// need to complete the instance offline function in time, so you need to check the heartbeat after registering the instance
	err = heartbeat.Instance().CheckInstance(ctx, instance)
	if err != nil {
		log.Error(fmt.Sprintf("fail to check instance, instance[%s]. operator %s", instance.InstanceId, remoteIP), err)
	}

	log.Info(fmt.Sprintf("register instance %s, instanceID %s, operator %s",
		instanceFlag, insertRes.InsertedID, remoteIP))
	return &discovery.RegisterInstanceResponse{
		Response:   discovery.CreateResponse(discovery.ResponseSuccess, "Register service instance successfully."),
		InstanceId: instanceID,
	}, nil
}

func registryInstances(ctx context.Context, instances []interface{}) (*discovery.RegisterInstanceResponse, error) {
	opts := options.InsertManyOptions{}
	opts.SetOrdered(false)
	opts.SetBypassDocumentValidation(true)
	_, err := client.GetMongoClient().BatchInsert(ctx, model.CollectionInstance, instances, &opts)

	if err != nil {
		log.Error("Batch register instance failed", err)
		return &discovery.RegisterInstanceResponse{
			Response: discovery.CreateResponse(discovery.ErrUnavailableBackend, err.Error()),
		}, err
	}

	return &discovery.RegisterInstanceResponse{
		Response: discovery.CreateResponse(discovery.ResponseSuccess, "Register service instance successfully."),
	}, nil
}

func (ds *MetadataManager) findSharedServiceInstance(ctx context.Context, request *discovery.FindInstancesRequest, provider *discovery.MicroServiceKey, rev string) (*discovery.FindInstancesResponse, error) {
	var err error
	// it means the shared micro-services must be the same env with SC.
	provider.Environment = apt.Service.Environment
	findFlag := func() string {
		return fmt.Sprintf("find shared provider[%s/%s/%s/%s]", provider.Environment, provider.AppId, provider.ServiceName, provider.Version)
	}
	basicFilterServices, err := servicesBasicFilter(ctx, provider)
	if err != nil {
		log.Error(fmt.Sprintf("find shared service instance failed %s", findFlag()), err)
		return &discovery.FindInstancesResponse{
			Response: discovery.CreateResponse(discovery.ErrInternal, err.Error()),
		}, err
	}
	services, err := filterServices(ctx, provider)
	if err != nil {
		log.Error(fmt.Sprintf("find shared service instance failed %s", findFlag()), err)
		return &discovery.FindInstancesResponse{
			Response: discovery.CreateResponse(discovery.ErrInternal, err.Error()),
		}, err
	}
	if services == nil && len(basicFilterServices) == 0 {
		mes := fmt.Errorf("%s failed, provider does not exist", findFlag())
		log.Error("find shared service instance failed", mes)
		return &discovery.FindInstancesResponse{
			Response: discovery.CreateResponse(discovery.ErrServiceNotExists, mes.Error()),
		}, nil
	}
	serviceIDs := filterServiceIDs(ctx, request.ConsumerServiceId, request.Tags, services)
	inFilter := mutil.NewFilter(mutil.In(serviceIDs))
	filter := mutil.NewFilter(mutil.InstanceServiceID(inFilter))
	option := &options.FindOptions{Sort: bson.M{mutil.ConnectWithDot([]string{model.ColumnInstance, model.ColumnVersion}): -1}}
	instances, err := dao.GetMicroServiceInstances(ctx, filter, option)
	if err != nil {
		log.Error(fmt.Sprintf("find shared service instance failed %s", findFlag()), err)
		return &discovery.FindInstancesResponse{
			Response: discovery.CreateResponse(discovery.ErrInternal, err.Error()),
		}, err
	}
	newRev, _ := formatRevision(request.ConsumerServiceId, instances)
	if rev == newRev {
		instances = nil // for gRPC
	}
	// TODO support gRPC output context
	_ = util.WithResponseRev(ctx, newRev)
	return &discovery.FindInstancesResponse{
		Response:  discovery.CreateResponse(discovery.ResponseSuccess, "Query service instances successfully."),
		Instances: instances,
	}, nil
}

func (ds *MetadataManager) findInstance(ctx context.Context, request *discovery.FindInstancesRequest, provider *discovery.MicroServiceKey, rev string) (*discovery.FindInstancesResponse, error) {
	var err error
	domainProject := util.ParseDomainProject(ctx)
	service := &model.Service{Service: &discovery.MicroService{Environment: request.Environment}}
	if len(request.ConsumerServiceId) > 0 {
		filter := mutil.NewBasicFilter(ctx, mutil.ServiceServiceID(request.ConsumerServiceId))
		service, err = dao.GetService(ctx, filter)
		if err != nil {
			if errors.Is(err, datasource.ErrNoData) {
				log.Debug(fmt.Sprintf("consumer does not exist, consumer %s find provider %s/%s/%s/%s",
					request.ConsumerServiceId, request.Environment, request.AppId, request.ServiceName, request.VersionRule))
				return &discovery.FindInstancesResponse{
					Response: discovery.CreateResponse(discovery.ErrServiceNotExists,
						fmt.Sprintf("Consumer[%s] does not exist.", request.ConsumerServiceId)),
				}, nil
			}
			log.Error(fmt.Sprintf("get consumer failed, consumer %s find provider %s/%s/%s/%s",
				request.ConsumerServiceId, request.Environment, request.AppId, request.ServiceName, request.VersionRule), err)
			return &discovery.FindInstancesResponse{
				Response: discovery.CreateResponse(discovery.ErrInternal, err.Error()),
			}, err
		}
		provider.Environment = service.Service.Environment
	}

	// provider is not a shared micro-service,
	// only allow shared micro-service instances found request different domains.
	ctx = util.SetTargetDomainProject(ctx, util.ParseDomain(ctx), util.ParseProject(ctx))
	provider.Tenant = util.ParseTargetDomainProject(ctx)

	findFlag := func() string {
		return fmt.Sprintf("Consumer[%s][%s/%s/%s/%s] find provider[%s/%s/%s/%s]",
			request.ConsumerServiceId, service.Service.Environment, service.Service.AppId, service.Service.ServiceName, service.Service.Version,
			provider.Environment, provider.AppId, provider.ServiceName, provider.Version)
	}
	basicFilterServices, err := servicesBasicFilter(ctx, provider)
	if err != nil {
		log.Error(fmt.Sprintf("find instance failed %s", findFlag()), err)
		return &discovery.FindInstancesResponse{
			Response: discovery.CreateResponse(discovery.ErrInternal, err.Error()),
		}, err
	}
	services, err := filterServices(ctx, provider)
	if err != nil {
		log.Error(fmt.Sprintf("find instance failed %s", findFlag()), err)
		return &discovery.FindInstancesResponse{
			Response: discovery.CreateResponse(discovery.ErrInternal, err.Error()),
		}, err
	}
	if services == nil && len(basicFilterServices) == 0 {
		mes := fmt.Errorf("%s failed, provider does not exist", findFlag())
		log.Error("find instance failed", mes)
		return &discovery.FindInstancesResponse{
			Response: discovery.CreateResponse(discovery.ErrServiceNotExists, mes.Error()),
		}, nil
	}
	serviceIDs := filterServiceIDs(ctx, request.ConsumerServiceId, request.Tags, services)
	inFilter := mutil.NewFilter(mutil.In(serviceIDs))
	filter := mutil.NewFilter(mutil.InstanceServiceID(inFilter))
	option := &options.FindOptions{Sort: bson.M{mutil.ConnectWithDot([]string{model.ColumnInstance, model.ColumnVersion}): -1}}
	instances, err := dao.GetMicroServiceInstances(ctx, filter, option)
	if err != nil {
		log.Error(fmt.Sprintf("find instance failed %s", findFlag()), err)
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
			err = AddServiceVersionRule(ctx, domainProject, service.Service, provider)
		} else {
			mes := fmt.Errorf("%s failed, provider does not exist", findFlag())
			log.Error("add service version rule failed", mes)
			return &discovery.FindInstancesResponse{
				Response: discovery.CreateResponse(discovery.ErrServiceNotExists, mes.Error()),
			}, nil
		}
		if err != nil {
			log.Error(fmt.Sprintf("add service version rule failed %s", findFlag()), err)
			return &discovery.FindInstancesResponse{
				Response: discovery.CreateResponse(discovery.ErrInternal, err.Error()),
			}, err
		}
	}
	newRev, _ := formatRevision(request.ConsumerServiceId, instances)
	if rev == newRev {
		instances = nil // for gRPC
	}
	// TODO support gRPC output context
	_ = util.WithResponseRev(ctx, newRev)
	return &discovery.FindInstancesResponse{
		Response:  discovery.CreateResponse(discovery.ResponseSuccess, "Query service instances successfully."),
		Instances: instances,
	}, nil
}

func (ds *MetadataManager) reshapeProviderKey(ctx context.Context, provider *discovery.MicroServiceKey, providerID string) (*discovery.MicroServiceKey, error) {
	//维护version的规则,service name 可能是别名，所以重新获取
	filter := mutil.NewBasicFilter(ctx, mutil.ServiceServiceID(providerID))
	providerService, err := dao.GetService(ctx, filter)
	if err != nil {
		return nil, err
	}

	versionRule := provider.Version
	provider = discovery.MicroServiceToKey(provider.Tenant, providerService.Service)
	provider.Version = versionRule
	return provider, nil
}

func AddServiceVersionRule(ctx context.Context, domainProject string, consumer *discovery.MicroService, provider *discovery.MicroServiceKey) error {
	consumerKey := discovery.MicroServiceToKey(domainProject, consumer)
	exist, err := DependencyRuleExist(ctx, provider, consumerKey)
	if exist || err != nil {
		return err
	}

	r := &discovery.ConsumerDependency{
		Consumer:  consumerKey,
		Providers: []*discovery.MicroServiceKey{provider},
		Override:  false,
	}
	err = syncDependencyRule(ctx, domainProject, r)

	if err != nil {
		return err
	}

	return nil
}

func DependencyRuleExist(ctx context.Context, provider *discovery.MicroServiceKey, consumer *discovery.MicroServiceKey) (bool, error) {
	targetDomainProject := provider.Tenant
	if len(targetDomainProject) == 0 {
		targetDomainProject = consumer.Tenant
	}
	consumerKey := GenerateConsumerDependencyRuleKey(consumer.Tenant, consumer)
	existed, err := DependencyRuleExistUtil(ctx, consumerKey, provider)
	if err != nil || existed {
		return existed, err
	}
	providerKey := GenerateProviderDependencyRuleKey(targetDomainProject, provider)
	return DependencyRuleExistUtil(ctx, providerKey, consumer)
}

func DependencyRuleExistUtil(ctx context.Context, key bson.M, target *discovery.MicroServiceKey) (bool, error) {
	compareData, err := TransferToMicroServiceDependency(ctx, key)
	if err != nil {
		return false, err
	}

	if len(compareData.Dependency) != 0 {
		isEqual, err := datasource.ContainServiceDependency(compareData.Dependency, target)
		if err != nil {
			return false, err
		}
		if isEqual {
			return true, nil
		}
	}
	return false, nil
}

func KeepAliveLease(ctx context.Context, request *discovery.HeartbeatRequest) *errsvc.Error {
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

func (ds *MetadataManager) batchFindServices(ctx context.Context, request *discovery.BatchFindInstancesRequest) (*discovery.BatchFindResult, error) {
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

func (ds *MetadataManager) batchFindInstances(ctx context.Context, request *discovery.BatchFindInstancesRequest) (*discovery.BatchFindResult, error) {
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

func preProcessRegisterInstance(ctx context.Context, instance *discovery.MicroServiceInstance) *errsvc.Error {
	if len(instance.Status) == 0 {
		instance.Status = discovery.MSI_UP
	}

	if len(instance.InstanceId) == 0 {
		instance.InstanceId = uuid.Generator().GetInstanceID(ctx)
	}

	instance.Timestamp = strconv.FormatInt(time.Now().Unix(), 10)
	instance.ModTimestamp = instance.Timestamp

	// 这里应该根据租约计时
	renewalInterval := apt.RegistryDefaultLeaseRenewalInterval
	retryTimes := apt.RegistryDefaultLeaseRetryTimes
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

	cacheService, ok := cache.GetServiceByID(ctx, instance.ServiceId)

	var microService *discovery.MicroService
	if ok {
		microService = cacheService.Service
		instance.Version = microService.Version
	} else {
		filter := mutil.NewBasicFilter(ctx, mutil.ServiceServiceID(instance.ServiceId))
		microservice, err := dao.GetService(ctx, filter)
		if err != nil {
			log.Error("Get service failed", err)
			return discovery.NewError(discovery.ErrServiceNotExists, "invalid 'serviceID' in request body.")
		}
		instance.Version = microservice.Service.Version
	}

	return nil
}

// servicesBasicFilter query services with domain, project, env, appID, serviceName, alias
func servicesBasicFilter(ctx context.Context, key *discovery.MicroServiceKey) ([]*model.Service, error) {
	tenant := strings.Split(key.Tenant, "/")
	if len(tenant) != 2 {
		return nil, errors.New("invalid 'domain' or 'project'")
	}
	serviceNameOption := mutil.ServiceServiceName(key.ServiceName)
	if len(key.Alias) > 0 {
		serviceNameOption = mutil.Or(serviceNameOption, mutil.ServiceAlias(key.Alias))
	}
	filter := mutil.NewDomainProjectFilter(tenant[0], tenant[1],
		mutil.ServiceEnv(key.Environment),
		mutil.ServiceAppID(key.AppId),
		serviceNameOption,
	)
	rangeIdx := strings.Index(key.Version, "-")
	// if the version number is clear, need to add the version number to query
	switch {
	case key.Version == "latest":
		return dao.GetServices(ctx, filter)
	case len(key.Version) > 0 && key.Version[len(key.Version)-1:] == "+":
		return dao.GetServices(ctx, filter)
	case rangeIdx > 0:
		return dao.GetServices(ctx, filter)
	default:
		filter[mutil.ConnectWithDot([]string{model.ColumnService, model.ColumnVersion})] = key.Version
		return dao.GetServices(ctx, filter)
	}
}

func filterServices(ctx context.Context, key *discovery.MicroServiceKey) ([]*model.Service, error) {
	tenant := strings.Split(key.Tenant, "/")
	if len(tenant) != 2 {
		return nil, errors.New("invalid 'domain' or 'project'")
	}
	rangeIdx := strings.Index(key.Version, "-")
	serviceNameOption := mutil.ServiceServiceName(key.ServiceName)
	if len(key.Alias) > 0 {
		serviceNameOption = mutil.Or(serviceNameOption, mutil.ServiceAlias(key.Alias))
	}
	filter := mutil.NewDomainProjectFilter(tenant[0], tenant[1],
		mutil.ServiceEnv(key.Environment),
		mutil.ServiceAppID(key.AppId),
		serviceNameOption,
	)
	switch {
	case key.Version == "latest":
		findOption := &options.FindOptions{Sort: bson.M{mutil.ConnectWithDot([]string{model.ColumnService, model.ColumnVersion}): -1}}
		return dao.GetServices(ctx, filter, findOption)
	case len(key.Version) > 0 && key.Version[len(key.Version)-1:] == "+":
		start := key.Version[:len(key.Version)-1]
		filter[mutil.ConnectWithDot([]string{model.ColumnService, model.ColumnVersion})] = bson.M{"$gte": start}
		return dao.GetServices(ctx, filter)
	case rangeIdx > 0:
		start := key.Version[:rangeIdx]
		end := key.Version[rangeIdx+1:]
		filter[mutil.ConnectWithDot([]string{model.ColumnService, model.ColumnVersion})] = bson.M{"$gte": start, "$lte": end}
		return dao.GetServices(ctx, filter)
	default:
		filter[mutil.ConnectWithDot([]string{model.ColumnService, model.ColumnVersion})] = key.Version
		return dao.GetServices(ctx, filter)
	}
}

func filterServiceIDs(ctx context.Context, consumerID string, tags []string, services []*model.Service) []string {
	var filterService []*model.Service
	serviceIDs := make([]string, 0)
	if len(services) == 0 {
		return serviceIDs
	}
	filterService = filterTags(services, tags)
	filterService = filterAccess(ctx, consumerID, filterService)
	for _, service := range filterService {
		serviceIDs = append(serviceIDs, service.Service.ServiceId)
	}
	return serviceIDs
}

func filterTags(services []*model.Service, tags []string) []*model.Service {
	if len(tags) == 0 {
		return services
	}
	var newServices []*model.Service
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

func filterAccess(ctx context.Context, consumerID string, services []*model.Service) []*model.Service {
	newServices := make([]*model.Service, 0)
	for _, service := range services {
		if err := accessible(ctx, consumerID, service.Service.ServiceId); err != nil {
			findFlag := fmt.Sprintf("consumer '%s' find provider %s/%s/%s", consumerID,
				service.Service.AppId, service.Service.ServiceName, service.Service.Version)
			log.Error(fmt.Sprintf("accessible filter failed, %s", findFlag), err)
			continue
		}
		newServices = append(newServices, service)
	}
	return newServices
}

func accessible(ctx context.Context, consumerID string, providerID string) *errsvc.Error {
	if len(consumerID) == 0 {
		return nil
	}

	consumerDomain, consumerProject := util.ParseDomain(ctx), util.ParseProject(ctx)
	providerDomain, providerProject := util.ParseTargetDomain(ctx), util.ParseTargetProject(ctx)

	filter := mutil.NewDomainProjectFilter(consumerDomain, consumerProject, mutil.ServiceServiceID(consumerID))
	consumerService, err := dao.GetService(ctx, filter)
	if err != nil {
		return discovery.NewError(discovery.ErrInternal, fmt.Sprintf("an error occurred in query consumer(%s)", err.Error()))
	}
	if consumerService == nil {
		return discovery.NewError(discovery.ErrServiceNotExists, "consumer serviceID is invalid")
	}

	filter = mutil.NewDomainProjectFilter(providerDomain, providerProject, mutil.ServiceServiceID(providerID))
	// 跨应用权限
	providerService, err := dao.GetService(ctx, filter)
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
	filter = mutil.NewDomainProjectFilter(providerDomain, providerProject, mutil.ServiceID(providerID))
	rules, err := dao.GetRules(ctx, filter)
	if err != nil {
		return discovery.NewError(discovery.ErrInternal, fmt.Sprintf("an error occurred in query provider rules(%s)", err.Error()))
	}

	if len(rules) == 0 {
		return nil
	}
	return MatchRules(rules, consumerService.Service, consumerService.Tags)
}

func MatchRules(rulesOfProvider []*model.Rule, consumer *discovery.MicroService, tagsOfConsumer map[string]string) *errsvc.Error {
	if consumer == nil {
		return discovery.NewError(discovery.ErrInvalidParams, "consumer is nil")
	}

	if len(rulesOfProvider) <= 0 {
		return nil
	}
	if rulesOfProvider[0].Rule.RuleType == "WHITE" {
		return patternWhiteList(rulesOfProvider, tagsOfConsumer, consumer)
	}
	return patternBlackList(rulesOfProvider, tagsOfConsumer, consumer)
}

func parsePattern(v reflect.Value, rule *discovery.ServiceRule, tagsOfConsumer map[string]string, consumerID string) (string, *errsvc.Error) {
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

func patternWhiteList(rulesOfProvider []*model.Rule, tagsOfConsumer map[string]string, consumer *discovery.MicroService) *errsvc.Error {
	v := reflect.Indirect(reflect.ValueOf(consumer))
	consumerID := consumer.ServiceId
	for _, rule := range rulesOfProvider {
		value, err := parsePattern(v, rule.Rule, tagsOfConsumer, consumerID)
		if err != nil {
			return err
		}
		if len(value) == 0 {
			continue
		}

		match, _ := regexp.MatchString(rule.Rule.Pattern, value)
		if match {
			log.Info(fmt.Sprintf("consumer[%s][%s/%s/%s/%s] match white list, rule.Pattern is %s, value is %s",
				consumerID, consumer.Environment, consumer.AppId, consumer.ServiceName, consumer.Version,
				rule.Rule.Pattern, value))
			return nil
		}
	}
	return discovery.NewError(discovery.ErrPermissionDeny, "not found in white list")
}

func patternBlackList(rulesOfProvider []*model.Rule, tagsOfConsumer map[string]string, consumer *discovery.MicroService) *errsvc.Error {
	v := reflect.Indirect(reflect.ValueOf(consumer))
	consumerID := consumer.ServiceId
	for _, rule := range rulesOfProvider {
		var value string
		value, err := parsePattern(v, rule.Rule, tagsOfConsumer, consumerID)
		if err != nil {
			return err
		}
		if len(value) == 0 {
			continue
		}

		match, _ := regexp.MatchString(rule.Rule.Pattern, value)
		if match {
			log.Warn(fmt.Sprintf("no permission to access, consumer[%s][%s/%s/%s/%s] match black list, rule.Pattern is %s, value is %s",
				consumerID, consumer.Environment, consumer.AppId, consumer.ServiceName, consumer.Version,
				rule.Rule.Pattern, value))
			return discovery.NewError(discovery.ErrPermissionDeny, "found in black list")
		}
	}
	return nil
}
func allowAcrossDimension(ctx context.Context, providerService *model.Service, consumerService *model.Service) error {
	if providerService.Service.AppId != consumerService.Service.AppId {
		if len(providerService.Service.Properties) == 0 {
			return fmt.Errorf("not allow across app access")
		}

		if allowCrossApp, ok := providerService.Service.Properties[discovery.PropAllowCrossApp]; !ok || strings.ToLower(allowCrossApp) != "true" {
			return fmt.Errorf("not allow across app access")
		}
	}
	if !datasource.IsGlobal(discovery.MicroServiceToKey(util.ParseTargetDomainProject(ctx), providerService.Service)) &&
		providerService.Service.Environment != consumerService.Service.Environment {
		return fmt.Errorf("not allow across environment access")
	}
	return nil
}

func DeleteDependencyForDeleteService(domainProject string, serviceID string, service *discovery.MicroServiceKey) error {
	conDep := new(discovery.ConsumerDependency)
	conDep.Consumer = service
	conDep.Providers = []*discovery.MicroServiceKey{}
	conDep.Override = true
	err := syncDependencyRule(context.TODO(), domainProject, conDep)
	if err != nil {
		return err
	}
	return nil
}

func formatRevision(consumerServiceID string, instances []*discovery.MicroServiceInstance) (string, error) {
	if instances == nil {
		return fmt.Sprintf("%x", sha1.Sum(util.StringToBytesWithNoCopy(consumerServiceID))), nil
	}
	copyInstance := make([]*discovery.MicroServiceInstance, len(instances))
	copy(copyInstance, instances)
	sort.Sort(InstanceSlice(copyInstance))
	data, err := json.Marshal(copyInstance)
	if err != nil {
		log.Error("fail to marshal instance json", err)
		return "", err
	}
	s := fmt.Sprintf("%s.%x", consumerServiceID, sha1.Sum(data))
	return fmt.Sprintf("%x", sha1.Sum(util.StringToBytesWithNoCopy(s))), nil
}
