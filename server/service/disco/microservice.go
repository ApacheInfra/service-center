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

package disco

import (
	"context"
	"fmt"

	pb "github.com/go-chassis/cari/discovery"
	"github.com/go-chassis/cari/pkg/errsvc"

	"github.com/apache/servicecomb-service-center/datasource"
	"github.com/apache/servicecomb-service-center/pkg/gopool"
	"github.com/apache/servicecomb-service-center/pkg/log"
	"github.com/apache/servicecomb-service-center/pkg/util"
	"github.com/apache/servicecomb-service-center/server/core"
	"github.com/apache/servicecomb-service-center/server/plugin/quota"
	"github.com/apache/servicecomb-service-center/server/service/validator"
)

type MicroServiceService struct {
}

func (s *MicroServiceService) Create(ctx context.Context, in *pb.CreateServiceRequest) (*pb.CreateServiceResponse, error) {
	if in == nil || in.Service == nil {
		log.Errorf(nil, "create micro-service failed: request body is empty")
		return &pb.CreateServiceResponse{
			Response: pb.CreateResponse(pb.ErrInvalidParams, "Request body is empty"),
		}, nil
	}

	//create service
	rsp, err := s.CreateServicePri(ctx, in)
	if err != nil || rsp.Response.GetCode() != pb.ResponseSuccess {
		return rsp, err
	}

	if !s.isCreateServiceEx(in) {
		return rsp, err
	}

	//create tag,rule,instances
	return s.CreateServiceEx(ctx, in, rsp.ServiceId)
}

func (s *MicroServiceService) CreateServicePri(ctx context.Context, in *pb.CreateServiceRequest) (*pb.CreateServiceResponse, error) {
	remoteIP := util.GetIPFromContext(ctx)
	service := in.Service
	serviceFlag := util.StringJoin([]string{
		service.Environment, service.AppId, service.ServiceName, service.Version}, "/")
	domainProject := util.ParseDomainProject(ctx)

	datasource.SetServiceDefaultValue(service)
	err := validator.Validate(in)
	if err != nil {
		log.Errorf(err, "create micro-service[%s] failed, operator: %s",
			serviceFlag, remoteIP)
		return &pb.CreateServiceResponse{
			Response: pb.CreateResponse(pb.ErrInvalidParams, err.Error()),
		}, nil
	}
	quotaErr := checkServiceQuota(ctx, domainProject)
	if quotaErr != nil {
		log.Error(fmt.Sprintf("create micro-service[%s] failed, operator: %s",
			serviceFlag, remoteIP), err)
		resp := &pb.CreateServiceResponse{
			Response: pb.CreateResponseWithSCErr(quotaErr),
		}
		if quotaErr.InternalError() {
			return resp, quotaErr
		}
		return resp, nil
	}

	return RegisterService(ctx, in)
}

func (s *MicroServiceService) Delete(ctx context.Context, in *pb.DeleteServiceRequest) (*pb.DeleteServiceResponse, error) {
	remoteIP := util.GetIPFromContext(ctx)
	err := validator.Validate(in)
	if err != nil {
		log.Errorf(err, "delete micro-service[%s] failed, operator: %s", in.ServiceId, remoteIP)
		return &pb.DeleteServiceResponse{
			Response: pb.CreateResponse(pb.ErrInvalidParams, err.Error()),
		}, nil
	}

	return UnregisterService(ctx, in)
}

func (s *MicroServiceService) DeleteServices(ctx context.Context, request *pb.DelServicesRequest) (*pb.DelServicesResponse, error) {
	remoteIP := util.GetIPFromContext(ctx)
	// 合法性检查
	if len(request.ServiceIds) == 0 {
		log.Errorf(nil, "delete all micro-services failed, 'serviceIDs' is empty, operator: %s", remoteIP)
		return &pb.DelServicesResponse{
			Response: pb.CreateResponse(pb.ErrInvalidParams, "'serviceIDs' is empty"),
			Services: nil,
		}, nil
	}

	existFlag := map[string]bool{}
	nuoMultilCount := 0
	// 批量删除服务
	serviceRespChan := make(chan *pb.DelServicesRspInfo, len(request.ServiceIds))
	for _, serviceID := range request.ServiceIds {
		//ServiceId重复性检查
		if _, ok := existFlag[serviceID]; ok {
			log.Warnf("duplicate micro-service[%s] serviceID, operator: %s", serviceID, remoteIP)
			continue
		} else {
			existFlag[serviceID] = true
			nuoMultilCount++
		}

		//检查服务ID合法性
		in := &pb.DeleteServiceRequest{
			ServiceId: serviceID,
			Force:     request.Force,
		}
		err := validator.Validate(in)
		if err != nil {
			log.Errorf(err, "delete micro-service[%s] failed, operator: %s", in.ServiceId, remoteIP)
			serviceRespChan <- &pb.DelServicesRspInfo{
				ServiceId:  serviceID,
				ErrMessage: err.Error(),
			}
			continue
		}

		//执行删除服务操作
		gopool.Go(s.getDeleteServiceFunc(ctx, serviceID, request.Force, serviceRespChan))
	}

	//获取批量删除服务的结果
	count := 0
	responseCode := pb.ResponseSuccess
	delServiceRspInfo := make([]*pb.DelServicesRspInfo, 0, len(serviceRespChan))
	for serviceRespItem := range serviceRespChan {
		count++
		if len(serviceRespItem.ErrMessage) != 0 {
			responseCode = pb.ErrInvalidParams
		}
		delServiceRspInfo = append(delServiceRspInfo, serviceRespItem)
		//结果收集over，关闭通道
		if count == nuoMultilCount {
			close(serviceRespChan)
		}
	}

	log.Infof("Batch delete micro-services by serviceIDs[%d]: %v, result code: %d, operator: %s",
		len(request.ServiceIds), request.ServiceIds, responseCode, remoteIP)

	resp := &pb.DelServicesResponse{
		Services: delServiceRspInfo,
	}
	if responseCode != pb.ResponseSuccess {
		resp.Response = pb.CreateResponse(responseCode, "Delete services failed.")
	} else {
		resp.Response = pb.CreateResponse(responseCode, "Delete services successfully.")
	}
	return resp, nil
}

func (s *MicroServiceService) getDeleteServiceFunc(ctx context.Context, serviceID string, force bool, serviceRespChan chan<- *pb.DelServicesRspInfo) func(context.Context) {
	return func(_ context.Context) {
		serviceRst := &pb.DelServicesRspInfo{
			ServiceId:  serviceID,
			ErrMessage: "",
		}
		resp, err := UnregisterService(ctx, &pb.DeleteServiceRequest{
			ServiceId: serviceID,
			Force:     force,
		})
		if err != nil {
			serviceRst.ErrMessage = err.Error()
		} else if resp.Response.GetCode() != pb.ResponseSuccess {
			serviceRst.ErrMessage = resp.Response.GetMessage()
		}

		serviceRespChan <- serviceRst
	}
}

func (s *MicroServiceService) GetOne(ctx context.Context, in *pb.GetServiceRequest) (*pb.GetServiceResponse, error) {
	err := validator.Validate(in)
	if err != nil {
		log.Errorf(err, "get micro-service[%s] failed", in.ServiceId)
		return &pb.GetServiceResponse{
			Response: pb.CreateResponse(pb.ErrInvalidParams, err.Error()),
		}, nil
	}

	return datasource.GetMetadataManager().GetService(ctx, in)
}

func (s *MicroServiceService) GetServices(ctx context.Context, in *pb.GetServicesRequest) (*pb.GetServicesResponse, error) {
	return datasource.GetMetadataManager().GetServices(ctx, in)
}

func (s *MicroServiceService) UpdateProperties(ctx context.Context, in *pb.UpdateServicePropsRequest) (*pb.UpdateServicePropsResponse, error) {
	err := validator.Validate(in)
	if err != nil {
		remoteIP := util.GetIPFromContext(ctx)
		log.Errorf(err, "update service[%s] properties failed, operator: %s", in.ServiceId, remoteIP)
		return &pb.UpdateServicePropsResponse{
			Response: pb.CreateResponse(pb.ErrInvalidParams, err.Error()),
		}, nil
	}

	return datasource.GetMetadataManager().UpdateService(ctx, in)
}

func (s *MicroServiceService) Exist(ctx context.Context, in *pb.GetExistenceRequest) (*pb.GetExistenceResponse, error) {
	switch in.Type {
	case datasource.ExistTypeMicroservice:
		err := validator.ExistenceReqValidator().Validate(in)
		if err != nil {
			serviceFlag := util.StringJoin([]string{in.Environment, in.AppId, in.ServiceName, in.Version}, "/")
			log.Errorf(err, "micro-service[%s] exist failed", serviceFlag)
			return &pb.GetExistenceResponse{
				Response: pb.CreateResponse(pb.ErrInvalidParams, err.Error()),
			}, nil
		}

		return datasource.GetMetadataManager().ExistService(ctx, in)
	case datasource.ExistTypeSchema:
		err := validator.GetSchemaReqValidator().Validate(in)
		if err != nil {
			log.Errorf(err, "schema[%s/%s] exist failed", in.ServiceId, in.SchemaId)
			return &pb.GetExistenceResponse{
				Response: pb.CreateResponse(pb.ErrInvalidParams, err.Error()),
			}, nil
		}

		return datasource.GetMetadataManager().ExistSchema(ctx, in)
	default:
		log.Warnf("unexpected type '%s' for existence query.", in.Type)
		return &pb.GetExistenceResponse{
			Response: pb.CreateResponse(pb.ErrInvalidParams, "Only micro-service and schema can be used as type."),
		}, nil
	}
}

func (s *MicroServiceService) CreateServiceEx(ctx context.Context, in *pb.CreateServiceRequest, serviceID string) (*pb.CreateServiceResponse, error) {
	result := &pb.CreateServiceResponse{
		ServiceId: serviceID,
		Response:  &pb.Response{},
	}
	var chanLen int = 0
	createRespChan := make(chan *pb.Response, 10)
	//create rules
	if in.Rules != nil && len(in.Rules) != 0 {
		chanLen++
		gopool.Go(func(_ context.Context) {
			req := &pb.AddServiceRulesRequest{
				ServiceId: serviceID,
				Rules:     in.Rules,
			}
			chanRsp := &pb.Response{}
			rsp, err := s.AddRule(ctx, req)
			if err != nil {
				chanRsp.Message = err.Error()
			}

			if rsp.Response.GetCode() != pb.ResponseSuccess {
				chanRsp.Message = rsp.Response.GetMessage()
			}
			createRespChan <- chanRsp
		})
	}
	//create tags
	if in.Tags != nil && len(in.Tags) != 0 {
		chanLen++
		gopool.Go(func(_ context.Context) {
			req := &pb.AddServiceTagsRequest{
				ServiceId: serviceID,
				Tags:      in.Tags,
			}
			chanRsp := &pb.Response{}
			rsp, err := s.AddTags(ctx, req)
			if err != nil {
				chanRsp.Message = err.Error()
			}

			if rsp.Response.GetCode() != pb.ResponseSuccess {
				chanRsp.Message = rsp.Response.GetMessage()
			}
			createRespChan <- chanRsp
		})
	}
	// create instance
	if in.Instances != nil && len(in.Instances) != 0 {
		chanLen++
		gopool.Go(func(_ context.Context) {
			chanRsp := &pb.Response{}
			for _, ins := range in.Instances {
				req := &pb.RegisterInstanceRequest{
					Instance: ins,
				}
				req.Instance.ServiceId = serviceID
				rsp, err := RegisterInstance(ctx, req)
				if err != nil {
					chanRsp.Message += fmt.Sprintf("{instance:%v,result:%s}", ins.Endpoints, err.Error())
				}
				if rsp.Response.GetCode() != pb.ResponseSuccess {
					chanRsp.Message += fmt.Sprintf("{instance:%v,result:%s}", ins.Endpoints, rsp.Response.GetMessage())
				}
				createRespChan <- chanRsp
			}
		})
	}

	// handle result
	var errMessages []string
	for createResp := range createRespChan {
		chanLen--
		if len(createResp.GetMessage()) != 0 {
			errMessages = append(errMessages, createResp.GetMessage())
		}

		if 0 == chanLen {
			close(createRespChan)
		}
	}

	if len(errMessages) != 0 {
		result.Response.Code = pb.ErrInvalidParams
		result.Response.Message = fmt.Sprintf("errMessages: %v", errMessages)
	} else {
		result.Response.Code = pb.ResponseSuccess
	}

	log.Infof("createServiceEx, serviceID: %s, result code: %s, operator: %s",
		result.ServiceId, result.Response.GetMessage(), util.GetIPFromContext(ctx))
	return result, nil
}

func (s *MicroServiceService) isCreateServiceEx(in *pb.CreateServiceRequest) bool {
	if len(in.Rules) == 0 && len(in.Tags) == 0 && len(in.Instances) == 0 {
		return false
	}
	return true
}

func checkServiceQuota(ctx context.Context, domainProject string) *errsvc.Error {
	if core.IsSCInstance(ctx) {
		log.Debugf("skip quota check")
		return nil
	}
	res := quota.NewApplyQuotaResource(quota.TypeService, domainProject, "", 1)
	rst := quota.Apply(ctx, res)
	return rst
}
