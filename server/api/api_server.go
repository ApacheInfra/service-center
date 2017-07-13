//Copyright 2017 Huawei Technologies Co., Ltd
//
//Licensed under the Apache License, Version 2.0 (the "License");
//you may not use this file except in compliance with the License.
//You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
//Unless required by applicable law or agreed to in writing, software
//distributed under the License is distributed on an "AS IS" BASIS,
//WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//See the License for the specific language governing permissions and
//limitations under the License.
package api

import (
	"errors"
	"fmt"
	"github.com/servicecomb/service-center/server/core"
	"github.com/servicecomb/service-center/server/core/mux"
	pb "github.com/servicecomb/service-center/server/core/proto"
	rs "github.com/servicecomb/service-center/server/rest"
	"github.com/servicecomb/service-center/server/rest/handlers"
	"github.com/servicecomb/service-center/util"
	"github.com/servicecomb/service-center/util/rest"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"net"
	"net/http"
	"strings"
	"time"
)

var (
	ErrLockFailed = errors.New("Get Etcd Lock failed")
)

type APIType int64

type APIServerConfig struct {
	SSL          bool
	VerifyClient bool
	HostName     string
	Endpoints    map[APIType]string
}

type APIServer struct {
	Config *APIServerConfig

	isClose bool
	err     chan error
}

const (
	GRPC APIType = 0
	REST APIType = 1
)

func (s *APIServer) Err() <-chan error {
	return s.err
}

func (s *APIServer) startGrpcServer() {
	var (
		svr *grpc.Server
		err error
	)

	ipAddr, ok := s.Config.Endpoints[GRPC]
	if !ok {
		return
	}

	if s.Config.SSL {
		tlsConfig, err := rest.GetServerTLSConfig(s.Config.VerifyClient)
		if err != nil {
			util.LOGGER.Error("error to get server tls config", err)
			s.err <- err
			return
		}
		creds := credentials.NewTLS(tlsConfig)
		svr = grpc.NewServer(grpc.Creds(creds))
	} else {
		svr = grpc.NewServer()
	}

	pb.RegisterServiceCtrlServer(svr, rs.ServiceAPI)
	pb.RegisterServiceInstanceCtrlServer(svr, rs.InstanceAPI)

	util.LOGGER.Infof("listen on server %s", ipAddr)
	ls, err := net.Listen("tcp", ipAddr)
	if err != nil {
		util.LOGGER.Error("error to start Grpc API server "+ipAddr, err)
		s.err <- err
		return
	}
	svr.Serve(ls)
}

func (s *APIServer) startRESTfulServer() {
	var err error

	ipAddr, ok := s.Config.Endpoints[REST]
	if !ok {
		return
	}

	http.Handle("/", handlers.DefaultServerHandler())

	if s.Config.SSL {
		err = rest.ListenAndServeTLS(ipAddr, nil)
	} else {
		err = rest.ListenAndServe(ipAddr, nil)
	}

	if err != nil {
		util.LOGGER.Error("error to start RESTful API server "+ipAddr, err)
		s.err <- err
		return
	}
}

func (s *APIServer) registerAPIServer() {
	err := s.registryService()
	if err != nil {
		s.err <- err
		return
	}
	// 实例信息
	err = s.registryInstance()
	if err != nil {
		util.LOGGER.Error(fmt.Sprintf("error register sc instance %s", err), err)
		s.err <- err
	}
}

func (s *APIServer) registryService() error {
	//分布式sc 都会一起抢注，导致注册了多个sc微服务静态信息，需要使用分布式同步锁解决
	lock, err := mux.Lock(mux.PROCESS_LOCK)
	if err != nil {
		util.LOGGER.Errorf(err, "could not create global lock %s", mux.PROCESS_LOCK)
		return err
	}
	defer lock.Unlock()

	ctx := core.AddDefaultContextValue(context.TODO())
	respE, err := rs.ServiceAPI.Exist(ctx, core.GetExistenceRequest())
	if err != nil {
		util.LOGGER.Error("query service center existence failed", err)
		return err
	}
	if respE.Response.Code == pb.Response_SUCCESS {
		util.LOGGER.Warnf(nil, "service center service already registered, service id %s", respE.ServiceId)
		respG, err := rs.ServiceAPI.GetOne(ctx, core.GetServiceRequest(respE.ServiceId))
		if err != nil {
			util.LOGGER.Error("query service center service info failed", err)
			return err
		}
		core.Service = respG.Service
		return nil
	}
	respS, err := rs.ServiceAPI.Create(ctx, core.CreateServiceRequest())
	if err != nil {
		util.LOGGER.Error("register service center failed", err)
		return err
	}
	core.Service.ServiceId = respS.ServiceId
	return nil
}

func (s *APIServer) registryInstance() error {
	core.Instance.ServiceId = core.Service.ServiceId

	endpoints := []string{}
	if address, ok := s.Config.Endpoints[GRPC]; ok {
		endpoints = append(endpoints, strings.Join([]string{"grpc", address}, "://"))
	}
	if address, ok := s.Config.Endpoints[REST]; ok {
		endpoints = append(endpoints, strings.Join([]string{"rest", address}, "://"))
	}

	ctx := core.AddDefaultContextValue(context.TODO())
	respI, err := rs.InstanceAPI.Register(ctx,
		core.RegisterInstanceRequest(s.Config.HostName, endpoints))
	if respI.GetResponse().Code != pb.Response_SUCCESS {
		err = fmt.Errorf("register service center instance failed, %s", respI.GetResponse().Message)
		util.LOGGER.Error(err.Error(), nil)
		return err
	}
	core.Instance.InstanceId = respI.InstanceId
	return nil
}

func (s *APIServer) unregisterInstance() error {
	if len(core.Instance.InstanceId) == 0 {
		return nil
	}
	ctx := core.AddDefaultContextValue(context.TODO())
	respI, err := rs.InstanceAPI.Unregister(ctx, core.UnregisterInstanceRequest())
	if respI.GetResponse().Code != pb.Response_SUCCESS {
		err = fmt.Errorf("unregister service center instance failed, %s", respI.GetResponse().Message)
		util.LOGGER.Error(err.Error(), nil)
		return err
	}
	return nil
}

func (s *APIServer) doAPIServerHeartBeat() {
	if s.isClose {
		return
	}
	ctx := core.AddDefaultContextValue(context.TODO())
	respI, err := rs.InstanceAPI.Heartbeat(ctx, core.HeartbeatRequest())
	if respI.GetResponse().Code != pb.Response_SUCCESS && err == nil {
		util.LOGGER.Errorf(err, "update service center %s instance %s heartbeat failed",
			core.Instance.ServiceId, core.Instance.InstanceId)

		//服务不存在，创建服务
		err := s.registryService()
		if err != nil {
			util.LOGGER.Errorf(err, "Service %s/%s/%s does not exist, and retry to create it failed.",
				core.REGISTRY_APP_ID, core.REGISTRY_SERVICE_NAME, core.REGISTRY_VERSION)
			return
		}
		// 重新注册实例信息
		s.registryInstance()
		return
	}
	util.LOGGER.Debugf("update service center %s heartbeat %s successfully",
		core.Instance.ServiceId, core.Instance.InstanceId)
}

// 需保证ETCD启动成功后才执行该方法
func (s *APIServer) StartAPIServer() {
	s.isClose = false
	s.err = make(chan error, 1)
	go func() {
		if s.Config == nil {
			s.err <- errors.New("do not find any config for APIServer")
			return
		}
		// 自注册
		s.registerAPIServer()

		go s.startRESTfulServer()

		go s.startGrpcServer()

		// 心跳
		go func() {
			for {
				<-time.After(time.Duration(core.Instance.HealthCheck.Interval) * time.Second)
				s.doAPIServerHeartBeat()
			}
		}()

		util.LOGGER.Info("api server is ready")
	}()
}

func (s *APIServer) Close() {
	// TODO 停止rest和grpc服务、cron
	s.unregisterInstance()
	s.isClose = true
	close(s.err)
}
