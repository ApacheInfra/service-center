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
package server

import (
	"context"
	"errors"
	"net/url"
	"strconv"
	"strings"
	"syscall"

	"github.com/apache/servicecomb-service-center/pkg/gopool"
	"github.com/apache/servicecomb-service-center/pkg/log"
	"github.com/apache/servicecomb-service-center/syncer/config"
	"github.com/apache/servicecomb-service-center/syncer/etcd"
	"github.com/apache/servicecomb-service-center/syncer/grpc"
	"github.com/apache/servicecomb-service-center/syncer/pkg/syssig"
	"github.com/apache/servicecomb-service-center/syncer/pkg/ticker"
	"github.com/apache/servicecomb-service-center/syncer/plugins"
	"github.com/apache/servicecomb-service-center/syncer/serf"
	"github.com/apache/servicecomb-service-center/syncer/servicecenter"
)

var stopChanErr = errors.New("stopped syncer by stopCh")

type moduleServer interface {
	// Starts launches the module server, the returned is not guaranteed that the server is ready
	// The moduleServer.Ready() channel will be transmit a message when server completed
	Start(ctx context.Context)

	// Returns a channel that will be closed when the module server is ready
	Ready() <-chan struct{}

	// Returns a channel that will be transmit a module server error
	Error() <-chan error
}

// Server struct for syncer
type Server struct {
	// Syncer configuration
	conf *config.Config

	// Ticker for Syncer
	tick *ticker.TaskTicker

	// Wrap the servicecenter
	servicecenter servicecenter.Servicecenter

	etcd *etcd.Agent

	// Wraps the serf agent
	agent *serf.Agent

	// Wraps the grpc server
	grpc *grpc.Server

	// The channel will be closed when receiving a system interrupt signal
	stopCh chan struct{}
}

// NewServer new server with Config
func NewServer(conf *config.Config) *Server {
	return &Server{
		conf:   conf,
		stopCh: make(chan struct{}),
	}
}

// Run syncer Server
func (s *Server) Run(ctx context.Context) {
	var err error
	s.initPlugin()
	if err = s.initialization(); err != nil {
		return
	}

	// Start system signal listening, wait for user interrupt program
	gopool.Go(syssig.Run)

	err = s.startModuleServer(s.agent)
	if err != nil {
		s.Stop()
		return
	}

	err = s.configureCluster()
	if err != nil {
		s.Stop()
		return
	}

	err = s.startModuleServer(s.etcd)
	if err != nil {
		s.Stop()
		return
	}

	err = s.startModuleServer(s.grpc)
	if err != nil {
		s.Stop()
		return
	}

	s.servicecenter.SetStorageEngine(s.etcd.Storage())

	s.agent.RegisterEventHandler(s)

	gopool.Go(s.tick.Start)
	<-s.stopCh

	s.Stop()
	return
}

// Stop Syncer Server
func (s *Server) Stop() {
	if s.tick != nil {
		s.tick.Stop()
	}

	if s.agent != nil {
		// removes the serf eventHandler
		s.agent.DeregisterEventHandler(s)
		//stop serf agent
		s.agent.Stop()
	}

	if s.grpc != nil {
		s.grpc.Stop()
	}

	if s.etcd != nil {
		s.etcd.Stop()
	}

	// Closes all goroutines in the pool
	gopool.CloseAndWait()
}

func (s *Server) startModuleServer(module moduleServer) (err error) {
	gopool.Go(module.Start)
	select {
	case <-module.Ready():
	case err = <-module.Error():
	case <-s.stopCh:
		err = stopChanErr
	}
	return err
}

// initialization Initialize the starter of the syncer
func (s *Server) initialization() (err error) {
	err = syssig.AddSignalsHandler(func() {
		log.Info("close svr stop chan")
		close(s.stopCh)
	}, syscall.SIGINT, syscall.SIGKILL, syscall.SIGTERM)
	if err != nil {
		log.Error("listen system signal failed", err)
		return
	}

	s.agent, err = serf.Create(s.conf.Config)
	if err != nil {
		log.Errorf(err, "Create serf failed, %s", err)
		return
	}

	s.etcd = etcd.NewAgent(s.conf.Etcd)

	s.tick = ticker.NewTaskTicker(s.conf.TickerInterval, s.tickHandler)

	s.servicecenter, err = servicecenter.NewServicecenter(strings.Split(s.conf.SCAddr, ","))
	if err != nil {
		log.Error("create servicecenter failed", err)
		return
	}

	s.grpc = grpc.NewServer(s.conf.RPCAddr, s)
	return nil
}

// initPlugin Initialize the plugin and load the external plugin according to the configuration
func (s *Server) initPlugin() {
	plugins.SetPluginConfig(plugins.PluginServicecenter.String(), s.conf.ServicecenterPlugin)
	plugins.LoadPlugins()
}

// configureCluster Configuring the cluster by serf group member information
func (s *Server) configureCluster() error {
	proto := "http" // todo：Introduce tls config to manage protocol
	initialCluster := ""

	// get local member of serf
	self := s.agent.LocalMember()
	peerUrl, err := url.Parse(proto + "://" + self.Addr.String() + ":" + strconv.Itoa(s.conf.ClusterPort))
	if err != nil {
		log.Error("parse url from serf local member failed", err)
		return err
	}

	// group members from serf as initial cluster members
	for _, member := range s.agent.GroupMembers(s.conf.ClusterName) {
		initialCluster += member.Name + "=" + proto + "://" + member.Addr.String() + ":" + member.Tags[serf.TagKeyClusterPort] + ","
	}

	leng := len(initialCluster)
	if leng == 0 {
		err = errors.New("serf group members is empty")
		log.Error("etcd peer not found", err)
		return err
	}
	s.conf.Etcd.APUrls = []url.URL{*peerUrl}
	s.conf.Etcd.LPUrls = []url.URL{*peerUrl}
	s.conf.Etcd.InitialCluster = initialCluster[:len(initialCluster)-1]
	return nil
}
