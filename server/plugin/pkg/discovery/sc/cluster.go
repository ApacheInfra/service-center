// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package sc

import (
	"fmt"
	"github.com/apache/incubator-servicecomb-service-center/pkg/client/sc"
	"github.com/apache/incubator-servicecomb-service-center/pkg/gopool"
	"github.com/apache/incubator-servicecomb-service-center/pkg/log"
	"github.com/apache/incubator-servicecomb-service-center/pkg/util"
	"github.com/apache/incubator-servicecomb-service-center/server/admin/model"
	"github.com/apache/incubator-servicecomb-service-center/server/core"
	"github.com/apache/incubator-servicecomb-service-center/server/core/backend"
	pb "github.com/apache/incubator-servicecomb-service-center/server/core/proto"
	"github.com/apache/incubator-servicecomb-service-center/server/plugin/pkg/discovery"
	"github.com/apache/incubator-servicecomb-service-center/server/plugin/pkg/registry"
	"golang.org/x/net/context"
	"strings"
	"sync"
	"time"
)

var (
	client     *ServiceCenterAggregate
	clientOnce sync.Once
)

type ServiceCenterInterface interface {
	discovery.Indexer
	AddCacher(t discovery.Type, cacher *ServiceCenterCacher)
	Sync(ctx context.Context) error
	Ready() <-chan struct{}
}

type ServiceCenterCluster struct {
	Client *sc.SCClient

	cachers map[discovery.Type]*ServiceCenterCacher

	ready chan struct{}
}

func (c *ServiceCenterCluster) Initialize() {
	c.ready = make(chan struct{})
	c.cachers = make(map[discovery.Type]*ServiceCenterCacher)
}

func (c *ServiceCenterCluster) Search(ctx context.Context, opts ...registry.PluginOpOption) (r *discovery.Response, err error) {
	op := registry.OpGet(opts...)
	key := util.BytesToStringWithNoCopy(op.Key)
	switch {
	case strings.Index(key, core.GetServiceSchemaRootKey("")) == 0:
		domainProject, serviceId, schemaId := core.GetInfoFromSchemaKV(op.Key)
		var schemas []*pb.Schema
		if op.Prefix && len(schemaId) == 0 {
			schemas, err = c.Client.GetSchemasByServiceId(domainProject, serviceId)
			if err != nil {
				return nil, err
			}
		} else {
			schema, err := c.Client.GetSchemaBySchemaId(domainProject, serviceId, schemaId)
			if err != nil {
				return nil, err
			}
			schemas = append(schemas, schema)
		}
		var response discovery.Response
		response.Count = int64(len(schemas))
		if op.CountOnly {
			return &response, nil
		}
		for _, schema := range schemas {
			response.Kvs = append(response.Kvs, &discovery.KeyValue{
				Key: util.StringToBytesWithNoCopy(
					core.GenerateServiceSchemaKey(domainProject, serviceId, schema.SchemaId)),
				Value: util.StringToBytesWithNoCopy(schema.Schema),
			})
		}
		return &response, nil
	default:
		return nil, fmt.Errorf("no implement")
	}
}

func (c *ServiceCenterCluster) Sync(ctx context.Context) error {
	cache, err := c.Client.GetScCache()
	if err != nil {
		log.Errorf(err, "sync failed")
		return err
	}

	defer util.SafeCloseChan(c.ready)

	// microservice
	serviceCacher, ok := c.cachers[backend.SERVICE]
	if ok {
		c.check(serviceCacher, &cache.Microservices)
	}
	aliasCacher, ok := c.cachers[backend.SERVICE_ALIAS]
	if ok {
		c.check(aliasCacher, &cache.Aliases)
	}
	indexCacher, ok := c.cachers[backend.SERVICE_INDEX]
	if ok {
		c.check(indexCacher, &cache.Indexes)
	}
	// instance
	instCacher, ok := c.cachers[backend.INSTANCE]
	if ok {
		c.check(instCacher, &cache.Instances)
	}
	// microservice meta
	tagCacher, ok := c.cachers[backend.SERVICE_TAG]
	if ok {
		c.check(tagCacher, &cache.Tags)
	}
	ruleCacher, ok := c.cachers[backend.RULE]
	if ok {
		c.check(ruleCacher, &cache.Rules)
	}
	ruleIndexCacher, ok := c.cachers[backend.RULE_INDEX]
	if ok {
		c.check(ruleIndexCacher, &cache.RuleIndexes)
	}
	depRuleCacher, ok := c.cachers[backend.DEPENDENCY_RULE]
	if ok {
		c.check(depRuleCacher, &cache.DependencyRules)
	}
	return nil
}

func (c *ServiceCenterCluster) check(local *ServiceCenterCacher, remote model.Getter) {
	init := !c.IsReady()

	remote.ForEach(func(_ int, v *model.KV) bool {
		kv := local.Cache().Get(v.Key)
		newKv := &discovery.KeyValue{
			Key:            util.StringToBytesWithNoCopy(v.Key),
			Value:          v.Value,
			Version:        v.Rev,
			CreateRevision: v.Rev,
			ModRevision:    v.Rev,
		}
		switch {
		case kv == nil && init:
			local.Notify(pb.EVT_INIT, v.Key, newKv)
		case kv == nil && !init:
			local.Notify(pb.EVT_CREATE, v.Key, newKv)
		case kv.ModRevision != v.Rev:
			local.Notify(pb.EVT_UPDATE, v.Key, newKv)
		}
		return true
	})

	var deletes []*discovery.KeyValue
	local.Cache().ForEach(func(key string, v *discovery.KeyValue) (next bool) {
		var exist bool
		remote.ForEach(func(_ int, v *model.KV) bool {
			exist = v.Key == key
			return !exist
		})
		if !exist {
			deletes = append(deletes, v)
		}
		return true
	})
	for _, v := range deletes {
		local.Notify(pb.EVT_DELETE, util.BytesToStringWithNoCopy(v.Key), v)
	}
}

func (c *ServiceCenterCluster) loop(ctx context.Context) {
	c.Sync(ctx)
	for {
		select {
		case <-ctx.Done():
			log.Debug("service center client is stopped")
			return
		case <-time.After(registry.Configuration().AutoSyncInterval):
			// TODO support watching sc
			c.Sync(ctx)
		}
	}
}

// unsafe
func (c *ServiceCenterCluster) AddCacher(t discovery.Type, cacher *ServiceCenterCacher) {
	c.cachers[t] = cacher
}

func (c *ServiceCenterCluster) Run() {
	gopool.Go(c.loop)
}

func (c *ServiceCenterCluster) Ready() <-chan struct{} {
	return c.ready
}

func (c *ServiceCenterCluster) IsReady() bool {
	select {
	case <-c.ready:
		return true
	default:
		return false
	}
}

type ServiceCenterAggregate []*ServiceCenterCluster

func (s *ServiceCenterAggregate) Initialize() {
	for _, lw := range *s {
		lw.Initialize()
		lw.Run()
	}
}

func (s *ServiceCenterAggregate) AddCacher(t discovery.Type, cacher *ServiceCenterCacher) {
	for _, lw := range *s {
		lw.AddCacher(t, cacher)
	}
}

func (s *ServiceCenterAggregate) Search(ctx context.Context, opts ...registry.PluginOpOption) (r *discovery.Response, err error) {
	op := registry.OpGet(opts...)
	key := util.BytesToStringWithNoCopy(op.Key)
	log.Debugf("search '%s' match special options, request service center, opts: %s", key, op)

	var response discovery.Response
	for _, lw := range *s {
		resp, err := lw.Search(ctx, opts...)
		if err != nil {
			continue
		}
		response.Kvs = append(response.Kvs, resp.Kvs...)
		response.Count += resp.Count
	}
	return &response, nil
}

func (s *ServiceCenterAggregate) Sync(ctx context.Context) error {
	for _, lw := range *s {
		lw.Sync(ctx)
	}
	return nil
}

func (s *ServiceCenterAggregate) Ready() <-chan struct{} {
	for _, lw := range *s {
		<-lw.Ready()
	}
	return closedCh
}

func NewServiceCenterAggregate() *ServiceCenterAggregate {
	c := &ServiceCenterAggregate{}
	clusters := registry.Configuration().Clusters()
	for name, addr := range clusters {
		if len(name) == 0 || name == registry.Configuration().ClusterName {
			continue
		}
		sc.Addr = addr
		client, err := sc.NewSCClient()
		if err != nil {
			log.Errorf(err, "new service center[%s][%s] client failed", name, addr)
			continue
		}
		client.Timeout = registry.Configuration().RequestTimeOut
		*c = append(*c, &ServiceCenterCluster{
			Client: client,
		})
		log.Infof("list and watch service center[%s][%s]", name, addr)
	}
	return c
}

func ServiceCenter() ServiceCenterInterface {
	clientOnce.Do(func() {
		client = NewServiceCenterAggregate()
		client.Initialize()
	})
	return client
}
