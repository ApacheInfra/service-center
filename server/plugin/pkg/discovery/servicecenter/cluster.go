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

package servicecenter

import (
	"fmt"
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
	cluster     *ServiceCenterCluster
	clusterOnce sync.Once
)

type ServiceCenterCluster struct {
	Client *SCClientAggregate

	cachers map[discovery.Type]*ServiceCenterCacher
}

func (c *ServiceCenterCluster) Initialize() {
	c.cachers = make(map[discovery.Type]*ServiceCenterCacher)
	c.Client = NewSCClientAggregate()
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
	cache, errs := c.Client.GetScCache()
	if cache == nil && len(errs) > 0 {
		err := fmt.Errorf("%v", errs)
		log.Errorf(err, "sync failed")
		return err
	}

	// microservice
	serviceCacher, ok := c.cachers[backend.SERVICE]
	if ok {
		c.check(serviceCacher, &cache.Microservices, errs)
	}
	aliasCacher, ok := c.cachers[backend.SERVICE_ALIAS]
	if ok {
		c.checkWithConflictHandleFunc(aliasCacher, &cache.Aliases, errs, c.logConflictFunc)
	}
	indexCacher, ok := c.cachers[backend.SERVICE_INDEX]
	if ok {
		c.checkWithConflictHandleFunc(indexCacher, &cache.Indexes, errs, c.logConflictFunc)
	}
	// instance
	instCacher, ok := c.cachers[backend.INSTANCE]
	if ok {
		c.check(instCacher, &cache.Instances, errs)
	}
	// microservice meta
	tagCacher, ok := c.cachers[backend.SERVICE_TAG]
	if ok {
		c.check(tagCacher, &cache.Tags, errs)
	}
	ruleCacher, ok := c.cachers[backend.RULE]
	if ok {
		c.check(ruleCacher, &cache.Rules, errs)
	}
	ruleIndexCacher, ok := c.cachers[backend.RULE_INDEX]
	if ok {
		c.check(ruleIndexCacher, &cache.RuleIndexes, errs)
	}
	depRuleCacher, ok := c.cachers[backend.DEPENDENCY_RULE]
	if ok {
		c.check(depRuleCacher, &cache.DependencyRules, errs)
	}
	return nil
}

func (c *ServiceCenterCluster) check(local *ServiceCenterCacher, remote model.Getter, skipClusters map[string]error) {
	c.checkWithConflictHandleFunc(local, remote, skipClusters, c.skipHandleFunc)
}

func (c *ServiceCenterCluster) checkWithConflictHandleFunc(local *ServiceCenterCacher, remote model.Getter, skipClusters map[string]error,
	conflictHandleFunc func(origin *model.KV, conflict model.Getter, index int)) {
	exists := make(map[string]*model.KV)
	remote.ForEach(func(i int, v *model.KV) bool {
		if kv, ok := exists[v.Key]; ok {
			conflictHandleFunc(kv, remote, i)
			return true
		}
		exists[v.Key] = v
		kv := local.Cache().Get(v.Key)
		newKv := &discovery.KeyValue{
			Key:            util.StringToBytesWithNoCopy(v.Key),
			Value:          v.Value,
			Version:        v.Rev,
			CreateRevision: v.Rev,
			ModRevision:    v.Rev,
			ClusterName:    v.ClusterName,
		}
		switch {
		case kv == nil:
			local.Notify(pb.EVT_CREATE, v.Key, newKv)
		case kv.ModRevision != v.Rev:
			// if lose some cluster kvs, then skip to notify changes of this cluster
			// to prevent publish the wrong changes events of kvs
			if err, ok := skipClusters[kv.ClusterName]; ok {
				log.Errorf(err, "cluster[%s] temporarily unavailable, skip cluster[%s] event %s %s",
					kv.ClusterName, v.ClusterName, pb.EVT_UPDATE, v.Key)
				break
			}
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
			if err, ok := skipClusters[v.ClusterName]; ok {
				log.Errorf(err, "cluster[%s] temporarily unavailable, skip event %s %s",
					v.ClusterName, pb.EVT_DELETE, v.Key)
				return true
			}
			deletes = append(deletes, v)
		}
		return true
	})
	for _, v := range deletes {
		local.Notify(pb.EVT_DELETE, util.BytesToStringWithNoCopy(v.Key), v)
	}
}

func (c *ServiceCenterCluster) skipHandleFunc(origin *model.KV, conflict model.Getter, index int) {
}

func (c *ServiceCenterCluster) logConflictFunc(origin *model.KV, conflict model.Getter, index int) {
	switch conflict.(type) {
	case *model.MicroserviceIndexSlice:
		slice := conflict.(*model.MicroserviceIndexSlice)
		kv := (*slice)[index]
		if serviceId := origin.Value.(string); kv.Value != serviceId {
			key := core.GetInfoFromSvcIndexKV(util.StringToBytesWithNoCopy(kv.Key))
			log.Warnf("conflict! can not merge microservice index[%s][%s][%s/%s/%s/%s], found one[%s] in cluster[%s]",
				kv.ClusterName, kv.Value, key.Environment, key.AppId, key.ServiceName, key.Version,
				serviceId, origin.ClusterName)
		}
	case *model.MicroserviceAliasSlice:
		slice := conflict.(*model.MicroserviceAliasSlice)
		kv := (*slice)[index]
		if serviceId := origin.Value.(string); kv.Value != serviceId {
			key := core.GetInfoFromSvcAliasKV(util.StringToBytesWithNoCopy(kv.Key))
			log.Warnf("conflict! can not merge microservice alias[%s][%s][%s/%s/%s/%s], found one[%s] in cluster[%s]",
				kv.ClusterName, kv.Value, key.Environment, key.AppId, key.ServiceName, key.Version,
				serviceId, origin.ClusterName)
		}
	}
}

func (c *ServiceCenterCluster) loop(ctx context.Context) {
	select {
	case <-ctx.Done():
	case <-time.After(minWaitInterval):
		c.Sync(ctx)
		d := registry.Configuration().AutoSyncInterval
		if d == 0 {
			return
		}
	loop:
		for {
			select {
			case <-ctx.Done():
				break loop
			case <-time.After(d):
				// TODO support watching sc
				c.Sync(ctx)
			}
		}
	}

	log.Debug("service center client is stopped")
}

// unsafe
func (c *ServiceCenterCluster) AddCacher(t discovery.Type, cacher *ServiceCenterCacher) {
	c.cachers[t] = cacher
}

func (c *ServiceCenterCluster) Run() {
	c.Initialize()
	gopool.Go(c.loop)
}

func (c *ServiceCenterCluster) Stop() {}

func (c *ServiceCenterCluster) Ready() <-chan struct{} {
	return closedCh
}

func ServiceCenter() *ServiceCenterCluster {
	clusterOnce.Do(func() {
		cluster = &ServiceCenterCluster{}
		cluster.Run()
	})
	return cluster
}
