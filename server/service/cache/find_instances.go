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
package cache

import (
	"github.com/apache/incubator-servicecomb-service-center/pkg/cache"
	pb "github.com/apache/incubator-servicecomb-service-center/server/core/proto"
	serviceUtil "github.com/apache/incubator-servicecomb-service-center/server/service/util"
	"golang.org/x/net/context"
	"math"
	"time"
)

var FindInstances = &FindInstancesCache{
	Tree: cache.NewTree(cache.Configure().
		WithTTL(2 * time.Minute).
		WithMaxSize(math.MaxInt64))}

func init() {
	FindInstances.AddFilter(
		&ServiceFilter{},
		&VersionRuleFilter{},
		&TagsFilter{},
		&AccessibleFilter{},
		&InstancesFilter{})
}

type VersionRuleCacheItem struct {
	Version    string
	ServiceIds []string
	Instances  []*pb.MicroServiceInstance
	Rev        string
}

type FindInstancesCache struct {
	*cache.Tree
}

func (f *FindInstancesCache) Get(ctx context.Context, consumer *pb.MicroService, provider *pb.MicroServiceKey, tags []string) (*VersionRuleCacheItem, error) {
	noCache := ctx.Value(serviceUtil.CTX_NOCACHE) == "1"
	cloneCtx := context.WithValue(context.WithValue(context.WithValue(ctx,
		CTX_FIND_CONSUMER, consumer),
		CTX_FIND_PROVIDER, provider),
		CTX_FIND_TAGS, tags)

	var (
		node *cache.Node
		err  error
	)
	if !noCache {
		node, err = f.Tree.Get(cloneCtx)
	} else {
		node, err = f.Tree.Simulate(cloneCtx)
	}
	if node == nil {
		return nil, err
	}
	return node.Cache.Get(CACHE_FIND).(*VersionRuleCacheItem), nil
}

func (f *FindInstancesCache) ExistVersionRule(ctx context.Context, provider *pb.MicroServiceKey) bool {
	cloneCtx := context.WithValue(ctx, CTX_FIND_PROVIDER, provider)
	node, _ := f.Tree.Get(cloneCtx, cache.Options().BeforeLevel(1))
	if node == nil {
		return false
	}
	v := node.Cache.Get(CACHE_FIND).(*VersionRuleCacheItem)
	if v.Version != provider.Version || node.Childs.Size() == 0 {
		v.Version = provider.Version
		node.Cache.Set(CACHE_FIND, v)
		return false
	}
	return true
}

func (f *FindInstancesCache) Remove(provider *pb.MicroServiceKey) {
	f.Tree.Remove(context.WithValue(context.Background(), CTX_FIND_PROVIDER, provider))
}
