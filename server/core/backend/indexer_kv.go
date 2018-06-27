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
package backend

import (
	"github.com/apache/incubator-servicecomb-service-center/pkg/util"
	"github.com/apache/incubator-servicecomb-service-center/server/core"
	pb "github.com/apache/incubator-servicecomb-service-center/server/core/proto"
	"github.com/apache/incubator-servicecomb-service-center/server/infra/registry"
	"golang.org/x/net/context"
	"strings"
	"sync"
	"time"
)

type CacheIndexer struct {
	*baseIndexer

	BuildTimeout time.Duration

	cacher           Cacher
	goroutine        *util.GoRoutine
	ready            chan struct{}
	prefixIndex      map[string]map[string]struct{}
	prefixBuildQueue chan KvEvent
	prefixLock       sync.RWMutex
	isClose          bool
}

func (i *CacheIndexer) Search(ctx context.Context, opts ...registry.PluginOpOption) (*Response, error) {
	op := registry.OpGet(opts...)

	key := util.BytesToStringWithNoCopy(op.Key)

	if !core.ServerInfo.Config.EnableCache ||
		op.Mode == registry.MODE_NO_CACHE ||
		op.Revision > 0 ||
		(op.Offset >= 0 && op.Limit > 0) {
		util.Logger().Debugf("search %s match special options, request etcd server, opts: %s",
			i.cacher.Name(), op)
		return i.baseIndexer.Search(ctx, opts...)
	}

	if op.Prefix {
		resp, err := i.searchPrefixKeyWithCache(ctx, op)
		if err != nil {
			return nil, err
		}

		if len(resp.Kvs) > 0 || op.Mode == registry.MODE_CACHE {
			return resp, nil
		}

		util.Logger().Debugf("can not find any key from %s cache with prefix, request etcd server, key: %s",
			i.cacher.Name(), key)
		return i.baseIndexer.Search(ctx, opts...)
	}

	resp := new(Response)
	if op.CountOnly {
		if i.Cache().Have(key) {
			resp.Count = 1
			return resp, nil
		}
		if op.Mode == registry.MODE_CACHE {
			return resp, nil
		}

		util.Logger().Debugf("%s cache does not store this key, request etcd server, key: %s",
			i.cacher.Name(), key)
		return i.baseIndexer.Search(ctx, opts...)
	}

	cacheData := i.Cache().Data(key)
	if cacheData == nil {
		if op.Mode == registry.MODE_CACHE {
			return resp, nil
		}

		util.Logger().Debugf("do not match any key in %s cache store, request etcd server, key: %s",
			i.cacher.Name(), key)
		return i.baseIndexer.Search(ctx, opts...)
	}

	resp.Count = 1
	resp.Kvs = []*KeyValue{cacheData.(*KeyValue)}
	return resp, nil
}

func (i *CacheIndexer) Cache() Cache {
	return i.cacher.Cache()
}

func (i *CacheIndexer) searchPrefixKeyWithCache(ctx context.Context, op registry.PluginOp) (*Response, error) {
	resp := new(Response)

	prefix := util.BytesToStringWithNoCopy(op.Key)

	i.prefixLock.RLock()
	resp.Count = int64(i.getPrefixKey(nil, prefix))
	if resp.Count == 0 || op.CountOnly {
		i.prefixLock.RUnlock()
		return resp, nil
	}

	t := time.Now()
	keys := make([]string, 0, resp.Count)
	i.getPrefixKey(&keys, prefix)
	i.prefixLock.RUnlock()

	kvs := make([]*KeyValue, resp.Count)
	idx := 0
	for _, key := range keys {
		c := i.Cache().Data(key) // TODO too slow when big data is requested
		if c == nil {
			// it means resp.Count is not equal to len(keys)
			util.Logger().Warnf(nil, "unexpected nil cache, maybe it is removed, key is %s", key)
			continue
		}
		kvs[idx] = c.(*KeyValue)
		idx++
	}
	util.LogNilOrWarnf(t, "too long to copy data[%d] from cache[%d] with prefix %s", idx, len(i.prefixIndex), prefix)

	resp.Kvs = kvs[:idx]
	return resp, nil
}

func (i *CacheIndexer) OnCacheEvent(evt KvEvent) {
	switch evt.Type {
	case pb.EVT_INIT, pb.EVT_CREATE, pb.EVT_DELETE:
	default:
		return
	}

	if i.isClose {
		return
	}
	defer util.RecoverAndReport()

	t := time.NewTimer(i.BuildTimeout)
	select {
	case <-t.C:
		key := util.BytesToStringWithNoCopy(evt.KV.Key)
		util.Logger().Errorf(nil, "add event to build index queue timed out(%s), key is %s [%s] event",
			i.BuildTimeout, key, evt.Type)
	case i.prefixBuildQueue <- evt:
		t.Stop()
	}
}

func (i *CacheIndexer) buildIndex() {
	i.goroutine.Do(func(ctx context.Context) {
		util.SafeCloseChan(i.ready)
		lastCompactTime := time.Now()
		max := 0
		for {
			select {
			case <-ctx.Done():
				return
			case evt, ok := <-i.prefixBuildQueue:
				if !ok {
					return
				}
				t := time.Now()
				key := util.BytesToStringWithNoCopy(evt.KV.Key)
				prefix := key[:strings.LastIndex(key[:len(key)-1], "/")+1]

				i.prefixLock.Lock()
				switch evt.Type {
				case pb.EVT_DELETE:
					i.deletePrefixKey(prefix, key)
				default:
					i.addPrefixKey(prefix, key)
				}

				// compact
				initSize, l := DEFAULT_CACHE_INIT_SIZE, len(i.prefixIndex)
				if max < l {
					max = l
				}
				if initSize >= l &&
					max >= initSize*DEFAULT_COMPACT_TIMES &&
					time.Now().Sub(lastCompactTime) >= DEFAULT_COMPACT_TIMEOUT {
					i.compact()
					max = l
					lastCompactTime = time.Now()
				}

				i.prefixLock.Unlock()

				util.LogNilOrWarnf(t, "too long to rebuild(action: %s) index[%d], key is %s",
					evt.Type, len(i.prefixIndex), key)
			}
		}
		util.Logger().Debugf("the goroutine building index %s is stopped", i.cacher.Name())
	})

	i.goroutine.Do(func(ctx context.Context) {
		ticker := time.NewTicker(DEFAULT_METRICS_INTERVAL)
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				i.prefixLock.RLock()
				// report metrics
				ReportCacheMetrics(i.cacher.Name(), "index", i.prefixIndex)
				i.prefixLock.RUnlock()
			}
		}
	})
	<-i.ready
}

func (i *CacheIndexer) compact() {
	n := make(map[string]map[string]struct{}, DEFAULT_CACHE_INIT_SIZE)
	for k, v := range i.prefixIndex {
		c, ok := n[k]
		if !ok {
			c = make(map[string]struct{}, len(v))
			n[k] = c
		}
		for ck, cv := range v {
			c[ck] = cv
		}
	}
	i.prefixIndex = n

	util.Logger().Infof("index %s: compact root capacity to size %d",
		i.cacher.Name(), DEFAULT_CACHE_INIT_SIZE)
}

func (i *CacheIndexer) getPrefixKey(arr *[]string, prefix string) (count int) {
	keysRef, ok := i.prefixIndex[prefix]
	if !ok {
		return 0
	}

	for key := range keysRef {
		n := i.getPrefixKey(arr, key)
		if n == 0 {
			count += len(keysRef)
			if arr != nil {
				// TODO support sort option
				for k := range keysRef {
					*arr = append(*arr, k)
				}
			}
			break
		}
		count += n
	}
	return count
}

func (i *CacheIndexer) addPrefixKey(prefix, key string) {
	if i.Cfg.Prefix == key {
		return
	}

	keys, ok := i.prefixIndex[prefix]
	if !ok {
		// build parent index key and new child nodes
		keys = make(map[string]struct{})
		i.prefixIndex[prefix] = keys
	} else if _, ok := keys[key]; ok {
		return
	}

	keys[key], key = struct{}{}, prefix
	prefix = key[:strings.LastIndex(key[:len(key)-1], "/")+1]

	i.addPrefixKey(prefix, key)
}

func (i *CacheIndexer) deletePrefixKey(prefix, key string) {
	m, ok := i.prefixIndex[prefix]
	if !ok {
		return
	}
	delete(m, key)

	// remove parent which has no child
	if len(m) == 0 {
		delete(i.prefixIndex, prefix)
		i.deletePrefixKey(prefix[:strings.LastIndex(prefix[:len(prefix)-1], "/")+1], prefix)
	}
}

func (i *CacheIndexer) Run() {
	i.prefixLock.Lock()
	if !i.isClose {
		i.prefixLock.Unlock()
		return
	}
	i.isClose = false
	i.prefixLock.Unlock()

	if !core.ServerInfo.Config.EnableCache {
		util.SafeCloseChan(i.ready)
		return
	}

	i.buildIndex()

	i.cacher.Run()
}

func (i *CacheIndexer) Stop() {
	i.prefixLock.Lock()
	if i.isClose {
		i.prefixLock.Unlock()
		return
	}
	i.isClose = true
	i.prefixLock.Unlock()

	i.cacher.Stop()

	i.goroutine.Close(true)

	close(i.prefixBuildQueue)

	util.SafeCloseChan(i.ready)
}

func (i *CacheIndexer) Ready() <-chan struct{} {
	<-i.cacher.Ready()
	return i.ready
}

func NewCacheIndexer(name string, cfg *Config) (indexer *CacheIndexer) {
	indexer = &CacheIndexer{
		baseIndexer:      NewBaseIndexer(cfg),
		BuildTimeout:     DEFAULT_ADD_QUEUE_TIMEOUT,
		cacher:           NullCacher,
		prefixIndex:      make(map[string]map[string]struct{}, DEFAULT_CACHE_INIT_SIZE),
		prefixBuildQueue: make(chan KvEvent, DEFAULT_MAX_EVENT_COUNT),
		goroutine:        util.NewGo(context.Background()),
		ready:            make(chan struct{}),
		isClose:          true,
	}

	switch {
	case core.ServerInfo.Config.EnableCache && cfg.InitSize > 0:
		indexer.cacher = NewKvCacher(name, cfg.AppendEventFunc(indexer.OnCacheEvent))
	default:
		util.Logger().Infof("service center will not cache '%s'", name)
	}
	return
}