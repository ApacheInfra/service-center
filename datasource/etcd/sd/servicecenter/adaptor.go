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
	"github.com/apache/servicecomb-service-center/datasource/etcd/kv"
	"github.com/apache/servicecomb-service-center/datasource/etcd/sd"
)

// Adaptor implements sd.Adaptor.
// Adaptor does service pkg with other service-centers
// as it's cache.
type Adaptor struct {
	sd.Cacher
	sd.Indexer
}

func (se *Adaptor) Run() {
	if r, ok := se.Cacher.(sd.Runnable); ok {
		r.Run()
	}
}

func (se *Adaptor) Stop() {
	if r, ok := se.Cacher.(sd.Runnable); ok {
		r.Stop()
	}
}

func (se *Adaptor) Ready() <-chan struct{} {
	if r, ok := se.Cacher.(sd.Runnable); ok {
		return r.Ready()
	}
	return closedCh
}

func NewServiceCenterAdaptor(t sd.Type, cfg *sd.Config) *Adaptor {
	if t == kv.SCHEMA {
		return &Adaptor{
			Indexer: NewClusterIndexer(t, sd.NullCache),
			Cacher:  sd.NullCacher,
		}
	}
	cache := sd.NewKvCache(t.String(), cfg)
	return &Adaptor{
		Indexer: NewClusterIndexer(t, cache),
		Cacher:  BuildCacher(t, cfg, cache),
	}
}
