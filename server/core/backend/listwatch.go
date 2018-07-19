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
	"fmt"
	"github.com/apache/incubator-servicecomb-service-center/pkg/util"
	"github.com/apache/incubator-servicecomb-service-center/server/infra/registry"
	"golang.org/x/net/context"
)

type ListWatch interface {
	List(op ListWatchConfig) (*registry.PluginResponse, error)
	DoWatch(ctx context.Context, f func(*registry.PluginResponse)) error
	// not support new multiple watchers
	Watch(op ListWatchConfig) Watcher
	Revision() int64
}

type PrefixListWatch struct {
	Client registry.Registry
	Prefix string

	rev int64
}

func (lw *PrefixListWatch) List(op ListWatchConfig) (*registry.PluginResponse, error) {
	otCtx, _ := context.WithTimeout(op.Context, op.Timeout)
	resp, err := lw.Client.Do(otCtx, registry.WatchPrefixOpOptions(lw.Prefix)...)
	if err != nil {
		util.Logger().Errorf(err, "list prefix %s failed, current rev: %d", lw.Prefix, lw.Revision())
		return nil, err
	}
	lw.setRevision(resp.Revision)
	return resp, nil
}

func (lw *PrefixListWatch) Revision() int64 {
	return lw.rev
}

func (lw *PrefixListWatch) setRevision(rev int64) {
	lw.rev = rev
}

func (lw *PrefixListWatch) Watch(op ListWatchConfig) Watcher {
	return NewWatcher(lw, op)
}

func (lw *PrefixListWatch) DoWatch(ctx context.Context, f func(*registry.PluginResponse)) error {
	rev := lw.Revision()
	opts := append(
		registry.WatchPrefixOpOptions(lw.Prefix),
		registry.WithRev(rev+1),
		registry.WithWatchCallback(
			func(message string, resp *registry.PluginResponse) error {
				if resp == nil || len(resp.Kvs) == 0 {
					return fmt.Errorf("unknown event %s, watch prefix %s", resp, lw.Prefix)
				}

				lw.setRevision(resp.Revision)

				f(resp)
				return nil
			}))

	err := lw.Client.Watch(ctx, opts...)
	if err != nil { // compact可能会导致watch失败 or message body size lager than 4MB
		util.Logger().Errorf(err, "watch prefix %s failed, start rev: %d+1->%d->0", lw.Prefix, rev, lw.Revision())

		lw.setRevision(0)
		f(nil)
	}
	return err
}
