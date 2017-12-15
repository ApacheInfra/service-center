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
package store

import (
	errorsEx "github.com/ServiceComb/service-center/pkg/errors"
	"github.com/ServiceComb/service-center/pkg/util"
	"github.com/ServiceComb/service-center/server/core/backend"
	"github.com/ServiceComb/service-center/server/infra/registry"
	"golang.org/x/net/context"
	"time"
)

type LeaseAsyncTask struct {
	key        string
	LeaseID    int64
	TTL        int64
	CreateTime time.Time
	StartTime  time.Time
	EndTime    time.Time
	err        error
}

func (lat *LeaseAsyncTask) Key() string {
	return lat.key
}

func (lat *LeaseAsyncTask) Do(ctx context.Context) error {
	lat.StartTime = time.Now()
	lat.TTL, lat.err = backend.Registry().LeaseRenew(ctx, lat.LeaseID)
	lat.EndTime = time.Now()
	if lat.err == nil {
		util.LogNilOrWarnf(lat.CreateTime, "renew lease %d(rev: %s, run: %s), key %s",
			lat.LeaseID,
			lat.CreateTime.Format(TIME_FORMAT),
			lat.StartTime.Format(TIME_FORMAT),
			lat.Key())
		return nil
	}

	util.Logger().Errorf(lat.err, "[%s]renew lease %d failed(rev: %s, run: %s), key %s",
		time.Now().Sub(lat.CreateTime),
		lat.LeaseID,
		lat.CreateTime.Format(TIME_FORMAT),
		lat.StartTime.Format(TIME_FORMAT),
		lat.Key())
	if _, ok := lat.err.(errorsEx.InternalError); !ok {
		return lat.err
	}
	return nil
}

func (lat *LeaseAsyncTask) Err() error {
	return lat.err
}

func NewLeaseAsyncTask(op registry.PluginOp) *LeaseAsyncTask {
	return &LeaseAsyncTask{
		key:        ToLeaseAsyncTaskKey(util.BytesToStringWithNoCopy(op.Key)),
		LeaseID:    op.Lease,
		CreateTime: time.Now(),
	}
}

func ToLeaseAsyncTaskKey(key string) string {
	return "LeaseAsyncTask_" + key
}
