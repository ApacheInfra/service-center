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
package util

import (
	"errors"
	apt "github.com/ServiceComb/service-center/server/core"
	"github.com/ServiceComb/service-center/server/core/backend/store"
	"github.com/ServiceComb/service-center/server/infra/registry"
	"golang.org/x/net/context"
)

func HeartbeatUtil(ctx context.Context, domainProject string, serviceId string, instanceId string) (leaseID int64, ttl int64, err error, isInnerErr bool) {
	leaseID, err = GetLeaseId(ctx, domainProject, serviceId, instanceId)
	if err != nil {
		return leaseID, ttl, err, true
	}
	ttl, err = KeepAliveLease(ctx, domainProject, serviceId, instanceId, leaseID)
	return leaseID, ttl, nil, false
}

func KeepAliveLease(ctx context.Context, domainProject, serviceId, instanceId string, leaseID int64) (ttl int64, err error) {
	if leaseID == -1 {
		return ttl, errors.New("leaseId not exist, instance not exist.")
	}
	ttl, err = store.Store().KeepAlive(ctx,
		registry.WithStrKey(apt.GenerateInstanceLeaseKey(domainProject, serviceId, instanceId)),
		registry.WithLease(leaseID))
	if err != nil {
		return ttl, err
	}
	return ttl, nil
}
