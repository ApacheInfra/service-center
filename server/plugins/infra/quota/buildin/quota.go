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
package buildin

import (
	"context"
	"fmt"
	"github.com/ServiceComb/service-center/server/core"
	"github.com/ServiceComb/service-center/server/core/registry"
	"github.com/ServiceComb/service-center/server/infra/quota"
	"github.com/ServiceComb/service-center/util"
)

type BuildInQuota struct {
}

func New() quota.QuotaManager {
	return &BuildInQuota{}
}
func init() {
	quota.QuotaPlugins["buildin"] = New
}

const (
	SERVICE_MAX_NUMBER  = 12000
	INSTANCE_MAX_NUMBER = 150000
)

//申请配额sourceType serviceinstance servicetype
func (q *BuildInQuota) Apply4Quotas(ctx context.Context, quotaType int, quotaSize int16) (bool, error) {
	var key string = ""
	var max int64 = 0
	tenant := ctx.Value("tenant").(string)
	switch quotaType {
	case quota.MicroServiceInstanceQuotaType:
		key = core.GetInstanceRootKey(tenant)
		max = INSTANCE_MAX_NUMBER
	case quota.MicroServiceQuotaType:
		key = core.GetServiceRootKey(tenant)
		max = SERVICE_MAX_NUMBER
	default:
		return false, fmt.Errorf("Unsurported Type %d", quotaType)
	}
	resp, err := registry.GetRegisterCenter().Do(ctx, &registry.PluginOp{
		Action:     registry.GET,
		Key:        []byte(key),
		CountOnly:  true,
		WithPrefix: true,
	})
	if err != nil {
		return false, err
	}
	num := resp.Count
	util.LOGGER.Debugf("resource num is %d", num)
	if num >= max {
		return false, nil
	}
	return true, nil
}

//向配额中心上报配额使用量
func (q *BuildInQuota) ReportCurrentQuotasUsage(ctx context.Context, quotaType int, usedQuotaSize int16) bool {

	return false
}
