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
package quota

import (
	"fmt"
	"golang.org/x/net/context"
	scerr "github.com/apache/incubator-servicecomb-service-center/server/error"
)

type ApplyQuotaResult struct {
	Reporter QuotaReporter
	Err      *scerr.Error
}

func NewApplyQuotaResult(reporter QuotaReporter, err *scerr.Error) *ApplyQuotaResult {
	return &ApplyQuotaResult{
		reporter,
		err,
	}
}

type ApplyQuotaRes struct {
	QuotaType     ResourceType
	DomainProject string
	ServiceId     string
	QuotaSize     int64
}

func NewApplyQuotaRes(quotaType ResourceType, domainProject, serviceId string, quotaSize int64) *ApplyQuotaRes {
	return &ApplyQuotaRes{
		quotaType,
		domainProject,
		serviceId,
		quotaSize,
	}
}

type QuotaManager interface {
	Apply4Quotas(ctx context.Context, req *ApplyQuotaRes) *ApplyQuotaResult
	RemandQuotas(ctx context.Context, quotaType ResourceType)
}

type QuotaReporter interface {
	ReportUsedQuota(ctx context.Context) error
	Close()
}

const (
	RuleQuotaType ResourceType = iota
	SchemaQuotaType
	TagQuotaType
	MicroServiceQuotaType
	MicroServiceInstanceQuotaType
	typeEnd
)

type ResourceType int

func (r ResourceType) String() string {
	switch r {
	case RuleQuotaType:
		return "RULE"
	case SchemaQuotaType:
		return "SCHEMA"
	case TagQuotaType:
		return "TAG"
	case MicroServiceQuotaType:
		return "SERVICE"
	case MicroServiceInstanceQuotaType:
		return "INSTANCE"
	default:
		return "RESOURCE" + fmt.Sprint(r)
	}
}
