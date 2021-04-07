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

package event

import (
	"github.com/go-chassis/cari/discovery"

	"github.com/apache/servicecomb-service-center/datasource/mongo/client/model"
	"github.com/apache/servicecomb-service-center/datasource/mongo/sd"
	"github.com/apache/servicecomb-service-center/server/metrics"
)

// DomainEventHandler report domain & project total number
type DomainEventHandler struct {
}

func NewDomainEventHandler() *DomainEventHandler {
	return &DomainEventHandler{}
}

func (h *DomainEventHandler) Type() string {
	return model.ColumnDomain
}

func (h *DomainEventHandler) OnEvent(evt sd.MongoEvent) {
	action := evt.Type
	switch action {
	case discovery.EVT_INIT, discovery.EVT_CREATE:
		metrics.ReportDomains(increaseOne)
	case discovery.EVT_DELETE:
		metrics.ReportDomains(decreaseOne)
	}
}
