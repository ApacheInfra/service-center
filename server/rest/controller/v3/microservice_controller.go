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
package v3

import (
	"github.com/apache/incubator-servicecomb-service-center/pkg/rest"
	"github.com/apache/incubator-servicecomb-service-center/server/rest/controller/v4"
)

type MicroServiceService struct {
	v4.MicroServiceService
}

func (this *MicroServiceService) URLPatterns() []rest.Route {
	return []rest.Route{
		{rest.HTTP_METHOD_GET, "/registry/v3/existence", this.GetExistence},
		{rest.HTTP_METHOD_GET, "/registry/v3/microservices", this.GetServices},
		{rest.HTTP_METHOD_GET, "/registry/v3/microservices/:serviceId", this.GetServiceOne},
		{rest.HTTP_METHOD_POST, "/registry/v3/microservices", this.Register},
		{rest.HTTP_METHOD_PUT, "/registry/v3/microservices/:serviceId/properties", this.Update},
		{rest.HTTP_METHOD_DELETE, "/registry/v3/microservices/:serviceId", this.Unregister},
		{rest.HTTP_METHOD_DELETE, "/registry/v3/microservices", this.UnregisterServices},
	}
}
