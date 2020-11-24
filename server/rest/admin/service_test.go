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
package admin_test

// initialize
import _ "github.com/apache/servicecomb-service-center/test"
import (
	"context"
	"github.com/apache/servicecomb-service-center/pkg/dump"
	"github.com/apache/servicecomb-service-center/pkg/registry"
	"github.com/apache/servicecomb-service-center/pkg/util"
	"github.com/apache/servicecomb-service-center/server/rest/admin"
	scerr "github.com/apache/servicecomb-service-center/server/scerror"
	"github.com/astaxie/beego"
	"github.com/stretchr/testify/assert"
	"testing"
)

func init() {
	beego.AppConfig.Set("registry_plugin", "etcd")
}
func TestAdminService_Dump(t *testing.T) {
	t.Log("execute 'dump' operation,when get all,should be passed")
	resp, err := admin.AdminServiceAPI.Dump(getContext(), &dump.Request{})
	assert.NoError(t, err)
	assert.Equal(t, registry.ResponseSuccess, resp.Response.GetCode())
	t.Log("execute 'dump' operation,when get by domain project,should be passed")
	resp, err = admin.AdminServiceAPI.Dump(
		util.SetDomainProject(context.Background(), "x", "x"),
		&dump.Request{})
	assert.NoError(t, err)
	assert.Equal(t, scerr.ErrForbidden, resp.Response.GetCode())
}

func getContext() context.Context {
	return util.SetContext(
		util.SetDomainProject(context.Background(), "default", "default"),
		util.CtxNocache, "1")
}
