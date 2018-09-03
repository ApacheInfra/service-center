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
package service_test

// initialize
import _ "github.com/apache/incubator-servicecomb-service-center/server/init"
import _ "github.com/apache/incubator-servicecomb-service-center/server/bootstrap"
import (
	"github.com/apache/incubator-servicecomb-service-center/pkg/util"
	"github.com/apache/incubator-servicecomb-service-center/server/core"
	pb "github.com/apache/incubator-servicecomb-service-center/server/core/proto"
	"github.com/apache/incubator-servicecomb-service-center/server/service"
	serviceUtil "github.com/apache/incubator-servicecomb-service-center/server/service/util"
	. "github.com/onsi/ginkgo"
	"github.com/onsi/ginkgo/reporters"
	. "github.com/onsi/gomega"
	"golang.org/x/net/context"
	"testing"
)

var serviceResource pb.ServiceCtrlServer
var instanceResource pb.ServiceInstanceCtrlServerEx

var _ = BeforeSuite(func() {
	//init plugin
	core.ServerInfo.Config.EnableCache = false
	serviceResource, instanceResource = service.AssembleResources()
})

func getContext() context.Context {
	return util.SetContext(
		util.SetDomainProject(context.Background(), "default", "default"),
		serviceUtil.CTX_NOCACHE, "1")
}

func TestGrpc(t *testing.T) {
	RegisterFailHandler(Fail)
	junitReporter := reporters.NewJUnitReporter("model.junit.xml")
	RunSpecsWithDefaultAndCustomReporters(t, "model Suite", []Reporter{junitReporter})
}

func TestRegisterGrpcServices(t *testing.T) {
	defer func() {
		recover()
	}()
	service.RegisterGrpcServices(nil)
}
