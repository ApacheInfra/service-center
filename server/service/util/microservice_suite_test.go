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
package util_test

import (
	"fmt"
	serviceUtil "github.com/ServiceComb/service-center/server/service/util"
	"github.com/coreos/etcd/mvcc/mvccpb"
	. "github.com/onsi/ginkgo"
	"github.com/onsi/ginkgo/reporters"
	. "github.com/onsi/gomega"
	"testing"
)

func init() {
}

func TestMicroservice(t *testing.T) {
	RegisterFailHandler(Fail)
	junitReporter := reporters.NewJUnitReporter("model.junit.xml")
	RunSpecsWithDefaultAndCustomReporters(t, "model Suite", []Reporter{junitReporter})
}

func TestFindServiceIds(t *testing.T) {

}

func BenchmarkVersionRule_Latest_GetServicesIds(b *testing.B) {
	var kvs = make([]*mvccpb.KeyValue, b.N)
	for i := 1; i <= b.N; i++ {
		kvs[i-1] = &mvccpb.KeyValue{
			Key:   []byte(fmt.Sprintf("/service/ver/1.%d", i)),
			Value: []byte(fmt.Sprintf("%d", i)),
		}
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		serviceUtil.VersionRule(serviceUtil.Latest).Match(kvs)
	}
	// 5000	  13191856 ns/op
}

func BenchmarkVersionRule_Range_GetServicesIds(b *testing.B) {
	var kvs = make([]*mvccpb.KeyValue, b.N)
	for i := 1; i <= b.N; i++ {
		kvs[i-1] = &mvccpb.KeyValue{
			Key:   []byte(fmt.Sprintf("/service/ver/1.%d", i)),
			Value: []byte(fmt.Sprintf("%d", i)),
		}
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		serviceUtil.VersionRule(serviceUtil.Range).Match(kvs, fmt.Sprintf("1.%d", i), fmt.Sprintf("1.%d", i+b.N/10))
	}
	// 5000	  19754095 ns/op
}

func BenchmarkVersionRule_AtLess_GetServicesIds(b *testing.B) {
	var kvs = make([]*mvccpb.KeyValue, b.N)
	for i := 1; i <= b.N; i++ {
		kvs[i-1] = &mvccpb.KeyValue{
			Key:   []byte(fmt.Sprintf("/service/ver/1.%d", i)),
			Value: []byte(fmt.Sprintf("%d", i)),
		}
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		serviceUtil.VersionRule(serviceUtil.AtLess).Match(kvs, fmt.Sprintf("1.%d", i))
	}
	// 5000	  18701493 ns/op
}
