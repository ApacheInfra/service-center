// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package discovery

import "testing"

func TestTypes(t *testing.T) {
	id, _ := Install(NewAddOn("TestTypes", Configure()))
	found := false
	for _, t := range Types() {
		if t == id {
			found = true
		}
	}
	if !found {
		t.Fatalf("TestTypes failed")
	}
	if id.String() != "TestTypes" {
		t.Fatalf("TestTypes failed")
	}
	if TypeError.String() != "TypeError" {
		t.Fatalf("TestTypes failed")
	}

	var kv KeyValue
	if kv.String() != "{key: '', value: null, version: 0}" {
		t.Fatalf("TestTypes failed, %v", kv.String())
	}
}
