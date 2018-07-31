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
package backend

import (
	"context"
	"fmt"
	errorsEx "github.com/apache/incubator-servicecomb-service-center/pkg/errors"
	"testing"
	"time"
)

func TestLeaseTask_Do(t *testing.T) {
	now := time.Now().UTC()
	c := &mockRegistry{}
	lt := &LeaseTask{
		Client:   c,
		key:      ToLeaseAsyncTaskKey("/a"),
		LeaseID:  1,
		recvSec:  now.Unix(),
		recvNsec: int64(now.Nanosecond()),
	}

	c.LeaseErr = errorsEx.InternalError("lease not found")
	err := lt.Do(context.Background())
	if err != nil || lt.Err() != nil {
		t.Fatalf("TestLeaseTask_Do failed")
	}

	c.LeaseErr = fmt.Errorf("network error")
	err = lt.Do(context.Background())
	if err == nil || lt.Err() == nil {
		t.Fatalf("TestLeaseTask_Do failed")
	}
}
