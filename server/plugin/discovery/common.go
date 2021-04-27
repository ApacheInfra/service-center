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
package discovery

import (
	"time"
)

const (
	DefaultTimeout       = 30 * time.Second
	DefaultCacheInitSize = 100
	// DefaultPageSize is the max record count of one request,
	// We assume the common Key/Value body size less then 384B.
	// 1. grpc does not allow to transport a large body more then 4MB in a request
	// See: google.golang.org/grpc/server.go
	// 2. etcdserver set the default size is 1.5MB
	// See: github.com/coreos/etcd/embed/config.go
	DefaultPageSize = 4096
)
