///*
// * Licensed to the Apache Software Foundation (ASF) under one or more
// * contributor license agreements.  See the NOTICE file distributed with
// * this work for additional information regarding copyright ownership.
// * The ASF licenses this file to You under the Apache License, Version 2.0
// * (the "License"); you may not use this file except in compliance with
// * the License.  You may obtain a copy of the License at
// *
// *     http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//
package sd

// Cacher manages cache of some data source, e.g. mongo.
// An user can do nothing but read the managed cache.
type Cacher interface {
	// Cache gets the cache that Cacher manages.
	Cache() CacheReader
}

// Cache stores k-v data.
type Cache interface {
	CacheReader

	Put(id string, v interface{})

	// Remove remove a value by id (example instance_id service_id)
	Remove(id string)

	MarkDirty()

	Dirty() bool

	Clear()
}

// CacheReader reads k-v data.
type CacheReader interface {
	Name() string // The name of implementation

	Size() int    // the bytes size of the cache

	// Get gets a value by id (example instance_id service_id)
	Get(id string) interface{}

	// ForEach executes the given function for each of the k-v
	ForEach(iter func(k string, v interface{}) (next bool))
}
