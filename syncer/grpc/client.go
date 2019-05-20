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
package grpc

import (
	"context"
	"sync"

	pb "github.com/apache/servicecomb-service-center/syncer/proto"
	"google.golang.org/grpc"
)

var (
	clients = make(map[string]*Client)
	lock    sync.RWMutex
)

// Client struct
type Client struct {
	addr string
	cli  pb.SyncClient
}

// newClient new grpc client
func newClient(addr string) (*Client, error) {
	conn, err := grpc.Dial(addr, grpc.WithInsecure())
	if err != nil {
		return nil, err
	}
	return &Client{cli: pb.NewSyncClient(conn), addr: addr}, nil
}

// Pull data to be synchronized from the specified datacenter
func (c *Client) Pull(ctx context.Context) (*pb.SyncData, error) {
	return c.cli.Pull(ctx, &pb.PullRequest{})
}

// GetClient Get the client from the client caches with addr
func GetClient(addr string) *Client {
	lock.RLock()
	cli, ok := clients[addr]
	lock.RUnlock()
	if !ok {
		nc, err := newClient(addr)
		if err != nil {
			return nil
		}
		cli = nc
		lock.Lock()
		clients[addr] = cli
		lock.Unlock()
	}
	return cli
}
