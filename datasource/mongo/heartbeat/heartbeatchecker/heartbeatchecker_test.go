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

package heartbeatchecker

import (
	"context"
	"testing"
	"time"

	"github.com/apache/servicecomb-service-center/datasource/mongo"
	"github.com/apache/servicecomb-service-center/datasource/mongo/client"
	pb "github.com/go-chassis/cari/discovery"
	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/bson"
)

func TestHeartbeat(t *testing.T) {
	t.Run("heartbeat: if the instance does not exist,the heartbeat should fail", func(t *testing.T) {
		heartBeatChecker := &HeartBeatChecker{}
		resp, err := heartBeatChecker.Heartbeat(context.Background(), &pb.HeartbeatRequest{
			ServiceId:  "not-exist-ins",
			InstanceId: "not-exist-ins",
		})
		assert.NotNil(t, err)
		assert.NotEqual(t, pb.ResponseSuccess, resp.Response.GetCode())
	})

	t.Run("heartbeat: if the instance does exist,the heartbeat should succeed", func(t *testing.T) {
		instance1 := mongo.Instance{
			RefreshTime: time.Now(),
			InstanceInfo: &pb.MicroServiceInstance{
				InstanceId: "instanceId1",
				ServiceId:  "serviceId1",
			},
		}
		_, err := client.GetMongoClient().Insert(context.Background(), mongo.CollectionInstance, instance1)
		assert.Equal(t, nil, err)
		heartBeatChecker := &HeartBeatChecker{}
		resp, err := heartBeatChecker.Heartbeat(context.Background(), &pb.HeartbeatRequest{
			ServiceId:  instance1.InstanceInfo.ServiceId,
			InstanceId: instance1.InstanceInfo.InstanceId,
		})
		assert.Nil(t, err)
		assert.Equal(t, pb.ResponseSuccess, resp.Response.GetCode())
		filter := bson.M{
			mongo.StringBuilder([]string{mongo.ColumnInstanceInfo, mongo.ColumnInstanceID}): instance1.InstanceInfo.InstanceId,
		}
		_, err = client.GetMongoClient().Delete(context.Background(), mongo.CollectionInstance, filter)
		assert.Nil(t, err)
	})
}
