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

package checker

import (
	"context"
	"github.com/apache/servicecomb-service-center/datasource/mongo/dao"
	mutil "github.com/apache/servicecomb-service-center/datasource/mongo/util"
	"time"

	"go.mongodb.org/mongo-driver/bson"
)

func updateInstanceRefreshTime(ctx context.Context, serviceID string, instanceID string) error {
	filter := bson.D{
		{mutil.ConnectWithDot([]string{dao.ColumnInstance, dao.ColumnServiceID}), serviceID},
		{mutil.ConnectWithDot([]string{dao.ColumnInstance, dao.ColumnInstanceID}), instanceID},
	}
	setValue := bson.D{
		{dao.ColumnRefreshTime, time.Now()},
	}
	updateFilter := bson.D{
		{"$set", setValue},
	}
	return dao.UpdateInstance(ctx, filter, updateFilter)
}
