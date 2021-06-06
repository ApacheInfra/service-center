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

package service

import (
	"context"

	"github.com/go-chassis/cari/discovery"
	"github.com/go-chassis/cari/pkg/errsvc"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/apache/servicecomb-service-center/datasource"
	"github.com/apache/servicecomb-service-center/datasource/mongo/client"
	"github.com/apache/servicecomb-service-center/datasource/mongo/model"
	"github.com/apache/servicecomb-service-center/pkg/log"
)

func findInstances(ctx context.Context, filter interface{}) ([]*model.Instance, error) {
	res, err := client.GetMongoClient().Find(ctx, model.CollectionInstance, filter)
	if err != nil {
		return nil, err
	}
	var instances []*model.Instance
	for res.Next(ctx) {
		var tmp *model.Instance
		err := res.Decode(&tmp)
		if err != nil {
			return nil, err
		}
		instances = append(instances, tmp)
	}
	return instances, nil
}

func findInstance(ctx context.Context, filter interface{}) (*model.Instance, error) {
	findRes, err := client.GetMongoClient().FindOne(ctx, model.CollectionInstance, filter)
	if err != nil {
		return nil, err
	}
	var instance *model.Instance
	if findRes.Err() != nil {
		//not get any service,not db err
		return nil, datasource.ErrNoData
	}
	err = findRes.Decode(&instance)
	if err != nil {
		log.Error("failed to decode instance", err)
		return nil, err
	}
	return instance, nil
}

func countInstance(ctx context.Context, filter interface{}) (int64, error) {
	count, err := client.GetMongoClient().Count(ctx, model.CollectionInstance, filter)
	if err != nil {
		return 0, err
	}
	return count, nil
}

func findMicroServiceInstances(ctx context.Context, filter interface{}, opts ...*options.FindOptions) ([]*discovery.MicroServiceInstance, error) {
	res, err := client.GetMongoClient().Find(ctx, model.CollectionInstance, filter, opts...)
	if err != nil {
		return nil, err
	}
	var instances []*discovery.MicroServiceInstance
	for res.Next(ctx) {
		var tmp model.Instance
		err := res.Decode(&tmp)
		if err != nil {
			return nil, err
		}
		instances = append(instances, tmp.Instance)
	}
	return instances, nil
}

func batchInsertMicroServiceInstances(ctx context.Context, document []interface{}, opts ...*options.InsertManyOptions) error {
	_, err := client.GetMongoClient().BatchInsert(ctx, model.CollectionInstance, document, opts...)
	return err
}

func existInstance(ctx context.Context, filter interface{}) (bool, error) {
	return client.GetMongoClient().DocExist(ctx, model.CollectionInstance, filter)
}

func updateInstance(ctx context.Context, filter interface{}, update interface{}, opts ...*options.UpdateOptions) *errsvc.Error {
	_, err := client.GetMongoClient().Update(ctx, model.CollectionInstance, filter, update, opts...)
	if err != nil {
		return discovery.NewError(discovery.ErrUnavailableBackend, err.Error())
	}
	return nil
}

func deleteInstance(ctx context.Context, filter interface{}) (bool, error) {
	return client.GetMongoClient().DocDelete(ctx, model.CollectionInstance, filter)
}
