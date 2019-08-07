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

package storage

import (
	"context"
	"sync"

	"github.com/apache/servicecomb-service-center/pkg/log"
	pb "github.com/apache/servicecomb-service-center/syncer/proto"
	"github.com/coreos/etcd/clientv3"
	"github.com/gogo/protobuf/proto"
)

type Storage interface {
	GetData() (data *pb.SyncData)
	UpdateData(data *pb.SyncData)
	GetMaps() (maps pb.SyncMapping)
	UpdateMaps(maps pb.SyncMapping)
	GetMapByCluster(clusterName string) (mapping pb.SyncMapping)
	UpdateMapByCluster(clusterName string, mapping pb.SyncMapping)
}

type storage struct {
	engine clientv3.KV
	data   *pb.SyncData
	lock   sync.RWMutex
}

func NewStorage(engine clientv3.KV) Storage {
	storage := &storage{
		engine: engine,
		data:   &pb.SyncData{},
	}
	return storage
}

// UpdateData Update data to storage
func (s *storage) UpdateData(data *pb.SyncData) {
	services := s.GetServices()
	s.UpdateServices(data.Services)
	s.compareAndDeleteServices(services, data.Services)

	instances := s.GetInstances()
	s.UpdateInstances(data.Instances)
	s.compareAndDeleteInstances(instances, data.Instances)
}

// GetData Get data from storage
func (s *storage) GetData() (data *pb.SyncData) {
	data = &pb.SyncData{
		Services:  s.GetServices(),
		Instances: s.GetInstances(),
	}
	return
}

func (s *storage) compareAndDeleteServices(origin, renew []*pb.SyncService) {
	expires := make([]*pb.SyncService, 0, len(origin))
next:
	for _, item := range origin {
		for _, service := range renew {
			if service.ServiceId == item.ServiceId {
				continue next
			}
		}
		expires = append(expires, item)
	}
	s.DeleteServices(expires)
}

func (s *storage) compareAndDeleteInstances(origin, renew []*pb.SyncInstance) {
	expires := make([]*pb.SyncInstance, 0, len(origin))
next:
	for _, item := range origin {
		for _, service := range renew {
			if service.InstanceId == item.InstanceId {
				continue next
			}
		}
		expires = append(expires, item)
	}
	s.DeleteInstances(expires)
}

// cleanExpired clean the expired instances in the maps
func (s *storage) cleanExpired(sources pb.SyncMapping, actives pb.SyncMapping) {
	expires := make(pb.SyncMapping, 0, len(sources)-len(actives))
next:
	for _, entry := range sources {
		for _, act := range actives {
			if act.CurInstanceID == entry.CurInstanceID {
				continue next
			}
		}
		expires = append(expires, entry)
	}
	if len(expires) == 0 {
		return
	}
	s.deleteInstancesAndMaps(expires)
}

// UpdateServices Update services to storage
func (s *storage) UpdateServices(services []*pb.SyncService) {
	ops := make([]clientv3.Op, 0, len(services))
	for _, val := range services {
		data, err1 := proto.Marshal(val)
		if err1 != nil {
			log.Errorf(err1, "Proto marshal failed: %s", err1)
			continue
		}
		ops = append(ops, putServiceOp(val.ServiceId, data))
	}

	_, err := s.doTxn(ops)
	if err != nil {
		log.Errorf(err, "Save services to etcd failed: %s", err)
	}
}

// GetServices Get services from storage
func (s *storage) GetServices() (services []*pb.SyncService) {
	services = make([]*pb.SyncService, 0, 10)
	s.getValue(getServicesOp(), func(key, val []byte) (next bool) {
		next = true
		item := &pb.SyncService{}
		if err := proto.Unmarshal(val, item); err != nil {
			log.Errorf(err, "Proto unmarshal '%s' failed: %s", val, err)
			return
		}
		services = append(services, item)
		return
	})
	return
}

// DeleteServices Delete services from storage
func (s *storage) DeleteServices(services []*pb.SyncService) {
	ops := make([]clientv3.Op, 0, len(services))
	for _, val := range services {
		ops = append(ops, deleteServiceOp(val.ServiceId))
	}
	_, err := s.doTxn(ops)
	if err != nil {
		log.Errorf(err, "Delete services from etcd failed: %s", err)
	}
}

// UpdateInstances Update instances to storage
func (s *storage) UpdateInstances(instances []*pb.SyncInstance) {
	ops := make([]clientv3.Op, 0, len(instances))
	for _, val := range instances {
		data, err1 := proto.Marshal(val)
		if err1 != nil {
			log.Errorf(err1, "Proto marshal failed: %s", err1)
			continue
		}
		ops = append(ops, putInstanceOp(val.InstanceId, data))
	}

	_, err := s.doTxn(ops)
	if err != nil {
		log.Errorf(err, "Save instances to etcd failed: %s", err)
	}
}

// GetInstances Get instances from storage
func (s *storage) GetInstances() (instances []*pb.SyncInstance) {
	instances = make([]*pb.SyncInstance, 0, 10)
	s.getValue(getInstancesOp(), func(key, val []byte) (next bool) {
		next = true
		item := &pb.SyncInstance{}
		if err := proto.Unmarshal(val, item); err != nil {
			log.Errorf(err, "Proto unmarshal '%s' failed: %s", val, err)
			return
		}
		instances = append(instances, item)
		return
	})
	return
}

// DeleteInstances Delete instances from storage
func (s *storage) DeleteInstances(instances []*pb.SyncInstance) {
	ops := make([]clientv3.Op, 0, len(instances))
	for _, val := range instances {
		ops = append(ops, deleteInstanceOp(val.InstanceId))
	}
	_, err := s.doTxn(ops)
	if err != nil {
		log.Errorf(err, "Delete instances from etcd failed: %s", err)
	}
}

func (s *storage) deleteInstancesAndMaps(mapping pb.SyncMapping) {
	ops := make([]clientv3.Op, 0, len(mapping))
	for _, val := range mapping {
		ops = append(ops, deleteInstanceOp(val.CurInstanceID), delMappingOp(val.ClusterName, val.OrgInstanceID))
	}
	_, err := s.doTxn(ops)
	if err != nil {
		log.Errorf(err, "Delete instances and maps from etcd failed: %s", err)
	}
}

// UpdateMapByCluster update map to storage by clusterName of other cluster
func (s *storage) UpdateMapByCluster(clusterName string, mapping pb.SyncMapping) {
	ops := make([]clientv3.Op, 0, len(mapping))
	for _, val := range mapping {
		data, err1 := proto.Marshal(val)
		if err1 != nil {
			log.Errorf(err1, "Proto marshal failed: %s", err1)
			continue
		}
		ops = append(ops, putMappingOp(val.ClusterName, val.OrgInstanceID, data))
	}

	_, err := s.doTxn(ops)
	if err != nil {
		log.Errorf(err, "Save mappings to etcd failed: %s", err)
		return
	}
	s.cleanExpired(s.GetMapByCluster(clusterName), mapping)
}

// GetMapByCluster get map by clusterName of other cluster
func (s *storage) GetMapByCluster(clusterName string) (mapping pb.SyncMapping) {
	s.getValue(getClusterMappingsOp(clusterName), func(key, val []byte) (next bool) {
		next = true
		item := &pb.MappingEntry{}
		if err := proto.Unmarshal(val, item); err != nil {
			log.Errorf(err, "Proto unmarshal '%s' failed: %s", val, err)
			return
		}
		mapping = append(mapping, item)
		return
	})
	return
}

// UpdateMaps update all maps to etcd
func (s *storage) UpdateMaps(maps pb.SyncMapping) {
	srcMaps := s.GetMaps()
	ops := make([]clientv3.Op, 0, len(maps))
	for _, val := range maps {
		data, err1 := proto.Marshal(val)
		if err1 != nil {
			log.Errorf(err1, "Proto marshal failed: %s", err1)
			continue
		}
		ops = append(ops, putMappingOp(val.ClusterName, val.OrgInstanceID, data))
	}

	_, err := s.doTxn(ops)
	if err != nil {
		log.Errorf(err, "Save mappings to etcd failed: %s", err)
		return
	}
	s.cleanExpired(srcMaps, maps)
}

// GetMaps Get maps from storage
func (s *storage) GetMaps() (mapping pb.SyncMapping) {
	s.getValue(getAllMappingsOp(), func(key, val []byte) (next bool) {
		next = true
		item := &pb.MappingEntry{}
		if err := proto.Unmarshal(val, item); err != nil {
			log.Errorf(err, "Proto unmarshal '%s' failed: %s", val, err)
			return
		}
		mapping = append(mapping, item)
		return
	})
	return
}

// getValue Get value from etcd by key
func (s *storage) getValue(opt clientv3.Op, handler func(key, val []byte) (next bool)) {
	resp, err := s.engine.Do(context.Background(), opt)
	if err != nil {
		log.Errorf(err, "Do etcd operation failed: %s", err)
		return
	}

	getResp := resp.Get()
	if getResp == nil {
		log.Error("Data from etcd is empty", nil)
		return
	}

	for _, kv := range resp.Get().Kvs {
		if !handler(kv.Key, kv.Value) {
			break
		}
	}
}

func (s *storage) doTxn(opts []clientv3.Op) (resp *clientv3.TxnResponse, err error) {
	resp, err = s.engine.Txn(context.Background()).Then(opts...).Commit()
	if err != nil {
		log.Errorf(err, "Do etcd transaction failed: %s", err)
	}
	return
}
