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
package store

import (
	apt "github.com/ServiceComb/service-center/server/core"
	pb "github.com/ServiceComb/service-center/server/core/proto"
	"github.com/ServiceComb/service-center/server/core/registry"
	"github.com/ServiceComb/service-center/util"
	"golang.org/x/net/context"
	"strconv"
	"sync"
)

const (
	SERVICE StoreType = iota
	INSTANCE
	DOMAIN
	SCHEMA // big data should not be stored in memory.
	RULE
	LEASE
	SERVICE_INDEX
	SERVICE_ALIAS
	SERVICE_TAG
	RULE_INDEX
	DEPENDENCY
	DEPENDENCY_RULE
	ENDPOINTS_INDEX
	typeEnd
)

var TypeNames = []string{
	SERVICE:         "SERVICE",
	INSTANCE:        "INSTANCE",
	DOMAIN:          "DOMAIN",
	SCHEMA:          "SCHEMA",
	RULE:            "RULE",
	LEASE:           "LEASE",
	SERVICE_INDEX:   "SERVICE_INDEX",
	SERVICE_ALIAS:   "SERVICE_ALIAS",
	SERVICE_TAG:     "SERVICE_TAG",
	RULE_INDEX:      "RULE_INDEX",
	DEPENDENCY:      "DEPENDENCY",
	DEPENDENCY_RULE: "DEPENDENCY_RULE",
	ENDPOINTS_INDEX: "ENDPOINTS_INDEX",
}

var TypeRoots = map[StoreType]string{
	SERVICE:  apt.GetServiceRootKey(""),
	INSTANCE: apt.GetInstanceRootKey(""),
	DOMAIN:   apt.GetDomainRootKey() + "/",
	// SCHEMA:
	RULE:            apt.GetServiceRuleRootKey(""),
	LEASE:           apt.GetInstanceLeaseRootKey(""),
	SERVICE_INDEX:   apt.GetServiceIndexRootKey(""),
	SERVICE_ALIAS:   apt.GetServiceAliasRootKey(""),
	SERVICE_TAG:     apt.GetServiceTagRootKey(""),
	RULE_INDEX:      apt.GetServiceRuleIndexRootKey(""),
	DEPENDENCY:      apt.GetServiceDependencyRootKey(""),
	DEPENDENCY_RULE: apt.GetServiceDependencyRuleRootKey(""),
	ENDPOINTS_INDEX: apt.GetInstancesEndpointsIndexRootKey(""),
}

var store *KvStore

func init() {
	store = &KvStore{
		indexers:    make(map[StoreType]*Indexer),
		asyncTasker: NewAsyncTasker(),
		ready:       make(chan struct{}),
	}
	for i := StoreType(0); i != typeEnd; i++ {
		store.newNullStore(i)
	}
	AddEventHandleFunc(DOMAIN, store.onDomainEvent)
	AddEventHandleFunc(LEASE, store.onLeaseEvent)
}

type LeaseAsyncTask struct {
	key     string
	LeaseID int64
	TTL     int64
	err     error
}

func (lat *LeaseAsyncTask) Key() string {
	return lat.key
}

func (lat *LeaseAsyncTask) Do(ctx context.Context) error {
	lat.TTL, lat.err = registry.GetRegisterCenter().LeaseRenew(ctx, lat.LeaseID)
	if lat.err != nil {
		util.Logger().Errorf(lat.err, "renew lease %d failed, key %s", lat.LeaseID, lat.Key())
	}
	return lat.err
}

func (lat *LeaseAsyncTask) Err() error {
	return lat.err
}

type StoreType int

func (st StoreType) String() string {
	if int(st) < len(TypeNames) {
		return TypeNames[st]
	}
	return "TYPE" + strconv.Itoa(int(st))
}

type KvStore struct {
	indexers    map[StoreType]*Indexer
	asyncTasker *AsyncTasker
	lock        sync.RWMutex
	ready       chan struct{}
	isClose     bool
}

func (s *KvStore) newStore(t StoreType, initSize int) {
	s.newCacherStore(t, NewCacher(initSize, TypeRoots[t],
		func(evt *KvEvent) {
			s.indexers[t].OnCacheEvent(evt)
			select {
			case <-s.Ready():
				EventHandler(t).OnEvent(evt)
			default:
			}
		}))
}

func (s *KvStore) newNullStore(t StoreType) {
	s.newCacherStore(t, NullCacher)
}

func (s *KvStore) newCacherStore(t StoreType, cacher Cacher) {
	indexer := NewCacheIndexer(t, cacher)
	s.indexers[t] = indexer
	indexer.Run()
}

func (s *KvStore) Run() {
	go s.store()
	s.asyncTasker.Run()
}

func (s *KvStore) store() {
	s.newStore(DOMAIN, 10)
	s.newStore(SERVICE, 100)
	s.newStore(INSTANCE, 1000)
	s.newStore(LEASE, 1000)
	s.newStore(SERVICE_INDEX, 100)
	s.newStore(SERVICE_ALIAS, 100)
	s.newStore(ENDPOINTS_INDEX, 1000)
	s.newStore(DEPENDENCY, 100)
	s.newStore(DEPENDENCY_RULE, 100)
	s.newStore(SERVICE_TAG, 100)
	s.newStore(RULE, 100)
	s.newStore(RULE_INDEX, 100)
	for _, i := range s.indexers {
		<-i.Ready()
	}
	util.SafeCloseChan(s.ready)

	util.Logger().Debugf("all indexers are ready")
}

func (s *KvStore) onDomainEvent(evt *KvEvent) {
	kv := evt.KV
	action := evt.Action
	tenant, _ := pb.GetInfoFromDomainKV(kv)

	if action != pb.EVT_CREATE {
		util.Logger().Infof("tenant '%s' is %s", tenant, action)
		return
	}

	if len(tenant) == 0 {
		util.Logger().Errorf(nil,
			"unmarshal tenant info failed, key %s [%s] event", util.BytesToStringWithNoCopy(kv.Key), action)
		return
	}

	util.Logger().Infof("new tenant %s is created", tenant)
}

func (s *KvStore) onLeaseEvent(evt *KvEvent) {
	if evt.Action != pb.EVT_DELETE {
		return
	}

	key := util.BytesToStringWithNoCopy(evt.KV.Key)
	leaseID := util.BytesToStringWithNoCopy(evt.KV.Value)

	s.removeAsyncTask(key)

	util.Logger().Debugf("push task to async remove queue successfully, key %s %s [%s] event",
		key, leaseID, evt.Action)
}

func (s *KvStore) removeAsyncTask(key string) {
	s.asyncTasker.DeferRemoveTask(key)
}

func (s *KvStore) closed() bool {
	return s.isClose
}

func (s *KvStore) Stop() {
	if s.isClose {
		return
	}
	s.isClose = true

	for _, i := range s.indexers {
		i.Stop()
	}

	s.asyncTasker.Stop()

	util.SafeCloseChan(s.ready)

	util.Logger().Debugf("store daemon stopped.")
}

func (s *KvStore) Ready() <-chan struct{} {
	<-s.asyncTasker.Ready()
	return s.ready
}

func (s *KvStore) Service() *Indexer {
	return s.indexers[SERVICE]
}

func (s *KvStore) Instance() *Indexer {
	return s.indexers[INSTANCE]
}

func (s *KvStore) Lease() *Indexer {
	return s.indexers[LEASE]
}

func (s *KvStore) ServiceIndex() *Indexer {
	return s.indexers[SERVICE_INDEX]
}

func (s *KvStore) ServiceAlias() *Indexer {
	return s.indexers[SERVICE_ALIAS]
}

func (s *KvStore) ServiceTag() *Indexer {
	return s.indexers[SERVICE_TAG]
}

func (s *KvStore) Rule() *Indexer {
	return s.indexers[RULE]
}

func (s *KvStore) RuleIndex() *Indexer {
	return s.indexers[RULE_INDEX]
}

func (s *KvStore) Schema() *Indexer {
	return s.indexers[SCHEMA]
}

func (s *KvStore) Dependency() *Indexer {
	return s.indexers[DEPENDENCY]
}

func (s *KvStore) DependencyRule() *Indexer {
	return s.indexers[DEPENDENCY_RULE]
}

func (s *KvStore) EndpointsIndex() *Indexer {
	return s.indexers[ENDPOINTS_INDEX]
}

func (s *KvStore) Domain() *Indexer {
	return s.indexers[DOMAIN]
}

func (s *KvStore) KeepAlive(ctx context.Context, op *registry.PluginOp) (int64, error) {
	t := NewLeaseAsyncTask(op)
	if op.Mode == registry.MODE_NO_CACHE {
		util.Logger().Debugf("keep alive lease WitchNoCache, request etcd server, op: %s", op)
		err := t.Do(ctx)
		ttl := t.TTL
		return ttl, err
	}

	err := s.asyncTasker.AddTask(ctx, t)
	if err != nil {
		return 0, err
	}
	itf, err := s.asyncTasker.LatestHandled(t.Key())
	if err != nil {
		return 0, err
	}
	pt := itf.(*LeaseAsyncTask)
	return pt.TTL, pt.Err()
}

func (s *KvStore) AsyncTasker() *AsyncTasker {
	return s.asyncTasker
}

func Store() *KvStore {
	return store
}

func NewLeaseAsyncTask(op *registry.PluginOp) *LeaseAsyncTask {
	return &LeaseAsyncTask{
		key:     "LeaseAsyncTask_" + util.BytesToStringWithNoCopy(op.Key),
		LeaseID: op.Lease,
	}
}

func Revision() (rev int64) {
	for _, i := range Store().indexers {
		if rev < i.Cache().Version() {
			rev = i.Cache().Version()
		}
	}
	return
}
