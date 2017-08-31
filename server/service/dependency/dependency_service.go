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
package dependency

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/ServiceComb/service-center/pkg/common/cache"
	apt "github.com/ServiceComb/service-center/server/core"
	pb "github.com/ServiceComb/service-center/server/core/proto"
	"github.com/ServiceComb/service-center/server/core/registry"
	"github.com/ServiceComb/service-center/server/core/registry/store"
	ms "github.com/ServiceComb/service-center/server/service/microservice"
	"github.com/ServiceComb/service-center/util"
	"golang.org/x/net/context"
	"strings"
	"time"
)

var consumerCache *cache.Cache
var providerCache *cache.Cache

/*
缓存2分钟过期
1分钟周期缓存consumers 遍历所有serviceid并查询consumers 做缓存
当发现新查询到的consumers列表变成0时则不做cache set操作
这样当consumers关系完全被删除也有1分钟的时间窗让实例变化推送到相应的consumers里 1分鐘后緩存也會自動清理
实例推送中的依赖发现实时性为T+1分钟
*/
func init() {
	d, _ := time.ParseDuration("2m")
	consumerCache = cache.New(d, d)
	providerCache = cache.New(d, d)
	go autoSyncConsumers()
}

//TODO
func autoSyncConsumers() {
	//ticker := time.NewTicker(time.Minute * 1)
	//for t := range ticker.C {
	//	util.Logger().Debug(fmt.Sprintf("sync consumers at %s", t))
	//	keys := microservice.GetServiceWithRev(context.TODO())
	//	for _, v := range keys {
	//		util.Logger().Debug(fmt.Sprintf("sync consumers for %s", v))
	//		domainAndId := strings.Split(v, ":::")
	//		// 查询所有consumer
	//		key := apt.GenerateProviderDependencyKey(domainAndId[0], domainAndId[1], "")
	//		resp, err := registry.GetRegisterCenter().Do(context.TODO(), &registry.PluginOp{
	//			Action:     registry.GET,
	//			Key:        util.StringToBytesWithNoCopy(key),
	//			WithPrefix: true,
	//			KeyOnly:    true,
	//		})
	//		if err != nil {
	//			util.Logger().Errorf(err, "query service consumers failed, provider id %s", domainAndId[1])
	//		}
	//		if len(resp.Kvs) != 0 {
	//			consumerCache.Set(v, resp.Kvs, 0)
	//		}
	//
	//	}
	//}
}
func GetConsumersInCache(tenant string, providerId string, provider *pb.MicroService) ([]string, error) {
	// 查询所有consumer
	if provider == nil {
		provider, found := ms.MsCache().Get(providerId)
		if !found {
			util.Logger().Errorf(nil, "service not exist, %s", providerId)
			return nil, fmt.Errorf("service not exist, %s", providerId)
		}
		provider = provider.(*pb.MicroService)
	}
	dr := NewProviderDependencyRelation(tenant, providerId, provider)
	consumerIds, err := dr.GetDependencyConsumerIds()
	if err != nil {
		util.Logger().Errorf(err, "Get dependency consumerIds failed.%s", providerId)
		return nil, err
	}

	if len(consumerIds) == 0 {
		util.Logger().Warnf(nil, "Get consumer for publish from database is empty.%s , get from cache", providerId)
		consumerList, found := consumerCache.Get(providerId)
		if found && len(consumerList.([]string)) > 0 {
			return consumerList.([]string), nil
		}
		return nil, nil
	}
	consumerCache.Set(providerId, consumerIds, 0)

	return consumerIds, nil
}

func GetProvidersInCache(tenant string, consumerId string, consumer *pb.MicroService) ([]string, error) {
	// 查询所有provider
	if consumer == nil {
		consumerTmp, found := ms.MsCache().Get(consumerId)
		if !found {
			util.Logger().Errorf(nil, "service not exist, %s", consumerId)
			return nil, fmt.Errorf("service not exist, %s", consumerId)
		}
		consumer = consumerTmp.(*pb.MicroService)
	}
	dr := NewConsumerDependencyRelation(tenant, consumerId, consumer)
	providerIds, err := dr.GetDependencyProviderIds()
	if err != nil {
		util.Logger().Errorf(err, "Get dependency providerIds failed.%s", consumerId)
		return nil, err
	}

	if len(providerIds) == 0 {
		util.Logger().Warnf(nil, "Get consumer for publish from database is empty.%s , get from cache", consumerId)
		providerIds, found := providerCache.Get(consumerId)
		if found && len(providerIds.([]string)) > 0 {
			return providerIds.([]string), nil
		}
		return nil, nil
	}
	consumerCache.Set(consumerId, providerIds, 0)

	return providerIds, nil
}

func RefreshDependencyCache(tenant string, providerId string, provider *pb.MicroService) error {
	dr := NewDependencyRelation(tenant, providerId, provider, providerId, provider)
	consumerIds, err := dr.GetDependencyConsumerIds()
	if err != nil {
		util.Logger().Errorf(err, "%s,refresh dependency cache failed, get consumerIds failed.", providerId)
		return err
	}
	providerIds, err := dr.GetDependencyProviderIds()
	if err != nil {
		util.Logger().Errorf(err, "%s,refresh dependency cache failed, get providerIds failed.", providerId)
		return err
	}
	ms.MsCache().Set(providerId, provider, 5*time.Minute)
	if len(consumerIds) == 0 {
		util.Logger().Infof("refresh dependency cache: this services %s has no consumer dependency.", providerId)
	} else {
		consumerCache.Set(providerId, consumerIds, 5*time.Minute)
	}
	if len(providerIds) == 0 {
		util.Logger().Infof("refresh dependency cache: this services %s has no consumer dependency.", providerId)
	} else {
		providerCache.Set(providerId, providerIds, 5*time.Minute)
	}
	return nil
}

func DeleteDependencyForService(ctx context.Context, consumer *pb.MicroServiceKey, serviceId string) ([]*registry.PluginOp, error) {
	opts := []*registry.PluginOp{}
	optsTmps := []*registry.PluginOp{}
	tenant := consumer.Tenant
	flag := map[string]bool{}
	//删除依赖规则
	conKey := apt.GenerateConsumerDependencyRuleKey(tenant, consumer)
	err, providerValue := TransferToMicroServiceDependency(ctx, conKey)
	if err != nil {
		return nil, err
	}
	if providerValue != nil && len(providerValue.Dependency) != 0 {
		proProkey := ""
		for _, providerRule := range providerValue.Dependency {
			proProkey = apt.GenerateProviderDependencyRuleKey(tenant, providerRule)
			err, consumers := TransferToMicroServiceDependency(ctx, proProkey)
			if err != nil {
				return nil, err
			}
			err = deleteDependencyRuleUtil(ctx, consumers, consumer, proProkey)
			if err != nil {
				return nil, err
			}
		}

		opt := &registry.PluginOp{
			Action: registry.DELETE,
			Key:    util.StringToBytesWithNoCopy(conKey),
		}
		util.Logger().Debugf("conKey is %s.", conKey)
		opts = append(opts, opt)
	}
	//作为provider的依赖规则
	providerKey := apt.GenerateProviderDependencyRuleKey(tenant, consumer)
	opt := &registry.PluginOp{
		Action: registry.DELETE,
		Key:    util.StringToBytesWithNoCopy(providerKey),
	}
	util.Logger().Debugf("providerKey is %s", providerKey)
	opts = append(opts, opt)

	//删除依赖关系
	optsTmps, err = deleteDependencyUtil(ctx, "c", tenant, serviceId, flag)
	if err != nil {
		return nil, err
	}
	opts = append(opts, optsTmps...)
	util.Logger().Debugf("flag is %s", flag)
	optsTmps, err = deleteDependencyUtil(ctx, "p", tenant, serviceId, flag)
	if err != nil {
		return nil, err
	}
	util.Logger().Debugf("flag is %s", flag)
	opts = append(opts, optsTmps...)
	return opts, nil
}

func TransferToMicroServiceDependency(ctx context.Context, key string) (error, *pb.MicroServiceDependency) {
	microServiceDependency := &pb.MicroServiceDependency{
		Dependency: []*pb.MicroServiceKey{},
	}
	opt := &registry.PluginOp{
		Action: registry.GET,
		Key:    util.StringToBytesWithNoCopy(key),
	}
	res, err := store.Store().DependencyRule().Search(ctx, opt)
	if err != nil {
		util.Logger().Errorf(nil, "Get dependency rule failed.")
		return err, nil
	}
	if len(res.Kvs) != 0 {
		err = json.Unmarshal(res.Kvs[0].Value, microServiceDependency)
		if err != nil {
			util.Logger().Errorf(nil, "Unmarshal res failed.")
			return err, nil
		}
	} else {
		util.Logger().Infof("for key %s, no mircroServiceDependency stored", key)
	}
	return nil, microServiceDependency
}

func deleteDependencyRuleUtil(ctx context.Context, microServiceDependency *pb.MicroServiceDependency, service *pb.MicroServiceKey, serviceKey string) error {
	for key, serviceTmp := range microServiceDependency.Dependency {
		if ok := equalServiceDependency(serviceTmp, service); ok {
			microServiceDependency.Dependency = append(microServiceDependency.Dependency[:key], microServiceDependency.Dependency[key+1:]...)
			util.Logger().Debugf("delete versionRule from %s", serviceTmp.ServiceName)
			break
		}
	}
	opt := &registry.PluginOp{}
	if len(microServiceDependency.Dependency) == 0 {
		opt = &registry.PluginOp{
			Action: registry.DELETE,
			Key:    util.StringToBytesWithNoCopy(serviceKey),
		}
		util.Logger().Debugf("serviceKey is .", serviceKey)
		util.Logger().Debugf("After deleting versionRule from %s,provider's consumer is empty.", serviceKey)

	} else {
		data, err := json.Marshal(microServiceDependency)
		if err != nil {
			util.Logger().Errorf(nil, "Marshal tmpValue failed.")
			return err
		}
		opt = &registry.PluginOp{
			Action: registry.PUT,
			Key:    util.StringToBytesWithNoCopy(serviceKey),
			Value:  data,
		}
		util.Logger().Debugf("serviceKey is %s.", serviceKey)
	}
	_, err := registry.GetRegisterCenter().Do(ctx, opt)
	if err != nil {
		util.Logger().Errorf(err, "Submit update dependency failed.")
		return err
	}
	return nil
}

func equalServiceDependency(serviceA *pb.MicroServiceKey, serviceB *pb.MicroServiceKey) bool {
	stringA := toString(serviceA)
	stringB := toString(serviceB)
	if stringA == stringB {
		return true
	}
	return false
}

func toString(in *pb.MicroServiceKey) string {
	return util.StringJoin([]string{
		in.Tenant,
		in.AppId,
		in.Stage,
		in.ServiceName,
		in.Version,
	}, "")
}

func deleteDependencyUtil(ctx context.Context, serviceType string, tenant string, serviceId string, flag map[string]bool) ([]*registry.PluginOp, error) {
	serviceKey := apt.GenerateServiceDependencyKey(serviceType, tenant, serviceId, "")
	opt := &registry.PluginOp{
		Action:     registry.GET,
		Key:        util.StringToBytesWithNoCopy(serviceKey),
		WithPrefix: true,
	}
	rsp, err := store.Store().Dependency().Search(ctx, opt)
	if err != nil {
		return nil, err
	}
	opts := []*registry.PluginOp{}
	if rsp != nil {
		serviceTmpId := ""
		serviceTmpKey := ""
		deleteKey := ""
		for _, kv := range rsp.Kvs {
			tmpKeyArr := strings.Split(util.BytesToStringWithNoCopy(kv.Key), "/")
			serviceTmpId = tmpKeyArr[len(tmpKeyArr)-1]
			if serviceType == "p" {
				serviceTmpKey = apt.GenerateConsumerDependencyKey(tenant, serviceTmpId, serviceId)
				deleteKey = util.StringJoin([]string{"c", serviceTmpId, serviceId}, "/")
			} else {
				serviceTmpKey = apt.GenerateProviderDependencyKey(tenant, serviceTmpId, serviceId)
				deleteKey = util.StringJoin([]string{"p", serviceTmpId, serviceId}, "/")
			}
			if _, ok := flag[serviceTmpKey]; ok {
				util.Logger().Debugf("serviceTmpKey is more exist.%s", serviceTmpKey)
				continue
			}
			flag[serviceTmpKey] = true
			util.Logger().Infof("delete dependency %s", deleteKey)
			opt = &registry.PluginOp{
				Action: registry.DELETE,
				Key:    util.StringToBytesWithNoCopy(serviceTmpKey),
			}
			opts = append(opts, opt)
		}
		opt = &registry.PluginOp{
			Action:     registry.DELETE,
			Key:        util.StringToBytesWithNoCopy(serviceKey),
			WithPrefix: true,
		}
		util.Logger().Infof("delete dependency serviceKey is %s", serviceType+"/"+serviceId)
		opts = append(opts, opt)
	}
	return opts, nil
}

func CreateDependencyRule(ctx context.Context, dep *Dependency) error {
	//更新consumer的providers的值,consumer的版本是确定的
	consumerFlag := strings.Join([]string{dep.Consumer.AppId, dep.Consumer.ServiceName, dep.Consumer.Version}, "/")

	conKey := apt.GenerateConsumerDependencyRuleKey(dep.Tenant, dep.Consumer)

	err, oldProviderRules := TransferToMicroServiceDependency(ctx, conKey)
	if err != nil {
		util.Logger().Errorf(err, "maintain dependency rule failed, consumer %s: get consumer depedency rule failed.", consumerFlag)
		return err
	}

	unExistDependencyRuleList := make([]*pb.MicroServiceKey, 0, len(oldProviderRules.Dependency))
	newDependencyRuleList := make([]*pb.MicroServiceKey, 0, len(dep.ProvidersRule))
	existDependencyRuleList := make([]*pb.MicroServiceKey, 0, len(oldProviderRules.Dependency))
	for _, oldProviderRule := range oldProviderRules.Dependency {
		if ok, _ := containerServiceDependency(dep.ProvidersRule, oldProviderRule); !ok {
			unExistDependencyRuleList = append(unExistDependencyRuleList, oldProviderRule)
		} else {
			existDependencyRuleList = append(existDependencyRuleList, oldProviderRule)
		}
	}
	for _, tmpProviderRule := range dep.ProvidersRule {
		if ok, _ := containerServiceDependency(existDependencyRuleList, tmpProviderRule); !ok {
			newDependencyRuleList = append(newDependencyRuleList, tmpProviderRule)
		}
	}

	dep.err = make(chan error, 5)
	dep.chanNum = 0
	if len(unExistDependencyRuleList) != 0 {
		util.Logger().Infof("Unexist dependency rule remove for consumer %s, %v, ", consumerFlag, unExistDependencyRuleList)
		dep.removedDependencyRuleList = unExistDependencyRuleList
		dep.RemoveConsumerOfProviderRule()
	}

	if len(newDependencyRuleList) != 0 {
		util.Logger().Infof("New dependency rule add for consumer %s, %v, ", consumerFlag, newDependencyRuleList)
		dep.NewDependencyRuleList = newDependencyRuleList
		dep.AddConsumerOfProviderRule()
	}

	err = dep.UpdateProvidersRuleOfConsumer(conKey)
	if err != nil {
		return err
	}

	if dep.chanNum != 0 {
		for tmpErr := range dep.err {
			dep.chanNum--
			if tmpErr != nil {
				return tmpErr
			}
			if 0 == dep.chanNum {
				close(dep.err)
			}
		}
	}
	return nil
}

func containerServiceDependency(services []*pb.MicroServiceKey, service *pb.MicroServiceKey) (bool, error) {
	if services == nil || service == nil {
		return false, errors.New("Invalid params input.")
	}
	for _, value := range services {
		rst := equalServiceDependency(service, value)
		if rst {
			return true, nil
		}
	}
	return false, nil
}

// fuzzyMatch: 是否使用模糊规则
func validateMicroServiceKey(in *pb.MicroServiceKey, fuzzyMatch bool) error {
	var err error
	if fuzzyMatch {
		// provider的ServiceName, Version支持模糊规则
		err = apt.ProviderMsValidator.Validate(in)
	} else {
		err = apt.DependencyMSValidator.Validate(in)
	}
	if err != nil {
		return err
	}
	if len(in.Stage) == 0 {
		in.Stage = "dev"
	}
	return nil
}

func BadParamsResponse(detailErr string) *pb.CreateDependenciesResponse {
	util.Logger().Errorf(nil, "Request params is Valid.")
	if len(detailErr) == 0 {
		detailErr = "Request params is Valid."
	}
	return &pb.CreateDependenciesResponse{
		Response: pb.CreateResponse(pb.Response_FAIL, detailErr),
	}
}

func ParamsChecker(consumerInfo *pb.MicroServiceKey, providersInfo []*pb.MicroServiceKey) *pb.CreateDependenciesResponse {
	if err := validateMicroServiceKey(consumerInfo, false); err != nil {
		return BadParamsResponse(err.Error())
	}
	if providersInfo == nil {
		return BadParamsResponse("Invalid request body for provider info.")
	}
	flag := make(map[string]bool, len(providersInfo))
	for _, providerInfo := range providersInfo {
		//存在带*的情况，后面的数据就不校验了
		if providerInfo.ServiceName == "*" {
			util.Logger().Debugf("%s 's provider contains *.", consumerInfo.ServiceName)
			break
		}
		if len(providerInfo.AppId) == 0 {
			providerInfo.AppId = consumerInfo.AppId
		}
		if err := validateMicroServiceKey(providerInfo, true); err != nil {
			return BadParamsResponse(err.Error())
		}

		version := providerInfo.Version
		providerInfo.Version = ""
		if _, ok := flag[toString(providerInfo)]; ok {
			return BadParamsResponse("Invalid request body for provider info.Duplicate provider or (serviceName and appid is same).")
		} else {
			flag[toString(providerInfo)] = true
		}
		providerInfo.Version = version
	}
	return nil
}

func AddServiceVersionRule(ctx context.Context, provider *pb.MicroServiceKey, tenant string, consumer *pb.MicroServiceKey) (error, bool) {
	if apt.VersionRegex.Match(util.StringToBytesWithNoCopy(provider.Version)) {
		return nil, false
	}

	providerKey := apt.GenerateProviderDependencyRuleKey(tenant, provider)
	err, consumers := TransferToMicroServiceDependency(ctx, providerKey)
	if err != nil {
		return err, false
	}
	if len(consumers.Dependency) != 0 {
		isEqual, err := containerServiceDependency(consumers.Dependency, consumer)
		if err != nil {
			return err, false
		}
		if isEqual {
			//删除之前的依赖
			return nil, true
		}
	}
	//添加依赖
	consumers.Dependency = append(consumers.Dependency, consumer)
	data, err := json.Marshal(consumers)
	if err != nil {
		util.Logger().Errorf(err, "Marshal dependency of find failed.")
		return err, false
	}
	opt := &registry.PluginOp{
		Action: registry.PUT,
		Key:    util.StringToBytesWithNoCopy(providerKey),
		Value:  data,
	}
	_, err = registry.GetRegisterCenter().Do(ctx, opt)
	return err, false
}

func UpdateServiceForAddDependency(ctx context.Context, consumerId string, providers []*pb.DependencyMircroService, tenant string) error {
	conServiceKey := apt.GenerateServiceKey(tenant, consumerId)
	service, err := ms.GetService(ctx, tenant, consumerId)
	if err != nil {
		util.Logger().Errorf(err, "create dependency faild: get service failed. consumerId %s", consumerId)
		return err
	}
	if service == nil {
		util.Logger().Errorf(nil, "create dependency faild: service not exist.serviceId %s", consumerId)
		return errors.New("Get service is empty")
	}

	service.Providers = providers
	data, err := json.Marshal(service)
	if err != nil {
		util.Logger().Errorf(err, "create dependency faild: marshal service failed.")
		return err
	}
	opt := &registry.PluginOp{
		Action: registry.PUT,
		Key:    util.StringToBytesWithNoCopy(conServiceKey),
		Value:  data,
	}
	_, err = registry.GetRegisterCenter().Do(ctx, opt)
	if err != nil {
		util.Logger().Errorf(err, "create dependency faild: commit service data into etcd failed.")
		return err
	}
	return nil
}

type Dependency struct {
	ConsumerId                string
	Tenant                    string
	removedDependencyRuleList []*pb.MicroServiceKey
	NewDependencyRuleList     []*pb.MicroServiceKey
	err                       chan error
	chanNum                   int8
	Consumer                  *pb.MicroServiceKey
	ProvidersRule             []*pb.MicroServiceKey
}

func (dep *Dependency) RemoveConsumerOfProviderRule() {
	dep.chanNum++
	go dep.removeConsumerOfProviderRule()
}

func (dep *Dependency) removeConsumerOfProviderRule() {
	ctx := context.TODO()
	opts := make([]*registry.PluginOp, 0, len(dep.removedDependencyRuleList))
	for _, providerRule := range dep.removedDependencyRuleList {
		proProkey := apt.GenerateProviderDependencyRuleKey(dep.Tenant, providerRule)
		util.Logger().Debugf("This proProkey is %s.", proProkey)
		err, consumerValue := TransferToMicroServiceDependency(ctx, proProkey)
		if err != nil {
			dep.err <- err
			return
		}
		for key, tmp := range consumerValue.Dependency {
			if ok := equalServiceDependency(tmp, dep.Consumer); ok {
				consumerValue.Dependency = append(consumerValue.Dependency[:key], consumerValue.Dependency[key+1:]...)
				break
			}
		}
		//删除后，如果不存在依赖规则了，就删除该provider的依赖规则，如果有，则更新该依赖规则
		if len(consumerValue.Dependency) == 0 {
			opt := &registry.PluginOp{
				Action: registry.DELETE,
				Key:    []byte(proProkey),
			}
			opts = append(opts, opt)
			continue
		}
		data, err := json.Marshal(consumerValue)
		if err != nil {
			util.Logger().Errorf(nil, "Marshal tmpValue failed.")
			dep.err <- err
			return
		}
		opt := &registry.PluginOp{
			Action: registry.PUT,
			Key:    util.StringToBytesWithNoCopy(proProkey),
			Value:  data,
		}
		opts = append(opts, opt)
	}
	if len(opts) != 0 {
		_, err := registry.GetRegisterCenter().Txn(ctx, opts)
		if err != nil {
			dep.err <- err
			return
		}
	}
	dep.err <- nil
}

func (dep *Dependency) AddConsumerOfProviderRule() {
	dep.chanNum++
	go dep.addConsumerOfProviderRule()
}

func (dep *Dependency) addConsumerOfProviderRule() {
	ctx := context.TODO()
	opts := []*registry.PluginOp{}
	for _, prividerRule := range dep.NewDependencyRuleList {
		proProkey := apt.GenerateProviderDependencyRuleKey(dep.Tenant, prividerRule)
		err, tmpValue := TransferToMicroServiceDependency(ctx, proProkey)
		if err != nil {
			dep.err <- err
			return
		}
		tmpValue.Dependency = append(tmpValue.Dependency, dep.Consumer)

		data, errMarshal := json.Marshal(tmpValue)
		if errMarshal != nil {
			util.Logger().Errorf(nil, "Marshal tmpValue failed.")
			dep.err <- errors.New("Marshal tmpValue failed.")
			return
		}
		opts = append(opts, &registry.PluginOp{
			Action: registry.PUT,
			Key:    util.StringToBytesWithNoCopy(proProkey),
			Value:  data,
		})
		if prividerRule.ServiceName == "*" {
			break
		}
	}
	if len(opts) != 0 {
		_, err := registry.GetRegisterCenter().Txn(ctx, opts)
		if err != nil {
			dep.err <- err
			return
		}
	}
	dep.err <- nil
}

func (dep *Dependency) updateProvidersRuleOfConsumer(conKey string) error {
	dependency := &pb.MicroServiceDependency{
		Dependency: dep.ProvidersRule,
	}
	data, err := json.Marshal(dependency)
	if err != nil {
		util.Logger().Errorf(nil, "Marshal tmpValue fialed.")
		return err
	}
	opt := &registry.PluginOp{
		Action: registry.PUT,
		Key:    util.StringToBytesWithNoCopy(conKey),
		Value:  data,
	}
	_, err = registry.GetRegisterCenter().Do(context.TODO(), opt)
	if err != nil {
		util.Logger().Errorf(nil, "Upload dependency rule failed.")
		return err
	}
	return nil
}

func (dep *Dependency) UpdateProvidersRuleOfConsumer(conKey string) error {
	return dep.updateProvidersRuleOfConsumer(conKey)
}


type DependencyRelation struct {
	tenant     string
	consumerId string
	consumer   *pb.MicroService
	providerId string
	provider   *pb.MicroService
}

func NewProviderDependencyRelation(tenant string, providerId string, provider *pb.MicroService) *DependencyRelation{
	return &DependencyRelation{
		tenant: tenant,
		providerId: providerId,
		provider: provider,
	}
}

func NewConsumerDependencyRelation(tenant string, consumerId string, consumer *pb.MicroService) *DependencyRelation{
	return &DependencyRelation{
		tenant: tenant,
		consumerId: consumerId,
		consumer: consumer,
	}
}

func NewDependencyRelation(tenant string, consumerId string, consumer *pb.MicroService, providerId string, provider *pb.MicroService) *DependencyRelation{
	return &DependencyRelation{
		tenant: tenant,
		consumerId: consumerId,
		consumer: consumer,
		providerId: providerId,
		provider: provider,
	}
}

func (dr *DependencyRelation) GetDependencyProviders() ([]*pb.MicroService, error) {
	providerIds, err := dr.GetDependencyProviderIds()
	if err != nil {
		return nil, err
	}
	services := make([]*pb.MicroService, 0)
	for _, providerId := range providerIds {
		provider, err := ms.GetService(context.TODO(), dr.tenant, providerId)
		if err != nil {
			return nil, err
		}
		if provider == nil {
			util.Logger().Warnf(nil, "Provider not exist, %s", providerId)
			continue
		}
		services = append(services, provider)
	}
	return services, nil
}

func (dr *DependencyRelation) GetDependencyProviderIds() ([]string, error) {
	consumerMicroServiceKey := pb.ToMicroServiceKey(dr.tenant, dr.consumer)

	conKey := apt.GenerateConsumerDependencyRuleKey(dr.tenant, consumerMicroServiceKey)
	err, consumerDependency := TransferToMicroServiceDependency(context.TODO(), conKey)
	if err != nil {
		return nil, err
	}
	return dr.getDependencyProviderIds(consumerDependency.Dependency)
}

func (dr *DependencyRelation) getDependencyProviderIds(providerRules []*pb.MicroServiceKey) ([]string, error) {
	tenant := dr.tenant
	provideServiceIds := make([]string, 0)
	for _, provider := range providerRules {
		switch {
		case provider.ServiceName == "*":
			util.Logger().Infof("Rely all service,* type, consumerId %s", dr.consumerId)
			allServiceKey := apt.GenerateServiceKey(tenant, "")
			resp, err := store.Store().Service().Search(context.TODO(), &registry.PluginOp{
				Action:     registry.GET,
				Key:        util.StringToBytesWithNoCopy(allServiceKey),
				WithPrefix: true,
			})
			if err != nil {
				util.Logger().Errorf(err, "Add dependency failed, rely all service: get all services failed.")
				return provideServiceIds, err
			}
			keyArr := []string{}
			providerId := ""
			for _, kvs := range resp.Kvs {
				keyArr = strings.Split(util.BytesToStringWithNoCopy(kvs.Key), "/")
				providerId = keyArr[len(keyArr)-1]
				provideServiceIds = append(provideServiceIds, providerId)
			}
			return provideServiceIds, nil
		default:
			serviceIds, err := ms.FindServiceIds(context.TODO(), provider.Version, &pb.MicroServiceKey{
				Tenant:      tenant,
				AppId:       provider.AppId,
				ServiceName: provider.ServiceName,
			})
			if err != nil {
				util.Logger().Errorf(err, "Get providerIds failed, service: %s/%s/%s",
					provider.AppId, provider.ServiceName, provider.Version)
				return provideServiceIds, err
			}
			if len(serviceIds) == 0 {
				util.Logger().Warnf(nil, "Get providerIds is empty, service: %s/%s/%s does not exist",
					provider.AppId, provider.ServiceName, provider.Version)
				continue
			}
			provideServiceIds = append(provideServiceIds, serviceIds...)
		}
	}
	return provideServiceIds, nil
}

func (dr *DependencyRelation) GetDependencyConsumers() ([]*pb.MicroService, error) {

	consumerDependAllList, err := dr.getDependencyConsumersOfProvider()
	if err != nil {
		util.Logger().Errorf(err, "Get consumers of provider rule failed, %s", dr.providerId)
		return nil, err
	}
	consumers := make([]*pb.MicroService, 0)

	for _, consumer := range consumerDependAllList {
		service, err := getServiceByMicroServiceKey(dr.tenant, consumer)
		if err != nil {
			return nil, err
		}
		if service == nil {
			util.Logger().Warnf(nil, "Consumer not exist,%v", service)
			continue
		}
		consumers = append(consumers, service)
	}
	return consumers, nil
}

func (dr *DependencyRelation) GetDependencyConsumerIds() ([]string, error) {
	consumerDependAllList, err := dr.getDependencyConsumersOfProvider()
	if err != nil {
		return nil, err
	}
	consumerIds := make([]string, 0)
	for _, consumer := range consumerDependAllList {
		consumerId, err := ms.GetServiceId(context.TODO(), consumer)
		if err != nil {
			util.Logger().Errorf(err, "Get consumer failed, %v", consumer)
			return nil, err
		}
		if len(consumerId) == 0 {
			util.Logger().Warnf(nil, "Get consumer not exist, %v", consumer)
			continue
		}
		consumerIds = append(consumerIds, consumerId)
	}
	return consumerIds, nil

}

func (dr *DependencyRelation) getDependencyConsumersOfProvider() ([]*pb.MicroServiceKey, error) {
	providerService := pb.ToMicroServiceKey(dr.tenant, dr.provider)
	consumerDependAllList, err := dr.getConsumerOfDependAllServices()
	if err != nil {
		util.Logger().Errorf(err, "Get consumer that depend on all services failed, %s", dr.providerId)
		return nil, err
	}

	consumerDependList, err := dr.getConsumerOfSameServiceNameAndAppId(providerService)
	if err != nil {
		util.Logger().Errorf(err, "Get consumer that depend on same serviceName and appid rule failed, %s", dr.providerId)
		return nil, err
	}
	consumerDependAllList = append(consumerDependAllList, consumerDependList...)
	return consumerDependAllList, nil
}

func getServiceByMicroServiceKey(tenant string, service *pb.MicroServiceKey) (*pb.MicroService, error) {
	serviceId, err := ms.GetServiceId(context.TODO(), service)
	if err != nil {
		return nil, err
	}
	if len(serviceId) == 0 {
		util.Logger().Warnf(nil, "Service not exist,%v", service)
		return nil, nil
	}
	return ms.GetService(context.TODO(), tenant, serviceId)
}

func (dr *DependencyRelation) getConsumerOfSameServiceNameAndAppId(provider *pb.MicroServiceKey) ([]*pb.MicroServiceKey, error) {
	providerVersion := provider.Version
	provider.Version = ""
	proKey := apt.GenerateProviderDependencyRuleKey(dr.tenant, provider)
	provider.Version = providerVersion
	opt := &registry.PluginOp{
		Action:     registry.GET,
		Key:        util.StringToBytesWithNoCopy(proKey),
		WithPrefix: true,
	}
	rsp, err := store.Store().DependencyRule().Search(context.TODO(), opt)
	if err != nil {
		util.Logger().Errorf(err, "get all dependency rule failed: provider rule key %v.", provider)
		return nil, err
	}
	if rsp == nil {
		util.Logger().Warnf(nil, "get same appId and serviceName dependency rule is nul: provider rule key %v.", provider)
		return nil, nil
	}
	allConsumers := make([]*pb.MicroServiceKey, 0)
	for _, kv := range rsp.Kvs {
		dependency := &pb.MicroServiceDependency{
			Dependency: []*pb.MicroServiceKey{},
		}
		providerVersionRuleArr := strings.Split(util.BytesToStringWithNoCopy(kv.Key), "/")
		providerVersionRule := providerVersionRuleArr[len(providerVersionRuleArr)-1]
		if providerVersionRule == "latest" {
			latestServiceId, err := ms.FindServiceIds(context.TODO(), providerVersionRule, &pb.MicroServiceKey{
				Tenant:      dr.tenant,
				AppId:       provider.AppId,
				ServiceName: provider.ServiceName,
			})
			if err != nil {
				util.Logger().Errorf(err, "Get latest service failed.")
				return nil, err
			}
			if len(latestServiceId) == 0 {
				util.Logger().Infof("%s 's providerId is empty,no this service.", provider.ServiceName)
				continue
			}
			if dr.providerId != latestServiceId[0] {
				continue
			}

		} else {
			if !ms.VersionMatchRule(providerVersion, providerVersionRule) {
				continue
			}
		}

		util.Logger().Debugf("providerETCD is %s", providerVersionRuleArr)
		err = json.Unmarshal(kv.Value, dependency)
		if err != nil {
			util.Logger().Errorf(err, "Unmarshal consumers failed.")
			return nil, err
		}
		allConsumers = append(allConsumers, dependency.Dependency...)
	}
	return allConsumers, nil
}

func (dr *DependencyRelation) getConsumerOfDependAllServices() ([]*pb.MicroServiceKey, error) {

	relyAllKey := apt.GenerateProviderDependencyRuleKey(dr.tenant, &pb.MicroServiceKey{
		ServiceName: "*",
	})
	opt := &registry.PluginOp{
		Action: registry.GET,
		Key:    util.StringToBytesWithNoCopy(relyAllKey),
	}
	rsp, err := store.Store().DependencyRule().Search(context.TODO(), opt)
	if err != nil {
		util.Logger().Errorf(err, "get consumer that rely all service failed.")
		return nil, err
	}
	if len(rsp.Kvs) != 0 {
		dependency := &pb.MicroServiceDependency{}
		util.Logger().Infof("consumer that rely all service exist.ServiceName: %s.", dr.provider.ServiceName)
		err = json.Unmarshal(rsp.Kvs[0].Value, dependency)
		if err != nil {
			return nil, err
		}
		return dependency.Dependency, nil
	}
	return nil, nil
}
