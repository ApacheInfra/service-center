/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package kie

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"strings"
	"time"

	"github.com/apache/servicecomb-service-center/pkg/log"

	"github.com/apache/servicecomb-service-center/pkg/gov"
	"github.com/apache/servicecomb-service-center/server/config"
	svc "github.com/apache/servicecomb-service-center/server/service/gov"
	"github.com/ghodss/yaml"
	"github.com/go-chassis/kie-client"
)

type Distributor struct {
	name   string
	client *kie.Client
}

const (
	KeyPrefix       = "servicecomb."
	KindMatchGroup  = "match-group"
	GroupNamePrefix = "scene-"
	StatusEnabled   = "enabled"
	TypeText        = "text"
	KeyApp          = "app"
	KeyEnvironment  = "environment"
	EnvAll          = "all"
	Alias           = "alias"
	Method          = "method"
	Matches         = "matches"
	Rules           = "rules"
)

var PolicyNames = []string{"retry", "rateLimiting", "circuitBreaker", "bulkhead"}

var rule = Validator{}

func (d *Distributor) Create(kind, project string, spec []byte) ([]byte, error) {
	p := &gov.Policy{
		GovernancePolicy: &gov.GovernancePolicy{Selector: &gov.Selector{}},
	}
	err := json.Unmarshal(spec, p)
	if err != nil {
		return nil, err
	}
	if kind == KindMatchGroup {
		err = d.generateID(project, p)
		if err != nil {
			return nil, err
		}
	}
	log.Info(fmt.Sprintf("create %+v", p))
	err = rule.Validate(kind, p.Spec)
	if err != nil {
		return nil, err
	}
	if kind == KindMatchGroup {
		setAliasIfEmpty(p.Spec, p.Name)
	}
	yamlByte, err := yaml.Marshal(p.Spec)
	if err != nil {
		return nil, err
	}
	kv := kie.KVRequest{
		Key:       toGovKeyPrefix(kind) + p.Name,
		Value:     string(yamlByte),
		Status:    StatusEnabled,
		ValueType: TypeText,
		Labels:    map[string]string{KeyApp: p.Selector.App, KeyEnvironment: p.Selector.Environment},
	}
	res, err := d.client.Create(context.TODO(), kv, kie.WithProject(project))
	if err != nil {
		log.Error("kie create failed", err)
		return nil, err
	}
	return []byte(res.ID), nil
}

func (d *Distributor) Update(kind, id, project string, spec []byte) error {
	p := &gov.Policy{}
	err := json.Unmarshal(spec, p)
	if err != nil {
		return err
	}
	log.Info(fmt.Sprintf("update %v", &p))
	err = rule.Validate(kind, p.Spec)
	if err != nil {
		return err
	}
	if kind == KindMatchGroup {
		setAliasIfEmpty(p.Spec, p.Name)
	}
	yamlByte, err := yaml.Marshal(p.Spec)
	if err != nil {
		return err
	}
	kv := kie.KVRequest{
		ID:     id,
		Value:  string(yamlByte),
		Status: p.Status,
	}
	_, err = d.client.Put(context.TODO(), kv, kie.WithProject(project))
	if err != nil {
		log.Error("kie update failed", err)
		return err
	}
	return nil
}

func (d *Distributor) Delete(kind, id, project string) error {
	if kind == KindMatchGroup {
		// should remove all policies of this group
		return d.DeleteMatchGroup(id, project)
	}

	err := d.client.Delete(context.TODO(), id, kie.WithProject(project))
	if err != nil {
		log.Error("kie delete failed", err)
		return err
	}
	return nil
}

func (d *Distributor) DeleteMatchGroup(id string, project string) error {
	policy, err := d.getPolicy(KindMatchGroup, id, project)
	if err != nil {
		log.Error("kie get failed", err)
		return err
	}

	ops := []kie.GetOption{
		kie.WithKey("wildcard(" + KeyPrefix + "*." + policy.Name + ")"),
		kie.WithRevision(0),
		kie.WithGetProject(project),
	}
	idList, _, err := d.client.List(context.TODO(), ops...)
	if err != nil {
		log.Error("kie list failed", err)
		return err
	}
	var ids string
	for _, res := range idList.Data {
		ids += res.ID + ","
	}
	if len(ids) == 0 {
		return nil
	}

	err = d.client.Delete(context.TODO(), ids[:len(ids)-1])
	if err != nil {
		log.Error("kie list failed", err)
		return err
	}
	return nil
}

func (d *Distributor) Display(project, app, env string) ([]byte, error) {
	list, _, err := d.listDataByKind(KindMatchGroup, project, app, env)
	if err != nil {
		return nil, err
	}
	policyMap := make(map[string]*gov.Policy)
	for _, kind := range PolicyNames {
		policies, _, err := d.listDataByKind(kind, project, app, env)
		if err != nil {
			continue
		}
		for _, policy := range policies.Data {
			item, err := d.transform(policy, kind)
			if err != nil {
				continue
			}
			policyMap[item.Name+kind] = item
		}
	}
	r := make([]*gov.DisplayData, 0, list.Total)
	for _, item := range list.Data {
		match, err := d.transform(item, KindMatchGroup)
		if err != nil {
			continue

		}
		var policies []*gov.Policy
		for _, kind := range PolicyNames {
			if policyMap[match.Name+kind] != nil {
				policies = append(policies, policyMap[match.Name+kind])
			}
		}
		result := &gov.DisplayData{
			Policies:   policies,
			MatchGroup: match,
		}
		r = append(r, result)
	}
	b, _ := json.MarshalIndent(r, "", "  ")
	return b, nil
}

func setAliasIfEmpty(val interface{}, name string) {
	spec := val.(map[string]interface{})
	alias := spec["alias"].(string)
	if alias == "" {
		spec["alias"] = name
	}
}

func (d *Distributor) List(kind, project, app, env string) ([]byte, error) {
	list, _, err := d.listDataByKind(kind, project, app, env)
	if err != nil {
		return nil, err
	}
	r := make([]*gov.Policy, 0, list.Total)
	for _, item := range list.Data {
		policy, err := d.transform(item, kind)
		if err != nil {
			continue
		}
		r = append(r, policy)
	}
	b, _ := json.MarshalIndent(r, "", "  ")
	return b, nil
}

func (d *Distributor) Get(kind, id, project string) ([]byte, error) {
	policy, err := d.getPolicy(kind, id, project)
	if err != nil {
		return nil, err
	}
	b, _ := json.MarshalIndent(policy, "", "  ")
	return b, nil
}

func (d *Distributor) getPolicy(kind string, id string, project string) (*gov.Policy, error) {
	kv, err := d.client.Get(context.TODO(), id, kie.WithGetProject(project))
	if err != nil {
		return nil, err
	}
	policy, err := d.transform(kv, kind)
	if err != nil {
		return nil, err
	}
	return policy, nil
}

func (d *Distributor) Type() string {
	return svc.ConfigDistributorKie
}
func (d *Distributor) Name() string {
	return d.name
}

func initClient(endpoint string) *kie.Client {
	client, err := kie.NewClient(
		kie.Config{Endpoint: endpoint,
			DefaultLabels: map[string]string{},
		})
	if err != nil {
		log.Fatal("init kie client failed, err: %s", err)
	}
	return client
}

func new(opts config.DistributorOptions) (svc.ConfigDistributor, error) {
	return &Distributor{name: opts.Name, client: initClient(opts.Endpoint)}, nil
}

func toSnake(name string) string {
	if name == "" {
		return ""
	}
	temp := strings.Split(name, "-")
	var buffer bytes.Buffer
	for num, v := range temp {
		vv := []rune(v)
		if num == 0 {
			buffer.WriteString(string(vv))
			continue
		}
		if len(vv) > 0 {
			if vv[0] >= 'a' && vv[0] <= 'z' { //首字母大写
				vv[0] -= 32
			}
			buffer.WriteString(string(vv))
		}
	}
	return buffer.String()
}

func (d *Distributor) listDataByKind(kind, project, app, env string) (*kie.KVResponse, int, error) {
	ops := []kie.GetOption{
		kie.WithKey("beginWith(" + toGovKeyPrefix(kind) + ")"),
		kie.WithRevision(0),
		kie.WithGetProject(project),
	}
	labels := map[string]string{}
	if env != EnvAll {
		labels[KeyEnvironment] = env
	}
	if app != "" {
		labels[KeyApp] = app
	}
	if len(labels) > 0 {
		ops = append(ops, kie.WithLabels(labels))
	}
	return d.client.List(context.TODO(), ops...)
}

func (d *Distributor) generateID(project string, p *gov.Policy) error {
	if p.Name != "" {
		return nil
	}
	kind := KindMatchGroup
	list, _, err := d.listDataByKind(kind, project, p.Selector.App, p.Selector.Environment)
	if err != nil {
		return err
	}
	var id string
	for {
		var repeat bool
		id = getID()
		govKey := toGovKeyPrefix(kind) + id
		for _, datum := range list.Data {
			if govKey == datum.Key {
				repeat = true
				break
			}
		}
		if !repeat {
			break
		}
	}
	p.Name = id
	return nil
}

func getID() string {
	str := "0123456789abcdefghijklmnopqrstuvwxyz"
	b := []byte(str)
	var result []byte
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	for i := 0; i < 4; i++ {
		result = append(result, b[r.Intn(len(b))])
	}
	return GroupNamePrefix + string(result)
}

func (d *Distributor) transform(kv *kie.KVDoc, kind string) (*gov.Policy, error) {
	goc := &gov.Policy{
		GovernancePolicy: &gov.GovernancePolicy{
			Selector: &gov.Selector{},
		},
	}
	spec := make(map[string]interface{})
	specJSON, _ := yaml.YAMLToJSON([]byte(kv.Value))
	err := json.Unmarshal(specJSON, &spec)
	if err != nil {
		log.Error("kie transform kv failed", err)
		return nil, err
	}
	goc.Kind = kind
	goc.ID = kv.ID
	goc.Status = kv.Status
	goc.Name = kv.Key[strings.LastIndex(kv.Key, ".")+1 : len(kv.Key)]
	goc.Spec = spec
	goc.Selector.App = kv.Labels[KeyApp]
	goc.Selector.Environment = kv.Labels[KeyEnvironment]
	goc.CreatTime = kv.CreatTime
	goc.UpdateTime = kv.UpdateTime
	return goc, nil
}

func toGovKeyPrefix(kind string) string {
	return KeyPrefix + toSnake(kind) + "."
}

func init() {
	svc.InstallDistributor(svc.ConfigDistributorKie, new)
}
