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
	lbPolicies map[string]*gov.Policy
	name       string
	client     *kie.Client
}

const (
	PREFIX         = "servicecomb."
	MatchGroup     = "match-group"
	EnableStatus   = "enabled"
	ValueType      = "text"
	AppKey         = "app"
	EnvironmentKey = "environment"
	EnvAll         = "all"
	BusinessPrefix = "scene-"
)

var PolicyNames = []string{"retry", "rateLimiting", "circuitBreaker", "bulkhead"}

var rule = Validator{}

func (d *Distributor) Create(kind, project string, spec []byte) ([]byte, error) {
	p := &gov.Policy{}
	err := json.Unmarshal(spec, p)
	if err != nil {
		return nil, err
	}
	if kind == MatchGroup {
		err = d.generateID(project, p)
	}
	if err != nil {
		return nil, err
	}
	log.Info(fmt.Sprintf("create %v", &p))
	err = rule.Validate(kind, p.Spec)
	if err != nil {
		return nil, err
	}
	yamlByte, err := yaml.Marshal(p.Spec)
	if err != nil {
		return nil, err
	}
	kv := kie.KVRequest{
		Key:       PREFIX + toSnake(kind) + "." + p.Name,
		Value:     string(yamlByte),
		Status:    EnableStatus,
		ValueType: ValueType,
		Labels:    map[string]string{AppKey: p.Selector.App, EnvironmentKey: p.Selector.Environment},
	}
	res, err := d.client.Create(context.TODO(), kv, kie.WithProject(project))
	if err != nil {
		log.Error("kie create failed", err)
		return nil, err
	}
	d.lbPolicies[p.GovernancePolicy.Name] = p
	b, _ := json.MarshalIndent(res.ID, "", "  ")
	return b, nil
}

func (d *Distributor) Update(id, kind, project string, spec []byte) error {
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
	d.lbPolicies[p.GovernancePolicy.Name] = p
	return nil
}

func (d *Distributor) Delete(id, project string) error {
	err := d.client.Delete(context.TODO(), id, kie.WithProject(project))
	if err != nil {
		log.Error("kie delete failed", err)
		return err
	}
	return nil
}

func (d *Distributor) Display(project, app, env string) ([]byte, error) {
	list, _, err := d.listDataByKind(MatchGroup, project, app, env)
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
		match, err := d.transform(item, MatchGroup)
		if err != nil {
			return nil, err
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

func (d *Distributor) List(kind, project, app, env string) ([]byte, error) {
	list, _, err := d.listDataByKind(kind, project, app, env)
	if err != nil {
		return nil, err
	}
	r := make([]*gov.Policy, 0, list.Total)
	for _, item := range list.Data {
		policy, err := d.transform(item, kind)
		if err != nil {
			return nil, err
		}
		r = append(r, policy)
	}
	b, _ := json.MarshalIndent(r, "", "  ")
	return b, nil
}

func (d *Distributor) Get(kind, id, project string) ([]byte, error) {
	kv, err := d.client.Get(context.TODO(), id, kie.WithGetProject(project))
	if err != nil {
		return nil, err
	}
	policy, err := d.transform(kv, kind)
	if err != nil {
		return nil, err
	}
	b, _ := json.MarshalIndent(policy, "", "  ")
	return b, nil
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
	return &Distributor{name: opts.Name, lbPolicies: map[string]*gov.Policy{}, client: initClient(opts.Endpoint)}, nil
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
		kie.WithKey("beginWith(" + PREFIX + toSnake(kind) + ")"),
		kie.WithRevision(0),
		kie.WithGetProject(project),
	}
	labels := map[string]string{}
	if env != EnvAll {
		labels[EnvironmentKey] = env
	}
	if app != "" {
		labels[AppKey] = app
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
	list, _, err := d.listDataByKind(MatchGroup, project, p.Selector.App, p.Selector.Environment)
	if err != nil {
		return err
	}
	alias, err := getAlias(p)
	if err != nil {
		return err
	}
	var id string
	for {
		var repeat bool
		id = getID()
		for _, datum := range list.Data {
			itemP, err := d.transform(datum, MatchGroup)
			if err != nil {
				return err
			}
			temp, err := getAlias(itemP)
			if err != nil {
				return err
			}
			if alias == temp {
				return &ErrIllegalItem{"alias can not repeat", alias}
			}
			if id == MatchGroup+datum.Key {
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

func getAlias(p *gov.Policy) (string, error) {
	spec, ok := p.Spec.(map[string]interface{})
	if !ok {
		return "", &ErrIllegalItem{"can not cast to map", spec}
	}
	alias, ok := spec["alias"].(string)
	if !ok {
		return "", nil
	}
	return alias, nil
}

func getID() string {
	str := "0123456789abcdefghijklmnopqrstuvwxyz"
	b := []byte(str)
	var result []byte
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	for i := 0; i < 4; i++ {
		result = append(result, b[r.Intn(len(b))])
	}
	return BusinessPrefix + string(result)
}

func (d *Distributor) transform(kv *kie.KVDoc, kind string) (*gov.Policy, error) {
	goc := &gov.Policy{
		GovernancePolicy: &gov.GovernancePolicy{},
	}
	spec := make(map[string]interface{})
	specJSON, _ := yaml.YAMLToJSON([]byte(kv.Value))
	err := json.Unmarshal(specJSON, &spec)
	if err != nil {
		log.Fatal("kie transform kv failed", err)
		return nil, err
	}
	goc.Kind = kind
	goc.ID = kv.ID
	goc.Status = kv.Status
	goc.Name = kv.Key[strings.LastIndex(kv.Key, ".")+1 : len(kv.Key)]
	goc.Spec = spec
	goc.Selector.App = kv.Labels[AppKey]
	goc.Selector.Environment = kv.Labels[EnvironmentKey]
	goc.CreatTime = kv.CreatTime
	goc.UpdateTime = kv.UpdateTime
	return goc, nil
}

func init() {
	svc.InstallDistributor(svc.ConfigDistributorKie, new)
}
