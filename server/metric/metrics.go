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
package metric

import (
	"reflect"
	"strings"

	"github.com/apache/servicecomb-service-center/pkg/buffer"
	dto "github.com/prometheus/client_model/go"
)

func NewMetrics() *Metrics {
	return &Metrics{
		mapper: make(map[string]*Details),
	}
}

func NewDetails() *Details {
	return &Details{
		mapper: make(map[string]float64),
		buffer: buffer.NewPool(bufferSize),
	}
}

// Details is the struct to hold the calculated result and index by metric label
type Details struct {
	// Summary is the calculation results of the details
	Summary float64

	mapper map[string]float64
	buffer *buffer.Pool
}

// to format 'N1=L1,N2=L2,N3=L3,...'
func (cm *Details) toKey(labels []*dto.LabelPair) string {
	b := cm.buffer.Get()
	for _, label := range labels {
		b.WriteString(label.GetName())
		b.WriteRune('=')
		b.WriteString(label.GetValue())
		b.WriteRune(',')
	}
	key := b.String()
	cm.buffer.Put(b)
	return key
}

func (cm *Details) toLabels(key string) (p []*dto.LabelPair) {
	pairs := strings.Split(key, ",")
	pairs = pairs[:len(pairs)-1]
	p = make([]*dto.LabelPair, 0, len(pairs))
	for _, pair := range pairs {
		kv := strings.Split(pair, "=")
		p = append(p, &dto.LabelPair{Name: &kv[0], Value: &kv[1]})
	}
	return
}

func (cm *Details) Get(labels []*dto.LabelPair) (val float64) {
	if v, ok := cm.mapper[cm.toKey(labels)]; ok {
		val = v
	}
	return
}

func (cm *Details) put(labels []*dto.LabelPair, val float64) {
	cm.mapper[cm.toKey(labels)] = val
	return
}

func (cm *Details) ForEach(f func(labels []*dto.LabelPair, v float64) (next bool)) {
	for k, v := range cm.mapper {
		if !f(cm.toLabels(k), v) {
			break
		}
	}
}

// Metrics is the struct to hold the Details objects store and index by metric name
type Metrics struct {
	mapper map[string]*Details
}

func (cm *Metrics) put(key string, val *Details) {
	cm.mapper[key] = val
}

func (cm *Metrics) Get(key string) (val *Details) {
	if v, ok := cm.mapper[key]; ok {
		val = v
	}
	return
}

func (cm *Metrics) ForEach(f func(k string, v *Details) (next bool)) {
	for k, v := range cm.mapper {
		if !f(k, v) {
			break
		}
	}
}

func (cm *Metrics) Summary(key string) (sum float64) {
	if v, ok := cm.mapper[key]; ok {
		sum = v.Summary
	}
	return
}

// ParseMetricsWithKey parses info from records
func ParseMetricsWithKey(key string, scan func(labels []*dto.LabelPair, v float64) (next bool)) {
	data := Gatherer.Records.Get(key)
	if data == nil {
		return
	}

	data.ForEach(func(labels []*dto.LabelPair, v float64) (next bool) {
		return scan(labels, v)
	})
}

const (
	labelCount = "count"
)

// ToRawData parses result form labels
func ToRawData(result interface{}, labels []*dto.LabelPair, v float64) {
	if reflect.TypeOf(result).Kind() != reflect.Ptr {
		return
	}

	t := reflect.TypeOf(result).Elem()
	value := reflect.ValueOf(result).Elem()

	for i := 0; i < t.NumField(); i++ {
		tag := t.Field(i).Tag.Get("json")

		kind := t.Kind()
		if tag == labelCount && kind != reflect.Float64 {
			value.Field(i).SetFloat(v)
			continue
		}

		for _, label := range labels {
			if *label.Name == tag && kind != reflect.String {
				value.Field(i).SetString(*label.Value)
			}
		}
	}
}

// ToLabelNames returns label names, count is special label of v of func ForEach
func ToLabelNames(structure interface{}) []string {
	t := reflect.TypeOf(structure)
	if t.Kind() != reflect.Struct {
		return nil
	}

	num := t.NumField()
	labelNames := make([]string, 0, num)
	for i := 0; i < num; i++ {
		tag := t.Field(i).Tag.Get("json")
		if tag == labelCount {
			continue
		}
		labelNames = append(labelNames, tag)
	}
	return labelNames
}
