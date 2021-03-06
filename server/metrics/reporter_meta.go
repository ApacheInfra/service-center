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

package metrics

import (
	"github.com/apache/servicecomb-service-center/datasource"
	"github.com/apache/servicecomb-service-center/pkg/log"
	metricsvc "github.com/apache/servicecomb-service-center/pkg/metrics"
	"github.com/go-chassis/go-chassis/v2/pkg/metrics"
)

var metaReporter = &MetaReporter{}

type MetaReporter struct {
}

func (m *MetaReporter) DomainAdd(delta float64) {
	instance := metricsvc.InstanceName()
	labels := map[string]string{
		"instance": instance,
	}
	if err := metrics.GaugeAdd(KeyDomainTotal, delta, labels); err != nil {
		log.Error("gauge add failed", err)
	}
}
func (m *MetaReporter) ServiceAdd(delta float64, ml datasource.MetricsLabels) {
	instance := metricsvc.InstanceName()
	labels := map[string]string{
		"instance":         instance,
		"framework":        ml.Framework,
		"frameworkVersion": ml.FrameworkVersion,
		"domain":           ml.Domain,
		"project":          ml.Project,
	}
	if err := metrics.GaugeAdd(KeyServiceTotal, delta, labels); err != nil {
		log.Error("gauge add failed", err)
	}
}
func (m *MetaReporter) InstanceAdd(delta float64, ml datasource.MetricsLabels) {
	instance := metricsvc.InstanceName()
	labels := map[string]string{
		"instance":         instance,
		"framework":        ml.Framework,
		"frameworkVersion": ml.FrameworkVersion,
		"domain":           ml.Domain,
		"project":          ml.Project,
	}
	if err := metrics.GaugeAdd(KeyInstanceTotal, delta, labels); err != nil {
		log.Error("gauge add failed", err)
	}
}
func (m *MetaReporter) SchemaAdd(delta float64, ml datasource.MetricsLabels) {
	instance := metricsvc.InstanceName()
	labels := map[string]string{
		"instance": instance,
		"domain":   ml.Domain,
		"project":  ml.Project,
	}
	if err := metrics.GaugeAdd(KeySchemaTotal, delta, labels); err != nil {
		log.Error("gauge add failed", err)
	}
}
func (m *MetaReporter) FrameworkSet(ml datasource.MetricsLabels) {
	instance := metricsvc.InstanceName()
	labels := map[string]string{
		"instance":         instance,
		"framework":        ml.Framework,
		"frameworkVersion": ml.FrameworkVersion,
		"domain":           ml.Domain,
		"project":          ml.Project,
	}
	if err := metrics.GaugeSet(KeyFrameworkTotal, 1, labels); err != nil {
		log.Error("gauge set failed", err)
	}
}
func GetMetaReporter() *MetaReporter {
	return metaReporter
}

func ResetMetaMetrics() {
	err := metrics.Reset(KeyDomainTotal)
	if err != nil {
		log.Error("reset metrics failed", err)
		return
	}
	err = metrics.Reset(KeyServiceTotal)
	if err != nil {
		log.Error("reset metrics failed", err)
		return
	}
	err = metrics.Reset(KeyInstanceTotal)
	if err != nil {
		log.Error("reset metrics failed", err)
		return
	}
	err = metrics.Reset(KeySchemaTotal)
	if err != nil {
		log.Error("reset metrics failed", err)
		return
	}
}
