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
package rest

import (
	"fmt"
	"github.com/apache/incubator-servicecomb-service-center/pkg/rest"
	"github.com/apache/incubator-servicecomb-service-center/server/core"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"net/http"
	"strconv"
	"strings"
	"time"
)

var (
	incomingRequests = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "service_center",
			Subsystem: "http",
			Name:      "request_total",
			Help:      "Counter of requests received into ROA handler",
		}, []string{"method", "code", "instance", "api"})

	successfulRequests = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "service_center",
			Subsystem: "http",
			Name:      "success_total",
			Help:      "Counter of successful requests processed by ROA handler",
		}, []string{"method", "code", "instance", "api"})

	reqDurations = prometheus.NewSummaryVec(
		prometheus.SummaryOpts{
			Namespace:  "service_center",
			Subsystem:  "http",
			Name:       "request_durations_microseconds",
			Help:       "HTTP request latency summary of ROA handler",
			Objectives: prometheus.DefObjectives,
		}, []string{"method", "instance", "api"})
)

func init() {
	prometheus.MustRegister(incomingRequests, successfulRequests, reqDurations)

	http.Handle("/metrics", prometheus.Handler())
}

func ReportRequestCompleted(w http.ResponseWriter, r *http.Request, start time.Time) {
	instance := fmt.Sprint(core.Instance.Endpoints)
	elapsed := float64(time.Since(start).Nanoseconds()) / 1000
	route, _ := r.Context().Value(rest.CTX_MATCH_PATTERN).(string)

	if strings.Index(r.Method, "WATCH") != 0 {
		reqDurations.WithLabelValues(r.Method, instance, route).Observe(elapsed)
	}

	success, code := codeOf(w.Header())

	incomingRequests.WithLabelValues(r.Method, code, instance, route).Inc()

	if success {
		successfulRequests.WithLabelValues(r.Method, code, instance, route).Inc()
	}
}

func codeOf(h http.Header) (bool, string) {
	statusCode := h.Get("X-Response-Status")
	if statusCode == "" {
		return true, "200"
	}

	if code, _ := strconv.Atoi(statusCode); code >= http.StatusOK && code <= http.StatusAccepted {
		return true, statusCode
	}

	return false, statusCode
}

// Get value of metricFamily
func MetricValueOf(mf *dto.MetricFamily) float64 {
	if len(mf.GetMetric()) == 0 {
		return 0
	}

	switch mf.GetType() {
	case dto.MetricType_GAUGE:
		return mf.GetMetric()[0].GetGauge().GetValue()
	case dto.MetricType_COUNTER:
		return metricCounterOf(mf.GetMetric())
	case dto.MetricType_SUMMARY:
		return metricSummaryOf(mf.GetMetric())
	default:
		return 0
	}
}

func metricCounterOf(m []*dto.Metric) float64 {
	var sum float64 = 0
	for _, d := range m {
		sum += d.GetCounter().GetValue()
	}
	return sum
}

func metricSummaryOf(m []*dto.Metric) float64 {
	var (
		count uint64  = 0
		sum   float64 = 0
	)
	for _, d := range m {
		count += d.GetSummary().GetSampleCount()
		sum += d.GetSummary().GetSampleSum()
	}

	if count == 0 {
		return 0
	}

	return sum / float64(count)
}
