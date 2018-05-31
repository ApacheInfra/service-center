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
	"github.com/apache/incubator-servicecomb-service-center/pkg/validate"
	"github.com/apache/incubator-servicecomb-service-center/server/infra/quota"
	"regexp"
)

var (
	getSchemaReqValidator     validate.Validator
	modifySchemasReqValidator validate.Validator
	modifySchemaReqValidator  validate.Validator
)

var (
	schemaIdUnlimitedRegex, _ = regexp.Compile(`^[a-zA-Z0-9]+$|^[a-zA-Z0-9][a-zA-Z0-9_\-.]*[a-zA-Z0-9]$`)
	schemaSummaryRegex, _     = regexp.Compile(`^[a-zA-Z0-9]*$`)
)

func GetSchemaReqValidator() *validate.Validator {
	schemaIdRule := &validate.ValidateRule{Min: 1, Max: 160, Regexp: schemaIdUnlimitedRegex}

	return getSchemaReqValidator.Init(func(v *validate.Validator) {
		v.AddRule("SchemaId", schemaIdRule)
	})
}

func ModifySchemasReqValidator() *validate.Validator {
	var subSchemaValidator validate.Validator
	subSchemaValidator.AddRule("SchemaId", GetSchemaReqValidator().GetRule("SchemaId"))
	subSchemaValidator.AddRule("Summary", &validate.ValidateRule{Min: 1, Max: 128, Regexp: schemaSummaryRegex})
	subSchemaValidator.AddRule("Schema", &validate.ValidateRule{Min: 1})

	return modifySchemasReqValidator.Init(func(v *validate.Validator) {
		v.AddRule("ServiceId", GetServiceReqValidator().GetRule("ServiceId"))
		v.AddRule("Schemas", &validate.ValidateRule{Min: 1, Max: quota.DefaultSchemaQuota})
		v.AddSub("Schemas", &subSchemaValidator)
	})
}

func ModifySchemaReqValidator() *validate.Validator {
	return modifySchemaReqValidator.Init(func(v *validate.Validator) {
		v.AddRules(ModifySchemasReqValidator().GetSub("Schemas").GetRules())
		v.AddRule("ServiceId", GetServiceReqValidator().GetRule("ServiceId"))
		// forward compatibility: allow empty
		v.AddRule("Summary", &validate.ValidateRule{Max: 128, Regexp: schemaSummaryRegex})
	})
}
