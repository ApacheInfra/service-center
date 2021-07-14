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

package datasource_test

import (
	"strconv"
	"testing"

	"github.com/apache/servicecomb-service-center/datasource"
	"github.com/apache/servicecomb-service-center/datasource/etcd"
	"github.com/apache/servicecomb-service-center/datasource/mongo"
	"github.com/apache/servicecomb-service-center/pkg/log"
	"github.com/apache/servicecomb-service-center/server/plugin/quota"
	pb "github.com/go-chassis/cari/discovery"
	"github.com/go-chassis/go-archaius"
	"github.com/stretchr/testify/assert"
)

func TestSchema_Create(t *testing.T) {
	var (
		serviceIdDev string
	)

	t.Run("create service, should pass", func(t *testing.T) {
		svc := &pb.MicroService{
			Alias:       "create_schema_group_service_ms",
			ServiceName: "create_schema_service_service_ms",
			Version:     "1.0.0",
			Level:       "FRONT",
			Status:      pb.MS_UP,
			Environment: pb.ENV_DEV,
		}
		resp, err := datasource.GetMetadataManager().RegisterService(getContext(), &pb.CreateServiceRequest{
			Service: svc,
		})
		assert.NoError(t, err)
		assert.NotEqual(t, "", resp.ServiceId)
		assert.Equal(t, pb.ResponseSuccess, resp.Response.GetCode())
		serviceIdDev = resp.ServiceId

		resp, err = datasource.GetMetadataManager().RegisterService(getContext(), &pb.CreateServiceRequest{
			Service: &pb.MicroService{
				AppId:       "create_schema_group_service_ms",
				ServiceName: "create_schema_service_service_ms",
				Version:     "1.0.0",
				Level:       "FRONT",
				Status:      pb.MS_UP,
				Environment: pb.ENV_PROD,
			},
		})
		assert.NoError(t, err)
		assert.NotEqual(t, "", resp.ServiceId)
		assert.Equal(t, pb.ResponseSuccess, resp.Response.GetCode())
	})

	t.Run("create schemas out of gauge", func(t *testing.T) {
		log.Info("create schemas out of gauge")
		size := quota.DefaultSchemaQuota + 1
		schemaIds := make([]string, 0, size)
		schemas := make([]*pb.Schema, 0, size)
		for i := 0; i < size; i++ {
			s := "ServiceCombTestTheLimitOfSchemasServiceMS" + strconv.Itoa(i)

			schemaIds = append(schemaIds, s)
			schemas = append(schemas, &pb.Schema{
				SchemaId: s,
				Schema:   s,
				Summary:  s,
			})
		}

		log.Info("batch modify schemas 1, should failed")
		resp, err := datasource.GetMetadataManager().ModifySchemas(getContext(), &pb.ModifySchemasRequest{
			ServiceId: serviceIdDev,
			Schemas:   schemas,
		})
		assert.NoError(t, err)
		assert.Equal(t, pb.ErrNotEnoughQuota, resp.Response.GetCode())

		log.Info("batch modify schemas 2")
		resp, err = datasource.GetMetadataManager().ModifySchemas(getContext(), &pb.ModifySchemasRequest{
			ServiceId: serviceIdDev,
			Schemas:   schemas[:quota.DefaultSchemaQuota],
		})
		assert.NoError(t, err)
		assert.Equal(t, pb.ResponseSuccess, resp.Response.GetCode())

		log.Info("should be failed in production env")
		resp, err = datasource.GetMetadataManager().ModifySchemas(getContext(), &pb.ModifySchemasRequest{
			ServiceId: serviceIdDev,
			Schemas:   schemas,
		})
		assert.NoError(t, err)
		assert.Equal(t, pb.ErrNotEnoughQuota, resp.Response.GetCode())
	})

	t.Run("modify schemas, should pass", func(t *testing.T) {
		var (
			serviceIdDev1 string
			serviceIdDev2 string
		)

		log.Info("register service, should pass")
		resp, err := datasource.GetMetadataManager().RegisterService(getContext(), &pb.CreateServiceRequest{
			Service: &pb.MicroService{
				AppId:       "create_schemas_dev_service_ms",
				ServiceName: "create_schemas_service_service_ms",
				Version:     "1.0.0",
				Level:       "FRONT",
				Status:      pb.MS_UP,
				Environment: pb.ENV_DEV,
			},
		})
		assert.NoError(t, err)
		assert.Equal(t, pb.ResponseSuccess, resp.Response.GetCode())
		serviceIdDev1 = resp.ServiceId

		resp, err = datasource.GetMetadataManager().RegisterService(getContext(), &pb.CreateServiceRequest{
			Service: &pb.MicroService{
				AppId:       "create_schemas_dev_service_ms",
				ServiceName: "create_schemas_service_service_ms",
				Version:     "1.0.1",
				Level:       "FRONT",
				Schemas: []string{
					"first_schemaId_service_ms",
				},
				Status:      pb.MS_UP,
				Environment: pb.ENV_DEV,
			},
		})
		assert.NoError(t, err)
		assert.Equal(t, pb.ResponseSuccess, resp.Response.GetCode())
		serviceIdDev2 = resp.ServiceId

		log.Info("create schemas with service schemaId set is empty")
		schemas := []*pb.Schema{
			{
				SchemaId: "first_schemaId_service_ms",
				Schema:   "first_schema_service_ms",
				Summary:  "first0summary_service_ms",
			},
			{
				SchemaId: "first_schemaId_service_ms",
				Schema:   "first_schema_service_ms",
				Summary:  "first0summary_service_ms",
			},
		}
		respCreateSchema, err := datasource.GetMetadataManager().ModifySchemas(getContext(), &pb.ModifySchemasRequest{
			ServiceId: serviceIdDev1,
			Schemas:   schemas,
		})
		assert.NoError(t, err)
		assert.Equal(t, pb.ResponseSuccess, respCreateSchema.Response.GetCode())

		// todo: test GetAllSchemaInfo interface refers to schema_test line 342

		log.Info("modify schemas")
		schemas = []*pb.Schema{
			{
				SchemaId: "first_schemaId_service_ms",
				Schema:   "first_schema_change_service_ms",
				Summary:  "first0summary1change_service_ms",
			},
		}
		respCreateSchema, err = datasource.GetMetadataManager().ModifySchemas(getContext(), &pb.ModifySchemasRequest{
			ServiceId: serviceIdDev1,
			Schemas:   schemas,
		})

		log.Info("add schemas")
		schemas = []*pb.Schema{
			{
				SchemaId: "second_schemaId_service_ms",
				Schema:   "second_schema_service_ms",
				Summary:  "second0summary_service_ms",
			},
		}
		respCreateSchema, err = datasource.GetMetadataManager().ModifySchemas(getContext(), &pb.ModifySchemasRequest{
			ServiceId: serviceIdDev1,
			Schemas:   schemas,
		})
		assert.NoError(t, err)
		assert.Equal(t, pb.ResponseSuccess, respCreateSchema.Response.GetCode())

		log.Info("query service by serviceID to obtain schema info")
		respGetService, err := datasource.GetMetadataManager().GetService(getContext(), &pb.GetServiceRequest{
			ServiceId: serviceIdDev1,
		})
		assert.NoError(t, err)
		assert.Equal(t, pb.ResponseSuccess, respGetService.Response.GetCode())
		assert.Equal(t, []string{"second_schemaId_service_ms"}, respGetService.Service.Schemas)

		log.Info("add new schemaId not exist in service's schemaId list")
		schemas = []*pb.Schema{
			{
				SchemaId: "second_schemaId_service_ms",
				Schema:   "second_schema_service_ms",
				Summary:  "second0summary_service_ms",
			},
		}
		respCreateSchema, err = datasource.GetMetadataManager().ModifySchemas(getContext(), &pb.ModifySchemasRequest{
			ServiceId: serviceIdDev2,
			Schemas:   schemas,
		})
		assert.NoError(t, err)
		assert.Equal(t, pb.ResponseSuccess, respCreateSchema.Response.GetCode())

		respGetService, err = datasource.GetMetadataManager().GetService(getContext(), &pb.GetServiceRequest{
			ServiceId: serviceIdDev2,
		})
		assert.NoError(t, err)
		assert.Equal(t, pb.ResponseSuccess, respGetService.Response.GetCode())
		assert.Equal(t, []string{"second_schemaId_service_ms"}, respGetService.Service.Schemas)
	})

	t.Run("when modify schemas and summary is empty", func(t *testing.T) {
		var (
			serviceIdPro string
		)

		log.Info("register service")
		respCreateService, err := datasource.GetMetadataManager().RegisterService(getContext(), &pb.CreateServiceRequest{
			Service: &pb.MicroService{
				AppId:       "create_schemas_prod_service_ms",
				ServiceName: "create_schemas_service_service_ms",
				Version:     "1.0.1",
				Level:       "FRONT",
				Schemas: []string{
					"first_schemaId_service_ms",
					"second_schemaId_service_ms",
				},
				Status:      pb.MS_UP,
				Environment: pb.ENV_PROD,
			},
		})
		assert.NoError(t, err)
		assert.Equal(t, pb.ResponseSuccess, respCreateService.Response.GetCode())
		serviceIdPro = respCreateService.ServiceId

		log.Info("add schema when summary is empty")
		respModifySchema, err := datasource.GetMetadataManager().ModifySchema(getContext(), &pb.ModifySchemaRequest{
			ServiceId: serviceIdPro,
			SchemaId:  "first_schemaId_service_ms",
			Schema:    "first_schema_service_ms",
		})
		assert.NoError(t, err)
		assert.Equal(t, pb.ResponseSuccess, respModifySchema.Response.GetCode())

		log.Info("add schemas when summary in database is empty")
		schemas := []*pb.Schema{
			{
				SchemaId: "first_schemaId_service_ms",
				Schema:   "first_schema_service_ms",
				Summary:  "first0summary_service_ms",
			},
		}
		respModifySchemas, err := datasource.GetMetadataManager().ModifySchemas(getContext(), &pb.ModifySchemasRequest{
			ServiceId: serviceIdPro,
			Schemas:   schemas,
		})
		assert.NoError(t, err)
		assert.Equal(t, pb.ResponseSuccess, respModifySchemas.Response.GetCode())
		respExist, err := datasource.GetMetadataManager().ExistSchema(getContext(), &pb.GetExistenceRequest{
			Type:      datasource.ExistTypeSchema,
			ServiceId: serviceIdPro,
			SchemaId:  "first_schemaId_service_ms",
		})
		assert.NoError(t, err)
		assert.Equal(t, "first0summary_service_ms", respExist.Summary)

		respModifySchemas, err = datasource.GetMetadataManager().ModifySchemas(getContext(), &pb.ModifySchemasRequest{
			ServiceId: serviceIdPro,
			Schemas:   schemas,
		})
		assert.NoError(t, err)
		assert.Equal(t, pb.ResponseSuccess, respModifySchemas.Response.GetCode())
	})

	t.Run("modify one schema, should pass", func(t *testing.T) {
		var (
			serviceIdDev1 string
			serviceIdDev2 string
		)

		log.Info("register service")
		respCreateService, err := datasource.GetMetadataManager().RegisterService(getContext(), &pb.CreateServiceRequest{
			Service: &pb.MicroService{
				AppId:       "create_schema_dev_service_ms",
				ServiceName: "create_schema_service_service_ms",
				Version:     "1.0.0",
				Level:       "FRONT",
				Status:      pb.MS_UP,
				Environment: pb.ENV_DEV,
			},
		})
		assert.NoError(t, err)
		assert.Equal(t, pb.ResponseSuccess, respCreateService.Response.GetCode())
		serviceIdDev1 = respCreateService.ServiceId

		respCreateService, err = datasource.GetMetadataManager().RegisterService(getContext(), &pb.CreateServiceRequest{
			Service: &pb.MicroService{
				AppId:       "create_schema_dev_service_ms",
				ServiceName: "create_schema_service_service_ms",
				Version:     "1.0.1",
				Level:       "FRONT",
				Schemas: []string{
					"first_schemaId_service_ms",
				},
				Status:      pb.MS_UP,
				Environment: pb.ENV_DEV,
			},
		})
		assert.NoError(t, err)
		assert.Equal(t, pb.ResponseSuccess, respCreateService.Response.GetCode())
		serviceIdDev2 = respCreateService.ServiceId

		log.Info("create a schema for service whose schemaID is empty")
		respModifySchema, err := datasource.GetMetadataManager().ModifySchema(getContext(), &pb.ModifySchemaRequest{
			ServiceId: serviceIdDev1,
			SchemaId:  "first_schemaId_service_ms",
			Schema:    "first_schema_service_ms",
		})
		assert.NoError(t, err)
		assert.Equal(t, pb.ResponseSuccess, respModifySchema.Response.GetCode())

		log.Info("create schema for the service whose schemaId already exist")
		respModifySchema, err = datasource.GetMetadataManager().ModifySchema(getContext(), &pb.ModifySchemaRequest{
			ServiceId: serviceIdDev2,
			SchemaId:  "first_schemaId_service_ms",
			Schema:    "first_schema_service_ms",
		})
		assert.NoError(t, err)
		assert.Equal(t, pb.ResponseSuccess, respModifySchema.Response.GetCode())

		log.Info("create schema for the service whose schema summary is empty")
		respModifySchema, err = datasource.GetMetadataManager().ModifySchema(getContext(), &pb.ModifySchemaRequest{
			ServiceId: serviceIdDev1,
			SchemaId:  "first_schemaId_service_ms",
			Schema:    "first_schema_change_service_ms",
			Summary:   "first0summary1change_service_ms",
		})
		assert.NoError(t, err)
		assert.Equal(t, pb.ResponseSuccess, respModifySchema.Response.GetCode())

		log.Info("create schema for the service whose schema summary already exist")
		respModifySchema, err = datasource.GetMetadataManager().ModifySchema(getContext(), &pb.ModifySchemaRequest{
			ServiceId: serviceIdDev1,
			SchemaId:  "first_schemaId_service_ms",
			Schema:    "first_schema_service_ms",
			Summary:   "first0summary_service_ms",
		})
		assert.NoError(t, err)
		assert.Equal(t, pb.ResponseSuccess, respModifySchema.Response.GetCode())

		log.Info("add schema")
		respModifySchema, err = datasource.GetMetadataManager().ModifySchema(getContext(), &pb.ModifySchemaRequest{
			ServiceId: serviceIdDev1,
			SchemaId:  "second_schemaId_service_ms",
			Schema:    "second_schema_service_ms",
		})
		assert.NoError(t, err)
		assert.Equal(t, pb.ResponseSuccess, respModifySchema.Response.GetCode())
	})

	t.Run("add a schemaId in production env while schema editable is set", func(t *testing.T) {
		var (
			serviceIdPro1 string
		)
		log.Info("register service")
		respCreateService, err := datasource.GetMetadataManager().RegisterService(getContext(), &pb.CreateServiceRequest{
			Service: &pb.MicroService{
				AppId:       "add_a_schemaId_prod_schema_lock_ms",
				ServiceName: "add_a_schemaId_prod_schema_lock_service_ms",
				Version:     "1.0.0",
				Level:       "FRONT",
				Status:      pb.MS_UP,
				Environment: pb.ENV_PROD,
			},
		})
		assert.NoError(t, err)
		assert.Equal(t, pb.ResponseSuccess, respCreateService.Response.GetCode())
		serviceIdPro1 = respCreateService.ServiceId

		log.Info("add a schema with new schemaId, should pass")
		schemas := []*pb.Schema{
			{
				SchemaId: "first_schemaId_ms",
				Schema:   "first_schema_ms",
				Summary:  "first0summary_ms",
			},
		}
		respModifySchemas, err := datasource.GetMetadataManager().ModifySchemas(getContext(), &pb.ModifySchemasRequest{
			ServiceId: serviceIdPro1,
			Schemas:   schemas,
		})
		assert.NoError(t, err)
		assert.Equal(t, pb.ResponseSuccess, respModifySchemas.Response.GetCode())

		respService, err := datasource.GetMetadataManager().GetService(getContext(), &pb.GetServiceRequest{
			ServiceId: serviceIdPro1,
		})
		assert.NoError(t, err)
		assert.Equal(t, pb.ResponseSuccess, respService.Response.GetCode())
		assert.Equal(t, []string{"first_schemaId_ms"}, respService.Service.Schemas)

		schemas = []*pb.Schema{
			{
				SchemaId: "second_schemaId_ms",
				Schema:   "second_schema_ms",
				Summary:  "second0summary_ms",
			},
		}
		log.Info("schema edit not allowed, add a schema with new schemaId should fail")

		localMicroServiceDs := genLocalDatasource(false).MetadataManager()
		respModifySchemas, err = localMicroServiceDs.ModifySchemas(getContext(), &pb.ModifySchemasRequest{
			ServiceId: serviceIdPro1,
			Schemas:   schemas,
		})
		assert.NoError(t, err)
		assert.Equal(t, pb.ErrUndefinedSchemaID, respModifySchemas.Response.GetCode())

		log.Info("schema edit allowed, add a schema with new schemaId, should pass")
		localMicroServiceDs = genLocalDatasource(true).MetadataManager()
		respModifySchemas, err = localMicroServiceDs.ModifySchemas(getContext(), &pb.ModifySchemasRequest{
			ServiceId: serviceIdPro1,
			Schemas:   schemas,
		})
		assert.NoError(t, err)
		assert.Equal(t, pb.ResponseSuccess, respModifySchemas.Response.GetCode())
	})

	t.Run("modify a schema in production env while schema editable is set", func(t *testing.T) {
		var (
			serviceIdPro1 string
		)
		log.Info("register service")
		respCreateService, err := datasource.GetMetadataManager().RegisterService(getContext(), &pb.CreateServiceRequest{
			Service: &pb.MicroService{
				AppId:       "modify_a_schema_prod_schema_lock_ms",
				ServiceName: "modify_a_schema_prod_schema_lock_service_ms",
				Version:     "1.0.0",
				Level:       "FRONT",
				Status:      pb.MS_UP,
				Environment: pb.ENV_PROD,
			},
		})
		assert.NoError(t, err)
		assert.Equal(t, pb.ResponseSuccess, respCreateService.Response.GetCode())
		serviceIdPro1 = respCreateService.ServiceId

		log.Info("add schemas, should pass")
		schemas := []*pb.Schema{
			{
				SchemaId: "first_schemaId_ms",
				Schema:   "first_schema_ms",
				Summary:  "first0summary_ms",
			},
		}
		respModifySchemas, err := datasource.GetMetadataManager().ModifySchemas(getContext(), &pb.ModifySchemasRequest{
			ServiceId: serviceIdPro1,
			Schemas:   schemas,
		})
		assert.NoError(t, err)
		assert.Equal(t, pb.ResponseSuccess, respModifySchemas.Response.GetCode())

		respService, err := datasource.GetMetadataManager().GetService(getContext(), &pb.GetServiceRequest{
			ServiceId: serviceIdPro1,
		})
		assert.NoError(t, err)
		assert.Equal(t, []string{"first_schemaId_ms"}, respService.Service.Schemas)

		log.Info("schema edit not allowed, modify schema should fail")
		localMicroServiceDs := genLocalDatasource(false).MetadataManager()
		respModifySchema, err := localMicroServiceDs.ModifySchema(getContext(), &pb.ModifySchemaRequest{
			ServiceId: serviceIdPro1,
			SchemaId:  schemas[0].SchemaId,
			Summary:   schemas[0].Summary,
			Schema:    schemas[0].SchemaId,
		})
		assert.NoError(t, err)
		assert.Equal(t, pb.ErrModifySchemaNotAllow, respModifySchema.Response.GetCode())

		log.Info("schema edit allowed, add a schema with new schemaId, should pass")
		localMicroServiceDs = genLocalDatasource(true).MetadataManager()
		respModifySchema, err = localMicroServiceDs.ModifySchema(getContext(), &pb.ModifySchemaRequest{
			ServiceId: serviceIdPro1,
			SchemaId:  schemas[0].SchemaId,
			Summary:   schemas[0].Summary,
			Schema:    schemas[0].SchemaId,
		})
		assert.NoError(t, err)
		assert.Equal(t, pb.ResponseSuccess, respModifySchema.Response.GetCode())
	})
}

func TestSchema_Exist(t *testing.T) {
	var (
		serviceId string
	)

	t.Run("register service and add schema", func(t *testing.T) {
		log.Info("register service")
		respCreateService, err := datasource.GetMetadataManager().RegisterService(getContext(), &pb.CreateServiceRequest{
			Service: &pb.MicroService{
				AppId:       "query_schema_group_ms",
				ServiceName: "query_schema_service_ms",
				Version:     "1.0.0",
				Level:       "FRONT",
				Status:      pb.MS_UP,
				Environment: pb.ENV_DEV,
			},
		})
		assert.NoError(t, err)
		assert.Equal(t, pb.ResponseSuccess, respCreateService.Response.GetCode())
		serviceId = respCreateService.ServiceId

		log.Info("add schemas, should pass")
		resp, err := datasource.GetMetadataManager().ModifySchema(getContext(), &pb.ModifySchemaRequest{
			ServiceId: serviceId,
			SchemaId:  "com.huawei.test.ms",
			Schema:    "query schema ms",
			Summary:   "summary_ms",
		})
		assert.NoError(t, err)
		assert.Equal(t, pb.ResponseSuccess, resp.Response.GetCode())

		resp, err = datasource.GetMetadataManager().ModifySchema(getContext(), &pb.ModifySchemaRequest{
			ServiceId: serviceId,
			SchemaId:  "com.huawei.test.no.summary.ms",
			Schema:    "query schema ms",
		})
		assert.NoError(t, err)
		assert.Equal(t, pb.ResponseSuccess, resp.Response.GetCode())
	})

	t.Run("check exists", func(t *testing.T) {
		log.Info("check schema exist, should pass")
		resp, err := datasource.GetMetadataManager().ExistSchema(getContext(), &pb.GetExistenceRequest{
			Type:      datasource.ExistTypeSchema,
			ServiceId: serviceId,
			SchemaId:  "com.huawei.test.ms",
		})
		assert.NoError(t, err)
		assert.Equal(t, pb.ResponseSuccess, resp.Response.GetCode())
		assert.Equal(t, "summary_ms", resp.Summary)

		resp, err = datasource.GetMetadataManager().ExistSchema(getContext(), &pb.GetExistenceRequest{
			Type:        datasource.ExistTypeSchema,
			ServiceId:   serviceId,
			SchemaId:    "com.huawei.test.ms",
			AppId:       "()",
			ServiceName: "",
			Version:     "()",
		})
		assert.NoError(t, err)
		assert.Equal(t, pb.ResponseSuccess, resp.Response.GetCode())

		resp, err = datasource.GetMetadataManager().ExistSchema(getContext(), &pb.GetExistenceRequest{
			Type:      datasource.ExistTypeSchema,
			ServiceId: serviceId,
			SchemaId:  "com.huawei.test.no.summary.ms",
		})
		assert.NoError(t, err)
		assert.Equal(t, pb.ResponseSuccess, resp.Response.GetCode())
		assert.Equal(t, "com.huawei.test.no.summary.ms", resp.SchemaId)
		assert.Equal(t, "", resp.Summary)
	})
}

func TestSchema_Get(t *testing.T) {
	var (
		serviceId  string
		serviceId1 string
	)

	var (
		schemaId1     string = "all_schema1_ms"
		schemaId2     string = "all_schema2_ms"
		schemaId3     string = "all_schema3_ms"
		summary       string = "this0is1a2test3ms"
		schemaContent string = "the content is vary large"
	)

	t.Run("register service and instance", func(t *testing.T) {
		respCreateService, err := datasource.GetMetadataManager().RegisterService(getContext(), &pb.CreateServiceRequest{
			Service: &pb.MicroService{
				AppId:       "get_schema_group_ms",
				ServiceName: "get_schema_service_ms",
				Version:     "1.0.0",
				Level:       "FRONT",
				Schemas: []string{
					"non-schema-content",
				},
				Status:      pb.MS_UP,
				Environment: pb.ENV_DEV,
			},
		})
		assert.NoError(t, err)
		assert.Equal(t, pb.ResponseSuccess, respCreateService.Response.GetCode())
		serviceId = respCreateService.ServiceId

		respCreateSchema, err := datasource.GetMetadataManager().ModifySchema(getContext(), &pb.ModifySchemaRequest{
			ServiceId: serviceId,
			SchemaId:  "com.huawei.test.ms",
			Schema:    "get schema ms",
			Summary:   "schema0summary1ms",
		})
		assert.NoError(t, err)
		assert.Equal(t, pb.ResponseSuccess, respCreateSchema.Response.GetCode())

		respCreateService, err = datasource.GetMetadataManager().RegisterService(getContext(), &pb.CreateServiceRequest{
			Service: &pb.MicroService{
				AppId:       "get_all_schema_ms",
				ServiceName: "get_all_schema_ms",
				Version:     "1.0.0",
				Level:       "FRONT",
				Schemas: []string{
					schemaId1,
					schemaId2,
					schemaId3,
				},
				Status: pb.MS_UP,
			},
		})
		assert.NoError(t, err)
		assert.Equal(t, pb.ResponseSuccess, respCreateService.Response.GetCode())
		serviceId1 = respCreateService.ServiceId

		respPutData, err := datasource.GetMetadataManager().ModifySchema(getContext(), &pb.ModifySchemaRequest{
			ServiceId: serviceId1,
			SchemaId:  schemaId2,
			Schema:    schemaContent,
		})
		assert.NoError(t, err)
		assert.Equal(t, pb.ResponseSuccess, respPutData.Response.GetCode())

		respPutData, err = datasource.GetMetadataManager().ModifySchema(getContext(), &pb.ModifySchemaRequest{
			ServiceId: serviceId1,
			SchemaId:  schemaId3,
			Schema:    schemaContent,
			Summary:   summary,
		})
		assert.NoError(t, err)
		assert.Equal(t, pb.ResponseSuccess, respPutData.Response.GetCode())

		respGetAllSchema, err := datasource.GetMetadataManager().GetAllSchemas(getContext(), &pb.GetAllSchemaRequest{
			ServiceId:  serviceId1,
			WithSchema: false,
		})
		assert.NoError(t, err)
		assert.Equal(t, pb.ResponseSuccess, respGetAllSchema.Response.GetCode())
		schemas := respGetAllSchema.Schemas
		for _, schema := range schemas {
			if schema.SchemaId == schemaId1 && schema.SchemaId == schemaId2 {
				assert.Empty(t, schema.Summary)
				assert.Empty(t, schema.Schema)
			}
			if schema.SchemaId == schemaId3 {
				assert.Equal(t, summary, schema.Summary)
				assert.Empty(t, schema.Schema)
			}
		}

		respGetAllSchema, err = datasource.GetMetadataManager().GetAllSchemas(getContext(), &pb.GetAllSchemaRequest{
			ServiceId:  serviceId1,
			WithSchema: true,
		})
		assert.NoError(t, err)
		assert.Equal(t, pb.ResponseSuccess, respGetAllSchema.Response.GetCode())
		schemas = respGetAllSchema.Schemas
		for _, schema := range schemas {
			switch schema.SchemaId {
			case schemaId1:
				assert.Empty(t, schema.Summary)
				assert.Empty(t, schema.Schema)
			case schemaId2:
				assert.Empty(t, schema.Summary)
				assert.Equal(t, schemaContent, schema.Schema)
			case schemaId3:
				assert.Equal(t, summary, schema.Summary)
				assert.Equal(t, schemaContent, schema.Schema)
			}
		}
	})

	t.Run("test get when request is invalid", func(t *testing.T) {
		log.Info("service does not exist")
		respGetSchema, err := datasource.GetMetadataManager().GetSchema(getContext(), &pb.GetSchemaRequest{
			ServiceId: "none_exist_service",
			SchemaId:  "com.huawei.test",
		})
		assert.NoError(t, err)
		assert.Equal(t, pb.ErrServiceNotExists, respGetSchema.Response.GetCode())

		respGetAllSchemas, err := datasource.GetMetadataManager().GetAllSchemas(getContext(), &pb.GetAllSchemaRequest{
			ServiceId: "none_exist_service",
		})
		assert.NoError(t, err)
		assert.Equal(t, pb.ErrServiceNotExists, respGetAllSchemas.Response.GetCode())

		log.Info("schema id doest not exist")
		respGetSchema, err = datasource.GetMetadataManager().GetSchema(getContext(), &pb.GetSchemaRequest{
			ServiceId: serviceId,
			SchemaId:  "none_exist_schema",
		})
		assert.NoError(t, err)
		assert.Equal(t, pb.ErrSchemaNotExists, respGetSchema.Response.GetCode())
	})

	t.Run("test get when request is valid", func(t *testing.T) {
		resp, err := datasource.GetMetadataManager().GetSchema(getContext(), &pb.GetSchemaRequest{
			ServiceId: serviceId,
			SchemaId:  "com.huawei.test.ms",
		})
		assert.NoError(t, err)
		assert.Equal(t, pb.ResponseSuccess, resp.Response.GetCode())
		assert.Equal(t, "get schema ms", resp.Schema)
		assert.Equal(t, "schema0summary1ms", resp.SchemaSummary)

	})
}

func TestSchema_Delete(t *testing.T) {
	var (
		serviceId string
	)

	t.Run("register service and instance", func(t *testing.T) {
		respCreateService, err := datasource.GetMetadataManager().RegisterService(getContext(), &pb.CreateServiceRequest{
			Service: &pb.MicroService{
				AppId:       "delete_schema_group_ms",
				ServiceName: "delete_schema_service_ms",
				Version:     "1.0.0",
				Level:       "FRONT",
				Status:      pb.MS_UP,
			},
		})
		assert.NoError(t, err)
		assert.Equal(t, pb.ResponseSuccess, respCreateService.Response.GetCode())
		serviceId = respCreateService.ServiceId

		resp, err := datasource.GetMetadataManager().ModifySchema(getContext(), &pb.ModifySchemaRequest{
			ServiceId: serviceId,
			SchemaId:  "com.huawei.test.ms",
			Schema:    "delete schema ms",
			Summary:   "summary_ms",
		})
		assert.NoError(t, err)
		assert.Equal(t, pb.ResponseSuccess, resp.Response.GetCode())
	})

	t.Run("test delete when request is invalid", func(t *testing.T) {
		log.Info("schema id does not exist")
		resp, err := datasource.GetMetadataManager().DeleteSchema(getContext(), &pb.DeleteSchemaRequest{
			ServiceId: serviceId,
			SchemaId:  "none_exist_schema",
		})
		assert.NoError(t, err)
		assert.NotEqual(t, pb.ResponseSuccess, resp.Response.GetCode())

		log.Info("service id does not exist")
		resp, err = datasource.GetMetadataManager().DeleteSchema(getContext(), &pb.DeleteSchemaRequest{
			ServiceId: "not_exist_service",
			SchemaId:  "com.huawei.test.ms",
		})
		assert.NoError(t, err)
		assert.NotEqual(t, pb.ResponseSuccess, resp.Response.GetCode())
	})

	t.Run("test delete when request is valid", func(t *testing.T) {
		resp, err := datasource.GetMetadataManager().DeleteSchema(getContext(), &pb.DeleteSchemaRequest{
			ServiceId: serviceId,
			SchemaId:  "com.huawei.test.ms",
		})
		assert.NoError(t, err)
		assert.Equal(t, pb.ResponseSuccess, resp.Response.GetCode())

		respGet, err := datasource.GetMetadataManager().GetSchema(getContext(), &pb.GetSchemaRequest{
			ServiceId: serviceId,
			SchemaId:  "com.huawei.test.ms",
		})
		assert.NoError(t, err)
		assert.Equal(t, pb.ErrSchemaNotExists, respGet.Response.GetCode())

		respExist, err := datasource.GetMetadataManager().ExistSchema(getContext(), &pb.GetExistenceRequest{
			Type:      "schema",
			ServiceId: serviceId,
			SchemaId:  "com.huawei.test.ms",
		})
		assert.NoError(t, err)
		assert.Equal(t, pb.ErrSchemaNotExists, respExist.Response.GetCode())
	})
}

func genLocalDatasource(editable bool) datasource.DataSource {
	t := archaius.Get("TEST_MODE")
	if t == nil {
		t = "etcd"
	}
	if t == "etcd" {
		ds, _ := etcd.NewDataSource(datasource.Options{
			Kind:              "etcd",
			SchemaNotEditable: !editable,
		})

		return ds
	}
	ds, _ := mongo.NewDataSource(datasource.Options{
		SchemaNotEditable: !editable,
	})
	return ds
}
