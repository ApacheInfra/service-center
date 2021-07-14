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
package disco_test

import (
	"strconv"
	"strings"

	"github.com/apache/servicecomb-service-center/datasource"

	"github.com/apache/servicecomb-service-center/server/plugin/quota"
	pb "github.com/go-chassis/cari/discovery"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

const (
	invalidSchemaId = "@"
)

var (
	TOO_LONG_SUMMARY = strings.Repeat("x", 129)
)

var _ = Describe("'Schema' service", func() {
	Describe("execute 'create' operation", func() {
		var (
			serviceIdDev string
			serviceId    string
		)

		It("should be passed, create service", func() {
			respCreateService, err := serviceResource.Create(getContext(), &pb.CreateServiceRequest{
				Service: &pb.MicroService{
					AppId:       "create_schema_group",
					ServiceName: "create_schema_service",
					Version:     "1.0.0",
					Level:       "FRONT",
					Status:      pb.MS_UP,
					Environment: pb.ENV_DEV,
				},
			})
			Expect(err).To(BeNil())
			Expect(respCreateService.Response.GetCode()).To(Equal(pb.ResponseSuccess))
			serviceIdDev = respCreateService.ServiceId

			respCreateService, err = serviceResource.Create(getContext(), &pb.CreateServiceRequest{
				Service: &pb.MicroService{
					AppId:       "create_schema_group",
					ServiceName: "create_schema_service",
					Version:     "1.0.0",
					Level:       "FRONT",
					Status:      pb.MS_UP,
					Environment: pb.ENV_PROD,
				},
			})
			Expect(err).To(BeNil())
			Expect(respCreateService.Response.GetCode()).To(Equal(pb.ResponseSuccess))
			serviceId = respCreateService.ServiceId
		})

		Context("when create an invalid schema", func() {
			f := func() {
				By("service id is empty")
				resp, err := serviceResource.ModifySchema(getContext(), &pb.ModifySchemaRequest{
					ServiceId: "",
					SchemaId:  "com.huawei.test",
					Schema:    "create schema",
				})
				Expect(err).To(BeNil())
				Expect(resp.Response.GetCode()).To(Equal(pb.ErrInvalidParams))

				By("service does not exist")
				resp, err = serviceResource.ModifySchema(getContext(), &pb.ModifySchemaRequest{
					ServiceId: "notExistService",
					SchemaId:  "com.huawei.test",
					Schema:    "create schema",
				})
				Expect(err).To(BeNil())
				Expect(resp.Response.GetCode()).To(Equal(pb.ErrServiceNotExists))

				By("schema id is invalid")
				resp, err = serviceResource.ModifySchema(getContext(), &pb.ModifySchemaRequest{
					ServiceId: serviceIdDev,
					SchemaId:  invalidSchemaId,
					Schema:    "create schema",
				})
				Expect(err).To(BeNil())
				Expect(resp.Response.GetCode()).To(Equal(pb.ErrInvalidParams))

				By("summary is invalid")
				resp, err = serviceResource.ModifySchema(getContext(), &pb.ModifySchemaRequest{
					ServiceId: serviceIdDev,
					SchemaId:  "com.huawei.test",
					Schema:    "create schema",
					Summary:   TOO_LONG_SUMMARY,
				})
				Expect(err).To(BeNil())
				Expect(resp.Response.GetCode()).To(Equal(pb.ErrInvalidParams))

				resp, err = serviceResource.ModifySchema(getContext(), &pb.ModifySchemaRequest{
					ServiceId: serviceIdDev,
					SchemaId:  "com.huawei.test",
					Schema:    "create schema",
					Summary:   "_",
				})
				Expect(err).To(BeNil())
				Expect(resp.Response.GetCode()).To(Equal(pb.ErrInvalidParams))
			}

			It("should be failed in prod env", func() {
				old := serviceIdDev
				serviceIdDev = serviceId
				f()
				serviceIdDev = old
			})
			It("should be failed in dev env", f)
		})

		Context("when batch create invalid schemas", func() {
			f := func() {
				By("service does not exist")
				respCreateService, err := serviceResource.ModifySchemas(getContext(), &pb.ModifySchemasRequest{
					ServiceId: "not_exist_serviceId",
				})
				Expect(err).To(BeNil())
				Expect(respCreateService.Response.GetCode()).ToNot(Equal(pb.ResponseSuccess))

				By("service id is empty")
				respCreateService, err = serviceResource.ModifySchemas(getContext(), &pb.ModifySchemasRequest{
					ServiceId: "",
				})
				Expect(err).To(BeNil())
				Expect(respCreateService.Response.GetCode()).ToNot(Equal(pb.ResponseSuccess))

				By("schema id is invalid")
				respCreateService, err = serviceResource.ModifySchemas(getContext(), &pb.ModifySchemasRequest{
					ServiceId: serviceIdDev,
					Schemas: []*pb.Schema{
						{
							SchemaId: invalidSchemaId,
						},
					},
				})
				Expect(err).To(BeNil())
				Expect(respCreateService.Response.GetCode()).ToNot(Equal(pb.ResponseSuccess))

				By("schema is empty")
				respCreateService, err = serviceResource.ModifySchemas(getContext(), &pb.ModifySchemasRequest{
					ServiceId: serviceIdDev,
					Schemas: []*pb.Schema{
						{
							SchemaId: "com.huawei.test",
						},
					},
				})
				Expect(err).To(BeNil())
				Expect(respCreateService.Response.GetCode()).ToNot(Equal(pb.ResponseSuccess))

				By("summary is empty")
				respCreateService, err = serviceResource.ModifySchemas(getContext(), &pb.ModifySchemasRequest{
					ServiceId: serviceIdDev,
					Schemas: []*pb.Schema{
						{
							SchemaId: "com.huawei.test",
							Schema:   "create schema",
						},
					},
				})
				Expect(err).To(BeNil())
				Expect(respCreateService.Response.GetCode()).ToNot(Equal(pb.ResponseSuccess))

				By("summery is invalid")
				respCreateService, err = serviceResource.ModifySchemas(getContext(), &pb.ModifySchemasRequest{
					ServiceId: serviceIdDev,
					Schemas: []*pb.Schema{
						{
							SchemaId: "com.huawei.test",
							Schema:   "create schema",
							Summary:  TOO_LONG_SUMMARY,
						},
					},
				})
				Expect(err).To(BeNil())
				Expect(respCreateService.Response.GetCode()).ToNot(Equal(pb.ResponseSuccess))
			}
			It("should be failed in dev env", f)

			It("should be failed in prod env", func() {
				old := serviceIdDev
				serviceIdDev = serviceId
				f()
				serviceIdDev = old
			})
		})

		Context("when create schemas out of gauge", func() {
			size := quota.DefaultSchemaQuota + 1
			schemaIds := make([]string, 0, size)
			schemas := make([]*pb.Schema, 0, size)
			for i := 0; i < size; i++ {
				s := "ServiceCombTestTheLimitOfSchemas" + strconv.Itoa(i)

				schemaIds = append(schemaIds, s)
				schemas = append(schemas, &pb.Schema{
					SchemaId: s,
					Schema:   s,
					Summary:  s,
				})
			}

			It("should be failed in dev env", func() {
				By("batch modify schemas 1")
				respCreateSchemas, err := serviceResource.ModifySchemas(getContext(), &pb.ModifySchemasRequest{
					ServiceId: serviceIdDev,
					Schemas:   schemas,
				})
				Expect(err).To(BeNil())
				Expect(respCreateSchemas.Response.GetCode()).To(Equal(pb.ErrInvalidParams))

				By("batch modify schemas 2")
				respCreateSchemas, err = serviceResource.ModifySchemas(getContext(), &pb.ModifySchemasRequest{
					ServiceId: serviceIdDev,
					Schemas:   schemas[:quota.DefaultSchemaQuota],
				})
				Expect(err).To(BeNil())
				Expect(respCreateSchemas.Response.GetCode()).To(Equal(pb.ResponseSuccess))

				By("modify one schema")
				respCreateService := &pb.ModifySchemaResponse{}
				schema := schemas[quota.DefaultSchemaQuota]
				respCreateService, err = serviceResource.ModifySchema(getContext(), &pb.ModifySchemaRequest{
					ServiceId: serviceIdDev,
					SchemaId:  schema.SchemaId,
					Schema:    schema.Schema,
				})
				Expect(err).To(BeNil())
				Expect(respCreateService.Response.GetCode()).To(Equal(pb.ErrNotEnoughQuota))
			})

			It("should be failed in prod env", func() {
				respCreateService, err := serviceResource.ModifySchemas(getContext(), &pb.ModifySchemasRequest{
					ServiceId: serviceIdDev,
					Schemas:   schemas,
				})
				Expect(err).To(BeNil())
				Expect(respCreateService.Response.GetCode()).To(Equal(pb.ErrInvalidParams))
			})

			It("should be failed when create service", func() {
				respServiceForSchema, err := serviceResource.Create(getContext(), &pb.CreateServiceRequest{
					Service: &pb.MicroService{
						AppId:       "check_schema_group",
						ServiceName: "check_schema_service",
						Version:     "1.0.0",
						Level:       "FRONT",
						Schemas:     schemaIds,
						Status:      pb.MS_UP,
					},
				})
				Expect(err).To(BeNil())
				Expect(respServiceForSchema.Response.GetCode()).ToNot(Equal(pb.ResponseSuccess))
			})
		})

		Context("when modify schemas", func() {
			var (
				serviceIdDev1 string
				serviceIdDev2 string
			)

			It("should be passed", func() {
				respCreateService, err := serviceResource.Create(getContext(), &pb.CreateServiceRequest{
					Service: &pb.MicroService{
						AppId:       "create_schemas_dev",
						ServiceName: "create_schemas_service",
						Version:     "1.0.0",
						Level:       "FRONT",
						Status:      pb.MS_UP,
						Environment: pb.ENV_DEV,
					},
				})
				Expect(err).To(BeNil())
				Expect(respCreateService.Response.GetCode()).To(Equal(pb.ResponseSuccess))
				serviceIdDev1 = respCreateService.ServiceId

				respCreateService, err = serviceResource.Create(getContext(), &pb.CreateServiceRequest{
					Service: &pb.MicroService{
						AppId:       "create_schemas_dev",
						ServiceName: "create_schemas_service",
						Version:     "1.0.1",
						Level:       "FRONT",
						Schemas: []string{
							"first_schemaId",
						},
						Status:      pb.MS_UP,
						Environment: pb.ENV_DEV,
					},
				})
				Expect(err).To(BeNil())
				Expect(respCreateService.Response.GetCode()).To(Equal(pb.ResponseSuccess))
				serviceIdDev2 = respCreateService.ServiceId
			})

			It("should be passed", func() {
				By("create schemas when service schema id set is empty")
				schemas := []*pb.Schema{
					{
						SchemaId: "first_schemaId",
						Schema:   "first_schema",
						Summary:  "first0summary",
					},
					{
						SchemaId: "first_schemaId",
						Schema:   "first_schema",
						Summary:  "first0summary",
					},
				}
				respCreateService, err := serviceResource.ModifySchemas(getContext(), &pb.ModifySchemasRequest{
					ServiceId: serviceIdDev1,
					Schemas:   schemas,
				})
				Expect(err).To(BeNil())
				Expect(respCreateService.Response.GetCode()).To(Equal(pb.ResponseSuccess))

				respGetAllSchema, err := serviceResource.GetAllSchemaInfo(getContext(), &pb.GetAllSchemaRequest{
					ServiceId: serviceIdDev1,
				})
				Expect(err).To(BeNil())
				Expect(respGetAllSchema.Response.GetCode()).To(Equal(pb.ResponseSuccess))
				Expect(len(respGetAllSchema.Schemas)).To(Equal(1))

				By("modify schemas when service schema id already exists")
				schemas = []*pb.Schema{}
				respCreateService, err = serviceResource.ModifySchemas(getContext(), &pb.ModifySchemasRequest{
					ServiceId: serviceIdDev2,
					Schemas:   schemas,
				})
				Expect(err).To(BeNil())
				Expect(respCreateService.Response.GetCode()).To(Equal(pb.ErrInvalidParams))

				By("modify schemas")
				schemas = []*pb.Schema{
					{
						SchemaId: "first_schemaId",
						Schema:   "first_schema_change",
						Summary:  "first0summary1change",
					},
				}
				respCreateService, err = serviceResource.ModifySchemas(getContext(), &pb.ModifySchemasRequest{
					ServiceId: serviceIdDev1,
					Schemas:   schemas,
				})
				Expect(err).To(BeNil())
				Expect(respCreateService.Response.GetCode()).To(Equal(pb.ResponseSuccess))

				By("add schemas")
				schemas = []*pb.Schema{
					{
						SchemaId: "second_schemaId",
						Schema:   "second_schema",
						Summary:  "second0summary",
					},
				}
				respCreateService, err = serviceResource.ModifySchemas(getContext(), &pb.ModifySchemasRequest{
					ServiceId: serviceIdDev1,
					Schemas:   schemas,
				})
				Expect(err).To(BeNil())
				Expect(respCreateService.Response.GetCode()).To(Equal(pb.ResponseSuccess))

				respGetOne, err := serviceResource.GetOne(getContext(), &pb.GetServiceRequest{
					ServiceId: serviceIdDev1,
				})
				Expect(err).To(BeNil())
				Expect(respGetOne.Response.GetCode()).To(Equal(pb.ResponseSuccess))
				Expect(respGetOne.Service.Schemas).To(Equal([]string{"second_schemaId"}))

				By("create empty")
				schemas = []*pb.Schema{}
				respCreateService, err = serviceResource.ModifySchemas(getContext(), &pb.ModifySchemasRequest{
					ServiceId: serviceIdDev1,
					Schemas:   schemas,
				})
				Expect(err).To(BeNil())
				Expect(respCreateService.Response.GetCode()).To(Equal(pb.ErrInvalidParams))

				By("add new schemaId not exist in service schemaId")
				schemas = []*pb.Schema{
					{
						SchemaId: "second_schemaId",
						Schema:   "second_schema",
						Summary:  "second0summary",
					},
				}
				respCreateService, err = serviceResource.ModifySchemas(getContext(), &pb.ModifySchemasRequest{
					ServiceId: serviceIdDev2,
					Schemas:   schemas,
				})
				Expect(err).To(BeNil())
				Expect(respCreateService.Response.GetCode()).To(Equal(pb.ResponseSuccess))

				respGetOne, err = serviceResource.GetOne(getContext(), &pb.GetServiceRequest{
					ServiceId: serviceIdDev2,
				})
				Expect(err).To(BeNil())
				Expect(respGetOne.Response.GetCode()).To(Equal(pb.ResponseSuccess))
				Expect(respGetOne.Service.Schemas).To(Equal([]string{"second_schemaId"}))
			})
		})

		Context("when modify schema and summary is empty", func() {
			var (
				serviceIdPro string
			)

			It("should be passed", func() {
				respCreateService, err := serviceResource.Create(getContext(), &pb.CreateServiceRequest{
					Service: &pb.MicroService{
						AppId:       "create_schemas_prod",
						ServiceName: "create_schemas_service",
						Version:     "1.0.1",
						Level:       "FRONT",
						Schemas: []string{
							"first_schemaId",
							"second_schemaId",
						},
						Status:      pb.MS_UP,
						Environment: pb.ENV_PROD,
					},
				})
				Expect(err).To(BeNil())
				Expect(respCreateService.Response.GetCode()).To(Equal(pb.ResponseSuccess))
				serviceIdPro = respCreateService.ServiceId
			})

			It("summary is empty", func() {
				By("add schema when summary is empty")
				respModifySchema, err := serviceResource.ModifySchema(getContext(), &pb.ModifySchemaRequest{
					ServiceId: serviceIdPro,
					SchemaId:  "first_schemaId",
					Schema:    "first_schema",
				})
				Expect(err).To(BeNil())
				Expect(respModifySchema.Response.GetCode()).To(Equal(pb.ResponseSuccess))

				By("add schemas when summary in database is empty")
				schemas := []*pb.Schema{
					{
						SchemaId: "first_schemaId",
						Schema:   "first_schema",
						Summary:  "first0summary",
					},
				}

				respModifySchemas, err := serviceResource.ModifySchemas(getContext(), &pb.ModifySchemasRequest{
					ServiceId: serviceIdPro,
					Schemas:   schemas,
				})
				Expect(err).To(BeNil())
				Expect(respModifySchemas.Response.GetCode()).To(Equal(pb.ResponseSuccess))

				respExist, err := serviceResource.Exist(getContext(), &pb.GetExistenceRequest{
					Type:      datasource.ExistTypeSchema,
					ServiceId: serviceIdPro,
					SchemaId:  "first_schemaId",
				})

				Expect(err).To(BeNil())
				Expect(respExist.Summary).To(Equal("first0summary"))

				schemas = []*pb.Schema{
					{
						SchemaId: "second_schemaId",
						Schema:   "second_schema",
						Summary:  "second0summary",
					},
				}
				respModifySchemas, err = serviceResource.ModifySchemas(getContext(), &pb.ModifySchemasRequest{
					ServiceId: serviceIdPro,
					Schemas:   schemas,
				})
				Expect(err).To(BeNil())
				Expect(respModifySchemas.Response.GetCode()).To(Equal(pb.ResponseSuccess))
			})
		})
	})

	Describe("execute 'exist' operation", func() {
		var (
			serviceId string
		)

		It("should be passed", func() {
			respCreateService, err := serviceResource.Create(getContext(), &pb.CreateServiceRequest{
				Service: &pb.MicroService{
					AppId:       "query_schema_group",
					ServiceName: "query_schema_service",
					Version:     "1.0.0",
					Level:       "FRONT",
					Status:      pb.MS_UP,
					Environment: pb.ENV_DEV,
				},
			})
			Expect(err).To(BeNil())
			Expect(respCreateService.Response.GetCode()).To(Equal(pb.ResponseSuccess))
			serviceId = respCreateService.ServiceId

			resp, err := serviceResource.ModifySchema(getContext(), &pb.ModifySchemaRequest{
				ServiceId: serviceId,
				SchemaId:  "com.huawei.test",
				Schema:    "query schema",
				Summary:   "summary",
			})
			Expect(err).To(BeNil())
			Expect(resp.Response.GetCode()).To(Equal(pb.ResponseSuccess))

			resp, err = serviceResource.ModifySchema(getContext(), &pb.ModifySchemaRequest{
				ServiceId: serviceId,
				SchemaId:  "com.huawei.test.no.summary",
				Schema:    "query schema",
			})
			Expect(err).To(BeNil())
			Expect(resp.Response.GetCode()).To(Equal(pb.ResponseSuccess))
		})

		Context("when request is invalid", func() {
			It("should be failed", func() {
				By("service id is empty")
				resp, err := serviceResource.Exist(getContext(), &pb.GetExistenceRequest{
					Type:      "schema",
					ServiceId: "",
					SchemaId:  "com.huawei.test",
				})
				Expect(err).To(BeNil())
				Expect(resp.Response.GetCode()).ToNot(Equal(pb.ResponseSuccess))

				By("schema id does not exist")
				resp, err = serviceResource.Exist(getContext(), &pb.GetExistenceRequest{
					Type:      "schema",
					ServiceId: serviceId,
					SchemaId:  "noneschema",
				})
				Expect(err).To(BeNil())
				Expect(resp.Response.GetCode()).ToNot(Equal(pb.ResponseSuccess))

				By("schema id is invalid")
				resp, err = serviceResource.Exist(getContext(), &pb.GetExistenceRequest{
					Type:      "schema",
					ServiceId: serviceId,
					SchemaId:  TOO_LONG_SCHEMAID,
				})
				Expect(err).To(BeNil())
				Expect(resp.Response.GetCode()).ToNot(Equal(pb.ResponseSuccess))
			})
		})

		Context("when request is valid", func() {
			It("should be passed", func() {
				resp, err := serviceResource.Exist(getContext(), &pb.GetExistenceRequest{
					Type:      "schema",
					ServiceId: serviceId,
					SchemaId:  "com.huawei.test",
				})
				Expect(err).To(BeNil())
				Expect(resp.Response.GetCode()).To(Equal(pb.ResponseSuccess))
				Expect(resp.Summary).To(Equal("summary"))

				resp, err = serviceResource.Exist(getContext(), &pb.GetExistenceRequest{
					Type:        "schema",
					ServiceId:   serviceId,
					SchemaId:    "com.huawei.test",
					AppId:       "()",
					ServiceName: "",
					Version:     "()",
				})
				Expect(err).To(BeNil())
				Expect(resp.Response.GetCode()).To(Equal(pb.ResponseSuccess))

				resp, err = serviceResource.Exist(getContext(), &pb.GetExistenceRequest{
					Type:      "schema",
					ServiceId: serviceId,
					SchemaId:  "com.huawei.test.no.summary",
				})
				Expect(err).To(BeNil())
				Expect(resp.Response.GetCode()).To(Equal(pb.ResponseSuccess))
				Expect(resp.SchemaId).To(Equal("com.huawei.test.no.summary"))
				Expect(resp.Summary).To(Equal(""))
			})
		})
	})

	Describe("execute 'get' operation", func() {
		var (
			serviceId  string
			serviceId1 string
		)

		var (
			schemaId1     string = "all_schema1"
			schemaId2     string = "all_schema2"
			schemaId3     string = "all_schema3"
			summary       string = "this0is1a2test"
			schemaContent string = "the content is vary large"
		)

		It("should be passed", func() {
			respCreateService, err := serviceResource.Create(getContext(), &pb.CreateServiceRequest{
				Service: &pb.MicroService{
					AppId:       "get_schema_group",
					ServiceName: "get_schema_service",
					Version:     "1.0.0",
					Level:       "FRONT",
					Schemas: []string{
						"non-schema-content",
					},
					Status:      pb.MS_UP,
					Environment: pb.ENV_DEV,
				},
			})
			Expect(err).To(BeNil())
			Expect(respCreateService.Response.GetCode()).To(Equal(pb.ResponseSuccess))
			serviceId = respCreateService.ServiceId

			resp, err := serviceResource.ModifySchema(getContext(), &pb.ModifySchemaRequest{
				ServiceId: serviceId,
				SchemaId:  "com.huawei.test",
				Schema:    "get schema",
				Summary:   "schema0summary",
			})
			Expect(err).To(BeNil())
			Expect(resp.Response.GetCode()).To(Equal(pb.ResponseSuccess))

			respCreateService, err = serviceResource.Create(getContext(), &pb.CreateServiceRequest{
				Service: &pb.MicroService{
					AppId:       "get_all_schema",
					ServiceName: "get_all_schema",
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
			Expect(err).To(BeNil())
			Expect(respCreateService.Response.GetCode()).To(Equal(pb.ResponseSuccess))
			serviceId1 = respCreateService.ServiceId

			respPutData, err := serviceResource.ModifySchema(getContext(), &pb.ModifySchemaRequest{
				ServiceId: serviceId1,
				SchemaId:  schemaId2,
				Schema:    schemaContent,
			})
			Expect(err).To(BeNil())
			Expect(respPutData.Response.GetCode()).To(Equal(pb.ResponseSuccess))

			respPutData, err = serviceResource.ModifySchema(getContext(), &pb.ModifySchemaRequest{
				ServiceId: serviceId1,
				SchemaId:  schemaId3,
				Schema:    schemaContent,
				Summary:   summary,
			})
			Expect(err).To(BeNil())
			Expect(respPutData.Response.GetCode()).To(Equal(pb.ResponseSuccess))

			respGetAllSchema, err := serviceResource.GetAllSchemaInfo(getContext(), &pb.GetAllSchemaRequest{
				ServiceId:  serviceId1,
				WithSchema: false,
			})
			Expect(err).To(BeNil())
			Expect(respGetAllSchema.Response.GetCode()).To(Equal(pb.ResponseSuccess))
			schemas := respGetAllSchema.Schemas
			for _, schema := range schemas {
				if schema.SchemaId == schemaId1 {
					Expect(schema.Summary).To(BeEmpty())
					Expect(schema.Schema).To(BeEmpty())
				}
				if schema.SchemaId == schemaId2 {
					Expect(schema.Summary).To(BeEmpty())
					Expect(schema.Schema).To(BeEmpty())
				}
				if schema.SchemaId == schemaId3 {
					Expect(schema.Summary).To(Equal(summary))
					Expect(schema.Schema).To(BeEmpty())
				}
			}

			respGetAllSchema, err = serviceResource.GetAllSchemaInfo(getContext(), &pb.GetAllSchemaRequest{
				ServiceId:  serviceId1,
				WithSchema: true,
			})
			Expect(err).To(BeNil())
			Expect(respGetAllSchema.Response.GetCode()).To(Equal(pb.ResponseSuccess))
			schemas = respGetAllSchema.Schemas
			for _, schema := range schemas {
				if schema.SchemaId == schemaId1 {
					Expect(schema.Schema).To(BeEmpty())
					Expect(schema.Schema).To(BeEmpty())
				}
				if schema.SchemaId == schemaId2 {
					Expect(schema.Summary).To(BeEmpty())
					Expect(schema.Schema).To(Equal(schemaContent))
				}
				if schema.SchemaId == schemaId3 {
					Expect(schema.Summary).To(Equal(summary))
					Expect(schema.Schema).To(Equal(schemaContent))
				}
			}

		})

		Context("when request is invalid", func() {
			It("should be failed", func() {
				By("service does not exist")
				resp, err := serviceResource.GetSchemaInfo(getContext(), &pb.GetSchemaRequest{
					ServiceId: "noneexistservice",
					SchemaId:  "com.huawei.test",
				})
				Expect(err).To(BeNil())
				Expect(resp.Response.GetCode()).To(Equal(pb.ErrServiceNotExists))

				respA, err := serviceResource.GetAllSchemaInfo(getContext(), &pb.GetAllSchemaRequest{
					ServiceId: "noneexistservice",
				})
				Expect(err).To(BeNil())
				Expect(respA.Response.GetCode()).To(Equal(pb.ErrServiceNotExists))

				By("service id is empty")
				resp, err = serviceResource.GetSchemaInfo(getContext(), &pb.GetSchemaRequest{
					ServiceId: "",
					SchemaId:  "com.huawei.test",
				})
				Expect(err).To(BeNil())
				Expect(resp.Response.GetCode()).To(Equal(pb.ErrInvalidParams))

				respA, err = serviceResource.GetAllSchemaInfo(getContext(), &pb.GetAllSchemaRequest{
					ServiceId: "",
				})
				Expect(err).To(BeNil())
				Expect(respA.Response.GetCode()).To(Equal(pb.ErrInvalidParams))

				By("service id is invalid")
				resp, err = serviceResource.GetSchemaInfo(getContext(), &pb.GetSchemaRequest{
					ServiceId: TOO_LONG_SERVICEID,
					SchemaId:  "com.huawei.test",
				})
				Expect(err).To(BeNil())
				Expect(resp.Response.GetCode()).To(Equal(pb.ErrInvalidParams))

				respA, err = serviceResource.GetAllSchemaInfo(getContext(), &pb.GetAllSchemaRequest{
					ServiceId: TOO_LONG_SERVICEID,
				})
				Expect(err).To(BeNil())
				Expect(respA.Response.GetCode()).To(Equal(pb.ErrInvalidParams))

				By("schema id does not exist")
				resp, err = serviceResource.GetSchemaInfo(getContext(), &pb.GetSchemaRequest{
					ServiceId: serviceId,
					SchemaId:  "nonexistschema",
				})
				Expect(err).To(BeNil())
				Expect(resp.Response.GetCode()).To(Equal(pb.ErrSchemaNotExists))

				By("schema id is invalid")
				resp, err = serviceResource.GetSchemaInfo(getContext(), &pb.GetSchemaRequest{
					ServiceId: serviceId,
					SchemaId:  "",
				})
				Expect(err).To(BeNil())
				Expect(resp.Response.GetCode()).To(Equal(pb.ErrInvalidParams))
				resp, err = serviceResource.GetSchemaInfo(getContext(), &pb.GetSchemaRequest{
					ServiceId: serviceId,
					SchemaId:  TOO_LONG_SCHEMAID,
				})
				Expect(err).To(BeNil())
				Expect(resp.Response.GetCode()).To(Equal(pb.ErrInvalidParams))
				resp, err = serviceResource.GetSchemaInfo(getContext(), &pb.GetSchemaRequest{
					ServiceId: serviceId,
					SchemaId:  invalidSchemaId,
				})
				Expect(err).To(BeNil())
				Expect(resp.Response.GetCode()).To(Equal(pb.ErrInvalidParams))

				By("schema content does not exist")
				resp, err = serviceResource.GetSchemaInfo(getContext(), &pb.GetSchemaRequest{
					ServiceId: serviceId,
					SchemaId:  "non-schema-content",
				})
				Expect(err).To(BeNil())
				Expect(resp.Response.GetCode()).To(Equal(pb.ErrSchemaNotExists))
			})
		})

		Context("when request is valid", func() {
			It("should be passed", func() {
				resp, err := serviceResource.GetSchemaInfo(getContext(), &pb.GetSchemaRequest{
					ServiceId: serviceId,
					SchemaId:  "com.huawei.test",
				})
				Expect(err).To(BeNil())
				Expect(resp.Response.GetCode()).To(Equal(pb.ResponseSuccess))
				Expect(resp.Schema).To(Equal("get schema"))
				Expect(resp.SchemaSummary).To(Equal("schema0summary"))
			})
		})
	})

	Describe("execute 'delete' operation", func() {
		var (
			serviceId string
		)

		It("should be passed", func() {
			respCreateService, err := serviceResource.Create(getContext(), &pb.CreateServiceRequest{
				Service: &pb.MicroService{
					AppId:       "delete_schema_group",
					ServiceName: "delete_schema_service",
					Version:     "1.0.0",
					Level:       "FRONT",
					Status:      pb.MS_UP,
				},
			})
			Expect(err).To(BeNil())
			Expect(respCreateService.Response.GetCode()).To(Equal(pb.ResponseSuccess))
			serviceId = respCreateService.ServiceId

			resp, err := serviceResource.ModifySchema(getContext(), &pb.ModifySchemaRequest{
				ServiceId: serviceId,
				SchemaId:  "com.huawei.test",
				Schema:    "delete schema",
				Summary:   "summary",
			})
			Expect(err).To(BeNil())
			Expect(resp.Response.GetCode()).To(Equal(pb.ResponseSuccess))
		})

		Context("when request is invalid", func() {
			It("should be failed", func() {
				By("schema id does not exist")
				resp, err := serviceResource.DeleteSchema(getContext(), &pb.DeleteSchemaRequest{
					ServiceId: serviceId,
					SchemaId:  "noneschema",
				})
				Expect(err).To(BeNil())
				Expect(resp.Response.GetCode()).ToNot(Equal(pb.ResponseSuccess))

				By("service id is empty")
				resp, err = serviceResource.DeleteSchema(getContext(), &pb.DeleteSchemaRequest{
					ServiceId: "",
					SchemaId:  "com.huawei.test",
				})
				Expect(err).To(BeNil())
				Expect(resp.Response.GetCode()).ToNot(Equal(pb.ResponseSuccess))

				By("service id does not exist")
				resp, err = serviceResource.DeleteSchema(getContext(), &pb.DeleteSchemaRequest{
					ServiceId: "noexistservice",
					SchemaId:  "com.huawei.test",
				})
				Expect(err).To(BeNil())
				Expect(resp.Response.GetCode()).ToNot(Equal(pb.ResponseSuccess))

				By("schema id is invalid")
				resp, err = serviceResource.DeleteSchema(getContext(), &pb.DeleteSchemaRequest{
					ServiceId: serviceId,
					SchemaId:  invalidSchemaId,
				})
				Expect(err).To(BeNil())
				Expect(resp.Response.GetCode()).ToNot(Equal(pb.ResponseSuccess))
			})
		})

		Context("when request is valid", func() {
			It("should be passed", func() {
				resp, err := serviceResource.DeleteSchema(getContext(), &pb.DeleteSchemaRequest{
					ServiceId: serviceId,
					SchemaId:  "com.huawei.test",
				})
				Expect(err).To(BeNil())
				Expect(resp.Response.GetCode()).To(Equal(pb.ResponseSuccess))

				respGet, err := serviceResource.GetSchemaInfo(getContext(), &pb.GetSchemaRequest{
					ServiceId: serviceId,
					SchemaId:  "com.huawei.test",
				})
				Expect(err).To(BeNil())
				Expect(respGet.Response.GetCode()).To(Equal(pb.ErrSchemaNotExists))

				respExist, err := serviceResource.Exist(getContext(), &pb.GetExistenceRequest{
					Type:      "schema",
					ServiceId: serviceId,
					SchemaId:  "com.huawei.test",
				})
				Expect(err).To(BeNil())
				Expect(respExist.Response.GetCode()).To(Equal(pb.ErrSchemaNotExists))
			})
		})
	})
})
