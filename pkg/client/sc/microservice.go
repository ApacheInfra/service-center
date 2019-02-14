package sc

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"

	"github.com/apache/servicecomb-service-center/server/core"
	pb "github.com/apache/servicecomb-service-center/server/core/proto"
	scerr "github.com/apache/servicecomb-service-center/server/error"
)

const (
	apiExistenceURL     = "/v4/%s/registry/existence"
	apiMicroServicesURL = "/v4/%s/registry/microservices"
	apiMicroServiceURL  = "/v4/%s/registry/microservices/%s"

	MicroServiceType existenceType = "microservice"
	SchemaType       existenceType = "schema"
)

type existenceType string

func (c *SCClient) CreateService(ctx context.Context, domainProject string, service *pb.MicroService) (string, *scerr.Error) {
	domain, project := core.FromDomainProject(domainProject)
	headers := c.CommonHeaders(ctx)
	headers.Set("X-Domain-Name", domain)

	reqBody, err := json.Marshal(&pb.CreateServiceRequest{Service: service})
	if err != nil {
		return "", scerr.NewError(scerr.ErrInternal, err.Error())
	}

	resp, err := c.RestDoWithContext(ctx, http.MethodPost,
		fmt.Sprintf(apiMicroServicesURL, project),
		headers, reqBody)
	if err != nil {
		return "", scerr.NewError(scerr.ErrInternal, err.Error())
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return "", scerr.NewError(scerr.ErrInternal, err.Error())
	}

	if resp.StatusCode != http.StatusOK {
		return "", c.toError(body)
	}

	serviceResp := &pb.CreateServiceResponse{}
	err = json.Unmarshal(body, serviceResp)
	if err != nil {
		return "", scerr.NewError(scerr.ErrInternal, err.Error())
	}
	return serviceResp.ServiceId, nil
}

func (c *SCClient) DeleteService(ctx context.Context, domainProject, serviceId string) *scerr.Error {
	domain, project := core.FromDomainProject(domainProject)
	headers := c.CommonHeaders(ctx)
	headers.Set("X-Domain-Name", domain)

	resp, err := c.RestDoWithContext(ctx, http.MethodDelete,
		fmt.Sprintf(apiMicroServiceURL, project, serviceId),
		headers, nil)
	if err != nil {
		return scerr.NewError(scerr.ErrInternal, err.Error())
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return scerr.NewError(scerr.ErrInternal, err.Error())
	}

	if resp.StatusCode != http.StatusOK {
		return c.toError(body)
	}

	return nil
}

func (c *SCClient) ServiceExistence(ctx context.Context, domainProject string, env existenceType, appId, serviceName, versionRule string) (string, *scerr.Error) {
	domain, project := core.FromDomainProject(domainProject)
	headers := c.CommonHeaders(ctx)
	headers.Set("X-Domain-Name", domain)

	query := url.Values{}
	query.Set("type", string(env))
	query.Set("appId", appId)
	query.Set("serviceName", serviceName)
	query.Set("version", versionRule)

	resp, err := c.RestDoWithContext(ctx, http.MethodGet,
		fmt.Sprintf(apiExistenceURL, project)+"?"+c.parseQuery(ctx)+"&"+query.Encode(),
		headers, nil)
	if err != nil {
		return "", scerr.NewError(scerr.ErrInternal, err.Error())
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return "", scerr.NewError(scerr.ErrInternal, err.Error())
	}

	if resp.StatusCode != http.StatusOK {
		return "", c.toError(body)
	}

	existenceResp := &pb.GetExistenceResponse{}
	err = json.Unmarshal(body, existenceResp)
	if err != nil {
		return "", scerr.NewError(scerr.ErrInternal, err.Error())
	}

	return existenceResp.ServiceId, nil
}
