package v1_test

import (
	"bytes"
	"encoding/json"
	"github.com/apache/servicecomb-service-center/pkg/log"
	"github.com/apache/servicecomb-service-center/pkg/model"
	"github.com/apache/servicecomb-service-center/pkg/rest"
	"github.com/apache/servicecomb-service-center/server/resource/v1"
	"github.com/apache/servicecomb-service-center/server/service/gov"
	"github.com/go-chassis/go-archaius"
	"github.com/stretchr/testify/assert"
	"net/http"
	"net/http/httptest"
	"testing"

	_ "github.com/apache/servicecomb-service-center/server/service/gov/mock"
)

func init() {
	err := gov.Init()
	if err != nil {
		log.Fatal("", err)
	}
}
func TestAuthResource_Login(t *testing.T) {
	err := archaius.Init(archaius.WithMemorySource(), archaius.WithENVSource())
	assert.NoError(t, err)

	gov.Init()
	rest.RegisterServant(&v1.Governance{})

	t.Run("create policy", func(t *testing.T) {
		b, _ := json.Marshal(&model.LoadBalancer{
			GovernancePolicy: &model.GovernancePolicy{Name: "test"},
			Spec: &model.LBSpec{
				Bo: &model.BackOffPolicy{InitialInterval: 1}}})

		r, _ := http.NewRequest(http.MethodPost, "/v1/default/gov/loadBalancer", bytes.NewBuffer(b))
		w := httptest.NewRecorder()
		rest.GetRouter().ServeHTTP(w, r)
		assert.Equal(t, http.StatusOK, w.Code)
	})

}
