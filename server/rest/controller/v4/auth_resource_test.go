package v4_test

import (
	"bytes"
	"context"
	"encoding/json"
	"github.com/apache/servicecomb-service-center/pkg/rbacframe"
	"github.com/apache/servicecomb-service-center/pkg/rest"
	v4 "github.com/apache/servicecomb-service-center/server/rest/controller/v4"
	"github.com/apache/servicecomb-service-center/server/service/rbac"
	"github.com/apache/servicecomb-service-center/server/service/rbac/dao"
	"github.com/astaxie/beego"
	"github.com/go-chassis/go-archaius"
	"github.com/go-chassis/go-chassis/security/secret"
	"github.com/stretchr/testify/assert"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"

	_ "github.com/apache/servicecomb-service-center/test"
)

func init() {
	beego.AppConfig.Set("rbac_enabled", "true")
	beego.AppConfig.Set(rbac.PubFilePath, "./rbac.pub")
	beego.AppConfig.Set("rbac_rsa_private_key_file", "./private.key")
}
func TestAuthResource_Login(t *testing.T) {
	err := archaius.Init(archaius.WithMemorySource(), archaius.WithENVSource())
	assert.NoError(t, err)

	pri, pub, err := secret.GenRSAKeyPair(4096)
	assert.NoError(t, err)

	b, err := secret.RSAPrivate2Bytes(pri)
	assert.NoError(t, err)
	ioutil.WriteFile("./private.key", b, 0600)
	b, err = secret.RSAPublicKey2Bytes(pub)
	err = ioutil.WriteFile("./rbac.pub", b, 0600)
	assert.NoError(t, err)

	archaius.Set(rbac.InitPassword, "Complicated_password1")

	ctx := context.TODO()
	dao.DeleteAccount(ctx, "root")
	archaius.Init(archaius.WithMemorySource())

	rbac.Init()
	rest.RegisterServant(&v4.AuthResource{})

	dao.DeleteAccount(ctx, "dev_account")

	t.Run("invalid user login", func(t *testing.T) {
		b, _ := json.Marshal(&rbacframe.Account{Name: "dev_account", Password: "Complicated_password1"})

		r, _ := http.NewRequest(http.MethodPost, "/v4/token", bytes.NewBuffer(b))
		w := httptest.NewRecorder()
		rest.GetRouter().ServeHTTP(w, r)
		assert.NotEqual(t, http.StatusOK, w.Code)
	})
	err = dao.CreateAccount(ctx, &rbacframe.Account{Name: "dev_account",
		Password: "Complicated_password1",
		Role:     "developer"})
	assert.NoError(t, err)

	t.Run("root login", func(t *testing.T) {
		b, _ := json.Marshal(&rbacframe.Account{Name: "root", Password: "Complicated_password1"})

		r, _ := http.NewRequest(http.MethodPost, "/v4/token", bytes.NewBuffer(b))
		w := httptest.NewRecorder()
		rest.GetRouter().ServeHTTP(w, r)
		assert.Equal(t, http.StatusOK, w.Code)
	})
	t.Run("invalid password", func(t *testing.T) {
		b, _ := json.Marshal(&rbacframe.Account{Name: "root", Password: "Complicated_password"})

		r, _ := http.NewRequest(http.MethodPost, "/v4/token", bytes.NewBuffer(b))
		w := httptest.NewRecorder()
		rest.GetRouter().ServeHTTP(w, r)
		assert.Equal(t, http.StatusUnauthorized, w.Code)
	})
	t.Run("dev_account login and change pwd", func(t *testing.T) {
		b, _ := json.Marshal(&rbacframe.Account{Name: "dev_account", Password: "Complicated_password1"})

		r, _ := http.NewRequest(http.MethodPost, "/v4/token", bytes.NewBuffer(b))
		w := httptest.NewRecorder()
		rest.GetRouter().ServeHTTP(w, r)
		assert.Equal(t, http.StatusOK, w.Code)
		jsonbody := w.Body.Bytes()
		to := &rbacframe.Token{}
		json.Unmarshal(jsonbody, to)

	})

}
