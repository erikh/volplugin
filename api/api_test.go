package api

import (
	"net/http"
	"net/http/httptest"
	"os"
	. "testing"

	"github.com/contiv/volplugin/config"

	. "gopkg.in/check.v1"
)

type mockServer http.HandlerFunc

func (m mockServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	m(w, r)
}

type apiSuite struct {
	api       *API
	server    *httptest.Server
	ServeHTTP func(w http.ResponseWriter, r *http.Request)
}

var _ = Suite(&apiSuite{})

func TestAPI(t *T) { TestingT(t) }

func (a *apiSuite) SetUpSuite(c *C) {
	hostname, err := os.Hostname()
	c.Assert(err, IsNil)

	client, err := config.NewClient("/volplugin", []string{"http://127.0.0.1:2379"})
	global := config.NewGlobalConfig()
	a.API = NewAPI(newMockVolplugin(), hostname, client, &global)
}
