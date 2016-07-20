package api

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/contiv/volplugin/config"
	"github.com/gorilla/mux"
)

type mockVolplugin struct {
	nextReturn []interface{}
}

func newMockVolplugin() Volplugin {
	return &mockVolplugin{}
}

func (m *mockVolplugin) setNextReturn(args ...interface{}) {
	m.nextReturn = args
}

func (m *mockVolplugin) Router(a *API) *mux.Router {
	var routeMap = map[string]func(http.ResponseWriter, *http.Request){
		"/create":  a.Create,
		"/remove":  a.Path, // we never actually remove through docker's interface.
		"/path":    a.Path,
		"/get":     a.Get,
		"/list":    a.List,
		"/mount":   a.Mount,
		"/unmount": a.Unmount,
	}

	router := mux.NewRouter()
	s := router.Methods("POST").Subrouter()

	for key, value := range routeMap {
		parts := strings.SplitN(key, ".", 2)
		s.HandleFunc(key, LogHandler(parts[1], true, value))
	}

	s.HandleFunc("{action:.*}", Action)

	return router
}

func (m *mockVolplugin) HTTPError(w http.ResponseWriter, err error) {
	RESTHTTPError(w, err)
}

func (m *mockVolplugin) ReadCreate(r *http.Request) (*config.VolumeRequest, error) {
	if len(m.nextReturn) != 2 {
		panic(fmt.Sprintf("Invalid args to ReadCreate: %v", m.nextReturn))
	}

	return m.nextReturn[0].(*config.VolumeRequest), m.nextReturn[1].(error)
}

func (m *mockVolplugin) WriteCreate(*config.Volume, http.ResponseWriter) error {
	if len(m.nextReturn) != 1 {
		panic(fmt.Sprintf("Invalid args to WriteCreate: %v", m.nextReturn))
	}

	return m.nextReturn[0].(error)
}

func (m *mockVolplugin) ReadGet(*http.Request) (string, error) {
	if len(m.nextReturn) != 2 {
		panic(fmt.Sprintf("Invalid args to ReadGet: %v", m.nextReturn))
	}

	return m.nextReturn[0].(string), m.nextReturn[1].(error)
}
func (m *mockVolplugin) WriteGet(string, string, http.ResponseWriter) error {
	if len(m.nextReturn) != 1 {
		panic(fmt.Sprintf("Invalid args to WriteGet: %v", m.nextReturn))
	}

	return m.nextReturn[0].(error)
}

func (m *mockVolplugin) ReadPath(*http.Request) (string, error) {
	if len(m.nextReturn) != 2 {
		panic(fmt.Sprintf("Invalid args to ReadPath: %v", m.nextReturn))
	}

	return m.nextReturn[0].(string), m.nextReturn[1].(error)
}

func (m *mockVolplugin) WritePath(string, http.ResponseWriter) error {
	if len(m.nextReturn) != 1 {
		panic(fmt.Sprintf("Invalid args to WritePath: %v", m.nextReturn))
	}

	return m.nextReturn[0].(error)
}

func (m *mockVolplugin) WriteList([]string, http.ResponseWriter) error {
	if len(m.nextReturn) != 1 {
		panic(fmt.Sprintf("Invalid args to WriteList: %v", m.nextReturn))
	}

	return m.nextReturn[0].(error)
}

func (m *mockVolplugin) ReadMount(*http.Request) (*Volume, error) {
	if len(m.nextReturn) != 2 {
		panic(fmt.Sprintf("Invalid args to ReadMount: %v", m.nextReturn))
	}

	return m.nextReturn[0].(*Volume), m.nextReturn[1].(error)
}

func (m *mockVolplugin) WriteMount(string, http.ResponseWriter) error {
	if len(m.nextReturn) != 1 {
		panic(fmt.Sprintf("Invalid args to WriteMount: %v", m.nextReturn))
	}

	return m.nextReturn[0].(error)
}
