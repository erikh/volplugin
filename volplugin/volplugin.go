package volplugin

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"path"
	"strings"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/codegangsta/cli"
	"github.com/contiv/errored"
	"github.com/contiv/volplugin/api"
	"github.com/contiv/volplugin/api/impl/docker"
	"github.com/contiv/volplugin/db"
	"github.com/contiv/volplugin/db/impl/consul"
	"github.com/contiv/volplugin/db/impl/etcd"
	"github.com/contiv/volplugin/info"
	consulapi "github.com/hashicorp/consul/api"
	"github.com/jbeda/go-wait"
)

const basePath = "/run/docker/plugins"

// DaemonConfig is the top-level configuration for the daemon. It is used by
// the cli package in volplugin/volplugin.
type DaemonConfig struct {
	Hostname   string
	Global     *db.Global
	Client     db.Client
	API        *api.API
	PluginName string
}

// NewDaemonConfig creates a DaemonConfig from the master host and hostname
// arguments.
func NewDaemonConfig(ctx *cli.Context) *DaemonConfig {
	datastore, storeURLs, prefix := ctx.String("store"), ctx.StringSlice("store-url"), ctx.String("prefix")

	var client db.Client
	var err error

retry:

	switch datastore {
	case "etcd":
		client, err = etcd.NewClient(storeURLs, prefix)
	case "consul":
		client, err = consul.NewClient(&consulapi.Config{Address: storeURLs[0]}, prefix)
	default:
		log.Fatalf("We do not support data store %q", datastore)
	}

	if err != nil {
		logrus.Warn("Could not establish client to %q: %v. Retrying.", datastore, err)
		time.Sleep(wait.Jitter(time.Second, 0))
		goto retry
	}

	dc := &DaemonConfig{
		Hostname:   ctx.String("host-label"),
		Client:     client,
		PluginName: ctx.String("plugin-name"),
	}

	if dc.PluginName == "" || strings.Contains(dc.PluginName, "/") || strings.Contains(dc.PluginName, ".") {
		logrus.Fatal("Cannot continue; socket name contains empty value or invalid characters")
	}

	return dc
}

// Daemon starts the volplugin service.
func (dc *DaemonConfig) Daemon() error {
	global := db.NewGlobal()

	if err := dc.Client.Get(global); err != nil {
		logrus.Errorf("Error fetching global configuration: %v", err)
		logrus.Infof("No global configuration. Proceeding with defaults...")
		global = db.NewGlobal()
	}

	dc.Global = global
	errored.AlwaysDebug = dc.Global.Debug
	errored.AlwaysTrace = dc.Global.Debug
	if dc.Global.Debug {
		logrus.SetLevel(logrus.DebugLevel)
	}

	go info.HandleDebugSignal()

	activity := make(chan db.Entity)
	activity, errChan := dc.Client.Watch(&db.Global{})
	go func() {
		for {
			select {
			case err := <-errChan:
				logrus.Errorf("Received error during global watch: %v", err)
			case tmp := <-activity:
				logrus.Debugf("Received global %#v", tmp)

				dc.Global = tmp.(*db.Global)

				errored.AlwaysDebug = dc.Global.Debug
				errored.AlwaysTrace = dc.Global.Debug
				if dc.Global.Debug {
					logrus.SetLevel(logrus.DebugLevel)
				}
			}
		}
	}()

	dc.API = api.NewAPI(docker.NewVolplugin(), dc.Hostname, dc.Client, &dc.Global)

	if err := dc.updateMounts(); err != nil {
		return err
	}

	go dc.pollRuntime()

	driverPath := path.Join(basePath, fmt.Sprintf("%s.sock", dc.PluginName))
	if err := os.Remove(driverPath); err != nil && !os.IsNotExist(err) {
		return err
	}
	if err := os.MkdirAll(basePath, 0700); err != nil {
		return err
	}

	l, err := net.ListenUnix("unix", &net.UnixAddr{Name: driverPath, Net: "unix"})
	if err != nil {
		return err
	}

	srv := http.Server{Handler: dc.API.Router(dc.API)}
	srv.SetKeepAlivesEnabled(false)
	if err := srv.Serve(l); err != nil {
		logrus.Fatalf("Fatal error serving volplugin: %v", err)
	}
	l.Close()
	return os.Remove(driverPath)
}
