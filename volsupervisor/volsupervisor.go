package volsupervisor

import (
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/codegangsta/cli"
	"github.com/hashicorp/consul/api"
	wait "github.com/jbeda/go-wait"

	"github.com/contiv/volplugin/db"
	"github.com/contiv/volplugin/db/impl/consul"
	"github.com/contiv/volplugin/db/impl/etcd"
	"github.com/contiv/volplugin/info"
)

// DaemonConfig is the top-level configuration for the daemon. It is used by
// the cli package in volplugin/volplugin.
type DaemonConfig struct {
	Global   *db.Global
	Client   db.Client
	Hostname string
}

// Daemon is the top-level entrypoint for the volsupervisor from the CLI.
func Daemon(ctx *cli.Context) {
	var client db.Client
	var err error

	switch ctx.String("store") {
	case "etcd":
		client, err = etcd.NewClient(ctx.StringSlice("store-url"), ctx.String("prefix"))
	case "consul":
		client, err = consul.NewClient(&api.Config{Address: ctx.StringSlice("store-url")[0]}, ctx.String("prefix"))
	default:
		logrus.Fatalf("Invalid cluster store %q", ctx.String("store"))
	}

	if err != nil {
		logrus.Fatalf("Could not establish link to %q: %v", ctx.String("store"), err)
	}

retry:
	global := db.NewGlobal()
	if err := client.Get(global); err != nil {
		logrus.Errorf("Could not retrieve global configuration: %v. Retrying in 1 second", err)
		time.Sleep(time.Second)
		goto retry
	}

	dc := &DaemonConfig{Client: client, Global: global, Hostname: ctx.String("host-label")}
	dc.setDebug()

	objChan, errChan := dc.Client.Watch(global)
	go dc.watchAndSetGlobal(objChan, errChan)
	go info.HandleDebugSignal()

	uc := db.NewVolsupervisor(dc.Hostname)
	stopChan, err := dc.Client.AcquireAndRefresh(uc, dc.Global.TTL)
	if err != nil {
		logrus.Fatalf("Could not start volsupervisor: failed to acquire lock: %v", err)
	}

	sigChan := make(chan os.Signal, 1)

	go func() {
		<-sigChan
		logrus.Infof("Removing volsupervisor global lock; waiting %v for lock to clear", dc.Global.TTL)
		stopChan <- struct{}{}
		os.Exit(0)
	}()

	signal.Notify(sigChan, syscall.SIGTERM, syscall.SIGINT)

	dc.signalSnapshot()
	dc.updateVolumes()
	// doing it here ensures the goroutine is created when the first poll completes.
	go func() {
		for {
			time.Sleep(wait.Jitter(time.Second, 0))
			dc.updateVolumes()
		}
	}()

	dc.loop()
}

func (dc *DaemonConfig) watchAndSetGlobal(globalChan chan db.Entity, errChan chan error) {
	for {
		select {
		case err := <-errChan:
			logrus.Errorf("Could not receive global configuration: %v", err)
			time.Sleep(100 * time.Millisecond) // throttle
		case global := <-globalChan:
			dc.Global = global.(*db.Global)
			dc.setDebug()
		}
	}
}

func (dc *DaemonConfig) setDebug() {
	if dc.Global.Debug {
		logrus.SetLevel(logrus.DebugLevel)
		logrus.Debug("Debug logging enabled")
	} else {
		logrus.SetLevel(logrus.InfoLevel)
	}
}
