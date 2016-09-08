package main

import (
	"fmt"
	"log"
	"os"

	"github.com/Sirupsen/logrus"
	"github.com/contiv/volplugin/apiserver"
	"github.com/contiv/volplugin/db"
	"github.com/contiv/volplugin/db/impl/consul"
	"github.com/contiv/volplugin/db/impl/etcd"
	"github.com/hashicorp/consul/api"

	"github.com/codegangsta/cli"
)

// version is provided by build
var (
	version  = ""
	hostname = ""
)

func init() {
	var err error
	hostname, err = os.Hostname()
	if err != nil {
		log.Fatalf("Error processing hostname syscall: %v", err)
	}
}

func start(ctx *cli.Context) {
	var cfg db.Client
	var err error

	switch ctx.String("store") {
	case "etcd":
		cfg, err = etcd.NewClient(ctx.StringSlice("store-url"), ctx.String("prefix"))
	case "consul":
		cfg, err = consul.NewClient(&api.Config{Address: ctx.StringSlice("store-url")[0]}, ctx.String("prefix"))
	default:
		log.Fatalf("Volplugin does not support store %v. Please supply a valid cluster store.", ctx.String("store"))
	}

	if err != nil {
		logrus.Fatal(err)
	}

	d := &apiserver.DaemonConfig{
		Client:   cfg,
		Hostname: hostname,
	}

	d.Daemon(ctx.String("listen"))
}

func main() {
	app := cli.NewApp()
	app.Version = version
	app.Usage = "Control many volplugins"
	app.Action = start
	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:   "listen",
			Usage:  "listen address for apiserver",
			EnvVar: "LISTEN",
			Value:  ":9005",
		},
		cli.StringFlag{
			Name:  "prefix",
			Usage: "prefix key used in etcd for namespacing",
			Value: "/volplugin",
		},
		cli.StringSliceFlag{
			Name:  "store-url",
			Usage: "URL for data store (etcd, consul etc). May be repeated to specify multiple servers.",
			Value: &cli.StringSlice{"http://localhost:2379"},
		},
		cli.StringFlag{
			Name:  "store",
			Value: "etcd",
			Usage: "[etcd | consul] select the type of data store to use",
		},
		cli.StringFlag{
			Name:   "host-label",
			Usage:  "Set the internal hostname for handling locks",
			EnvVar: "HOSTLABEL",
			Value:  hostname,
		},
	}

	if err := app.Run(os.Args); err != nil {
		fmt.Fprintf(os.Stderr, "\nError: %v\n\n", err)
		os.Exit(1)
	}
}
