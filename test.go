package main

import (
	"fmt"

	"github.com/hashicorp/consul/api"
)

func main() {
	config := api.DefaultConfig()
	config.Address = "192.168.163.12:8500"

	client, err := api.NewClient(config)
	if err != nil {
		panic(err)
	}

	kv := client.KV()
	if _, err := kv.Put(&api.KVPair{Key: "volplugin/foo/bar", Value: []byte("fart")}, nil); err != nil {
		panic(err)
	}

	pair, _, err := kv.Get("volplugin/foo/bar", nil)
	if err != nil {
		panic(err)
	}
	fmt.Printf("KV: %v", string(pair.Value))
}
