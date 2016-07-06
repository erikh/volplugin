#!/bin/sh

set -xe

docker run --net host --name apiserver \
  --privileged -it -d \
  -v /dev:/dev \
  -v /lib/modules:/lib/modules:ro \
  --volumes-from "$(hostname)" \
  -v /var/run/docker.sock:/var/run/docker.sock \
  -v /mnt:/mnt:shared \
  contiv/volplugin apiserver

sleep 1

docker exec -i apiserver volcli policy upload policy1 < systemtests/testdata/ceph/policy1.json
docker exec -i apiserver volcli global upload < systemtests/testdata/globals/global1.json

sleep 1

docker run --net host --name volplugin \
  --privileged -it -d \
  -v /dev:/dev \
  -v /lib/modules:/lib/modules:ro \
  --volumes-from "$(hostname)" \
  -v /var/run/docker.sock:/var/run/docker.sock \
  -v /run/docker/plugins:/run/docker/plugins \
  -v /mnt:/mnt:shared \
  contiv/volplugin volplugin
