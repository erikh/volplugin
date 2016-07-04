#!/bin/sh

set -xe

if [ ! -n "${SUBNET}" ] || [ ! -n "${IP}" ]
then
  echo 1>&2 "Please supply IP and SUBNET in the environment corresponding to your network parameters."
  exit 1
fi

docker rm -f apiserver volplugin "ceph-$(hostname)" etcd || :

ceph_dir=$(mktemp -d /tmp/volplugin.XXXXXX)

docker run -d --name etcd --net host quay.io/coreos/etcd

docker run --name "ceph-$(hostname)" -d -v ${ceph_dir}:/etc/ceph --net host -e MON_IP=${IP} -e CEPH_NETWORK=${SUBNET} -e CEPH_PUBLIC_NETWORK=${SUBNET} ceph/demo
docker run --net host --name apiserver \
  --privileged -it -d \
  -v /dev:/dev \
  -v /lib/modules:/lib/modules:ro \
  -v ${ceph_dir}:/etc/ceph \
  -v /var/run/docker.sock:/var/run/docker.sock \
  -v /mnt:/mnt:shared \
  contiv/volplugin apiserver

sleep 1

docker exec -i apiserver volcli policy upload policy1 < systemtests/testdata/ceph/policy1.json || \
  docker exec -i apiserver volcli policy upload policy1 < policy.json

docker exec -i apiserver volcli global upload < systemtests/testdata/globals/global1.json || \
  docker exec -i apiserver volcli global upload < global.json

sleep 1

docker run --net host --name volplugin \
  --privileged -it -d \
  -v /dev:/dev \
  -v /lib/modules:/lib/modules:ro \
  -v ${ceph_dir}:/etc/ceph \
  -v /var/run/docker.sock:/var/run/docker.sock \
  -v /run/docker/plugins:/run/docker/plugins \
  -v /mnt:/mnt:shared \
  contiv/volplugin volplugin

