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

docker run --name "ceph-$(hostname)" -d \
  -v ${ceph_dir}:/etc/ceph --net host \
  -e MON_IP=${IP} -e CEPH_NETWORK=${SUBNET} -e CEPH_PUBLIC_NETWORK=${SUBNET} \
  ceph/demo

. bootstrap.sh
