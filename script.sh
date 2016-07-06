#!/bin/sh

set -xe

(docker ps -aq | xargs docker rm -fv) || :

sudo rm -rf /etc/ceph /var/lib/ceph
sudo rmmod rbd libceph || :

count=2

for i in one two three 
do 
  docker run --net ceph -d --ip 192.168.55.${count} --name ${i} \
    -v /etc/ceph:/etc/ceph -v /var/lib/ceph:/var/lib/ceph \
    --privileged -e CEPH_DAEMON=MON -e NETWORK_AUTO_DETECT=4 -e MON_NAME=${i} ceph/daemon

  docker run --net ceph -d --ip 192.168.55.$((${count} + 1)) --name ${i}-osd \
    -v /var/lib/ceph/osd -v /etc/ceph:/etc/ceph -v /var/lib/ceph:/var/lib/ceph \
    --privileged -e CEPH_DAEMON=OSD -e OSD_TYPE=directory -e NETWORK_AUTO_DETECT=4 ceph/daemon

  count=$((${count} + 2))
done

docker run -it -d --ip 192.168.55.240 --net ceph --name etcd \
  quay.io/coreos/etcd etcd --listen-client-urls http://0.0.0.0:2379 --advertise-client-urls http://0.0.0.0:2379

docker run --net ceph --name volplugin \
  --privileged -it -d \
  -v /lib/modules:/lib/modules:ro \
  -v /etc/ceph:/etc/ceph -v /var/run/ceph:/var/run/ceph -v /var/run/docker.sock:/var/run/docker.sock \
  -v /run/docker/plugins:/run/docker/plugins \
  -v /mnt:/mnt:shared \
  contiv/volplugin volplugin --etcd http://192.168.55.240:2379

docker run --net ceph --name apiserver \
  --privileged -it -d \
  -v /lib/modules:/lib/modules:ro \
  -v /etc/ceph:/etc/ceph -v /var/run/ceph:/var/run/ceph -v /var/run/docker.sock:/var/run/docker.sock \
  -v /mnt:/mnt:shared \
  contiv/volplugin apiserver --etcd http://192.168.55.240:2379

sleep 1

docker exec -i apiserver volcli policy upload policy1 < systemtests/testdata/ceph/policy1.json
docker exec -i apiserver volcli global upload < systemtests/testdata/globals/global1.json
