#!/bin/sh

set -x 
export DOCKER_HOST=:2375 

volcli policy upload policy1 < /testdata/ceph/cassandra.json
docker volume create -d volplugin --name /policy1/cassandra-seed
docker run -d --name cassandra-seed -v policy1/cassandra-seed:/var/lib/cassandra cassandra:3.7
