#!/bin/sh

if [ -z "$1" ]
then
  echo 1>&2 "Please enter a number as an argument. Strongly recommended to not increase this value above 3 on our demo cluster."
  exit 1
fi

set -x
export DOCKER_HOST=:2375 

volcli volume snapshot take policy1/cassandra-seed
snapid=$(volcli volume snapshot list policy1/cassandra-seed | head -1)

for i in $(seq 0 $1)
do
  volcli volume snapshot copy policy1/cassandra-seed "$snapid" cassandra-db$i
  docker volume create -d volplugin --name /policy1/cassandra-seed
  docker run -d --name cassandra-db$i -e CASSANDRA_SEEDS="$(docker inspect --format='{{ .NetworkSettings.IPAddress }}' cassandra-seed)" -v /policy1/cassandra-db${i}:/var/lib/cassandra cassandra:3.7
done
