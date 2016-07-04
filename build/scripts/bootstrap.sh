#!/bin/bash

set -xe

doit() {
  command="/bin/bash /opt/golang/src/github.com/contiv/volplugin/build/scripts/bootstrap-stage2.sh"
  if [ ! -z "${USE_SUPERVISOR}" ]
  then
    command="export USE_SUPERVISOR=1; ${command}"
  fi

  vagrant ssh "$1" -c "${command}"
}

USE_SUPERVISOR=1 doit mon0

for i in mon1 mon2
do
  doit $i
done
