#!/usr/bin/env bash

IP=`hostname --ip-address`

sed -i -e "s/^interface.*/interface = $IP/" /etc/opscenter/opscenterd.conf

/usr/share/opscenter/bin/opscenter
