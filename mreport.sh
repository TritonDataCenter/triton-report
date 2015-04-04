#!/bin/bash
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#

#
# Copyright (c) 2015, Joyent, Inc.
#

set -o errexit

DATACENTER=$1
ADMIN_UUID=$2

if [[ -z ${DATACENTER} || -z ${ADMIN_UUID} || -n $3 ]]; then
    echo "Usage: $0 <DATACENTER> <ADMIN_UUID>" >&2
    exit 2
fi

mjob create -o --close \
    --init '(cd /root && git clone https://github.com/joyent/triton-report.git && cd triton-report && npm install)' \
    -r "cd /root/triton-report && DATACENTER='${DATACENTER}' ADMIN_UUID='${ADMIN_UUID}' ./report.js"

