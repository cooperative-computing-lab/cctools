#!/bin/bash

set -ex

if [ -z "$DOCKER_IMAGE" ]; then
    ./configure --strict && make && make test
else
    docker run \
        --privileged \
        --ulimit nofile=65536 \
        -v "$(pwd):/root" -w '/root' \
        "$DOCKER_IMAGE" \
        /bin/sh -c "./configure --strict --with-cvmfs-path /opt/libcvmfs --with-uuid-path /opt/uuid && make && make test"
fi
