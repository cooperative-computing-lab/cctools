#!/bin/bash

set -ex

BUILD_ID="$(basename ${TRAVIS_TAG:-${TRAVIS_COMMIT:0:8}})"
D="/tmp/cctools-$BUILD_ID-${DOCKER_IMAGE#cclnd/cctools-env:}"

if [ -z "$DOCKER_IMAGE" ]; then
    ./configure --strict && make && make test
else
    docker run \
        --privileged \
        --ulimit nofile=65536 \
        -v "$(pwd):/root" \
        -v /tmp:/tmp \
        -w '/root' \
        "$DOCKER_IMAGE" \
        /bin/sh -c "./configure --strict --prefix $D --with-irods-path /opt/irods/ --with-xrootd-path /opt/xrootd/ --with-uuid-path /opt/uuid/ --with-cvmfs-path /opt/libcvmfs && make install && make test"
    tar -cz -C $(dirname $D) -f $D.tar.gz $(basename $D)
fi
