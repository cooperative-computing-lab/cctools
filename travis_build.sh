#!/bin/bash

set -ex

if [ -z "$DOCKER_IMAGE" ]; then
    ./configure --strict && make && make test
else
    D=/tmp/cctools-${TRAVIS_COMMIT:0:8}-${DOCKER_IMAGE#cclnd/cctools-env:}
    docker run \
        --privileged \
        --ulimit nofile=65536 \
        -v "$(pwd):/root" \
        -v /tmp:/tmp \
        -w '/root' \
        "$DOCKER_IMAGE" \
        /bin/sh -c "./configure --strict --prefix $D --with-cvmfs-path /opt/libcvmfs --with-uuid-path /opt/uuid && make install && make test"
    tar -cz -C $(dirname $D) -f $D.tar.gz $(basename $D)
fi
