#!/bin/bash

set -ex

if [ -z "$DOCKER_IMAGE" ]; then
    ./travis_build.sh
else
    docker run \
        --privileged \
        --ulimit nofile=65536 \
        -v "$(pwd):/root" \
        -v /tmp:/tmp \
        -w /root \
        -e TRAVIS_TAG \
        -e TRAVIS_COMMIT \
        -e DOCKER_IMAGE \
        "$DOCKER_IMAGE" \
        ./travis_build.sh
fi
