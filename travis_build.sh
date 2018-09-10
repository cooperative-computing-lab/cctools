#!/bin/bash

set -ex

COMMAND="./configure --strict && make && make test"

if [ -z "$DOCKER_IMAGE" ]; then
    eval "$COMMAND"
else
    docker run \
        --privileged \
        --ulimit nofile=65536 \
        -v "$(pwd):/root" -w '/root' \
        "$DOCKER_IMAGE" \
        /bin/sh -c "$COMMAND"
fi
