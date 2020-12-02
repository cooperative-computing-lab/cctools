#! /bin/bash

set -ex

# Find cctools src directory
CCTOOLS_SRC="$(cd "$(dirname "${BASH_SOURCE[0]}")"/../.. && pwd)"

if [[ -z "$DOCKER_IMAGE" ]]
then
    ${CCTOOLS_SRC}/packaging/travis/travis_build.sh
else
    if [[ -n "${DOCKER_USERNAME}" ]]
    then
        echo "$DOCKER_PASSWORD" | docker login -u "$DOCKER_USERNAME" --password-stdin
    else
        echo "Using dockerhub without authentication..."
    fi

    docker run \
        --privileged \
        --ulimit nofile=65536 \
        -v "${CCTOOLS_SRC}:/root" \
        -v /tmp:/tmp \
        -w /root \
        -e TRAVIS_TAG \
        -e TRAVIS_COMMIT \
        -e DOCKER_IMAGE \
        "$DOCKER_IMAGE" \
        ./packaging/travis/travis_build.sh
fi

