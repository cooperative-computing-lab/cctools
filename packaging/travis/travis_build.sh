#!/bin/bash

set -ex

BUILD_ID=$(basename "${TRAVIS_TAG:-${TRAVIS_COMMIT:0:8}}")
case "$TRAVIS_OS_NAME" in
    osx)
    IMAGE_ID="x86_64-osx10.13"
    ;;
    *)
    IMAGE_ID=$(basename "${DOCKER_IMAGE:-travis}")
    ;;
esac
D=/tmp/cctools-$BUILD_ID-${IMAGE_ID#cctools-env:}


# Find cctools src directory, so we can call the configure script
CCTOOLS_SRC="$(cd "$(dirname "${BASH_SOURCE[0]}")"/../.. && pwd)"

"${CCTOOLS_SRC}"/packaging/scripts/configure-from-image --prefix "${D}"

if [ -n "$DOCKER_IMAGE" ] || [ "$TRAVIS_OS_NAME" = osx ]; then
    tar -cz -C "$(dirname "$D")" -f "$D.tar.gz" "$(basename "$D")"
fi

