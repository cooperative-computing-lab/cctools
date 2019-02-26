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

DEPS_DIR=/opt/vc3/cctools-deps
DEPS=$(/bin/ls "$DEPS_DIR" || true)
DEP_ARGS=""
for dep in $DEPS; do
    DEP_ARGS="$DEP_ARGS --with-$dep-path $DEPS_DIR/$dep"
done

./configure --strict --prefix "$D" $DEP_ARGS
make install
make test

if [ -n "$DOCKER_IMAGE" ] || [ "$TRAVIS_OS_NAME" = osx ]; then
    tar -cz -C "$(dirname "$D")" -f "$D.tar.gz" "$(basename "$D")"
fi
