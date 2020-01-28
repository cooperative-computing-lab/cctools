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

WITH_PERL=
if [ "$TRAVIS_OS_NAME" = osx ]
then
    WITH_PERL="--with-perl-path no"
fi

./configure --strict --prefix "$D" $DEP_ARGS $WITH_PERL
make install

if ! make test
then
    cat cctools.test.fail
    exit 1
fi


if [ -n "$DOCKER_IMAGE" ] || [ "$TRAVIS_OS_NAME" = osx ]; then
    tar -cz -C "$(dirname "$D")" -f "$D.tar.gz" "$(basename "$D")"
fi
