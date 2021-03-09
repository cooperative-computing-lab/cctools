#!/bin/bash
set -ex

BUILD_ID=$(basename "${GITHUB_REF:-${GITHUB_SHA:0:8}}")
case "${OS_NAME}" in
    osx)
    IMAGE_ID="x86_64-osx10.13"
    ;;
    *)
    IMAGE_ID=$(basename "${DOCKER_IMAGE:-${BUILD_ID}}")
    ;;
esac
D=/tmp/cctools-$BUILD_ID-${IMAGE_ID#cctools-env:}

"${GITHUB_WORKSPACE}"/packaging/scripts/configure-from-image --prefix "${D}"

tar -cz -C "$(dirname "$D")" -f "$D.tar.gz" "$(basename "$D")"
