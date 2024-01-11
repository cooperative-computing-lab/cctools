#!/bin/bash
set -ex

CCTOOLS_OUTPUT="${CCTOOLS_OUTPUT:-cctools-x86_64.tar.gz}"
D="/tmp/${CCTOOLS_OUTPUT}-dir"

rm -rf ${D}

"${GITHUB_WORKSPACE}"/packaging/build-docker/configure-from-image --prefix "${D}"

cd $(dirname "$D") && tar czf "${CCTOOLS_OUTPUT}" "$(basename $D)"
