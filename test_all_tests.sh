#!/bin/sh

if [ ! -r config.mk ]; then
    echo "Please run ./configure && make before executing the test script" 
    exit 1
fi

CCTOOLS_PACKAGES=$(grep CCTOOLS_PACKAGES config.mk | cut -d = -f 2)
if [ -z "$CCTOOLS_TEST_LOG" ]; then
	CCTOOLS_TEST_LOG="./cctools.test.log"
fi

#absolute path?
if [ -z "$(echo $CCTOOLS_TEST_LOG | sed -n 's:^/.*$:x:p')" ]; then
	CCTOOLS_TEST_LOG="$(pwd)/${CCTOOLS_TEST_LOG}"
fi
export CCTOOLS_TEST_LOG

echo "[$(date)] Testing on $(uname -a)." > "$CCTOOLS_TEST_LOG"

SUCCESS=0
FAILURE=0
START_TIME=$(date +%s)
for package in ${CCTOOLS_PACKAGES}; do
	if [ -d "${package}/test" ]; then
		cd "${package}/test"
		for script in TR_*; do
			echo $script
		done
		cd ../..
	fi
done
	

