#!/bin/sh

if [ ! -r config.mk ]; then
    echo "Please run ./configure && make before executing the test script" 
    exit 1
fi

if [ -z "$CCTOOLS_PACKAGES_TEST" ]
then
	CCTOOLS_PACKAGES_TEST=$(grep CCTOOLS_PACKAGES config.mk | cut -d = -f 2)
fi

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
for package in ${CCTOOLS_PACKAGES_TEST}; do
	if [ -d "${package}/test" ]; then
		cd "./${package}/test"
		for script in TR_*; do
			if [ -x "$script" ]; then
				printf "%-66s" "--- Testing ${package}/test/${script} ... "
				TEST_START_TIME=$(date +%s)
				(
					echo "======== ${script} PREPARE ========"	
					"./${script}" prepare
					echo "======== ${script} RUN ========"	
					"./${script}" run
					result=$?
					echo "======== ${script} CLEAN ========"	
					"./${script}" clean
					exit $result
				) >> "$CCTOOLS_TEST_LOG" 2>&1
				result=$?
				TEST_STOP_TIME=$(date +%s)
				TEST_ELAPSED=$(($TEST_STOP_TIME-$TEST_START_TIME))
				if [ "$result" -eq 0 ]; then
					SUCCESS=$((SUCCESS+1))
					echo "success ${TEST_ELAPSED}s"
					echo "=== Test ${package}/test/${script}: success." >> $CCTOOLS_TEST_LOG
				else
					FAILURE=$((FAILURE+1))
					echo "failure ${TEST_ELAPSED}s"
					echo "=== Test ${package}/test/${script}: failure." >> $CCTOOLS_TEST_LOG
				fi
			fi
		done
		cd ../..
	fi
done
STOP_TIME=$(date +%s)

TOTAL=$((SUCCESS+FAILURE))
ELAPSED=$((STOP_TIME-START_TIME))

echo ""
echo "Test Results: ${FAILURE} of ${TOTAL} tests failed in ${ELAPSED} seconds."
echo ""


# vim: set noexpandtab tabstop=4:
