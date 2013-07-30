#!/bin/sh

if [ ! -r Makefile.config ]; then
    echo "Please run ./configure && make before executing the test script" 
    exit 1
fi

CCTOOLS_PACKAGES=$(grep CCTOOLS_PACKAGES Makefile.config | cut -d = -f 2)
if [ -z "$CCTOOLS_TEST_LOG" ]; then
	CCTOOLS_TEST_LOG="./cctools.test.log"
fi

#absolute path?
if [ -z "$(echo $CCTOOLS_TEST_LOG | sed -n 's:^/.*$:x:p')" ]; then
	CCTOOLS_TEST_LOG="$(pwd)/${CCTOOLS_TEST_LOG}"
fi
export CCTOOLS_TEST_LOG

SUCCESS=0
FAILURE=0
START_TIME=$(date +%s)
for package in $CCTOOLS_PACKAGES; do
	if [ -d "${package}/test" ]; then
		cd "${package}/test"
		for script in TR_*; do
			if [ -x "$script" ]; then
				printf "%-70s" "--- Testing ${package}/test/${script} ... "
				(
					set -e
					echo "./${script}" prepare
					"./${script}" prepare
					set +e
					echo "./${script}" run
					"./${script}" run
					result=$?
					set -e
					echo "./${script}" clean
					"./${script}" clean
					exit $result
				) >> "$CCTOOLS_TEST_LOG" 2>&1
				if [ "$?" -eq 0 ]; then
					SUCCESS=$((SUCCESS+1))
					echo "success."
					echo "=== Test ${package}/test/${script}: success." >> $CCTOOLS_TEST_LOG
				else
					FAILURE=$((FAILURE+1))
					echo "failure."
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

