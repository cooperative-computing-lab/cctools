#!/bin/sh

if [ X$1 != X ]
then
	CCTOOLS_PACKAGES_TEST=$1
fi

if [ ! -r config.mk ]; then
	echo "Please run ./configure && make before executing the test script"
	exit 1
fi

if [ -z "$CCTOOLS_PACKAGES_TEST" ]
then
	CCTOOLS_PACKAGES_TEST=$(grep CCTOOLS_PACKAGES config.mk | cut -d = -f 2)
	if [ -n "${CCTOOLS_DOCKER_GITHUB}" ]
	then
		if ! parrot/src/parrot_run /bin/ls > /dev/null 2>&1
		then
			echo "Skipping parrot tests inside docker build."
			export PARROT_SKIP_TEST=yes
		fi
	fi
fi

if [ -z "$CCTOOLS_TEST_LOG" ]; then
	CCTOOLS_TEST_LOG="./cctools.test.log"
fi

#absolute path?
if [ -z "$(echo $CCTOOLS_TEST_LOG | sed -n 's:^/.*$:x:p')" ]; then
	CCTOOLS_TEST_LOG="$(pwd)/${CCTOOLS_TEST_LOG}"
fi
export CCTOOLS_TEST_LOG
export CCTOOLS_TEST_FAIL=${CCTOOLS_TEST_LOG%.log}.fail
export CCTOOLS_TEST_TMP=${CCTOOLS_TEST_LOG%.log}.tmp

echo "[$(date)] Testing on $(uname -a)." > "$CCTOOLS_TEST_LOG"
rm -f "${CCTOOLS_TEST_FAIL}"

# we need resource_monitor in the path.
PATH="$(pwd)/resource_monitor/src:$PATH"
export PATH

SUCCESS=0
FAILURE=0
SKIP=0
START_TIME=$(date +%s)
for package in ${CCTOOLS_PACKAGES_TEST}; do
    if [ ${package} != "makeflow" ]; then
        continue;
    fi
	if [ -d "${package}/test" ]; then
		cd "./${package}/test"
		for script in TR_*; do
			if [ -x "$script" ]; then
				printf "%-66s" "--- Testing ${package}/test/${script} ... "
				TEST_START_TIME=$(date +%s)
				(
					"./${script}" check_needed
				) >> "$CCTOOLS_TEST_LOG" 2>&1
				result=$?
				if [ "$result" -ne 0 ]; then
					skip=1
				else
					skip=0
					(
						echo "======== ${script} PREPARE ========"
						"./${script}" prepare
						result=$?
						if [ $result = 0 ]; then
							echo "======== ${script} RUN ========"
							"./${script}" run
							result=$?
						fi
						echo "======== ${script} CLEAN ========"
						"./${script}" clean
						exit $result
					) > "$CCTOOLS_TEST_TMP" 2>&1
					result=$?
					cat "$CCTOOLS_TEST_TMP" >> "$CCTOOLS_TEST_LOG"
					if [ "$result" -ne 0 ]; then
						cat "$CCTOOLS_TEST_TMP" >> "$CCTOOLS_TEST_FAIL"
					fi
				fi
				TEST_STOP_TIME=$(date +%s)
				TEST_ELAPSED=$(($TEST_STOP_TIME-$TEST_START_TIME))
				if [ "$skip" -eq 1 ]; then
					SKIP=$((SKIP+1))
					echo "skipped ${TEST_ELAPSED}s"
					echo "=== Test ${package}/test/${script}: skipped." >> $CCTOOLS_TEST_LOG
				elif [ "$result" -eq 0 ]; then
					SUCCESS=$((SUCCESS+1))
					echo "success ${TEST_ELAPSED}s"
					echo "=== Test ${package}/test/${script}: success." >> $CCTOOLS_TEST_LOG
				else
					FAILURE=$((FAILURE+1))
					echo "failure ${TEST_ELAPSED}s"
					echo "=== Test ${package}/test/${script}: failure." >> $CCTOOLS_TEST_LOG
				fi
			fi
            break
		done
		cd ../..
	fi
done
STOP_TIME=$(date +%s)

TOTAL=$((SUCCESS+FAILURE-SKIP))
ELAPSED=$((STOP_TIME-START_TIME))

echo ""
echo "Test Results: ${FAILURE} of ${TOTAL} tests failed (${SKIP} skipped) in ${ELAPSED} seconds."
echo ""

if [ "$FAILURE" -eq 0 ]; then
	exit 0
else
	exit 1
fi

# vim: set noexpandtab tabstop=4:
