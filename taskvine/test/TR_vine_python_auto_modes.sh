#!/bin/sh

set -e

. ../../dttools/test/test_runner_common.sh

import_config_val CCTOOLS_PYTHON_TEST_EXEC
import_config_val CCTOOLS_PYTHON_TEST_DIR
import_config_val CCTOOLS_OPSYS

export PYTHONPATH=$(pwd)/../../test_support/python_modules/${CCTOOLS_PYTHON_TEST_DIR}:$PYTHONPATH
export PATH=$(pwd)/../../batch_job/src:$PATH
export PATH=$(pwd)/../src/worker:$PATH

STATUS_FILE=vine.status

check_needed()
{
    # Temporarily disabling this test because it fails intermittently in github CI.
    return 1

    [ -n "${CCTOOLS_PYTHON_TEST_EXEC}" ] || return 1

    # disable on mac because the resource_monitor does not work there
    [ "${CCTOOLS_OPSYS}" = DARWIN ] && return 1

    return 0
}

prepare()
{
    rm -f $STATUS_FILE

    return 0
}

run()
{
    (${CCTOOLS_PYTHON_TEST_EXEC} vine_python_auto_modes.py; echo $? > $STATUS_FILE) &

    wait_for_file_creation $STATUS_FILE 30

    # retrieve exit status
    status=$(cat $STATUS_FILE)
    if [ $status -ne 0 ]
    then
        # display log files in case of failure.
        logfile=$(latest_vine_debug_log)
        if [ -f ${logfile}  ]
        then
            echo "manager log:"
            cat ${logfile}
        fi

        exit 1
    fi

    exit 0
}

clean()
{
    rm -f $STATUS_FILE
    rm -rf vine-run-info

    exit 0
}


dispatch "$@"
