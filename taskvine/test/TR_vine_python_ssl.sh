#!/bin/sh
set -e

. ../../dttools/test/test_runner_common.sh

import_config_val CCTOOLS_PYTHON_TEST_EXEC
import_config_val CCTOOLS_PYTHON_TEST_DIR
import_config_val CCTOOLS_OPENSSL_AVAILABLE

export PYTHONPATH=$(pwd)/../../test_support/python_modules/${CCTOOLS_PYTHON_TEST_DIR}:$PYTHONPATH

STATUS_FILE=vine.status
PORT_FILE=vine.port

KEY_FILE=key.pem
CERT_FILE=cert.pem

check_needed()
{
	[ -n "${CCTOOLS_PYTHON_TEST_EXEC}" ] || return 1
	[ "${CCTOOLS_OPENSSL_AVAILABLE}" = yes ] || return 1
}

prepare()
{
	rm -f $STATUS_FILE
	rm -f $PORT_FILE

	rm -rf input.file
	rm -rf output.file
	rm -rf executable.file

	/bin/echo hello world > input.file
	/bin/cp /bin/echo executable.file

	mkdir -p testdir
	cp input.file executable.file testdir

	# Make a small tarball for testing url downloads.
	tar czf dummy.tar.gz TR*.sh

	subj="/C=US/ST=Indiana/L=South Bend/O=University of Notre Dame/OU=CCL-CSE/CN=ccl.cse.nd.edu"

	openssl req -x509 -newkey rsa:4096 -keyout ${KEY_FILE} -out ${CERT_FILE} -sha256 -days 1 -nodes -subj "${subj}"

	return 0
}

run()
{
	# send command to the background, saving its exit status.
	( ${CCTOOLS_PYTHON_TEST_EXEC} vine_python.py $PORT_FILE --ssl_key ${KEY_FILE} --ssl_cert ${CERT_FILE}; echo $? > $STATUS_FILE ) &

	# wait at most 15 seconds for the command to find a port.
	wait_for_file_creation $PORT_FILE 15

	run_taskvine_worker $PORT_FILE worker.log --ssl

	# wait for command to exit.
	wait_for_file_creation $STATUS_FILE 15

	# retrieve taskvine exit status
	status=$(cat $STATUS_FILE)
	if [ $status -ne 0 ]
	then
		exit 1
	fi

	exit 0
}

clean()
{
	rm -f $STATUS_FILE
	rm -f $PORT_FILE

	rm -f $KEY_FILE
	rm -f $CERT_FILE

	rm -rf input.file
	rm -rf output.file
	rm -rf executable.file
	rm -rf testdir
	rm -rf dummy.tar.gz

	rm -rf vine-run-info

	exit 0
}


dispatch "$@"

# vim: set noexpandtab tabstop=4:
