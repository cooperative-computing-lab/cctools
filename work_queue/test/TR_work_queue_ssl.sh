#!/bin/sh
set -e

. ../../dttools/test/test_runner_common.sh

python=${CCTOOLS_PYTHON_TEST_EXEC}
python_dir=${CCTOOLS_PYTHON_TEST_DIR}

STATUS_FILE=wq.status
PORT_FILE=wq.port

KEY_FILE=key.pem
CERT_FILE=cert.pem

check_needed()
{
	[ -n "${python}" ] || return 1
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

	subj="/C=US/ST=Indiana/L=South Bend/O=University of Notre Dame/OU=CCL-CSE/CN=ccl.cse.nd.edu"

	openssl req -x509 -newkey rsa:4096 -keyout ${KEY_FILE} -out ${CERT_FILE} -sha256 -days 1 -nodes -subj "${subj}"

	return 0
}

run()
{
	# send command to the background, saving its exit status.
	(PYTHONPATH=$(pwd)/../src/bindings/${python_dir} ${python} wq_test.py $PORT_FILE --ssl_key ${KEY_FILE} --ssl_cert ${CERT_FILE}; echo $? > $STATUS_FILE) &

	# wait at most 15 seconds for the command to find a port.
	wait_for_file_creation $PORT_FILE 15

	run_local_worker $PORT_FILE worker.log --ssl

	# wait for command to exit.
	wait_for_file_creation $STATUS_FILE 15

	# retrieve makeflow exit status
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

	exit 0
}


dispatch "$@"

# vim: set noexpandtab tabstop=4:
