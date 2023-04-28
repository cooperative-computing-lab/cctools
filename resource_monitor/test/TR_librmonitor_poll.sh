#!/bin/sh
#
TEST_INPUT=R

. ../../dttools/test/test_runner_common.sh

import_config_val CCTOOLS_OPSYS

check_needed()
{
	[ "${CCTOOLS_OPSYS}" = LINUX ] || return 1

	return 0
}

prepare()
{
	exit 0
}

run()
{

	# do not run the test if not on linux.
	[ -d /proc ] || exit 0


	answer=2
	output=$(../src/rmonitor_poll_example)
	result=$(echo $output | awk -F: 'BEGIN{RS=","} /wall time/ {print $2}')
	result=$(echo $result | sed 's/\..*$//p')

	echo $output

	if [ $result = $answer ]
	then
		exit 0
	else
		exit 1
	fi
}

clean()
{
	exit 0
}

dispatch "$@"

# vim: set noexpandtab tabstop=4:

# vim: set noexpandtab tabstop=4:
