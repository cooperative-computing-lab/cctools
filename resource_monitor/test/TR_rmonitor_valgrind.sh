#!/bin/sh

. ../../dttools/test/test_runner_common.sh

export VALGRIND="valgrind --error-exitcode=1 --leak-check=full"

check_needed()
{
	# do not run the test if not on linux.
	[ -d /proc ] || exit 0

	if ! ${VALGRIND} --version > /dev/null 2>&1
	then
		exit 1
	fi
}

prepare()
{
	exit 0
}

run()
{
	if ! ${VALGRIND} --log-file=rmontior.valgrind -- ../src/resource_monitor -Ovalgrind -i 1 -- sleep 5
	then
		echo "valgrind found errors with the resource_monitor"
		[ -f rmontior.valgrind ] && cat rmontior.valgrind
	fi
}

clean()
{
	rm -rf rmontior.valgrind valgrind.*
}

dispatch "$@"

# vim: set noexpandtab tabstop=4:
