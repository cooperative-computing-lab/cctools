#! /bin/sh

. ../../dttools/test/test_runner_common.sh
. ./parrot-test.sh

tmp_dir_main=${PWD}/parrot_temp_dir
tmp_dir_hitcher=${PWD}/parrot_temp_dir_hitcher

test_file=/cvmfs/atlas.cern.ch/repo/conditions/logDir/lastUpdate

prepare()
{
	$0 clean
}

run()
{
	if parrot --check-driver cvmfs
	then
		parrot -t${tmp_dir_main} -- sh -c "head $test_file > /dev/null; sleep 10" &
		pid_main=$!

		parrot -t${tmp_dir_hitcher} --cvmfs-alien-cache=${tmp_dir_main}/cvmfs -- sh -c "stat $test_file"
		status=$?

		kill $pid_main

		return $status
	else
		return 0
	fi
}

clean()
{
	if [ -n "${tmp_dir_main}" -a -d "${tmp_dir_main}" ]
	then
		rm -rf ${tmp_dir_main}
	fi

	if [ -n "${tmp_dir_hitcher}" -a -d ${tmp_dir_hitcher} ]
	then
		rm -rf ${tmp_dir_hitcher}
	fi

	return 0
}

dispatch "$@"

# vim: set noexpandtab tabstop=4:
