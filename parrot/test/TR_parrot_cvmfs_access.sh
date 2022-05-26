#! /bin/sh

. ../../dttools/test/test_runner_common.sh
. ./parrot-test.sh

tmp_dir=${PWD}/parrot_temp_dir

prepare()
{
	$0 clean
}

run()
{
	if parrot --check-driver cvmfs
	then
		parrot -t${tmp_dir} -- stat /cvmfs/atlas.cern.ch/repo
		return $?
	else
		return 0
	fi
}

clean()
{
	if [ -n "${tmp_dir}" -a -d "${tmp_dir}" ]
	then
		rm -rf ${tmp_dir}
	else
		return 0
	fi
}

dispatch "$@"

# vim: set noexpandtab tabstop=4:
