#! /bin/sh

. ../../dttools/test/test_runner_common.sh

export PARROT_ALLOW_SWITCHING_CVMFS_REPOSITORIES="yes"
export HTTP_PROXY=http://cache01.hep.wisc.edu:3128
export PARROT_CVMFS_REPO='*.cern.ch:pubkey=<BUILTIN-cern.ch.pub>,url=http://cvmfs-stratum-one.cern.ch/opt/*'

tmp_dir_master=${PWD}/parrot_temp_dir
tmp_dir_hitcher=${PWD}/parrot_temp_dir_hitcher

test_file=/cvmfs/atlas.cern.ch/repo/conditions/logDir/lastUpdate

prepare()
{
	$0 clean
}

run()
{
	../src/parrot_run -t${tmp_dir_master} -dcvmfs sh -c "head $test_file > /dev/null; sleep 10" &
	pid_master=$!

	../src/parrot_run -t${tmp_dir_hitcher} -dcvmfs --cvmfs-alien-cache=${tmp_dir_master}/cvmfs sh -c "stat $test_file"
	status=$?

	kill $pid_master

	return $status
}

clean()
{
	if [ -n "${tmp_dir_master}" -a -d "${tmp_dir_master}" ]
	then
		rm -rf ${tmp_dir_master}
	fi

	if [ -n "${tmp_dir_hitcher}" -a -d ${tmp_dir_hitcher} ]
	then
		rm -rf ${tmp_dir_hitcher}
	fi

	return 0
}

dispatch "$@"

# vim: set noexpandtab tabstop=4:
