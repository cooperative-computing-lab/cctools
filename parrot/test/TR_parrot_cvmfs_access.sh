#! /bin/sh

. ../../dttools/test/test_runner_common.sh

export PARROT_ALLOW_SWITCHING_CVMFS_REPOSITORIES="yes"
export HTTP_PROXY=http://eddie.crc.nd.edu:3128
export PARROT_CVMFS_REPO='*.cern.ch:pubkey=<BUILTIN-cern.ch.pub>,url=http://cvmfs-stratum-one.cern.ch/cvmfs/*.cern.ch'

tmp_dir=${PWD}/parrot_temp_dir

prepare()
{
	$0 clean
}

run()
{
	if ../src/parrot_run --check-driver cvmfs
	then
		../src/parrot_run -t${tmp_dir} -dcvmfs stat /cvmfs/atlas.cern.ch/repo
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
