#!/bin/sh

. ../../dttools/test/test_runner_common.sh
. ./parrot-test.sh

file=hello.txt
link=link.txt
expected=expected.out
from_parrot=parrot.out

tmp_dir=${PWD}/parrot_temp_dir

prepare()
{
	set -e 

	echo hello > $file
	ln -sf $file $link

	return 0
}

FORMAT="%a %b %g %h %i %s %u %W %X %Y %Z"

run()
{
	set -e

	stat --format "$FORMAT" $file $link 2>/dev/null > $expected
	parrot -- stat --format "$FORMAT" $file $link > $from_parrot
	
	diff $expected $from_parrot

	if ! parrot_run --check-driver cvmfs
	then
		return 0
	fi

	# check that symlinks are correctly detected
	cvmfs_symlink=/cvmfs/cms.cern.ch/bin/scramv1
	parrot_run -t${tmp_dir} -- sh -c "[ -L ${cvmfs_symlink} ]";

	target=$(parrot -t${tmp_dir} -- realpath $cvmfs_symlink | tail -n1)
	parrot_run -t${tmp_dir}  /bin/sh -c "[ -f $target ]";

	return 0
}

clean()
{
	rm -rf $file $link $expected $from_parrot $tmp_dir
	return 0
}

dispatch "$@"

# vim: set noexpandtab tabstop=4:
