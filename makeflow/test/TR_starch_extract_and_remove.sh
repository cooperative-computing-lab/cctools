#!/bin/sh

. ../../dttools/test/test_runner_common.sh

sfxfile=extract_and_remove.sfx
tarfile=starch.tar.gz

prepare()
{
	(cd .. && tar czvf $tarfile src) && mv ../$tarfile .
	exit 0
}

run()
{
	../src/starch -v -x tar -x rm -c 'tar_test() { for f in "$@"; do if ! tar xvf $f; then exit 1; fi ; done; }; tar_test' $sfxfile
	exec ./$sfxfile $tarfile
}

clean()
{
	rm -rf src
	rm -f $sfxfile $tarfile
	rm -rf $(basename $tarfile .tar.gz)
	exit 0
}

dispatch "$@"

# vim: set noexpandtab tabstop=4:
