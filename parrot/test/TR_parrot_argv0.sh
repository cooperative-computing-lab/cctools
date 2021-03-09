#!/bin/sh

set -ex

. ../../dttools/test/test_runner_common.sh
. ./parrot-test.sh


prepare()
{
	echo '#!/bin/sh' > target.sh
	echo 'echo $0' >> target.sh
	chmod +x target.sh
	ln -sf target.sh source.sh
}

run()
{
	[ "$(parrot ./target.sh)" =  "$(./target.sh)" ]
	[ "$(parrot ./source.sh)" =  "$(./source.sh)" ]
}

clean()
{
	rm -f target.sh source.sh
}

dispatch "$@"

# vim: set noexpandtab tabstop=4:
