#!/bin/sh

. ../../dttools/src/test_runner.common.sh

test_dir=`basename $0 .sh`.dir

prepare()
{
	mkdir $test_dir
	cd $test_dir
	CONVERT=`which convert`

	if [ -z "$CONVERT" ]; then
		if [ -f /usr/bin/convert ]; then
			CONVERT=/usr/bin/convert
		elif [ -f /usr/local/bin/convert ]; then
			CONVERT=/usr/local/bin/convert
		elif [ -f /opt/local/bin/convert ]; then
			CONVERT=/opt/local/bin/convert
		else
			echo "No convert program available, quitting!"
			return 1
		fi
	fi

# example of makeflow provided with the source
	MAKE_FILE_ORG=../../example/example.makeflow
	MAKE_FILE=test.makeflow

	sed -e "s:^CONVERT.*:CONVERT=$CONVERT:" > $MAKE_FILE < $MAKE_FILE_ORG

	ln -sf ../syntax/recursive.makeflow Makeflow
	ln -sf ../syntax/options.makeflow .
	return 0
}

run()
{
	cd $test_dir
	../../src/makeflow -dall
	return $?
}

clean()
{
	rm -fr $test_dir
	return 0
}

dispatch "$@"

# vim: set noexpandtab tabstop=4:
