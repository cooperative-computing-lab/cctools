#!/bin/sh

. ../../dttools/src/test_runner.common.sh

function find_absolute_path() {
	TARGET_FILE=$1

	cd `dirname $TARGET_FILE`
	TARGET_FILE=`basename $TARGET_FILE`

	while [ -L "$TARGET_FILE" ]
	do
		TARGET_FILE=`readlink $TARGET_FILE`
		cd `dirname $TARGET_FILE`
		TARGET_FILE=`basename $TARGET_FILE`
	done

	PHYS_DIR=`pwd -P`
	RESULT=$PHYS_DIR/$TARGET_FILE
	echo "$RESULT"
}

PATH=$(find_absolute_path ../src/):$(find_absolute_path ../../makeflow/src/):$PATH

out_dir=makeflow_linker.001.out
expected=001
workflow_description=001.mf

prepare() {
	cd ../src; make
	exit $?
}

run() {
	../src/makeflow_linker --use-named -o $out_dir input/001/$workflow_description
	named_dependency=$(cat $out_dir/named | awk '{print $1}')
	if [ "$named_dependency" != "Python" ]; then
		exit 1
	fi
	cp expected/$expected/named $out_dir/named

	if [ ! -d "$out_dir"/a.py/b/gzip ]; then
		exit 1
	fi

	rm -rf $out_dir/a.py/b/gzip

	diff -bur expected/$expected $out_dir
	exit $?
}

clean() {
	rm -rf $out_dir
}

dispatch "$@"

