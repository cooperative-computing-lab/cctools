#!/bin/sh

. ../../dttools/src/test_runner.common.sh

test_dir=`basename $0 .sh`.dir
test_output=`basename $0 .sh`.output

prepare()
{
    mkdir $test_dir
    cd $test_dir
    ln -sf ../../src/makeflow .
    ln -sf ../syntax/collect.makeflow Makeflow
cat > ../$test_output <<EOF
7
8
9
11
EOF
    exit 0
}

run()
{
    cd $test_dir
    ./makeflow -g none -d all
    if [ $? -eq 0 ]; then
    	exec diff -w ../$test_output _collect.7
    else
    	exit 1
    fi
}

clean()
{
    rm -fr $test_dir $test_output
    exit 0
}

dispatch $@
