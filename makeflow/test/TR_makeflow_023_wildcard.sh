#!/bin/sh

. ../../dttools/src/test_runner.common.sh

test_dir=`basename $0 .sh`.dir
test_output=`basename $0 .sh`.output

prepare()
{
    mkdir $test_dir
    cd $test_dir
    ln -sf ../../src/makeflow .
    ln -sf ../syntax/wildcard.makeflow .
cat > ../$test_output <<EOF
wildcard.makeflow
EOF
    exit 0
}

run()
{
    cd $test_dir
    ./makeflow -d all wildcard.makeflow
    if [ $? -eq 0 ]; then
    	exec diff -w ../$test_output makeflow.list
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
