#!/bin/sh

. ../../dttools/src/test_runner.common.sh

test_dir=`basename $0 .sh`.dir
test_output=`basename $0 .sh`.output

prepare()
{
    mkdir $test_dir
    cd $test_dir
    ln -sf ../syntax/export.makeflow Makeflow
cat > ../$test_output <<EOF

1
2
$
$
EOF
    exit 0
}

run()
{
    cd $test_dir
    ../../src/makeflow -dall
    if [ $? -eq 0 ]; then
    	exec diff ../$test_output out.all
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
