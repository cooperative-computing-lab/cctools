#!/bin/sh

. ../../dttools/src/test_runner.common.sh

prepare()
{
    clean $@
}

run()
{
    if ../src/makeflow -d all syntax/variable_scope.makeflow; then
    	tmpfile=`mktemp`
cat > $tmpfile <<EOF
0
1
1 2
0
1
EOF
	if diff out.all $tmpfile; then
	    rm -f $tmpfile
	    exit 0;
	else
	    rm -f $tmpfile
	    exit 1;
	fi
    else
    	exit 1
    fi
}

clean()
{
    exec ../src/makeflow -d all syntax/variable_scope.makeflow -c
}

i=01

dispatch $@
