#!/bin/sh

. ../../dttools/test/test_runner_common.sh

# Evaluate whether Makeflow is invoking sub-workflows correctly.
# The top level workflow nested.jx should invoke the sub-workflow
# nested2.jx 10 times.

export PATH=`pwd`/../../makeflow/src:$PATH

prepare()
{
cat > nested.jx << EOF
{
	"rules": [
		{
			"workflow" : "nested2.jx",
 			"args" : { "nested_var": n },
			"outputs" : [ "output." + n ]
        } for n in range(10)
	]
}
EOF

cat > nested2.jx << EOF
{
	"rules": [
 		{
            "command":format("echo %d > output.%d", nested_var, nested_var),
   			"outputs" : [ "output."+nested_var ]
   		}
	]
}
EOF

cat > nested.expected << EOF
9
EOF
}

run()
{
	../src/makeflow --jx nested.jx
	require_identical_files nested.expected output.9
	../src/makeflow --jx nested.jx --clean
	if [ -f output.9 ]
	then
		echo "ERROR: output.9 should have been deleted on clean!"
		exit 1
	else
		echo "output.9 deleted as expected"
		exit 0
	fi

}

clean()
{
	rm nested.jx nested2.jx nested.expected output.*
    exit 0
}

dispatch "$@"

