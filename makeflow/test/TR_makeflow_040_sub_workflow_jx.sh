#!/bin/sh

. ../../dttools/test/test_runner_common.sh

# Evaluate whether Makeflow is invoking sub-workflows correctly.
# The top level workflow nested.jx should invoke the sub-workflow
# nested2.jx with an arguments array such that it creates the
# output file output.10.  Then, a clean operation should be 
# propagated to the sub-workflow.

# makeflow must be in the path for the sub-workflow to work:
export PATH=`pwd`/../../makeflow/src:$PATH

prepare()
{
cat > nested.jx << EOF
{
	"rules": [
		{
			"workflow" : "nested2.jx",
 			"args" : { "a":5, "b":"hello", "n":10 },
			"outputs" : [ "output.10" ]
   		}
	]
}
EOF

cat > nested2.jx << EOF
{
	"rules": [
 		{
 			"command":"echo " + a + " " + b + " > output."+n,
   			"outputs" : [ "output."+n ]
   		}
	]
}
EOF

cat > nested.expected << EOF
5 hello
EOF
}

run()
{
	../src/makeflow --jx nested.jx --sandbox
	require_identical_files nested.expected output.10
	../src/makeflow --jx nested.jx --sandbox --clean
	if [ -f output.10 ]
	then
		echo "ERROR: output.10 should have been deleted on clean!"
		exit 1
	else
		echo "output.10 deleted as expected"
		exit 0
	fi

}

clean()
{
	rm nested.jx nested2.jx nested.expected output.10
    exit 0
}

dispatch "$@"

