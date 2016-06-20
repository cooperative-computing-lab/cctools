#!/bin/sh

. ../../dttools/test/test_runner_common.sh

prepare()
{
cat >test.mf <<EOF
a:b
	echo hello >a
EOF

cat >test.dot.expected <<EOF
digraph {

node [shape=ellipse,color = green,style = unfilled,fixedsize = false];
N0 [label="echo"];

node [shape=box,color=blue,style=unfilled,fixedsize=false];
F1 [label = "a"];
F0 [label = "b"];

F0 -> N0;
N0 -> F1;
}
EOF
}

run()
{
	echo "creating dot viz of test.mf"
	../../makeflow/src/makeflow_viz -D dot test.mf > test.dot

	if diff test.dot test.dot.expected
	then
		echo "output matches"
		return 0
	else
		echo "output does not match"
		return 1
	fi
}

clean()
{
	rm -f test.mf test.dot test.dot.expected
}

dispatch "$@"

# vim: set noexpandtab tabstop=4:
