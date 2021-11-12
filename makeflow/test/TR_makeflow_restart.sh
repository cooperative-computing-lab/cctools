#!/bin/sh

. ../../dttools/test/test_runner_common.sh

test_dir=`basename $0 .sh`.dir
test_output=`basename $0 .sh`.output

prepare()
{
	mkdir $test_dir
	cd $test_dir
	ln -sf ../../src/makeflow .
	echo "hello" > file.1

cat > test.jx << EOF
{
	"rules" :
	[
		{
			"command" : format("cp file.%d file.%d",i,i+1),
			"inputs"  : [ "file."+i ],
			"outputs" : [ "file."+(i+1) ]
		} for i in range(1,10)
	]
}
EOF
	exit 0
}

run()
{
	cd $test_dir

	echo "+++++ first run: should make 10 files +++++"
	./makeflow --jx test.jx | tee output.1

	echo "+++++ deleting file.5 manually +++++"
	rm file.5

	echo "+++++ second run: should rebuild 6 files +++++"
	./makeflow --jx test.jx | tee output.2

	count=`grep "deleted file" output.2 | wc -l`

	echo "+++++ $count files deleted +++++"

	if [ $count -ne 6 ]
	then
		exit 1
	fi

	# Note: sleep to ensure different timestamp
	echo "+++++ changing file.2 manually +++++"
	sleep 2
	touch file.2

	echo "+++++ third run: should rebuild 8 files +++++"
	./makeflow --jx test.jx | tee output.3

	count=`grep "deleted" output.3 | wc -l`
	echo "+++++ $count files deleted +++++"

	if [ $count -ne 8 ]
	then
		exit 1
	fi
	
	exit 0
}

clean()
{
	rm -fr $test_dir $test_output
	exit 0
}

dispatch "$@"

# vim: set noexpandtab tabstop=4:
