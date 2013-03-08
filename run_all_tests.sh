#!/bin/sh

if [ ! -r Makefile.config ]; then
    echo "Please run ./configure && make before executing the test script" 
    exit 1
fi

CCTOOLS_PACKAGES="`grep CCTOOLS_PACKAGES Makefile.config | cut -d = -f 2`"
CCTOOLS_TEST_LOG=`pwd`/cctools.test.log
CCTOOLS_TEST_RESULTS=`pwd`/cctools.test.results

export CCTOOLS_TEST_LOG

rm -f $CCTOOLS_TEST_LOG $CCTOOLS_TEST_RESULTS > /dev/null 2>&1

nsuccess=0
nfail=0

start_time=`date +%s`

for p in ${CCTOOLS_PACKAGES}; do 
	if [ -d ${p}/test ]
	then
		cd ${p}/test
		for t in `find . -name TR\*.sh | sort`
		do
			if  ../../dttools/src/test_runner.sh $t
			then
				((nsuccess++))
			else
				((nfail++))
			fi
		done
		cd ../..
	fi
done
stop_time=`date +%s`

((ntotal=$nsuccess+$nfail))

((elapsed=$stop_time-$start_time))

echo ""
echo "Test Results: $nfail of $ntotal tests failed in $elapsed seconds."
echo ""

exit $nfail
