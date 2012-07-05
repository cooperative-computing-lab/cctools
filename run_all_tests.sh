#!/bin/sh

if [ ! -r Makefile.config ]; then
    echo "Please run ./configure && make before executing the test script" 
    exit 1
fi

#CCTOOLS_PACKAGES="`grep CCTOOLS_PACKAGES Makefile.config | cut -d = -f 2`"
CCTOOLS_PACKAGES="sand makeflow"
CCTOOLS_TEST_LOG=`pwd`/cctools.test.log
CCTOOLS_TEST_RESULTS=`pwd`/cctools.test.results

export CCTOOLS_TEST_LOG

rm -f $CCTOOLS_TEST_LOG $CCTOOLS_TEST_RESULTS > /dev/null 2>&1

success=1
start_time=`date +%s`

echo "Running all tests ..."
for p in ${CCTOOLS_PACKAGES}; do 
    echo -n "    Running $p tests ... "
    cd $p > /dev/null
    if make -s test >> $CCTOOLS_TEST_RESULTS 2> /dev/null; then
    	echo ok
    else
    	echo fail
    	success=0
    fi
    cd .. > /dev/null
done
stop_time=`date +%s`

nfail=`grep '... fail' $CCTOOLS_TEST_RESULTS | wc -l`
ntotal=`grep testing $CCTOOLS_TEST_RESULTS | wc -l`

echo ""
echo "Results:"
echo "    Total running time: $(($stop_time - $start_time)) seconds"
echo "    $nfail of $ntotal tests failed:"
for t in `grep '... fail' $CCTOOLS_TEST_RESULTS | awk '{print $2}'`; do
    echo "         $t"
done

echo ""
echo "Test logs are stored in $CCTOOLS_TEST_LOG"
echo "Test results are stored in $CCTOOLS_TEST_RESULTS"

exit $success
