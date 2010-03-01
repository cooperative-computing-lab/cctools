#!/bin/sh

TEST_FILE=test.$$

../chirp_server -p 9095 -E &
../chirp_benchmark localhost:9095 $TEST_FILE 2 2 2
EXIT_STATUS=$?

rm -f $TEST_FILE

exit $EXIT_STATUS
