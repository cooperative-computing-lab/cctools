#!/bin/bash

TESTFILE="testfile"
echo "test1" > in1
echo "test2" > in2


./makeflow/src/makeflow -d batch -T amazon --amazon-credentials-filepath "$(pwd)/amazon-credentials" -l /dev/null <(cat <<EOF
MAKEFLOW_INPUTS = in1 in2
MAKEFLOW_OUTPUTS =

$TESTFILE: in1 in2
    cat in2 > $TESTFILE && cat in1 >> $TESTFILE


EOF)

if [[ -f $TESTFILE ]];
then
    echo "File generation: passed"
    rm $TESTFILE in1 in2
else
    echo "File generation: failed"
fi
