#!/bin/bash

TESTFILE="testfile"


./makeflow/src/makeflow -d batch -T amazon -l /dev/null <(cat <<EOF
MAKEFLOW_INPUTS =
MAKEFLOW_OUTPUTS =

$TESTFILE:
    echo "hi" >> $TESTFILE

EOF)

if [[ -f $TESTFILE ]];
then
    echo "File generation: passed"
    rm $TESTFILE
else
    echo "File generation: failed"
fi
