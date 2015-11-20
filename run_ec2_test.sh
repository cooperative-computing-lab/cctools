#!/bin/bash

echo "file 0" >> in1

./makeflow/src/makeflow -d batch -T amazon --amazon-credentials-filepath "$(pwd)/amazon-credentials" -l /dev/null <(cat <<EOF
MAKEFLOW_INPUTS = in1
MAKEFLOW_OUTPUTS =

final_file: file1 file2 file3
    cat file1 file2 file3 > final_file

file1: in1
    cat in1 > file1

file2:
    echo "file 2" > file2

file3:
    echo "file 3" > file3


EOF)

if [[ -f final_file ]]
then
    echo "File generation: passed"
    rm final_file in1 file1 file2 file3
else
    echo "File generation: failed"
fi
