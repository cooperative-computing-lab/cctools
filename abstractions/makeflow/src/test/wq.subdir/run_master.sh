#!/bin/bash

./makeflow -c subdir.makeflow
rm makeflow
cp ~ccl/work/lyu2/cctools/abstractions/makeflow/src/makeflow .

./makeflow -d all -T wq -p 9091 subdir.makeflow
