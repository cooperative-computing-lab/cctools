#!/bin/bash

rm worker
cp ~ccl/work/lyu2/cctools/dttools/src/worker .

./worker -d all localhost 9091
