#!/bin/sh

. ../../dttools/src/test_runner.common.sh

prepare()
{
    cd ../src/; make
    exit 0
}

run()
{
    exec ./test_example.sh
}

clean()
{
    rm -rf banded.log test_20.cfa test_20.cand.output test_20.sw.ovl test_20.banded.ovl test_20.cand repeats.meryl rect.bcand rect001.cfa rect005.cfa  sand_filter_mer_seq sand_sw_alignment sand_banded_alignment filter.log sw.log worker.log
    exit 0
}

dispatch $@
