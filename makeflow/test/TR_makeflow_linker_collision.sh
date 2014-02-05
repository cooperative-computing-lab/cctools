#!/bin/sh

. ../../dttools/src/test_runner.common.sh

out_dir="linker_collision_out"

prepare() {
  ln ../src/makeflow ../src/makeflow_analyze
  if [ -d "$out_dir" ]; then
    exit 1
  fi

  touch linker/ls

  cd ../src/; make
  exit $?
}

run() {
  cd linker
  ../../src/makeflow_analyze -b "$out_dir" collision.mf &> tmp
  cat tmp | awk '{print $2}' | sort > tmp2

  `diff tmp2 expected/collision.mf`
  exit $?
}

clean() {
  rm ../src/makeflow
  cd linker
  rm -r "$out_dir"
  rm -f /tmp/asdf asdf tmp tmp2 ls
  exit 0
}

dispatch $@

# vim: set noexpandtab tabstop=4:
