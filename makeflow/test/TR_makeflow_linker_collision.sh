#!/bin/sh

. ../../dttools/src/test_runner.common.sh

out_dir="linker_collision_out"

prepare() {
  if [ -d "$out_dir" ]; then
    exit 1
  fi

  touch linker/ls

  cd ../src/; make
  exit $?
}

run() {
  cd linker
  ../../src/makeflow -b "$out_dir" collision.mf &> tmp

  `diff tmp expected/collision.mf`
  exit $?
}

clean() {
  cd linker
  rm -r "$out_dir"
  rm /tmp/asdf
  rm asdf
  rm tmp
  exit 0
}

dispatch $@
