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
  ../../src/makeflow -b "$out_dir" collision.mf

  files=`ls "$out_dir" | wc -l`
  if [ $files != "3" ]; then
    exit 1
  fi

  exit 0
}

clean() {
  cd linker
  rm -rf "$out_dir"
  rm -f ls
  exit 0
}

dispatch $@
