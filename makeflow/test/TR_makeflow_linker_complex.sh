#!/bin/sh

. ../../dttools/src/test_runner.common.sh

out_dir="linker_complex_out"

prepare() {
  if [ -d "$out_dir" ]; then
    exit 1
  fi

  
  touch /tmp/makeflow_test_complex_path
  mkdir -p /tmp/makeflow_test_complex/a/b/x
  touch /tmp/makeflow_test_complex/a/b/x/y
  chmod 777 /tmp/makeflow_test_complex/a/b/x/y
  cd ../src/; make
  exit $?
}

run() {
  cd linker
  ../../src/makeflow -b "$out_dir" complex.mf &> tmp

  `diff tmp expected/complex.mf`
  exit $?
}

clean() {
  cd linker
  rm -r "$out_dir"
  rm -r /tmp/a
  rm -r /tmp/makeflow_test_complex_path
  rm tmp
  exit 0
}

dispatch $@
  
