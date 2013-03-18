#!/bin/sh

. ../../dttools/src/test_runner.common.sh

out_dir="linker_complex_out"

prepare() {
  if [ -d "$out_dir" ]; then
    exit 1
  fi
  
  touch /tmp/makeflow_test_complex_path
  mkdir -p /tmp/a/b/x
  touch /tmp/a/b/x/y
  chmod 777 /tmp/a/b/x/y
  cd ../src/; make
  exit $?
}

run() {
  cd linker
  ../../src/makeflow -b "$out_dir" complex.mf

  if [ ! -f "$out_dir"/makeflow_test_complex_path ]; then
    exit 1
  fi

  filename="$out_dir"/y
  if [ ! -f $filename ]; then
    exit 1
  fi

  case `uname -s` in
    Darwin)
        cmd=`stat $filename | awk '{print $3}'`
      ;;
    *)
        cmd=`stat -c %A $filename`
  ;;
  esac

  if [ ! "-rwxrwxrwx" == "$cmd" ]; then
    exit 1
  fi

  first_line=`head -n 1 $out_dir/complex.mf`
  if [ ! $? == 0 ]; then
    exit 1
  fi
  if [ ! "VARIABLE=\"testing\"" == "$first_line" ]; then
    exit 1
  fi

  exit 0
}

clean() {
  cd linker
  rm -r "$out_dir"
  rm -r /tmp/a
  rm -r /tmp/makeflow_test_complex_path
  exit 0
}

dispatch $@
  
