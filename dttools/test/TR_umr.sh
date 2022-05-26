#!/bin/sh

. ../../dttools/test/test_runner_common.sh

exe="$0.test"

prepare()
{
	${CC} -I../src/ -g $CCTOOLS_TEST_CCFLAGS -o "$exe" -x c - -x none ../src/libdttools.a -lm <<EOF
#include "create_dir.h"
#include "debug.h"
#include "unlink_recursive.h"

#include <errno.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define check(cmp,expr) \
	do {\
		int rc = (expr);\
		if (!(cmp rc))\
			fatal("[%s:%d]: unexpected failure: %s -> %d '%s'", __FILE__, __LINE__, #cmp " " #expr, rc, strerror(errno));\
	} while (0)

int main (int argc, char *argv[])
{
	debug_config(argv[0]);
	debug_flags_set("all");
	check(0 ==, mkdir("foo", S_IRWXU));
	check(0 ==, mkdir("foo/bar", S_IRWXU));
	check(0 ==, symlink("../", "foo/baz"));
	check(0 ==, unlink_recursive("foo"));

	check(1 ==, create_dir("foo/bar", 0777));
	check(0 ==, access("foo/bar", F_OK));
	check(0 ==, unlink_recursive("foo"));

	return 0;
}
EOF
	return $?
}

run()
{
	./"$exe"
	return $?
}

clean()
{
	rm -f "$exe"
	return 0
}

dispatch "$@"

# vim: set noexpandtab tabstop=4:
