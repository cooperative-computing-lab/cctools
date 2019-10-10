#!/bin/sh

. ../../dttools/test/test_runner_common.sh

exe="pattern.test"

prepare()
{
	${CC} -I../src/ -g $CCTOOLS_TEST_CCFLAGS -o "$exe" -x c - -x none ../src/libdttools.a -lm <<EOF
#include "pattern.h"
#include "debug.h"

#include <errno.h>
#include <limits.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define check(expected,rc) \\
	do {\\
		ptrdiff_t e = (expected);\\
		if (e != rc)\\
			fatal("[%s:%d]: unexpected failure: %d (got) -> %d (expected)\n\t%s", __FILE__, __LINE__, (int)e, rc, #expected);\\
	} while (0)

int main (int argc, char *argv[])
{
	char *cap1;
	char *cap2;
	size_t ncap;
	check(pattern_match("foo", "foo"), 0);
	check(pattern_match("foo", "o"), 1);
	check(pattern_match("foo", "oo"), 1);
	check(pattern_match("foo", "o$"), 2);
	check(pattern_match("foo", "^foo"), 0);
	check(pattern_match("foo", "^f"), 0);
	check(pattern_match("foo", "^"), 0);
	check(pattern_match("foo", "^o"), -1);
	check(pattern_match("foo", "(foo)", &cap1), 0);
	check(strcmp(cap1, "foo"), 0);
	free(cap1);
	check(pattern_match("foo", "bar"), -1);
	check(pattern_match("foo", "(foo)()", &cap1, &ncap), 0);
	check(strcmp(cap1, "foo"), 0);
	check(ncap, 3);
	free(cap1);
	check(pattern_match("foobar", "(foo)()b(.*)", &cap1, &ncap, &cap2), 0);
	check(strcmp(cap1, "foo"), 0);
	check(ncap, 3);
	check(strcmp(cap2, "ar"), 0);
	free(cap1);
	free(cap2);
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
