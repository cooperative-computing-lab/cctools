#!/bin/sh

. ../../dttools/test/test_runner_common.sh

exe="path_extension.test"

prepare()
{
	${CC} -g $CCTOOLS_TEST_CCFLAGS -o "$exe" -x c - -x none -I ../src ../src/libdttools.a -lm <<EOF
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "path.h"

void test (const char *in, const char *expected)
{
	const char *out = path_extension(in);
	if (expected) {
		if (strcmp(out, expected) != 0) {
			fprintf(stderr, "for %s, got %s expected %s\n", in, out, expected);
			exit(EXIT_FAILURE);
		}
	} else {
		if (out){
			fprintf(stderr, "for %s, got %s expected NULL\n", in, out);
			exit(EXIT_FAILURE);
		}
	}
}

int main(int argc, char *argv[])
{
	test("foo", NULL);
	test("foo.bar", "bar");
	test(".foobar", NULL);
	test("foobar.", "");
	test("/foo.bar/baz", NULL);
	test("foo.bar.baz", "baz");
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
