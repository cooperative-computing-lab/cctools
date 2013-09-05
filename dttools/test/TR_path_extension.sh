#!/bin/sh

. ../../dttools/src/test_runner.common.sh

exe="path_extension.test"

prepare()
{
	gcc -g -o "$exe" -x c - -x none ../src/libdttools.a -lm <<EOF
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

void test (const char *in, const char *expected)
{
	char *out = path_extension(in);
	if (strcmp(out, expected) != 0) {
		fprintf(stderr, "for %s, got %s expected %s\n", in, out, expected);
		exit(EXIT_FAILURE);
	}
}

int main(int argc, char *argv[])
{
	test("foo", "");
	test("foo.bar", "bar");
	test(".foobar", "");
	test("foobar.", "");

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
