#!/bin/sh

. ../../dttools/src/test_runner.common.sh

exe="path_dirname.test"

prepare()
{
	gcc -g -o "$exe" -x c - -x none ../src/libdttools.a -lm <<EOF
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

void test (const char *in, const char *expected)
{
	char out[PATH_MAX];
    path_dirname(in, out);
	if (strcmp(out, expected) != 0) {
		fprintf(stderr, "for %s, got %s expected %s\n", in, out, expected);
		exit(EXIT_FAILURE);
	}
}
int main (int argc, char *argv[])
{
	test("/foo/bar", "/foo");
	test("/foo/./bar", "/foo/.");
	test("/foo/./bar/", "/foo/.");
	test("/foo/./bar//", "/foo/.");
	test("/foo/.//bar//", "/foo/.");

	test("/foo/", "/");
	//test("/foo/.", "/");

	test("", ".");

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
