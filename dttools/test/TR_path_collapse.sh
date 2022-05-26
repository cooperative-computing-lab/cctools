#!/bin/sh

. ../../dttools/test/test_runner_common.sh

exe="path_collapse.test"

prepare()
{
	${CC} -g $CCTOOLS_TEST_CCFLAGS -o "$exe" -x c - -x none -I ../src ../src/libdttools.a -lm <<EOF
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "path.h"

void test (const char *in, const char *expected, int dots)
{
	static char out[4096];
	memset(out, 0, sizeof(out));
	path_collapse(in, out, dots);
	if (strcmp(out, expected) != 0) {
		fprintf(stderr, "for %s, got %s expected %s (dots = %d)\n", in, out, expected, dots);
		exit(EXIT_FAILURE);
	}
}
int main (int argc, char *argv[])
{
	test("/foo/bar", "/foo/bar", 1);
	test("/foo/bar", "/foo/bar", 0);
	test("/foo//bar", "/foo/bar", 0);
	test("/foo//bar//", "/foo/bar/", 0);
	test("/foo//bar/.", "/foo/bar/", 0);
	test("/foo//../.", "/foo/..", 0);
	test("/foo//../.", "/", 1);
	test("/foo//../.././/", "/foo/../..", 0);
	test("/foo//../.././/", "/", 1);
	test("/foo//../.././..//", "/foo/../../..", 0);
	test("/foo//../.././..//", "/", 1);
	test("/foo//.././..", "/foo/../..", 0);
	test("/foo//.././.././/", "/foo/../..", 0);
	test("/foo//.././.././/.", "/foo/../..", 0);
	test("/foo//.././.././/.//", "/foo/../..", 0);
	test("/.//foo/bar", "/foo/bar", 0);
	test("/.//foo/bar", "/foo/bar", 1);

	test("foo/bar", "foo/bar", 0);
	test("foo/bar", "foo/bar", 1);
	test("./foo/bar", "./foo/bar", 0);
	test("./foo/bar", "./foo/bar", 1);
	test(".//foo/bar", "./foo/bar", 0);
	test(".//foo/bar", "./foo/bar", 1);
	test(".//foo/bar/", "./foo/bar/", 0);
	test(".//foo/bar/", "./foo/bar/", 1);
	test(".//foo/bar//", "./foo/bar/", 0);
	test(".//foo/bar//", "./foo/bar/", 1);

	test("", "/", 0);
	test("/", "/", 0);
	test("/.", "/", 0);
	test("/./", "/", 0);

	test(".", ".", 0);
	test("./", ".", 0);

	test("...", "...", 0);
	test("/...", "/...", 0);
	test("/.../", "/.../", 0);
	test("./...", "./...", 0);
	test("./.../", "./.../", 0);

	test("./../", "./..", 0);
	test("./../", "/", 1);
	test("foo/../", "/", 1);
	test("./foo/../", ".", 1);
	test("./foo/../..", "/", 1);
	test("./foo/../../", "/", 1);
	test("foo/../..", "/", 1);
	test("foo/../../", "/", 1);

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
