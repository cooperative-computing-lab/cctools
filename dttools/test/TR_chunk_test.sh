#!/bin/sh

. ../../dttools/test/test_runner_common.sh

exe="chunk.test"
output="chunk.output"

prepare()
{
	${CC} -g $CCTOOLS_TEST_CCFLAGS -o "$exe" -I ../src/ -x c - -x none ../src/libdttools.a -lm <<EOF
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "chunk.h"

int main(int argc, char **argv)
{
	const char * const files[] = {
	  "../src/chunk.c",
	  "../src/chunk.h",
	};

	if(!chunk_concat(argv[1], files, sizeof(files)/sizeof(char *), "> ", NULL)) {
		fprintf(stderr, "chunk_test: chunk_concat failed\n");
		exit(1);
	}

	return 0;
}
EOF
	return $?
}

run()
{
	./"$exe" "$output"
	return $?
}

clean()
{
	rm -f "$exe" "$output"
	return 0
}

dispatch "$@"

# vim: set noexpandtab tabstop=4:
