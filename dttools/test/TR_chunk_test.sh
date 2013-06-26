#!/bin/sh

. ../../dttools/src/test_runner.common.sh

exe="chunk.test"
output="chunk.output"

prepare()
{
	gcc -g -o "$exe" -I ../src/ -x c - -x none ../src/libdttools.a -lm <<EOF
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "chunk.h"

int main(int argc, char **argv)
{
	char **files = malloc(2 * sizeof(*files));

	files[0] = malloc(8 * sizeof(*files[0]));
	strcpy(files[0], "../src/chunk.c");

	files[1] = malloc(8 * sizeof(*files[0]));
	strcpy(files[1], "../src/chunk.h");

	if(!chunk_concat(argv[1], (char **) files, 2, "> ", NULL)) {
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

dispatch $@
