#!/bin/sh

. ../../dttools/src/test_runner.common.sh

exe="buffer.test"

prepare()
{
	gcc -I../src/ -g -o "$exe" -x c - -x none ../src/libdttools.a -lm <<EOF
#include "buffer.h"
#include "debug.h"

#include <errno.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define check(expected,rc) \
	do {\
		if (expected != rc)\
			fatal("[%s:%d]: unexpected failure: %d -> %d '%s'", __FILE__, __LINE__, expected, rc, strerror(errno));\
	} while (0)

int main (int argc, char *argv[])
{
	buffer_t B;
	int i;
	size_t len;
	static char test[1<<20];
	char buf1[4];
	char buf2[1<<12];
	char buf3[1<<13] = "a";

	/* test that buffer starts with empty string */
	buffer_init(&B);
	buffer_ubuf(&B, buf3, sizeof(buf3));
	check(0, strcmp(buffer_tostring(&B, NULL), ""));
	buffer_free(&B);

	buffer_init(&B);
	buffer_ubuf(&B, buf1, sizeof(buf1));
	check(0, buffer_putliteral(&B, "a"));
	check(0, buffer_putstring(&B, "b"));
	check(0, buffer_putlstring(&B, "cd", 1));
	check(0, buffer_putstring(&B, "de"));
	check(0, strcmp(buffer_tostring(&B, NULL), "abcde"));
	buffer_free(&B);

	/* small buffer shouldn't be used */
	buffer_init(&B);
	buffer_ubuf(&B, buf1, sizeof(buf1));
	strcpy(test, "");
	for (i = 0; i < 1<<12; i++) {
		check(0, buffer_putstring(&B, "a"));
		strcat(test, "a");
	}
	check(0, strcmp(buffer_tostring(&B, &len), test));
	buffer_free(&B);

	/* this buffer is equal to initial and won't be used */
	buffer_init(&B);
	buffer_ubuf(&B, buf2, sizeof(buf2));
	strcpy(test, "");
	for (i = 0; i < 1<<12; i++) {
		check(0, buffer_putstring(&B, "a"));
		strcat(test, "a");
	}
	check(0, strcmp(buffer_tostring(&B, &len), test));
	buffer_free(&B);

	/* testing max */
	buffer_init(&B);
	buffer_ubuf(&B, buf2, sizeof(buf2));
	buffer_max(&B, 1<<12);
	for (i = 0; i < (1<<12)-1; i++) {
		check(0, buffer_putstring(&B, "a"));
	}
	check(-1, buffer_putstring(&B, "a"));
	buffer_free(&B);

	/* this buffer should be used */
	buffer_init(&B);
	buffer_ubuf(&B, buf3, sizeof(buf3));
	buffer_max(&B, 1<<13);
	strcpy(test, "");
	for (i = 0; i < 1<<12; i++) {
		check(0, buffer_putstring(&B, "a"));
		strcat(test, "a");
	}
	check(0, strcmp(buffer_tostring(&B, &len), test));
	buffer_free(&B);

	/* test max again */
	buffer_init(&B);
	buffer_ubuf(&B, buf3, sizeof(buf3));
	buffer_max(&B, 1<<14);
	for (i = 0; i < (1<<14)-1; i++) {
		check(0, buffer_putstring(&B, "a"));
	}
	check(-1, buffer_putstring(&B, "a"));
	buffer_free(&B);

	/* testing heap growth */
	buffer_init(&B);
	buffer_ubuf(&B, buf3, sizeof(buf3));
	for (i = 0; i < 1<<20; i++)
		test[i] = 'a';
	for (i = 0; i < 1<<20; i++) {
		check(0, buffer_putstring(&B, "a"));
	}
	check(0, strcmp(buffer_tostring(&B, &len), test));
	check(1<<20, len);
	buffer_free(&B);

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
