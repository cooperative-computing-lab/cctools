#!/bin/sh

. ../../dttools/test/test_runner_common.sh

exe="b64.test"

prepare()
{
	${CC} -I../src/ -g $CCTOOLS_TEST_CCFLAGS -o "$exe" -x c - -x none ../src/libdttools.a -lm <<EOF
#include "b64.h"
#include "buffer.h"
#include "debug.h"
#include "xxmalloc.h"

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
	int i;
	size_t l;
	char *b64 = NULL;
	buffer_t Bblob[1];
	buffer_t Bbase[1];

	buffer_init(Bblob);
	buffer_abortonfailure(Bblob, 1);
	buffer_init(Bbase);
	buffer_abortonfailure(Bbase, 1);

	{
		static const char b64test[] = "";

		buffer_rewind(Bblob, 0);
		buffer_rewind(Bbase, 0);
		l = b64_size(buffer_pos(Bblob));
		check(sizeof(b64test) ==, l);
		check(0 ==, b64_encode(buffer_tostring(Bblob), buffer_pos(Bblob), Bbase));
		check(0 ==, memcmp(buffer_tostring(Bbase), b64test, sizeof(b64test)));

		buffer_rewind(Bbase, 0);
		check(0 ==, b64_decode(b64test, Bbase));
		check(buffer_pos(Bblob) ==, buffer_pos(Bbase));
		check(0 ==, memcmp(buffer_tostring(Bblob), buffer_tostring(Bbase), buffer_pos(Bblob)));
	}

	{
		static const char b64test[] = "YQ==";

		buffer_rewind(Bblob, 0);
		buffer_rewind(Bbase, 0);
		buffer_putliteral(Bblob, "a");
		l = b64_size(buffer_pos(Bblob));
		check(sizeof(b64test) ==, l);
		check(0 ==, b64_encode(buffer_tostring(Bblob), buffer_pos(Bblob), Bbase));
		check(0 ==, memcmp(buffer_tostring(Bbase), b64test, sizeof(b64test)));

		buffer_rewind(Bbase, 0);
		check(0 ==, b64_decode(b64test, Bbase));
		check(buffer_pos(Bblob) ==, buffer_pos(Bbase));
		check(0 ==, memcmp(buffer_tostring(Bblob), buffer_tostring(Bbase), buffer_pos(Bblob)));
	}

	{
		static const char b64test[] = "Q0E=";

		buffer_rewind(Bblob, 0);
		buffer_rewind(Bbase, 0);
		buffer_putliteral(Bblob, "CA");
		l = b64_size(buffer_pos(Bblob));
		check(sizeof(b64test) ==, l);
		check(0 ==, b64_encode(buffer_tostring(Bblob), buffer_pos(Bblob), Bbase));
		check(0 ==, memcmp(buffer_tostring(Bbase), b64test, sizeof(b64test)));

		buffer_rewind(Bbase, 0);
		check(0 ==, b64_decode(b64test, Bbase));
		check(buffer_pos(Bblob) ==, buffer_pos(Bbase));
		check(0 ==, memcmp(buffer_tostring(Bblob), buffer_tostring(Bbase), buffer_pos(Bblob)));
	}

	{
		static const char b64test[] = "WllB";

		buffer_rewind(Bblob, 0);
		buffer_rewind(Bbase, 0);
		buffer_putliteral(Bblob, "ZYA");
		l = b64_size(buffer_pos(Bblob));
		check(sizeof(b64test) ==, l);
		check(0 ==, b64_encode(buffer_tostring(Bblob), buffer_pos(Bblob), Bbase));
		check(0 ==, memcmp(buffer_tostring(Bbase), b64test, sizeof(b64test)));

		buffer_rewind(Bbase, 0);
		check(0 ==, b64_decode(b64test, Bbase));
		check(buffer_pos(Bblob) ==, buffer_pos(Bbase));
		check(0 ==, memcmp(buffer_tostring(Bblob), buffer_tostring(Bbase), buffer_pos(Bblob)));
	}

	{
		static const char b64test[] =
			"AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8gISIjJCUmJygpKissLS4vMDEyMzQ1Njc4"
			"OTo7PD0+P0BBQkNERUZHSElKS0xNTk9QUVJTVFVWV1hZWltcXV5fYGFiY2RlZmdoaWprbG1ub3Bx"
			"cnN0dXZ3eHl6e3x9fn+AgYKDhIWGh4iJiouMjY6PkJGSk5SVlpeYmZqbnJ2en6ChoqOkpaanqKmq"
			"q6ytrq+wsbKztLW2t7i5uru8vb6/wMHCw8TFxsfIycrLzM3Oz9DR0tPU1dbX2Nna29zd3t/g4eLj"
			"5OXm5+jp6uvs7e7v8PHy8/T19vf4+fr7/P3+/w==";

		buffer_rewind(Bblob, 0);
		buffer_rewind(Bbase, 0);
		for (i = 0; i <= UCHAR_MAX; i++) {
			unsigned char c = i;
			buffer_putlstring(Bblob, (const char *)&c, sizeof(c));
		}
		l = b64_size(buffer_pos(Bblob));
		check(sizeof(b64test) ==, l);
		check(0 ==, b64_encode(buffer_tostring(Bblob), buffer_pos(Bblob), Bbase));
		check(0 ==, memcmp(buffer_tostring(Bbase), b64test, sizeof(b64test)));

		buffer_rewind(Bbase, 0);
		check(0 ==, b64_decode(b64test, Bbase));
		check(buffer_pos(Bblob) ==, buffer_pos(Bbase));
		check(0 ==, memcmp(buffer_tostring(Bblob), buffer_tostring(Bbase), buffer_pos(Bblob)));
	}

	buffer_free(Bblob);
	buffer_free(Bbase);

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
