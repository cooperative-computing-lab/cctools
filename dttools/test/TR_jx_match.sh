#!/bin/sh

. ../../dttools/test/test_runner_common.sh

exe="match.test"

prepare()
{
	${CC} -g -o "$exe" -I ../src/ -x c - -x none ../src/libdttools.a -lm <<EOF
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "jx.h"
#include "jx_match.h"


int main(int argc, char **argv)
{
	char *ee;

	struct jx *a = jx_boolean(10);
	int aa = 42;
	assert(jx_match_boolean(a, &aa));
	assert(!jx_match_symbol(a, &ee));
	assert(aa == 1);
	jx_delete(a);

	struct jx *b = jx_double(2.71);
	double bb = 100.0;
	assert(jx_match_double(b, &bb));
	assert(!jx_match_boolean(b, &aa));
	assert(bb == 2.71);
	jx_delete(b);

	struct jx *c = jx_integer(17);
	jx_int_t cc = -10;
	assert(jx_match_integer(c, &cc));
	assert(!jx_match_double(c, &bb));
	assert(cc == 17);
	jx_delete(c);

	struct jx *d = jx_string("d");
	char *dd = NULL;
	assert(jx_match_string(d, &dd));
	assert(!jx_match_integer(d, &cc));
	jx_delete(d);
	assert(strlen(dd) == 1);
	free(dd);

	struct jx *e = jx_symbol("e");
	assert(jx_match_symbol(e, &ee));
	assert(!jx_match_string(e, &ee));
	jx_delete(e);
	assert(strlen(ee) == 1);
	free(ee);

	int f1;
	char *f2;
	double f3;
	struct jx *f4 = jx_null();
	struct jx *f = jx_array(NULL);
	jx_array_insert(f, jx_double(3.14));
	jx_array_insert(f, jx_string("pi"));
	jx_array_insert(f, jx_boolean(0));
	assert(!jx_match_boolean(f, &f1));
	assert(!jx_match_array(f, NULL));
	assert(!jx_match_array(f4, &f1, JX_BOOLEAN, NULL));
	jx_delete(f4);
	assert(jx_match_array(f, &f1, JX_BOOLEAN, &f3, JX_DOUBLE, NULL) == 1);
	assert(jx_match_array(f, &f1, JX_BOOLEAN, &f2, JX_STRING, &f3, JX_DOUBLE, NULL) == 3);
	assert(f1 == 0);
	assert(strlen(f2) == 2);
	assert(f3 == 3.14);
	free(f2);
	assert(jx_match_array(f, &f4, JX_ANY, NULL) == 1);
	assert(jx_match_boolean(f4, &f1));
	assert(f1 == 0);

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
