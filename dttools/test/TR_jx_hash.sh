#!/bin/sh

. ../../dttools/test/test_runner_common.sh

exe="hash.test"

prepare()
{
	gcc -g -o "$exe" -I ../src/ -x c - -x none ../src/libdttools.a -lm <<EOF
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>

#include "jx.h"
#include "jx_print.h"

bool check(struct jx *j, const char *e) {
	fprintf(stderr, "jx: ");
	jx_print_stream(j, stderr);
	fprintf(stderr, "\n");

	fprintf(stderr, "expected: %s\n", e);

	char *h = jx_hash(j);
	fprintf(stderr, "hash: %s\n", h);

	return (!h && !e) || !strcmp(h, e);
}

int main(int argc, char **argv)
{
	struct jx *n = jx_null();
	struct jx *b = jx_boolean(10);
	struct jx *bb = jx_boolean(1);
	struct jx *i = jx_integer(10);
	struct jx *d = jx_double(10.0);
	struct jx *s = jx_string("10");
	struct jx *y = jx_symbol("foo");
	struct jx *a = jx_array(NULL);
	struct jx *o = jx_object(NULL);
	struct jx *oo = jx_object(NULL);
	jx_insert_string(oo, "source", "x");
	jx_insert_string(oo, "name", "y");
	jx_insert_string(oo, "message", "y");
	struct jx *ooo = jx_object(NULL);
	jx_insert_string(ooo, "source", "x");
	jx_insert_string(ooo, "message", "y");
	jx_insert_string(ooo, "name", "y");
	struct jx *aa = jx_array(NULL);
	jx_array_append(aa, n);
	jx_array_append(aa, b);
	jx_array_append(aa, i);
	jx_array_append(aa, d);
	jx_array_append(aa, s);
	jx_array_append(aa, oo);
	struct jx *aaa = jx_array(NULL);
	jx_array_append(aaa, b);
	jx_array_append(aaa, n);
	jx_array_append(aaa, i);
	jx_array_append(aaa, d);
	jx_array_append(aaa, s);
	jx_array_append(aaa, oo);

	struct jx *p = jx_operator(JX_OP_EQ, n, b);
	struct jx *f = jx_function("func", JX_BUILTIN_LAMBDA, NULL, NULL);
	struct jx *e = jx_error(oo);
	struct jx *x = jx_copy(aa);
	jx_array_append(x, y);

	assert(check(n,		"d1854cae891ec7b29161ccaf79a24b00c274bdaa"));
	assert(check(b,		"7e83ca2a65d6f90a809c8570c6c905a941b87732"));
	assert(check(bb,	"7e83ca2a65d6f90a809c8570c6c905a941b87732"));
	assert(check(i,		"0b4193a8f1a19e4d1c5e5f690e2773a7f5b74e4a"));
	assert(check(d,		"c547c0e5aefa68135c191581883fe27f9aace03c"));
	assert(check(s,		"da711ada135f3605d52e620647de79b081a3c858"));
	assert(check(y,		NULL));
	assert(check(a,		"86f7e437faa5a7fce15d1ddcb9eaeaea377667b8"));
	assert(check(aa,	"5b666218f4133e8a55b4bb7fc24210a5ebbb6a9f"));
	assert(check(aaa,	"3b5046445d317da2ffb14ec1aaadb3a71fffe523"));
	assert(check(o,		"7a81af3e591ac713f81ea1efe93dcf36157d8376"));
	assert(check(oo,	"812cb8682c2feef39aa31b787a891c2b3bf780e2"));
	assert(check(ooo,	"812cb8682c2feef39aa31b787a891c2b3bf780e2"));
	assert(check(p,		NULL));
	assert(check(f,		NULL));
	assert(check(e,		NULL));
	assert(check(x,		NULL));

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
