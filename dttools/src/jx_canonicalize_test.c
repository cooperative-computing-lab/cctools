#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "jx.h"
#include "jx_canonicalize.h"


int main(int argc, char **argv) {
	char *s;

	struct jx *x_null = jx_null();
	struct jx *x_true = jx_boolean(1);
	struct jx *x_false = jx_boolean(0);
	struct jx *x_integer = jx_integer(42);
	struct jx *x_double = jx_double(42);
	struct jx *x_string = jx_string("s");
	struct jx *x_string2 = jx_string("t");
	struct jx *x_symbol = jx_symbol("sym");
	struct jx *x_array = jx_array(NULL);
	struct jx *x_object = jx_object(NULL);

	s = jx_canonicalize(x_null);
	assert(s);
	assert(!strcmp(s, "null"));

	s = jx_canonicalize(x_true);
	assert(s);
	assert(!strcmp(s, "true"));

	s = jx_canonicalize(x_false);
	assert(s);
	assert(!strcmp(s, "false"));

	s = jx_canonicalize(x_integer);
	assert(s);
	assert(!strcmp(s, "42"));

	s = jx_canonicalize(x_double);
	assert(s);
	assert(!strcmp(s, "4.200000e+01"));

	s = jx_canonicalize(x_string);
	assert(s);
	assert(!strcmp(s, "\"s\""));

	s = jx_canonicalize(x_symbol);
	assert(!s);

	s = jx_canonicalize(x_array);
	assert(s);
	assert(!strcmp(s, "[]"));

	jx_array_append(x_array, x_null);
	jx_array_append(x_array, x_string);
	jx_array_append(x_array, x_string2);
	jx_array_append(x_array, x_integer);
	jx_array_append(x_array, x_string);
	s = jx_canonicalize(x_array);
	assert(s);
	assert(!strcmp(s, "[null,\"s\",\"t\",42,\"s\"]"));

	jx_array_append(x_array, x_symbol);
	s = jx_canonicalize(x_array);
	assert(!s);

	s = jx_canonicalize(x_object);
	assert(s);
	assert(!strcmp(s, "{}"));

	jx_insert(x_object, x_string, x_integer);
	jx_insert(x_object, x_string2, x_null);
	s = jx_canonicalize(x_object);
	assert(s);
	assert(!strcmp(s, "{\"s\":42,\"t\":null}"));

	x_object = jx_object(NULL);
	jx_insert(x_object, x_string2, x_null);
	jx_insert(x_object, x_string, x_integer);
	s = jx_canonicalize(x_object);
	assert(s);
	assert(!strcmp(s, "{\"s\":42,\"t\":null}"));

	jx_insert(x_object, x_string, x_true);
	s = jx_canonicalize(x_object);
	assert(!s);

	x_object = jx_object(NULL);
	jx_insert(x_object, x_integer, x_false);
	s = jx_canonicalize(x_object);
	assert(!s);

	x_object = jx_object(NULL);
	jx_insert(x_object, x_string, x_symbol);
	s = jx_canonicalize(x_object);
	assert(!s);

	return 0;
}

// vim: set noexpandtab tabstop=8:
