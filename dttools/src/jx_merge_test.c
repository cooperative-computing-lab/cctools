#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "jx.h"
#include "jx_parse.h"

int main(int argc, char **argv) {
	struct jx *a;
	struct jx *b;
	struct jx *c;
	struct jx *r;
	struct jx *s;
	struct jx *t;

	a = jx_object(NULL);
	jx_insert(a, jx_string("k"), jx_integer(5));
	jx_insert(a, jx_string("e"), jx_integer(6));
	jx_insert(a, jx_string("y"), jx_integer(7));

	b = jx_object(NULL);

	c = jx_object(NULL);
	jx_insert(c, jx_string("x"), jx_integer(2));
	jx_insert(c, jx_string("x"), jx_integer(3));


	t = jx_parse_string("{\"k\": 5, \"e\": 6, \"y\": 7}");

	s = jx_merge(a, NULL);
	assert(jx_equals(s, t));
	jx_delete(s);

	s = jx_merge(a, b, NULL);
	assert(jx_equals(s, t));
	jx_delete(s);

	s = jx_merge(b, a, NULL);
	assert(jx_equals(s, t));
	jx_delete(s);

	jx_delete(t);

	t = jx_integer(3);
	s = jx_lookup(c, "x");
	assert(jx_equals(s, t));
	jx_delete(t);

	r = jx_merge(c, NULL);
	s = jx_lookup(r, "x");
	t = jx_integer(2);
	// probably not desirable...
	assert(jx_equals(s, t));
	jx_delete(r);
	jx_delete(t);

	s = jx_merge(a, b, c, NULL);
	t = jx_parse_string("{\"x\":2,\"k\":5,\"e\":6,\"y\":7}");
	assert(jx_equals(s, t));
	jx_delete(s);
	jx_delete(t);

	s = jx_merge(a, c, a, NULL);
	t = jx_parse_string("{\"k\":5,\"e\":6,\"y\":7,\"x\":2}");
	assert(jx_equals(s, t));
	jx_delete(s);
	jx_delete(t);

	jx_delete(a);
	jx_delete(b);
	jx_delete(c);

	return 0;
}

// vim: set noexpandtab tabstop=8:
