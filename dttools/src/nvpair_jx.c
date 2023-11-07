/*
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "jx.h"
#include "jx_parse.h"
#include "jx_print.h"
#include "nvpair.h"
#include "stringtools.h"

/*
This is a compatibility layer between jx and nvpair,
to allow for mixed code during the transition.
An nvpair is internally just (unquoted) string values,
so all JX values are just printed out, except for strings,
which are quoted.  For more complex jx expressions within
a value, they are printed out exactly.   For example,
the following objects are equivalent.
*/

/*
nvpair:

port 1234
load 1.25
hostname ccl
url  http://ccl.cse.nd.edu:1234
working true
alist ["one","two","three"]

jx:

{
port: 1234,
load: 1.25,
hostname: "ccl"
url: "http://ccl.cse.nd.edu:1234",
working: true,
alist: ["one","two","three"]
}
*/

struct nvpair *jx_to_nvpair(struct jx *object)
{
	struct nvpair *nv = nvpair_create();
	struct jx_pair *p;

	for (p = object->u.pairs; p; p = p->next) {
		if (p->value->type == JX_STRING) {
			nvpair_insert_string(nv, p->key->u.string_value, p->value->u.string_value);
		} else {
			char *s = jx_print_string(p->value);
			nvpair_insert_string(nv, p->key->u.string_value, s);
			free(s);
		}
	}

	return nv;
}

struct jx *nvpair_to_jx(struct nvpair *nv)
{
	struct jx *object = jx_object(0);

	char *key;
	char *value;
	struct jx *jvalue;

	long long integer_value;
	double double_value;

	nvpair_first_item(nv);
	while (nvpair_next_item(nv, &key, &value)) {
		if (!strcmp(value, "true")) {
			jvalue = jx_boolean(1);
		} else if (!strcmp(value, "false")) {
			jvalue = jx_boolean(0);
		} else if (!strcmp(value, "null")) {
			jvalue = jx_null();
		} else if (string_is_integer(value, &integer_value)) {
			jvalue = jx_integer(integer_value);
		} else if (string_is_float(value, &double_value)) {
			jvalue = jx_double(double_value);
		} else if (value[0] == '[' || value[0] == '{') {
			jvalue = jx_parse_string(value);
			if (!jvalue)
				jvalue = jx_string(value);
		} else {
			jvalue = jx_string(value);
		}
		jx_insert(object, jx_string(key), jvalue);
	}

	return object;
}

struct jx *jx_parse_nvpair_file(const char *path)
{
	struct jx *j = 0;

	FILE *file = fopen(path, "r");
	if (file) {
		struct nvpair *nv = nvpair_create();
		if (nv) {
			nvpair_parse_stream(nv, file);
			j = nvpair_to_jx(nv);
			nvpair_delete(nv);
		}
		fclose(file);
	}

	return j;
}
