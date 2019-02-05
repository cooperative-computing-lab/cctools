/*
Copyright (C) 2019- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include <assert.h>
#include <stdbool.h>

#include "jx.h"
#include "jx_print.h"
#include "jx_canonicalize.h"
#include "list.h"

static bool jx_canonicalize_buffer(struct jx *j, buffer_t *b);

static int pair_compare(const void *p1, const void *p2) {
	assert(p1);
	assert(p2);
	struct jx_pair *i1 = *(struct jx_pair **) p1;
	struct jx_pair *i2 = *(struct jx_pair **) p2;
	assert(i1);
	assert(i2);
	return strcmp(i1->key->u.string_value, i2->key->u.string_value);
}

static bool jx_canonicalize_array(struct jx_item *i, buffer_t *b) {
	assert(b);

	buffer_putstring(b, "[");

	const char *sep = "";
	for (; i; i = i->next) {
		buffer_putstring(b, sep);
		if (!jx_canonicalize_buffer(i->value, b)) return false;
		sep = ",";
	}

	buffer_putstring(b, "]");
	return true;
}

static bool jx_canonicalize_object(struct jx_pair *p, buffer_t *b) {
	assert(b);

	bool rc = false;
	struct list *pairs = list_create();
	struct list_cursor *csr = list_cursor_create(pairs);

	buffer_putstring(b, "{");

	for (; p; p = p->next) {
		if (!jx_istype(p->key, JX_STRING)) goto OUT;
		list_push_tail(pairs, p);
	}
	list_sort(pairs, pair_compare);
	
	struct jx_pair *cur;
	struct jx_pair *prev = NULL;
	const char *sep = "";
	for (list_seek(csr, 0); list_get(csr, (void **) &cur); list_next(csr)) {
		assert(cur);
		if (prev && !pair_compare(&cur, &prev)) goto OUT;
		buffer_putstring(b, sep);
		jx_canonicalize_buffer(cur->key, b); // known to be a string
		buffer_putstring(b, ":");
		if (!jx_canonicalize_buffer(cur->value, b)) goto OUT;
		prev = cur;
		sep = ",";
	}

	buffer_putstring(b, "}");
	rc = true;

OUT:
	list_cursor_destroy(csr);
	list_delete(pairs);
	return rc;
}

static bool jx_canonicalize_buffer(struct jx *j, buffer_t *b) {
	assert(j);
	assert(b);

	switch (j->type) {
	case JX_NULL:
	case JX_BOOLEAN:
	case JX_INTEGER:
	case JX_STRING:
		jx_print_buffer(j, b);
		return true;
	case JX_DOUBLE:
		buffer_printf(b,"%e",j->u.double_value);
		return true;
	case JX_ARRAY:
		return jx_canonicalize_array(j->u.items, b);
	case JX_OBJECT:
		return jx_canonicalize_object(j->u.pairs, b);
	default:
		return false;
	}
}

char *jx_canonicalize(struct jx *j) {
	assert(j);

	char *out = NULL;
	buffer_t buffer;
	buffer_init(&buffer);
	if (!jx_canonicalize_buffer(j, &buffer)) goto OUT;
	buffer_dup(&buffer, &out);
OUT:
	buffer_free(&buffer);
	return out;
}
