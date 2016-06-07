/*
Copyright (C) 2016- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include <stdarg.h>
#include <string.h>

#include "jx.h"
#include "jx_match.h"

int jx_match_boolean(struct jx *j, int *v) {
	if (jx_istype(j, JX_BOOLEAN)) {
		if (v) {
			*v = !!j->u.boolean_value;
		}
		return 1;
	} else {
		return 0;
	}
}

int jx_match_integer(struct jx *j, jx_int_t *v) {
	if (jx_istype(j, JX_INTEGER)) {
		if (v) {
			*v = j->u.integer_value;
		}
		return 1;
	} else {
		return 0;
	}
}

int jx_match_double(struct jx *j, double *v) {
	if (jx_istype(j, JX_DOUBLE)) {
		if (v) {
			*v = j->u.double_value;
		}
		return 1;
	} else {
		return 0;
	}
}

int jx_match_string(struct jx *j, char **v) {
	if (jx_istype(j, JX_STRING)) {
		if (v) {
			if (!(*v = strdup(j->u.string_value))) return 0;
		}
		return 1;
	} else {
		return 0;
	}
}

int jx_match_symbol(struct jx *j, char **v) {
	if (jx_istype(j, JX_SYMBOL)) {
		if (v) {
			if (!(*v = strdup(j->u.symbol_name))) return 0;
		}
		return 1;
	} else {
		return 0;
	}
}

int jx_match_array(struct jx *j, ...) {
	va_list ap;
	int matched = 0;

	va_start(ap, j);
	void *i = NULL;
	struct jx *item;
	while ((item = jx_iterate_array(j, &i))) {
		void *out = va_arg(ap, void *);
		if (!out) goto DONE;
		jx_type_t t = va_arg(ap, jx_type_t);

		if (t == (jx_type_t) JX_ANY) {
			*((struct jx **) out) = jx_copy(item);
		} else {
			switch (t) {
			case JX_INTEGER:
				if (!jx_match_integer(item, out)) goto DONE;
				break;
			case JX_BOOLEAN:
				if (!jx_match_boolean(item, out)) goto DONE;
				break;
			case JX_DOUBLE:
				if (!jx_match_double(item, out)) goto DONE;
				break;
			case JX_STRING:
				if (!jx_match_string(item, out)) goto DONE;
				break;
			case JX_SYMBOL:
				if (!jx_match_symbol(item, out)) goto DONE;
				break;
			case JX_OBJECT:
				if (!jx_istype(item, JX_OBJECT)) goto DONE;
				if (!(*((struct jx **) out) = jx_copy(item))) goto DONE;
				break;
			case JX_ARRAY:
				if (!jx_istype(item, JX_ARRAY)) goto DONE;
				if (!(*((struct jx **) out) = jx_copy(item))) goto DONE;
				break;
			case JX_OPERATOR:
				if (!jx_istype(item, JX_OPERATOR)) goto DONE;
				if (!(*((struct jx **) out) = jx_copy(item))) goto DONE;
				break;
			case JX_NULL:
				if (!jx_istype(item, JX_NULL)) goto DONE;
				if (!(*((struct jx **) out) = jx_copy(item))) goto DONE;
				break;
			default:
				goto DONE;
			}
		}
		matched++;
	}

DONE:
	va_end(ap);
	return matched;
}
