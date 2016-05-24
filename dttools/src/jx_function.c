/*
Copyright (C) 2016- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include <string.h>
#include <stdarg.h>
#include "jx.h"
#include "jx_eval.h"
#include "jx_print.h"
#include "jx_function.h"

struct jx *jx_function_str( struct jx_operator *o, struct jx *context ) {
	struct jx *args = jx_eval(o->right, context);
	if (!args) return NULL;
	const char *result = jx_print_string(args);
	jx_delete(args);
	return jx_string(result);
}

// see https://docs.python.org/2/library/functions.html#range
struct jx *jx_function_range( struct jx_operator *o, struct jx *context ) {
	jx_int_t start, stop, step;
	struct jx *args = jx_eval(o->right, context);
	switch (jx_function_parse_args(args, 3, JX_INTEGER, &start, JX_INTEGER, &stop, JX_INTEGER, &step)) {
	case 1:
		stop = start;
		start = 0;
		step = 1;
		break;
	case 2:
		step = 1;
		break;
	case 3:
		break;
	default:
		jx_delete(args);
		return NULL;
	}
	jx_delete(args);

	struct jx *result = jx_array(NULL);
	for (jx_int_t i = start; i < stop; i += step) {
		jx_array_append(result, jx_integer(i));
	}

	return result;
}

int jx_function_parse_args(struct jx *array, int argc, ...) {
	if (!jx_istype(array, JX_ARRAY)) return 0;

	va_list ap;
	int matched = 0;
	struct jx_item *item = array->u.items;

	va_start(ap, argc);
	for (int i = 0; i < argc; i++) {
		if (!item) goto DONE;
		switch (va_arg(ap, jx_type_t)) {
		case JX_INTEGER:
			if (!jx_istype(item->value, JX_INTEGER)) goto DONE;
			*va_arg(ap, jx_int_t *) = item->value->u.integer_value;
			break;
		case JX_BOOLEAN:
			if (!jx_istype(item->value, JX_BOOLEAN)) goto DONE;
			*va_arg(ap, int *) = item->value->u.boolean_value;
			break;
		case JX_DOUBLE:
			if (!jx_istype(item->value, JX_DOUBLE)) goto DONE;
			*va_arg(ap, double *) = item->value->u.double_value;
			break;
		case JX_STRING:
			if (!jx_istype(item->value, JX_STRING)) goto DONE;
			strcpy(va_arg(ap, char *), item->value->u.string_value);
			break;
		case JX_SYMBOL:
			if (!jx_istype(item->value, JX_SYMBOL)) goto DONE;
			strcpy(va_arg(ap, char *), item->value->u.symbol_name);
			break;
		case JX_OBJECT:
			if (!jx_istype(item->value, JX_OBJECT)) goto DONE;
			*va_arg(ap, struct jx **) = jx_copy(item->value);
			break;
		case JX_ARRAY:
			if (!jx_istype(item->value, JX_ARRAY)) goto DONE;
			*va_arg(ap, struct jx **) = jx_copy(item->value);
			break;
		default:
			goto DONE;
		}
		matched++;
		item = item->next;
	}

DONE:
	va_end(ap);
	return matched;
}

