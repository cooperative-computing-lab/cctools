/*
Copyright (C) 2016- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/
#include <stdio.h>

#include <string.h>
#include <stdarg.h>
#include "jx.h"
#include "jx_eval.h"
#include "jx_print.h"
#include "jx_function.h"
#include "jx_match.h"
#include "xxmalloc.h"
#include "stringtools.h"

#define STR "str"
#define RANGE "range"
#define FOREACH "foreach"
#define JOIN "join"
#define DBG "dbg"

const char *jx_function_name_to_string(jx_function_t func) {
	switch (func) {
	case JX_FUNCTION_STR: return STR;
	case JX_FUNCTION_RANGE: return RANGE;
	case JX_FUNCTION_FOREACH: return FOREACH;
	case JX_FUNCTION_JOIN: return JOIN;
	case JX_FUNCTION_DBG: return DBG;
	default: return "???";
	}
}

jx_function_t jx_function_name_from_string(const char *name) {
	if (!strcmp(name, STR)) return JX_FUNCTION_STR;
	else if (!strcmp(name, RANGE)) return JX_FUNCTION_RANGE;
	else if (!strcmp(name, FOREACH)) return JX_FUNCTION_FOREACH;
	else if (!strcmp(name, JOIN)) return JX_FUNCTION_JOIN;
	else if (!strcmp(name, DBG)) return JX_FUNCTION_DBG;
	else return JX_FUNCTION_INVALID;
}

struct jx *jx_function_dbg(struct jx_function *f, struct jx *context, jx_eval_mode_t mode, struct jx *default_value) {
	struct jx *result;
	// we want to detect more than one arg, so try to match twice
	if (jx_match_array(f->arguments, &result, JX_ANY, &result, JX_ANY, NULL) != 1) {
		struct jx *err = jx_object(NULL);
		int code = 6;
		jx_insert_integer(err, "code", code);
		jx_insert(err, jx_string("function"), jx_function(f->function, jx_copy(f->arguments)));
		jx_insert_string(err, "message", "only one argument is allowed");
		jx_insert_string(err, "name", jx_error_name(code));
		jx_insert_string(err, "source", "jx_eval");
		return jx_error(err);
	}
	fprintf(stderr, "dbg  in: ");
	jx_print_stream(result, stderr);
	fprintf(stderr, "\n");
	result = jx_eval_1(result, context, mode, default_value);
	fprintf(stderr, "dbg out: ");
	jx_print_stream(result, stderr);
	fprintf(stderr, "\n");
	return result;
}

struct jx *jx_function_str(struct jx_function *f, struct jx *context, jx_eval_mode_t mode, struct jx *default_value) {
	struct jx *args;
	struct jx *err;
	struct jx *result;
	int code;

	switch (jx_array_length(f->arguments)) {
	case 0:
		return jx_string("");
	case 1:
		args = jx_eval_1(f->arguments->u.items->value, context, mode, default_value);
		break;
	default:
		code = 6;
		err = jx_object(NULL);
		jx_insert_integer(err, "code", code);
		jx_insert(err, jx_string("function"), jx_function(f->function, jx_copy(f->arguments)));
		jx_insert_string(err, "message", "at most one argument is allowed");
		jx_insert_string(err, "name", jx_error_name(code));
		jx_insert_string(err, "source", "jx_eval");
		return jx_error(err);

	}
	if (!args) return jx_null();
	switch (args->type) {
	case JX_ERROR:
	case JX_STRING:
		return args;
	default:
		if ((mode == JX_EVAL_MODE_PARTIAL) && !jx_is_constant(args)) {
			return jx_function(JX_FUNCTION_STR, jx_array(jx_item(args, NULL)));
		} else {
			result = jx_string(jx_print_string(args));
			jx_delete(args);
			return result;
		}
	}
}

struct jx *jx_function_foreach(struct jx_function *f, struct jx *context, jx_eval_mode_t mode, struct jx *default_value) {
	char *symbol = NULL;
	struct jx *array = NULL;
	struct jx *body = NULL;
	struct jx *result = NULL;
	struct jx *err;
	int code;

	if ((jx_match_array(f->arguments, &symbol, JX_SYMBOL, &array, JX_ANY, &body, JX_ANY, NULL) != 3)) {
		code = 6;
		err = jx_object(NULL);
		jx_insert_integer(err, "code", code);
		jx_insert(err, jx_string("function"), jx_function(f->function, jx_copy(f->arguments)));
		jx_insert_string(err, "message", "invalid arguments");
		jx_insert_string(err, "name", jx_error_name(code));
		jx_insert_string(err, "source", "jx_eval");
		result =  jx_error(err);
		goto DONE;
	}
	// shuffle around to avoid leaking memory
	result = array;
	array = jx_eval_1(array, context, mode, default_value);
	jx_delete(result);
	result = NULL;
	if (!jx_istype(array, JX_ARRAY)) {
		if ((mode == JX_EVAL_MODE_PARTIAL) && !jx_is_constant(array) ) {
			struct jx *s = jx_symbol(symbol);
			free(symbol);
			return jx_function(JX_FUNCTION_FOREACH, jx_array(jx_item(s, jx_item(array, jx_item(body, NULL)))));
		} else {
			code = 6;
			err = jx_object(NULL);
			jx_insert_integer(err, "code", code);
			jx_insert(err, jx_string("function"), jx_function(f->function, jx_copy(f->arguments)));
			jx_insert_string(err, "message", "second argument must evaluate to an array");
			jx_insert_string(err, "name", jx_error_name(code));
			jx_insert_string(err, "source", "jx_eval");
			result =  jx_error(err);
			goto DONE;
		}
	}

	result = jx_array(NULL);
	void *i = NULL;
	struct jx *item;
	while ((item = jx_iterate_array(array, &i))) {
		struct jx *local_context = jx_copy(context);
		if (!local_context) local_context = jx_object(NULL);
		jx_insert(local_context, jx_string(symbol), jx_copy(item));
		struct jx *local_result = jx_eval_1(body, local_context, mode, default_value);
		jx_array_append(result, local_result);
		jx_delete(local_context);
	}

DONE:
	if (symbol) free(symbol);
	jx_delete(array);
	jx_delete(body);
	return result;
}

// see https://docs.python.org/2/library/functions.html#range
struct jx *jx_function_range(struct jx_function *f, struct jx *context, jx_eval_mode_t mode, struct jx *default_value) {
	jx_int_t start, stop, step;
	int code;
	struct jx *err;
	struct jx *args = jx_eval_1(f->arguments, context, mode, default_value);
	if (jx_istype(args, JX_ERROR)) {
		return args;
	}
	if ((mode == JX_EVAL_MODE_PARTIAL) && !jx_is_constant(args)) {
		return jx_function(JX_FUNCTION_RANGE, args);
	}

	switch (jx_match_array(args, &start, JX_INTEGER, &stop, JX_INTEGER, &step, JX_INTEGER, NULL)) {
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
		code = 6;
		err = jx_object(NULL);
		jx_insert_integer(err, "code", code);
		jx_insert(err, jx_string("function"), jx_function(f->function, jx_copy(f->arguments)));
		jx_insert_string(err, "message", "invalid arguments");
		jx_insert_string(err, "name", jx_error_name(code));
		jx_insert_string(err, "source", "jx_eval");
		return jx_error(err);
	}
	jx_delete(args);

	if (step == 0) {
		code = 6;
		err = jx_object(NULL);
		jx_insert_integer(err, "code", code);
		jx_insert(err, jx_string("function"), jx_function(f->function, jx_copy(f->arguments)));
		jx_insert_string(err, "message", "step must be nonzero");
		jx_insert_string(err, "name", jx_error_name(code));
		jx_insert_string(err, "source", "jx_eval");
		return jx_error(err);
	}

	struct jx *result = jx_array(NULL);

	if (((stop - start) * step) < 0) {
		// step is pointing the wrong way
		return result;
	}

	for (jx_int_t i = start; stop >= start ? i < stop : i > stop; i += step) {
		jx_array_append(result, jx_integer(i));
	}

	return result;
}

struct jx *jx_function_join(struct jx_function *f, struct jx *context, jx_eval_mode_t mode, struct jx *default_value) {
	char *sep = NULL;
	struct jx *result;
	struct jx *array = NULL;
	struct jx *args = jx_eval_1(f->arguments, context, mode, default_value);
	struct jx *err;
	int code;
	if (jx_istype(args, JX_ERROR)) {
		return args;
	}
	if ((mode == JX_EVAL_MODE_PARTIAL) && !jx_is_constant(args)) {
		return jx_function(JX_FUNCTION_JOIN, args);
	}

	switch (jx_match_array(args, &array, JX_ARRAY, &sep, JX_STRING, NULL)) {
	case 1:
	case 2:
		break;
	default:
		code = 6;
		err = jx_object(NULL);
		jx_insert_integer(err, "code", code);
		jx_insert(err, jx_string("func"), jx_function(f->function, jx_copy(f->arguments)));
		jx_insert_string(err, "message", "invalid arguments");
		jx_insert_string(err, "name", jx_error_name(code));
		jx_insert_string(err, "source", "jx_eval");
		result = jx_error(err);
		goto DONE;
	}
	if (!sep) sep = xxstrdup(" ");

	result = jx_string("");
	void *i = NULL;
	struct jx *item;
	while ((item = jx_iterate_array(array, &i))) {
		if (!jx_istype(item, JX_STRING)) {
			jx_delete(result);
			code = 6;
			err = jx_object(NULL);
			jx_insert_integer(err, "code", code);
			jx_insert(err, jx_string("func"), jx_function(f->function, jx_copy(f->arguments)));
			jx_insert_string(err, "message", "joined items must be strings");
			jx_insert_string(err, "name", jx_error_name(code));
			jx_insert_string(err, "source", "jx_eval");
			result = jx_error(err);
			goto DONE;
		}
		result->u.string_value = string_combine(result->u.string_value, item->u.string_value);
		void *peek = i;
		if (jx_iterate_array(NULL, &peek)) {
			result->u.string_value = string_combine(result->u.string_value, sep);
		}
	}

DONE:
	free(sep);
	jx_delete(array);
	jx_delete(args);
	return result;
}
