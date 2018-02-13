/*
Copyright (C) 2016- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include <assert.h>
#include <math.h>
#include <inttypes.h>
#include <stdarg.h>
#include <stdbool.h>
#include <stdio.h>
#include <string.h>

#include "jx.h"
#include "jx_match.h"
#include "jx_print.h"
#include "stringtools.h"
#include "xxmalloc.h"

// FAIL(const char *name, jx_builtin_t b struct jx *args, const char *message)
#define FAIL(name, b, args, message)                                           \
	do {                                                                   \
		int ciuygssd = 6;                                              \
		struct jx *ebijuaef = jx_object(NULL);                         \
		jx_insert_integer(ebijuaef, "code", ciuygssd);                 \
		jx_insert(ebijuaef, jx_string("function"),                     \
			jx_operator(JX_OP_CALL,                                \
				jx_function(name, b, NULL, NULL),              \
				jx_copy(args)));                               \
		if (args->line)                                                \
			jx_insert_integer(ebijuaef, "line", args->line);       \
		jx_insert_string(ebijuaef, "message", message);                \
		jx_insert_string(ebijuaef, "name", jx_error_name(ciuygssd));   \
		jx_insert_string(ebijuaef, "source", "jx_eval");               \
		return jx_error(ebijuaef);                                     \
	} while (false)

static char *jx_function_format_value(char spec, struct jx *args) {
	if (spec == '%') return xxstrdup("%");
	char *result = NULL;
	struct jx *j = jx_array_shift(args);
	switch (spec) {
		case 'd':
		case 'i':
			if (jx_istype(j, JX_INTEGER))
				result = string_format(
					"%" PRIi64, j->u.integer_value);
			break;
		case 'e':
			if (jx_istype(j, JX_DOUBLE))
				result = string_format("%e", j->u.double_value);
			break;
		case 'E':
			if (jx_istype(j, JX_DOUBLE))
				result = string_format("%E", j->u.double_value);
			break;
		case 'f':
			if (jx_istype(j, JX_DOUBLE))
				result = string_format("%f", j->u.double_value);
			break;
		case 'F':
			if (jx_istype(j, JX_DOUBLE))
				result = string_format("%F", j->u.double_value);
			break;
		case 'g':
			if (jx_istype(j, JX_DOUBLE))
				result = string_format("%g", j->u.double_value);
			break;
		case 'G':
			if (jx_istype(j, JX_DOUBLE))
				result = string_format("%G", j->u.double_value);
			break;
		case 's':
			if (jx_istype(j, JX_STRING))
				result = xxstrdup(j->u.string_value);
			break;
		default: break;
	}
	jx_delete(j);
	return result;
}

struct jx *jx_function_format(struct jx *orig_args) {
	assert(orig_args);
	const char *funcname = "format";
	const char *err = NULL;
	char *format = NULL;
	char *result = xxstrdup("");
	struct jx *args = jx_copy(orig_args);
	struct jx *j = jx_array_shift(args);
	if (!jx_match_string(j, &format)) {
		jx_delete(j);
		err = "invalid/missing format string";
		goto FAILURE;
	}
	jx_delete(j);
	char *i = format;
	bool spec = false;
	while (*i) {
		if (spec) {
			spec = false;
			char *next = jx_function_format_value(*i, args);
			if (!next) {
				err = "mismatched format specifier";
				goto FAILURE;
			}
			result = string_combine(result, next);
			free(next);
		} else if (*i == '%') {
			spec = true;
		} else {
			char next[2];
			snprintf(next, 2, "%c", *i);
			result = string_combine(result, next);
		}
		++i;
	}
	if (spec) {
		err = "truncated format specifier";
		goto FAILURE;
	}
	if (jx_array_length(args) > 0) {
		err = "too many arguments for format specifier";
		goto FAILURE;
	}
	jx_delete(args);
	free(format);
	j = jx_string(result);
	free(result);
	return j;
FAILURE:
	jx_delete(args);
	free(result);
	free(format);
	FAIL(funcname, JX_BUILTIN_FORMAT, orig_args, err);
}

// see https://docs.python.org/2/library/functions.html#range
struct jx *jx_function_range(struct jx *args) {
	const char *funcname = "range";
	jx_int_t start, stop, step;

	assert(args);
	switch (jx_match_array(args, &start, JX_INTEGER, &stop, JX_INTEGER, &step, JX_INTEGER, NULL)) {
		case 1:
			stop = start;
			start = 0;
			step = 1;
			break;
		case 2: step = 1; break;
		case 3: break;
		default:
			FAIL(funcname, JX_BUILTIN_RANGE, args,
				"invalid arguments");
	}

	if (step == 0)
		FAIL(funcname, JX_BUILTIN_RANGE, args, "step must be nonzero");

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


struct jx *jx_function_join(struct jx *orig_args) {
	assert(orig_args);
	const char *funcname = "join";
	const char *err = NULL;
	char *result = NULL;

	struct jx *args = jx_copy(orig_args);
	struct jx *list = NULL;
	struct jx *delimeter= NULL;	

	int length = jx_array_length(args);
	if(length>2){
		err = "too many arguments to join";
		goto FAILURE;
	}
	else if(length<=0){
		err = "too few arguments to join";
		goto FAILURE;
	}
	
	list = jx_array_shift(args);
	if (!jx_istype(list, JX_ARRAY)){
		err = "A list must be the first argument in join";
		goto FAILURE;
	}
	
	if (length==2){
		delimeter  = jx_array_shift(args);
		if(!jx_istype(delimeter, JX_STRING)){
			err = "A delimeter must be defined as a string";
			goto FAILURE;
		}
	}
	
	result=xxstrdup("");	
	struct jx *value=NULL;
	for (size_t location = 0; (value = jx_array_shift(list)); location++){
		if (!jx_istype(value, JX_STRING)){
			err = "All array values must be strings";
			goto FAILURE;
		}
		if(location > 0){	
			if(delimeter) result = string_combine(result, delimeter->u.string_value);
			else result = string_combine(result, " ");
		}
		result = string_combine(result, value->u.string_value);
		jx_delete(value);
	}

	jx_delete(args);
	jx_delete(list);
	jx_delete(delimeter);
	assert(result);
	struct jx *j = jx_string(result);
	free(result);
	assert(j);
	return j;
	
	FAILURE:
	    jx_delete(args);
	    jx_delete(list);
		jx_delete(delimeter);
		free(result);
	    FAIL(funcname, JX_BUILTIN_JOIN, orig_args, err);
}

struct jx *jx_function_ceil(struct jx *orig_args) {
	assert(orig_args);
	const char *funcname = "ceil";
	const char *err = NULL;

	struct jx *args = jx_copy(orig_args);
	struct jx *val = jx_array_shift(args);
	struct jx *result = NULL;	

	int length = jx_array_length(orig_args);
	if(length>1){
		err = "too many arguments";
		goto FAILURE;
	} else if(length<=0){
		err = "too few arguments";
		goto FAILURE;
	}

	switch (val->type) {
		case JX_DOUBLE:
			result = jx_double(ceil(val->u.double_value));
			break;
		case JX_INTEGER:
			result = jx_integer(ceil(val->u.integer_value));
			break;
		default: 
			err = "arg of invalid type";
			goto FAILURE;
	}	

	jx_delete(args);
	jx_delete(val);
	return result;
	
	FAILURE:
	    jx_delete(args);
	    jx_delete(val);
	    FAIL(funcname, JX_BUILTIN_CEIL, orig_args, err);
}

struct jx *jx_function_floor(struct jx *orig_args) {
	assert(orig_args);
	const char *funcname = "floor";
	const char *err = NULL;

	struct jx *args = jx_copy(orig_args);
	struct jx *val = jx_array_shift(args);
	struct jx *result = NULL;	

	int length = jx_array_length(orig_args);
	if(length>1){
		err = "too many arguments";
		goto FAILURE;
	} else if(length<=0){
		err = "too few arguments";
		goto FAILURE;
	}

	switch (val->type) {
		case JX_DOUBLE:
			result = jx_double(floor(val->u.double_value));
			break;
		case JX_INTEGER:
			result = jx_integer(floor(val->u.integer_value));
			break;
		default: 
			err = "arg of invalid type";
			goto FAILURE;
	}	

	jx_delete(args);
	jx_delete(val);
	return result;
	
	FAILURE:
	    jx_delete(args);
	    jx_delete(val);
	    FAIL(funcname, JX_BUILTIN_FLOOR, orig_args, err);
}


struct jx *jx_function_basename(struct jx *args) {
	assert(args);
	const char *funcname = "basename";
	const char *err = NULL;

	struct jx *result = NULL;

	int length = jx_array_length(args);
	if (length != 1){
		err = "basename takes one argument";
		goto FAILURE;
	}

	struct jx *a = jx_array_index(args, 0);
	assert(a);

	if (!jx_istype(a, JX_STRING)) {
		err = "basename takes a string";
		goto FAILURE;
	}
	char *val = xxstrdup(a->u.string_value);
	result = jx_string(basename(val));
	free(val);

	return result;

	FAILURE:
	FAIL(funcname, JX_BUILTIN_FLOOR, args, err);
}
