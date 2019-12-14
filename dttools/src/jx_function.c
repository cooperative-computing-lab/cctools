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
#include <libgen.h>
#include <sys/types.h>
#include <dirent.h>
#include <errno.h>
#include <ctype.h>

#include "jx.h"
#include "jx_match.h"
#include "jx_print.h"
#include "stringtools.h"
#include "xxmalloc.h"

// FAIL(const char *name, struct jx *args, const char *message)
#define FAIL(name, args, message) \
	do { \
		assert(name); \
		assert(args); \
		assert(message); \
		return jx_error(jx_format( \
			"function %s on line %d: %s", \
			name, \
			args->line, \
			message \
		)); \
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
	FAIL(funcname, orig_args, err);
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
			FAIL(funcname, args, "invalid arguments");
	}

	if (step == 0)
		FAIL(funcname, args, "step must be nonzero");

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
	FAIL(funcname, orig_args, err);
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
	FAIL(funcname, orig_args, err);
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
	FAIL(funcname, orig_args, err);
}


struct jx *jx_function_basename(struct jx *args) {
	assert(args);
	const char *funcname = "basename";
	const char *err = NULL;

	struct jx *result = NULL;

	int length = jx_array_length(args);
	if (length < 1){
		err = "one argument is required";
		goto FAILURE;
	}
	if (length > 2){
		err = "only two arguments are allowed";
		goto FAILURE;
	}

	struct jx *path = jx_array_index(args, 0);
	assert(path);
	struct jx *suffix = jx_array_index(args, 1);

	if (!jx_istype(path, JX_STRING)) {
		err = "path must be a string";
		goto FAILURE;
	}
	if (suffix && !jx_istype(suffix, JX_STRING)) {
		err = "suffix must be a string";
		goto FAILURE;
	}

	char *tmp = xxstrdup(path->u.string_value);
	char *b = basename(tmp);
	char *s = suffix ? suffix->u.string_value : NULL;
	if (s && string_suffix_is(b, s)) {
		result = jx_string(string_front(b, strlen(b) - strlen(s)));
	} else {
		result = jx_string(b);
	}
	free(tmp);

	return result;

	FAILURE:
	FAIL(funcname, args, err);
}

struct jx *jx_function_dirname(struct jx *args) {
	assert(args);
	const char *funcname = "dirname";
	const char *err = NULL;

	struct jx *result = NULL;

	int length = jx_array_length(args);
	if (length != 1){
		err = "dirname takes one argument";
		goto FAILURE;
	}

	struct jx *a = jx_array_index(args, 0);
	assert(a);

	if (!jx_istype(a, JX_STRING)) {
		err = "dirname takes a string";
		goto FAILURE;
	}
	char *val = xxstrdup(a->u.string_value);
	result = jx_string(dirname(val));
	free(val);

	return result;

	FAILURE:
	FAIL(funcname, args, err);
}

struct jx *jx_function_listdir(struct jx *args) {
	assert(args);

	int length = jx_array_length(args);
	if (length != 1) return jx_error(jx_format(
		"function listdir on line %d takes one argument, %d given",
		args->line,
		length
	));

	struct jx *a = jx_array_index(args, 0);
	assert(a);

	if (!jx_istype(a, JX_STRING)) return jx_error(jx_format(
		"function listdir on line %d takes a string path",
		args->line
	));

	DIR *d = opendir(a->u.string_value);
	if (!d) return jx_error(jx_format(
		"function listdir on line %d: %s, %s",
		args->line,
		a->u.string_value,
		strerror(errno)
	));

	struct jx *out = jx_array(NULL);
	for (struct dirent *e; (e = readdir(d));) {
		if (!strcmp(".", e->d_name)) continue;
		if (!strcmp("..", e->d_name)) continue;
		jx_array_append(out, jx_string(e->d_name));
	}
	closedir(d);
	return out;
}

struct jx *jx_function_escape(struct jx *args) {
	assert(args);
	const char *funcname = "escape";
	const char *err = NULL;

	struct jx *result = NULL;

	int length = jx_array_length(args);
	if (length != 1){
		err = "escape takes one argument";
		goto FAILURE;
	}

	struct jx *a = jx_array_index(args, 0);
	assert(a);

	if (!jx_istype(a, JX_STRING)) {
		err = "escape takes a string";
		goto FAILURE;
	}
	char *val = string_escape_shell(a->u.string_value);
	result = jx_string(val);
	free(val);

	return result;

	FAILURE:
	FAIL(funcname, args, err);
}

static struct jx *expand_template(struct jx *template, struct jx *ctx, struct jx *overrides) {
	const char *funcname = "template";

	assert(template);
	assert(jx_istype(template, JX_STRING));
	assert(!ctx || jx_istype(ctx, JX_OBJECT));
	assert(!overrides || jx_istype(overrides, JX_OBJECT));

	const char *message = NULL;
	char *s = template->u.string_value;

	buffer_t buf;
	buffer_t var;
	buffer_init(&buf);
	buffer_init(&var);

	while (*s) {
		// regular character
		if (*s != '{' && *s != '}') {
			buffer_putlstring(&buf, s, 1);
			s++;
			continue;
		}
		// quoted {
		if (*s == '{' && *(s+1) == '{') {
			buffer_putliteral(&buf, "{");
			s += 2;
			continue;
		}
		// quoted }
		if (*s == '}' && *(s+1) == '}') {
			buffer_putliteral(&buf, "}");
			s += 2;
			continue;
		}

		// got to here, so must be an expression
		if (*s != '{') {
			message = "unmatched } in template";
			goto FAIL;
		}
		s++;
		while (isspace(*s)) s++; // eat leading whitespace

		if (*s == 0) {
			message = "unterminated template expression";
			goto FAIL;
		}
		if (!isalpha(*s) && *s != '_') {
			message = "invalid template; each expression must be a single identifier";
			goto FAIL;
		}
		buffer_putlstring(&var, s, 1); // copy identifier to buffer
		s++;
		while (isalnum(*s) || *s == '_') {
			buffer_putlstring(&var, s, 1);
			s++;
		}
		while (isspace(*s)) s++; // eat trailing whitespace

		if (*s == 0) {
			message = "unterminated template expression";
			goto FAIL;
		}
		if (*s != '}') {
			message = "invalid template; each expression must be a single identifier";
			goto FAIL;
		}
		s++;
		struct jx *k = jx_lookup(overrides, buffer_tostring(&var));
		if (!k) {
			k = jx_lookup(ctx, buffer_tostring(&var));
		}
		if (!k) {
			message = "undefined symbol in template";
			goto FAIL;
		}
		switch (k->type) {
			case JX_INTEGER:
			case JX_DOUBLE:
				jx_print_buffer(k, &buf);
				break;
			case JX_STRING:
				buffer_putstring(&buf, k->u.string_value);
				break;
			default:
				message = "cannot format expression in template";
				goto FAIL;
		}
		buffer_rewind(&var, 0);
	}

FAIL:
	buffer_free(&buf);
	buffer_free(&var);
	if (message) {
		FAIL(funcname, template, message);
	}
	return jx_string(buffer_tostring(&buf));
}

struct jx *jx_function_template(struct jx *args, struct jx *ctx) {
	assert(args);
	assert(jx_istype(args, JX_ARRAY));
	assert(!ctx || jx_istype(ctx, JX_OBJECT));

	const char *funcname = "template";
	struct jx *template = jx_array_index(args, 0);
	struct jx *overrides = jx_array_index(args, 1);

	switch (jx_array_length(args)) {
	case 0:
		FAIL(funcname, args, "template string is required");
	case 2:
		if (!jx_istype(overrides, JX_OBJECT)) {
			FAIL(funcname, args, "overrides must be an object");
		}
		/* Else falls through. */
	case 1:
		if (!jx_istype(template, JX_STRING)) {
			FAIL(funcname, args, "template must be a string");
		}
		return expand_template(template, ctx, overrides);
	default:
		FAIL(funcname, args, "at most two arguments are allowed");
	}
}

struct jx *jx_function_len(struct jx *args){

	assert(args);
	assert(jx_istype(args, JX_ARRAY));

	struct jx* item = jx_array_index(args, 0);
	assert(jx_istype(item, JX_ARRAY));

	int length = jx_array_length(item);

	return jx_integer(length);

}

/*vim: set noexpandtab tabstop=4: */
