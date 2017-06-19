/*
Copyright (C) 2016- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/
#include <stdio.h>

#include <string.h>
#include <stdarg.h>
#include "jx.h"
#include "jx_match.h"
#include "xxmalloc.h"
#include "stringtools.h"

// see https://docs.python.org/2/library/functions.html#range
struct jx *jx_function_range(struct jx *args) {
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
		default: {
			int code = 6;
			struct jx *err = jx_object(NULL);
			jx_insert_integer(err, "code", code);
			jx_insert(err, jx_string("function"),
				jx_operator(JX_OP_CALL,
					jx_function("range", NULL, NULL),
					jx_copy(args)));
			if (args->line)
				jx_insert_integer(err, "line", args->line);
			jx_insert_string(err, "message", "invalid arguments");
			jx_insert_string(err, "name", jx_error_name(code));
			jx_insert_string(err, "source", "jx_eval");
			return jx_error(err);
		}
	}

	if (step == 0) {
		int code = 6;
		struct jx *err = jx_object(NULL);
		jx_insert_integer(err, "code", code);
		jx_insert(err, jx_string("function"),
			jx_operator(JX_OP_CALL,
				jx_function("range", NULL, NULL),
				jx_copy(args)));
		if (args->line) jx_insert_integer(err, "line", args->line);
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
