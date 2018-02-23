/*
Copyright (C) 2018- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include <assert.h>
#include <inttypes.h>
#include <stdbool.h>
#include <string.h>

#include "list.h"
#include "jx_getopt.h"
#include "stringtools.h"
#include "xxmalloc.h"

static struct list *jx_argv_stack = NULL;
static struct list *jx_argv = NULL;
struct jx *jx_optarg = NULL;

void jx_getopt_push(struct jx *j) {
	assert(j);
	if (!jx_argv) {
		jx_argv = list_create();
	}
	if (!jx_argv_stack) {
		jx_argv_stack = list_create();
	}
	list_push_head(jx_argv, jx_copy(j));
	list_push_head(jx_argv_stack, NULL);
}

static const struct option *option_from_name(const struct option *opt, const char *name, int *indexptr) {
	assert(opt);
	assert(name);
	for (int i = 0; opt[i].name; ++i) {
		if (!strcmp(name, opt[i].name)) {
			if (indexptr) {
				*indexptr = i;
			}
			return &opt[i];
		}
	}
	return NULL;
}

static char *optarg_from_jx(struct jx *j) {
	assert(j);
	switch (j->type) {
	case JX_BOOLEAN:
		return j->u.boolean_value ? xxstrdup("true") : xxstrdup("false");
	case JX_INTEGER:
		return string_format("%" PRIi64, j->u.integer_value);
	case JX_DOUBLE:
		return string_format("%g", j->u.double_value);
	case JX_STRING:
		return xxstrdup(j->u.string_value);
	default:
		return NULL;
	}
}

static bool wrong_arg_type(const struct option *opt, struct jx *j) {
	assert(opt);
	assert(j);
	switch (opt->has_arg) {
	case no_argument:
		if (!jx_istype(j, JX_NULL)) return true;
		break;
	case required_argument:
		if (jx_istype(j, JX_NULL)) return true;
		break;
	default:
		break;
	} 
	return false;
}

static int write_opt_val(const struct option *opt) {
	assert(opt);
	if (opt->flag) {
		*opt->flag = opt->val;
	}
	return opt->val;
}

int jx_getopt(int argc, char *const argv[], const char *optstring, const struct option *longopts, int *longindex) {
	static char *val = NULL;
	struct jx *jx_val = NULL;

	free(val);
	val = NULL;
	jx_delete(jx_val);
	jx_val = NULL;
	if (!jx_argv) {
		jx_argv = list_create();
	}

	struct jx *head = list_peek_head(jx_argv);
	if (head) {
		assert(jx_argv_stack);
		void *i = list_pop_head(jx_argv_stack);
		const char *key = jx_iterate_keys(head, &i);
		if (key) {
			list_push_head(jx_argv_stack, i);
			const struct option *opt = option_from_name(longopts, key, longindex);
			if (!opt) return 0;
			jx_val = jx_copy(jx_get_value(&i));
			assert(jx_val);
			if (wrong_arg_type(opt, jx_val)) return 0;
			val = optarg_from_jx(jx_val);
			optarg = val;
			jx_optarg = jx_val;
			return write_opt_val(opt);
		} else {
			jx_delete(list_pop_head(jx_argv));
			return jx_getopt(argc, argv, optstring, longopts, longindex);
		}
	} else {
		return getopt_long(argc, argv, optstring, longopts, longindex);
	}
}
