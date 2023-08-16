/*
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include <assert.h>
#include <string.h>
#include <errno.h>
#include <stdlib.h>
#include <stdio.h>

#include "batch_wrapper.h"
#include "list.h"
#include "debug.h"
#include "stringtools.h"
#include "xxmalloc.h"
#include "random.h"

struct batch_wrapper {
	struct list *pre;
	struct list *post;
	struct list *argv;
	char *cmd;
	char *prefix;
};

struct batch_wrapper *batch_wrapper_create(void) {
	return xxcalloc(1, sizeof(struct batch_wrapper));
}

void batch_wrapper_delete(struct batch_wrapper *w) {
	if (!w) return;
	list_free(w->pre);
	list_delete(w->pre);
	list_free(w->post);
	list_delete(w->post);
	list_free(w->argv);
	list_delete(w->argv);
	free(w->cmd);
	free(w->prefix);
	free(w);
}

void batch_wrapper_pre(struct batch_wrapper *w, const char *cmd) {
	assert(w);
	assert(cmd);

	if (!w->pre) {
		w->pre = list_create();
		assert(w->pre);
	}
	int rc = list_push_tail(w->pre, string_escape_shell(cmd));
	assert(rc == 1);
}

void batch_wrapper_argv(struct batch_wrapper *w, char *const argv[]) {
	assert(w);
	assert(argv);
	assert(!w->argv);
	assert(!w->cmd);

	w->argv = list_create();
	assert(w->argv);
	for (unsigned i = 0; argv[i]; i++) {
		int rc = list_push_tail(w->argv, string_escape_shell(argv[i]));
		assert(rc == 1);
	}
}

void batch_wrapper_args(struct batch_wrapper *w, char *const args[]) {
	assert(w);
	assert(args);
	assert(!w->argv);
	assert(!w->cmd);

	w->argv = list_create();
	assert(w->argv);
	for (unsigned i = 0; args[i]; i++) {
		int rc = list_push_tail(w->argv, string_quote_shell(args[i]));
		assert(rc == 1);
	}
}

void batch_wrapper_cmd(struct batch_wrapper *w, const char *cmd) {
	assert(w);
	assert(cmd);
	assert(!w->argv);
	assert(!w->cmd);

	w->cmd = string_escape_shell(cmd);
}

void batch_wrapper_post(struct batch_wrapper *w, const char *cmd) {
	assert(w);
	assert(cmd);

	if (!w->post) {
		w->post = list_create();
		assert(w->post);
	}
	int rc = list_push_tail(w->post, string_escape_shell(cmd));
	assert(rc == 1);
}

void batch_wrapper_prefix(struct batch_wrapper *w, const char *prefix) {
	assert(w);
	assert(prefix);
	assert(!w->prefix);
	w->prefix = xxstrdup(prefix);
}

char *batch_wrapper_write(struct batch_wrapper *w, struct batch_task *task) {
	assert(w);
	assert(task);

	char *name = string_format("%s_XXXXXX", w->prefix ? w->prefix : "./wrapper");
	int wrapper_fd = mkstemp(name);
	if (wrapper_fd == -1) {
		int saved_errno = errno;
		debug(D_NOTICE|D_BATCH, "failed to create wrapper: %s", strerror(errno));
		free(name);
		errno = saved_errno;
		return NULL;
	}

	batch_task_add_input_file(task, name, NULL);

	if (fchmod(wrapper_fd, 0700) == -1) {
		int saved_errno = errno;
		debug(D_NOTICE|D_BATCH, "failed to make wrapper executable: %s", strerror(errno));
		free(name);
		close(wrapper_fd);
		errno = saved_errno;
		return NULL;
	}

	FILE *wrapper = fdopen(wrapper_fd, "w");
	if (!wrapper) {
		int saved_errno = errno;
		debug(D_NOTICE|D_BATCH, "failed to open wrapper: %s", strerror(errno));
		free(name);
		close(wrapper_fd);
		errno = saved_errno;
		return NULL;
	}

	fprintf(wrapper, "#!/bin/sh\n");
	fprintf(wrapper, "set -e\n");

	if (w->post) {
		char fresh[16];
		random_hex(fresh, sizeof(fresh));
		fprintf(wrapper, "CLEANUP_%s () {\n", fresh);
		list_first_item(w->post);
		for (const char *c; (c = list_next_item(w->post));) {
			fprintf(wrapper, "eval %s\n", c);
		}
		fprintf(wrapper, "}\n");
		fprintf(wrapper, "trap CLEANUP_%s EXIT INT TERM\n", fresh);
	}

	if (w->pre) {
		list_first_item(w->pre);
		for (const char *c; (c = list_next_item(w->pre));) {
			fprintf(wrapper, "eval %s\n", c);
		}
	}

	if (w->argv) {
		list_first_item(w->argv);
		for (const char *c; (c = list_next_item(w->argv));) {
			fprintf(wrapper, " %s", c);
		}
		fprintf(wrapper, "\n");
	}

	if (w->cmd) {
		fprintf(wrapper, "eval %s\n", w->cmd);
	}

	fclose(wrapper);
	return name;
}

static void free_argv(char *argv[]) {
	if (!argv) return;
	for (size_t i = 0; argv[i]; i++)
		free(argv[i]);
	free(argv);
}

static char **jx_array_to_argv(struct batch_task *t, struct jx *argv) {
	if (!jx_istype(argv, JX_ARRAY)) {
		debug(D_NOTICE|D_BATCH, "arguments must be in an array");
		return NULL;
	}

	int len = jx_array_length(argv);
	assert(len >= 0);
	char **ptrs = calloc(len + 1, sizeof(char *));

	struct jx *j;
	unsigned i = 0;
	for (void *x = NULL; (j = jx_iterate_array(argv, &x));) {
		if (jx_istype(j, JX_STRING)) {
			ptrs[i] = xxstrdup(j->u.string_value);
		} else if (jx_istype(j, JX_OBJECT)) {
			ptrs[i] = batch_wrapper_expand(t, j);
			if (!ptrs[i]) {
				free_argv(ptrs);
				return NULL;
			}
		} else {
			free_argv(ptrs);
			debug(D_NOTICE|D_BATCH, "arguments must be strings");
			return NULL;
		}
		++i;
	}

	return ptrs;
}

char *batch_wrapper_expand(struct batch_task *t, struct jx *spec) {
	assert(t);
	assert(spec);

	struct jx *j;
	unsigned commands = 0;
	char *result = NULL;
	struct batch_wrapper *w = batch_wrapper_create();

	if (!jx_istype(spec, JX_OBJECT)) {
		debug(D_NOTICE|D_BATCH, "wrapper command spec must be a JX object");
		goto FAIL;
	}

	struct jx *prefix = jx_lookup(spec, "prefix");
	if (prefix) {
		if (!jx_istype(prefix, JX_STRING)) {
			debug(D_NOTICE|D_BATCH, "prefix must be a string");
			goto FAIL;
		}
		batch_wrapper_prefix(w, prefix->u.string_value);
	}

	struct jx *pre = jx_lookup(spec, "pre");
	if (pre) {
		if (!jx_istype(pre, JX_ARRAY)) {
			debug(D_NOTICE|D_BATCH, "pre commands must be specified in an array");
			goto FAIL;
		}
		for (void *i = NULL; (j = jx_iterate_array(pre, &i));) {
			if (!jx_istype(j, JX_STRING)) {
				debug(D_NOTICE|D_BATCH, "pre commands must be strings");
				goto FAIL;
			}
			batch_wrapper_pre(w, j->u.string_value);
		}
	}

	struct jx *post = jx_lookup(spec, "post");
	if (post) {
		if (!jx_istype(post, JX_ARRAY)) {
			debug(D_NOTICE|D_BATCH, "post commands must be specified in an array");
			goto FAIL;
		}
		for (void *i = NULL; (j = jx_iterate_array(post, &i));) {
			if (!jx_istype(j, JX_STRING)) {
				debug(D_NOTICE|D_BATCH, "post commands must be strings");
				goto FAIL;
			}
			batch_wrapper_post(w, j->u.string_value);
		}
	}

	struct jx *argv = jx_lookup(spec, "argv");
	if (argv) {
		if (commands++) {
			debug(D_NOTICE|D_BATCH, "only one command is allowed");
			goto FAIL;
		}

		char **ptrs = jx_array_to_argv(t, argv);
		if (ptrs) {
			batch_wrapper_argv(w, ptrs);
			free_argv(ptrs);
		} else {
			free_argv(ptrs);
			goto FAIL;
		}
	}

	struct jx *args = jx_lookup(spec, "args");
	if (args) {
		if (commands++) {
			debug(D_NOTICE|D_BATCH, "only one command is allowed");
			goto FAIL;
		}

		char **ptrs = jx_array_to_argv(t, args);
		if (ptrs) {
			batch_wrapper_args(w, ptrs);
			free_argv(ptrs);
		} else {
			free_argv(ptrs);
			goto FAIL;
		}
	}

	struct jx *cmd = jx_lookup(spec, "cmd");
	if (cmd) {
		if (commands++) {
			debug(D_NOTICE|D_BATCH, "only one command is allowed");
			goto FAIL;
		}

		if (jx_istype(cmd, JX_OBJECT)) {
			char *nested = batch_wrapper_expand(t, cmd);
			if (!nested) goto FAIL;
			batch_wrapper_cmd(w, nested);
			free(nested);
		} else if (jx_istype(cmd, JX_STRING)) {
			batch_wrapper_cmd(w, cmd->u.string_value);
		} else {
			debug(D_NOTICE|D_BATCH, "cmd must be a string");
			goto FAIL;
		}
	}

	if (commands != 1) {
		debug(D_NOTICE|D_BATCH, "a command is required to generate a wrapper");
		goto FAIL;
	}

	result = batch_wrapper_write(w, t);
FAIL:
	batch_wrapper_delete(w);
	return result;
}

/* vim: set noexpandtab tabstop=8: */
