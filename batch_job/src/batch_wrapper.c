/*
Copyright (C) 2018- The University of Notre Dame
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

struct batch_wrapper {
	struct list *pre;
	struct list *post;
	struct list *argv;
	char *cmd;
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

char *batch_wrapper_write(struct batch_wrapper *w, struct batch_task *task, const char *prefix) {
	assert(w);
	assert(task);
	assert(prefix);

	char *name = string_format("%sXXXXXX", prefix);
	int wrapper_fd = mkstemp(name);
	if (wrapper_fd == -1)
			fatal("failed to create wrapper: %s", strerror(errno));

	batch_task_add_input_file(task, name, NULL);

	if (fchmod(wrapper_fd, 0700) == -1)
			fatal("failed to make wrapper executable: %s", strerror(errno));

	FILE *wrapper = fdopen(wrapper_fd, "w");
	if (!wrapper)
			fatal("failed to open wrapper: %s", strerror(errno));

	fprintf(wrapper, "#!/bin/sh\n");
	fprintf(wrapper, "set -e\n");

	if (w->post) {
		// function name unlikely to collide with user's stuff
		fprintf(wrapper, "CLEANUP_76tnb43rr7 () {\n");
		list_first_item(w->post);
		for (const char *c; (c = list_next_item(w->post));) {
			fprintf(wrapper, "eval %s\n", c);
		}
		fprintf(wrapper, "}\n");
		fprintf(wrapper, "trap CLEANUP_76tnb43rr7 EXIT INT TERM\n");
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

/* vim: set noexpandtab tabstop=4: */
