/*
Copyright (C) 2018- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include <assert.h>
#include <string.h>
#include "xxmalloc.h"
#include "list.h"
#include "dag_node_hash.h"
#include "sha1.h"
#include "stringtools.h"

struct dag_node_hash {
	char *command;
	char *makeflow;
	char *cwd;
	struct list *sources;
	struct list *targets;
};

struct dag_node_hash *dag_node_hash_create(void) {
	struct dag_node_hash *h = xxcalloc(1, sizeof(*h));
	h->sources = list_create();
	h->targets = list_create();
	return h;
}

void dag_node_hash_command(struct dag_node_hash *h, const char *cmd) {
	if (!h) return;
	assert(cmd);
	assert(!h->command);
	assert(!h->makeflow);
	assert(!h->cwd);
	h->command = xxstrdup(cmd);
}

void dag_node_hash_makeflow(struct dag_node_hash *h, const char *dag, const char *cwd) {
	assert(h);
	assert(dag);
	assert(cwd);
	assert(!h->command);
	assert(!h->makeflow);
	assert(!h->cwd);
	h->makeflow = xxstrdup(dag);
	h->cwd = xxstrdup(cwd);
}


void dag_node_hash_source(struct dag_node_hash *h, const char *src) {
	assert(h);
	assert(src);
	list_push_tail(h->sources, xxstrdup(src));
}

void dag_node_hash_target(struct dag_node_hash *h, const char *tgt) {
	assert(h);
	assert(tgt);
	list_push_tail(h->targets, xxstrdup(tgt));
}

void dag_node_hash(struct dag_node_hash *h, unsigned char digest[SHA1_DIGEST_LENGTH]) {
	assert(h);
	assert(digest);
	assert(!(h->command && h->makeflow));

	char *tmp;
	struct list_cursor *cur;
	sha1_context_t ctx;
	sha1_init(&ctx);

	list_sort(h->sources, string_compare);
	list_sort(h->targets, string_compare);

	if (h->command) {
		sha1_update(&ctx, "C", 1);
		sha1_update(&ctx, h->command, strlen(h->command));
		sha1_update(&ctx, "\0", 1);
	}
	if (h->makeflow) {
		assert(h->cwd);
		sha1_update(&ctx, "M", 1);
		sha1_update(&ctx, h->makeflow, strlen(h->makeflow));
		sha1_update(&ctx, "\0", 1);
		sha1_update(&ctx, h->cwd, strlen(h->cwd));
		sha1_update(&ctx, "\0", 1);
	}

	cur = list_cursor_create(h->sources);
	for (list_seek(cur, 0); list_get(cur, (void **) &tmp); list_next(cur)) {
		sha1_update(&ctx, "S", 1);
		sha1_update(&ctx, tmp, strlen(tmp));
		sha1_update(&ctx, "\0", 1);
	}
	list_cursor_destroy(cur);

	cur = list_cursor_create(h->targets);
	for (list_seek(cur, 0); list_get(cur, (void **) &tmp); list_next(cur)) {
		sha1_update(&ctx, "T", 1);
		sha1_update(&ctx, tmp, strlen(tmp));
		sha1_update(&ctx, "\0", 1);
	}
	list_cursor_destroy(cur);

	sha1_final(digest, &ctx);

	free(h->command);
	free(h->makeflow);
	free(h->cwd);
	list_free(h->sources);
	list_free(h->targets);
	list_delete(h->sources);
	list_delete(h->targets);
	free(h);
}
