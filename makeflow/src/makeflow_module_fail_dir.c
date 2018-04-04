
#include "makeflow_hook.h"
#include "xxmalloc.h"
#include "debug.h"
#include "stringtools.h"
#include "makeflow_gc.h"
#include "makeflow_log.h"
#include "dag.h"
#include "dag_node.h"
#include "dag_file.h"
#include "jx.h"

#include <assert.h>
#include <unistd.h>
#include <errno.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>

#define FAIL_DIR "makeflow.failed.%d"

struct dag_file *makeflow_module_lookup_fail_dir(struct dag *d, struct batch_queue *q, const char *path) {
	assert(d);
	assert(q);
	assert(path);
	struct stat buf;
	struct dag_file *f = dag_file_from_name(d, path);
	if (f) {
		if (f->type == DAG_FILE_TYPE_INPUT) {
			debug(D_MAKEFLOW_HOOK,
					"skipping %s since it's specified as an input",
					path);
			return NULL;
		}
		return f;
	} else {
		if (!batch_fs_stat(q, path, &buf)) {
			debug(D_MAKEFLOW_HOOK,
					"skipping %s since it already exists",
					path);
			return NULL;
		}
		return dag_file_lookup_or_create(d, path);
	}
}

int makeflow_module_move_fail_file(struct dag *d, struct dag_node *n, struct batch_queue *q, struct dag_file *f) {
	assert(d);
	assert(n);
	assert(q);
	assert(f);

	char *failout = string_format( FAIL_DIR "/%s", n->nodeid, f->filename);
	struct dag_file *o = makeflow_module_lookup_fail_dir(d, q, failout);
	if (o) {
		if(f->state == DAG_FILE_STATE_DELETE){
			debug(D_MAKEFLOW_HOOK, "File %s has already been deleted by another hook",f->filename);
			return MAKEFLOW_HOOK_SUCCESS;
		}

		if (batch_fs_rename(q, f->filename, o->filename) < 0) {
			debug(D_MAKEFLOW_HOOK, "Failed to rename %s -> %s: %s",
					f->filename, o->filename, strerror(errno));
		} else {
			makeflow_log_file_state_change(d, f, DAG_FILE_STATE_DELETE);
			debug(D_MAKEFLOW_HOOK, "Renamed %s -> %s",
					f->filename, o->filename);
			return MAKEFLOW_HOOK_SUCCESS;
		}
	} else {
		fprintf(stderr, "Skipping rename %s -> %s", f->filename, failout);
	}
	free(failout);
	return MAKEFLOW_HOOK_FAILURE;
}

int makeflow_module_prep_fail_dir(struct dag *d, struct dag_node *n, struct batch_queue *q) {
	assert(d);
	assert(n);
	assert(q);

	int rc = MAKEFLOW_HOOK_FAILURE;
	char *faildir = string_format(FAIL_DIR, n->nodeid);
	struct dag_file *f = makeflow_module_lookup_fail_dir(d, q, faildir);
	if (!f) goto FAILURE;

	if (makeflow_clean_file(d, q, f)) {
		debug(D_MAKEFLOW_HOOK, "Unable to clean failed output");
		goto FAILURE;
	}
	if (mkdir(f->filename, 0755)) {
		debug(D_MAKEFLOW_HOOK, "Unable to create failed output directory: %s", strerror(errno));
		goto FAILURE;
	}

	makeflow_log_file_state_change(d, f, DAG_FILE_STATE_COMPLETE);
	fprintf(stderr, "rule %d failed, moving any outputs to %s\n",
			n->nodeid, faildir);
	rc = MAKEFLOW_HOOK_SUCCESS;
FAILURE:
	free(faildir);
	return rc;
}

static int node_success( void * instance_struct, struct dag_node *n, struct batch_task *task){
	struct dag *d = n->d;
	struct batch_queue *q = makeflow_get_remote_queue();

	assert(d);
	assert(n);
	assert(q);

	int rc = MAKEFLOW_HOOK_SUCCESS;
	char *faildir = string_format(FAIL_DIR, n->nodeid);
	struct dag_file *f = dag_file_from_name(d, faildir);
	free(faildir);

	if (f && makeflow_clean_file(n->d, q, f)) {
		debug(D_MAKEFLOW_HOOK, "Unable to clean failed output");
		rc = MAKEFLOW_HOOK_FAILURE;
	}
	return rc;
}

static int node_fail( void * instance_struct, struct dag_node *n, struct batch_task *task){
	struct batch_file *bf = NULL;
	struct dag_file *df = NULL;
	int prep_failed =  makeflow_module_prep_fail_dir(n->d, n, task->queue); 
	if (prep_failed) { 
		debug(D_ERROR|D_MAKEFLOW_HOOK, "rule %d failed, cannot move outputs\n", 
					n->nodeid); 
		return MAKEFLOW_HOOK_FAILURE;
	}

	/* Move temp inputs(wrappers) of failed node. Mark deleted if successful rename. */
	list_first_item(task->input_files);
	while((bf = list_next_item(task->input_files))) {
		df = dag_file_lookup_or_create(n->d, bf->outer_name);
		if(df->type == DAG_FILE_TYPE_TEMP) {
			makeflow_module_move_fail_file(n->d, n, makeflow_get_queue(n), df);
		}
	}

	/* Move all outputs of failed node. Mark deleted if successful rename. */
	list_first_item(task->output_files);
	while((bf = list_next_item(task->output_files))) {
		df = dag_file_lookup_or_create(n->d, bf->outer_name);
		makeflow_module_move_fail_file(n->d, n, makeflow_get_queue(n), df);
	}

	return MAKEFLOW_HOOK_SUCCESS;
}

struct makeflow_hook makeflow_hook_fail_dir = {
	.module_name = "Fail Dir",

	.node_success = node_success,
	.node_fail = node_fail,

};


