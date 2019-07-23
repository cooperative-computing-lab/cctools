/*
Copyright (C) 2015- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "debug.h"
#include "xxmalloc.h"
#include "set.h"
#include "timestamp.h"
#include "host_disk_info.h"
#include "stringtools.h"
#include "copy_tree.h"
#include "unlink_recursive.h"
#include "path.h"

#include "dag.h"
#include "makeflow_log.h"
#include "makeflow_wrapper.h"
#include "makeflow_gc.h"

#include <assert.h>
#include <dirent.h>
#include <sys/types.h>
#include <limits.h>
#include <string.h>
#include <errno.h>
#include <stdlib.h>

/*
XXX This module is implemented using Unix operations, and so
only make sense for batch modes that rely on the local disk.
An improved implementation should pass all filesystem operations
through the batch_job interface, which must be expanded to
include measuring available space and inodes consumed.
*/

/* XXX this should be configurable. */
#define	MAKEFLOW_MIN_SPACE 10*1024*1024	/* 10 MB */

#define FAIL_DIR "makeflow.failed.%d"

static int makeflow_gc_collected = 0;

/*
Return true if disk space falls below the fixed minimum. (inexpensive!)
XXX this value should be configurable.
*/

static int directory_low_disk( const char *path, uint64_t size )
{
	UINT64_T avail, total;

	if(host_disk_info_get(path, &avail, &total) >= 0)
		return avail <= size;

	return 0;
}

/* Prepare the dag for garbage collection by identifying which files may or may not be gcd. */

void makeflow_parse_input_outputs( struct dag *d )
{
	/* Check if GC_*_LIST is specified and warn user about deprecated usage */
	char *collect_list   = dag_variable_lookup_global_string("GC_COLLECT_LIST" , d);
	if(collect_list)
		debug(D_NOTICE, "GC_COLLECT_LIST is specified: Please refer to manual about MAKEFLOW_INPUTS/OUTPUTS");

	char *preserve_list  = dag_variable_lookup_global_string("GC_PRESERVE_LIST", d);
	if(preserve_list)
		debug(D_NOTICE, "GC_PRESERVE_LIST is specified: Please refer to manual about MAKEFLOW_INPUTS/OUTPUTS");

	/* Parse INPUT and OUTPUT lists */
	struct dag_file *f;
	char *filename;

	int i, argc;
	char **argv;

	char *input_list  = dag_variable_lookup_global_string("MAKEFLOW_INPUTS" , d);
	char *output_list = dag_variable_lookup_global_string("MAKEFLOW_OUTPUTS", d);

	if(input_list) {
		/* add collect_list, for sink_files that should be removed */
		string_split_quotes(input_list, &argc, &argv);
		for(i = 0; i < argc; i++) {
			d->completed_files += 1;
			f = dag_file_lookup_or_create(d, argv[i]);
			set_insert(d->inputs, f);
			f->type = DAG_FILE_TYPE_INPUT;
			debug(D_MAKEFLOW_RUN, "Added %s to input list", f->filename);
		}
		free(input_list);
		free(argv);
	} else {
		debug(D_MAKEFLOW_RUN, "MAKEFLOW_INPUTS is not specified");
	}
	/* add all source files */
	hash_table_firstkey(d->files);
	while((hash_table_nextkey(d->files, &filename, (void **) &f)))
		if(dag_file_is_source(f)) {
			set_insert(d->inputs, f);
			f->type = DAG_FILE_TYPE_INPUT;
			debug(D_MAKEFLOW_RUN, "Added %s to input list", f->filename);
		}

	if(output_list) {
		/* remove files from preserve_list */
		string_split_quotes(output_list, &argc, &argv);
		for(i = 0; i < argc; i++) {
			/* Must initialize to non-zero for hash_table functions to work properly. */
			f = dag_file_lookup_or_create(d, argv[i]);
			set_insert(d->outputs, f);
			f->type = DAG_FILE_TYPE_OUTPUT;
			debug(D_MAKEFLOW_RUN, "Added %s to output list", f->filename);
		}
		free(output_list);
		free(argv);
	} else {
		debug(D_MAKEFLOW_RUN, "MAKEFLOW_OUTPUTS is not specified");
		/* add all sink if OUTPUTS not specified */
		hash_table_firstkey(d->files);
		while((hash_table_nextkey(d->files, &filename, (void **) &f)))
			if(dag_file_is_sink(f)) {
				set_insert(d->outputs, f);
				f->type = DAG_FILE_TYPE_OUTPUT;
				debug(D_MAKEFLOW_RUN, "Added %s to output list", f->filename);
			}
	}
}

/* Clean a specific file, while emitting an appropriate message. */

int makeflow_clean_file( struct dag *d, struct batch_queue *queue, struct dag_file *f)
{
	if(!f || f->type == DAG_FILE_TYPE_GLOBAL)
		return 1;

	makeflow_hook_file_clean(f);

	if(batch_fs_unlink(queue, f->filename) == 0) {
		debug(D_MAKEFLOW_RUN, "File deleted %s\n", f->filename);
		d->total_file_size -= f->actual_size;
		makeflow_log_file_state_change(d, f, DAG_FILE_STATE_DELETE);
		makeflow_hook_file_deleted(f);

	} else if(errno != ENOENT) {
		if(f->state == DAG_FILE_STATE_EXPECT || dag_file_should_exist(f))
			makeflow_log_file_state_change(d, f, DAG_FILE_STATE_DELETE);

		debug(D_MAKEFLOW_RUN, "Makeflow: Couldn't delete %s: %s\n", f->filename, strerror(errno));
		return 1;
	}
	return 0;
}

/* Clean an individual node.  This only applies if the node itself is a workflow,
in which case, we want to construct its full command, add the clean option and
then run the command. */

extern struct batch_task * makeflow_node_to_task( struct dag_node *node, struct batch_queue *queue, int send_env);

void makeflow_clean_node(struct dag *d, struct batch_queue *queue, struct dag_node *n)
{
	if(n->type==DAG_NODE_TYPE_WORKFLOW) {
		printf("cleaning sub-workflow %s\n",n->workflow_file);
		struct batch_task *task = makeflow_node_to_task(n,queue,1);
		char *command = string_format("%s --clean",task->command);
		printf("%s\n",command);
		jx_export(task->envlist);
		system(command);
		printf("done cleaning sub-workflow %s\n",n->workflow_file);
		free(command);
		batch_task_delete(task);
	}
}

/* Clean the entire dag by cleaning all nodes. */

int makeflow_clean(struct dag *d, struct batch_queue *queue, makeflow_clean_depth clean_depth)
{
	struct dag_file *f;
	char *name;

	hash_table_firstkey(d->files);
	while(hash_table_nextkey(d->files, &name, (void **) &f)) {

		/* We have a record of the file, but it is no longer created or used so delete */
		if(dag_file_is_source(f) && dag_file_is_sink(f) && !set_lookup(d->inputs, f))
			makeflow_clean_file(d, queue, f);

		if(dag_file_is_source(f)) {
			if(f->source && (clean_depth == MAKEFLOW_CLEAN_CACHE || clean_depth == MAKEFLOW_CLEAN_ALL)) { 
				/* this file is specified in the mountfile */
				if(makeflow_clean_mount_target(f->filename)) {
					fprintf(stderr, "Failed to remove %s!\n", f->filename);
					return -1;
				}
			}
			continue;
		}

		if(clean_depth == MAKEFLOW_CLEAN_ALL){
			makeflow_clean_file(d, queue, f);
		} else if(set_lookup(d->outputs, f) && (clean_depth == MAKEFLOW_CLEAN_OUTPUTS)) {
			makeflow_clean_file(d, queue, f);
		} else if(!set_lookup(d->outputs, f) && (clean_depth == MAKEFLOW_CLEAN_INTERMEDIATES)){
			makeflow_clean_file(d, queue, f);
		}
	}

	/* clean up the cache dir created due to the usage of mountfile */
	if(clean_depth == MAKEFLOW_CLEAN_CACHE || clean_depth == MAKEFLOW_CLEAN_ALL) {
		if(d->cache_dir && unlink_recursive(d->cache_dir)) {
			fprintf(stderr, "Failed to clean up the cache dir (%s) created due to the usage of the mountfile!\n", d->cache_dir);
			dag_mount_clean(d);
			return -1;
		}
		dag_mount_clean(d);
	}

	/* clean each of the node-specific state. */
	struct dag_node *n;
	for(n = d->nodes; n; n = n->next) {
		makeflow_clean_node(d,queue,n);
	}

	return 0;
}

/* Collect available garbage, up to a limit of maxfiles. */

static void makeflow_gc_all( struct dag *d, struct batch_queue *queue, int maxfiles)
{
	int collected = 0;
	struct dag_file *f;
	char *name;

	timestamp_t start_time, stop_time;

	/* This will walk the table of files to collect and will remove any
	 * that are below or equal to the threshold. */
	start_time = timestamp_get();
	hash_table_firstkey(d->files);
	while(hash_table_nextkey(d->files, &name, (void **) &f) && collected < maxfiles) {
		if(f->state == DAG_FILE_STATE_COMPLETE
			&& !dag_file_is_source(f)
			&& !set_lookup(d->outputs, f)
			&& !set_lookup(d->inputs, f)
			&& makeflow_clean_file(d, queue, f)){
			collected++;
		}
	}

	stop_time = timestamp_get();

	/* Record total amount of files collected to Makeflowlog. */
	if(collected > 0) {
		makeflow_gc_collected += collected;
		makeflow_log_gc_event(d,collected,stop_time-start_time,makeflow_gc_collected);
	}
}

/* Collect garbage only if conditions warrant. */

void makeflow_gc( struct dag *d, struct batch_queue *queue, makeflow_gc_method_t method, uint64_t size, int count)
{
	if(size == 0)
		size = MAKEFLOW_MIN_SPACE;
	switch (method) {
	case MAKEFLOW_GC_NONE:
		break;
	case MAKEFLOW_GC_COUNT:
		debug(D_MAKEFLOW_RUN, "Performing incremental file (%d) garbage collection", count);
		makeflow_gc_all(d, queue, count);
		break;
	case MAKEFLOW_GC_ON_DEMAND:
		if(d->completed_files - d->deleted_files > count || directory_low_disk(".",size)){
			debug(D_MAKEFLOW_RUN, "Performing on demand (%d) garbage collection", count);
			makeflow_gc_all(d, queue, INT_MAX);
		}
		break;
	case MAKEFLOW_GC_SIZE:
		if(directory_low_disk(".", size)) {
			debug(D_MAKEFLOW_RUN, "Performing size (%d) garbage collection", count);
			makeflow_gc_all(d, queue, INT_MAX);
		}
		break;
	case MAKEFLOW_GC_ALL:
		makeflow_gc_all(d, queue, INT_MAX);
		break;
	}
}

int makeflow_clean_mount_target(const char *target) {
	file_type t_type;

	if(!target || !*target) return 0;

	/* Check whether target already exists. */
	if(access(target, F_OK)) {
		debug(D_DEBUG, "the target (%s) does not exist!\n", target);
		return 0;
	}

	/* Check whether the target is an absolute path. */
	if(target[0] == '/') {
		debug(D_DEBUG, "the target (%s) should not be an absolute path!\n", target);
		fprintf(stderr, "the target (%s) should not be an absolute path!\n", target);
		return -1;
	}

	/* check whether target includes .. */
	if(path_has_doubledots(target)) {
		debug(D_DEBUG, "the target (%s) include ..!\n", target);
		fprintf(stderr, "the target (%s) include ..!\n", target);
		return -1;
	}

	/* check whether target is REG, LNK, DIR */
	if((t_type = check_file_type(target)) == FILE_TYPE_UNSUPPORTED)
		return -1;

	if(unlink_recursive(target)) {
		debug(D_DEBUG, "Failed to remove %s!\n", target);
		fprintf(stderr, "Failed to remove %s!\n", target);
		return -1;
	}

	return 0;
}

/* vim: set noexpandtab tabstop=4: */
