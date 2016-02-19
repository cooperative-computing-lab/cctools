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

#include "dag.h"
#include "makeflow_log.h"
#include "makeflow_wrapper.h"
#include "makeflow_gc.h"


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

/*
For a given dag node, export all variables into the environment.
This is currently only used when cleaning a makeflow recurisvely,
and would be better handled by invoking batch_job_local.
*/

static void makeflow_node_export_variables( struct dag *d, struct dag_node *n )
{
	struct jx *j = dag_node_env_create(d,n);
	if(j) {
		jx_export(j);
		jx_delete(j);
	}
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
			debug(D_MAKEFLOW_RUN, "Added %s to input list", f->filename);
		}

	if(output_list) {
		/* remove files from preserve_list */
		string_split_quotes(output_list, &argc, &argv);
		for(i = 0; i < argc; i++) {
			/* Must initialize to non-zero for hash_table functions to work properly. */
			f = dag_file_lookup_or_create(d, argv[i]);
			set_remove(d->outputs, f);
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
				debug(D_MAKEFLOW_RUN, "Added %s to output list", f->filename);
			}
	}
}

/* Clean a specific file, while emitting an appropriate message. */

int makeflow_clean_file( struct dag *d, struct batch_queue *queue, struct dag_file *f, int silent)
{
	if(!f)
		return 1;

	if(batch_fs_unlink(queue, f->filename) == 0) {
		makeflow_log_file_state_change(d, f, DAG_FILE_STATE_DELETE);
		debug(D_MAKEFLOW_RUN, "File deleted %s\n", f->filename);

	} else if(errno != ENOENT) {
		if(f->state == DAG_FILE_STATE_EXPECT || dag_file_should_exist(f))
			makeflow_log_file_state_change(d, f, DAG_FILE_STATE_DELETE);

			debug(D_MAKEFLOW_RUN, "Makeflow: Couldn't delete %s: %s\n", f->filename, strerror(errno));
			return 1;
	}
	return 0;
}

void makeflow_clean_node(struct dag *d, struct batch_queue *queue, struct dag_node *n, int silent)
{
	if(n->nested_job){
		char *command = xxmalloc(sizeof(char) * (strlen(n->command) + 4));
		sprintf(command, "%s -c", n->command);

		/* XXX this should use the batch job interface for consistency */
		makeflow_node_export_variables(d, n);
		system(command);
		free(command);
	}
}

/* Clean the entire dag by cleaning all nodes. */

void makeflow_clean(struct dag *d, struct batch_queue *queue, makeflow_clean_depth clean_depth)//, struct makeflow_wrapper *w, struct makeflow_monitor *m)
{
	struct dag_file *f;
	char *name;

	hash_table_firstkey(d->files);
	while(hash_table_nextkey(d->files, &name, (void **) &f)) {
		int silent = 1;
		if(dag_file_should_exist(f))
			silent = 0;

		/* We have a record of the file, but it is no longer created or used so delete */
		if(dag_file_is_source(f) && dag_file_is_sink(f) && !set_lookup(d->inputs, f))
			makeflow_clean_file(d, queue, f, silent);
		if(dag_file_is_source(f))
			continue;

		if(clean_depth == MAKEFLOW_CLEAN_ALL){
			makeflow_clean_file(d, queue, f, silent);
		} else if(set_lookup(d->outputs, f) && (clean_depth == MAKEFLOW_CLEAN_OUTPUTS)) {
			makeflow_clean_file(d, queue, f, silent);
		} else if(!set_lookup(d->outputs, f) && (clean_depth == MAKEFLOW_CLEAN_INTERMEDIATES)){
			makeflow_clean_file(d, queue, f, silent);
		}
	}

	struct dag_node *n;
	for(n = d->nodes; n; n = n->next) {
		/* If the node is a Makeflow job, then we should recursively call the *
		 * clean operation on it. */
		if(n->nested_job) {
			char *command = xxmalloc(sizeof(char) * (strlen(n->command) + 4));
			sprintf(command, "%s -c", n->command);

			/* XXX this should use the batch job interface for consistency */
			makeflow_node_export_variables(d, n);
			system(command);
			free(command);
		}
	}
}

/* Collect available garbage, up to a limit of maxfiles. */

static void makeflow_gc_all( struct dag *d, struct batch_queue *queue, int maxfiles )
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
			&& makeflow_clean_file(d, queue, f, 0)){
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

void makeflow_gc( struct dag *d, struct batch_queue *queue, makeflow_gc_method_t method, uint64_t size, int count )
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

/* vim: set noexpandtab tabstop=4: */
