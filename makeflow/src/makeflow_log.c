/*
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "batch_file.h"
#include "makeflow_log.h"
#include "makeflow_gc.h"
#include "dag.h"
#include "get_line.h"
#include "makeflow_mounts.h"

#include "timestamp.h"
#include "list.h"
#include "debug.h"
#include "xxmalloc.h"

#include <limits.h>
#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>

#define MAX_BUFFER_SIZE 4096

/*
The makeflow log file records every essential event in the execution of a workflow,
so that after a failure, the workflow can either be continued or aborted cleanly,
without leaving behind stranded jobs in the batch system, temporary files,
and so forth.  As a secondary purpose, the log file is also easy to feed into
gnuplot for the purpose of visualizing the progress of the workflow over time.

Various items have been added to the log over time, so it contains several kinds
of records.  The original record type only logged a change in state of a single
task, and begins with a timestamp, followed by node information.  The other event
types begin with a hash (#) so as to clearly distinguish them from the original
event type.

The current log format is showing its age as its purpose has evolved.
A redesign of the log is in order, to include events for indicating the
creation and deletion of files for caching, monitoring, wrapping, etc,
so that they can be automatically deleted.

----

Line format : timestamp node_id new_state job_id nodes_waiting nodes_running nodes_complete nodes_failed nodes_aborted node_id_counter

timestamp - the unix time (in microseconds) when this line is written to the log file.
node_id - the id of this node (task).
new_state - a integer represents the new state this node (whose id is in the node_id column) has just entered. The value of the integer ranges from 0 to 4 and the states they are representing are:
   0. waiting
   1. running
   2. complete
   3. failed
   4. aborted
job_id - the job id of this node in the underlying execution system (local or batch system). If the makeflow is executed locally, the job id would be the process id of the process that executes this node. If the underlying execution system is a batch system, such as Condor or UGE, the job id would be the job id assigned by the batch system when the task was sent to the batch system for execution.
nodes_waiting - the number of nodes are waiting to be executed.
nodes_running - the number of nodes are being executed.
nodes_complete - the number of nodes has been completed.
nodes_failed - the number of nodes has failed.
nodes_aborted - the number of nodes has been aborted.
node_id_counter - total number of nodes in this makeflow.

Line format: # GC timestamp collected time_spent total_collected

timestamp - the unix time (in microseconds) when this line is written to the log file.
collected - the number of files were collected in this garbage collection cycle.
time_spent - the length of time this cycle took.
total_collected - the total number of files has been collected so far since the start this makeflow execution.

Line format: # CACHE timestamp cache_dir

timestamp - the unix time (in microseconds) when this line is written to the log file.
cache_dir - the cache dir storing the files specified in a mountfile

Line format: # MOUNT timestamp target source cache_name type

timestamp - the unix time (in microseconds) when this line is written to the log file.
target - the target of a dependency specified in a mountfile
source - the source of a dependency specified in a mountfile
cache_name - the file name of the dependency in the cache directory
type - the type of this dependency source
	0. LOCAL - The dependency comes from the local filesystem.
	1. HTTP  - The dependency comes from the network via http.

Line format: # STARTED timestamp
Line format: # ABORTED timestamp
Line format: # FAILED timestamp
Line format: # COMPLETED timestamp

Line format: # dag_file_state filename timestamp
dag_file_state - the new DAG_FILE_STATE_* of the file mentioned:
	0. UNKNOWN  - The file is in an unknown state, where it may exists(input) but we have not checked yet.
	1. EXPECT   - The file is the expected output of a task. Transition meant to more explicitly describe GC.
	2. EXISTS   - The file exists on disk accessible to the workflow. Can result from checking an unknown file with stat, receiving a task's result files (EXPECT->EXISTS), or a file is retrieved from another source (DOWN->EXISTS).
	3. COMPLETE - An existing file is no longer used as a source for remaining tasks.
	4. DELETE   - The file was complete, then removed with either GC or clean.
	5. DOWN     - Intermediate state when retrieving from different location. For acknowledging it may be partially existent in the file system.
	6. UP       - Intermediate state for putting file to different lcoation. For acknowledging partial upload.
filename - the filename specified within the dag_file whose state has changed.
timestamp - the unix time (in microseconds) when this line is written to the log file.

These event types indicate that the workflow as a whole has started or completed in the indicated manner.
*/

void makeflow_node_decide_reset( struct dag *d, struct dag_node *n, int silent );

/*
To balance between performance and consistency, we sync the log every 60 seconds
on ordinary events, but sync immediately on important events like a makeflow restart.
*/

static void makeflow_log_sync( struct dag *d, int force )
{
	static time_t last_fsync = 0;

	/* Force buffered data to the kernel. */
	fflush(d->logfile);

	/* Every 60 seconds, force kernel buffered data to disk. */
	if(force || (time(NULL)-last_fsync) > 60) {
		fsync(fileno(d->logfile));
		last_fsync = time(NULL);
	}
}

void makeflow_log_close( struct dag *d )
{
	/* In the case where Makeflow exits prior to creating the DAG or opening log. */
	if(!d || !d->logfile) return;

	makeflow_log_sync(d,1);
	fclose(d->logfile);
	d->logfile = 0;
}

void makeflow_log_started_event( struct dag *d )
{
	fprintf(d->logfile, "# STARTED %" PRIu64 "\n", timestamp_get());
	makeflow_log_sync(d,1);
}

void makeflow_log_aborted_event( struct dag *d )
{
	/* In the case where Makeflow exits prior to creating the DAG or opening log. */
	if(!d || !d->logfile) return;

	fprintf(d->logfile, "# ABORTED %" PRIu64 "\n", timestamp_get());
	makeflow_log_sync(d,1);
}

void makeflow_log_failed_event( struct dag *d )
{
	/* In the case where Makeflow exits prior to creating the DAG or opening log. */
	if(!d || !d->logfile) return;

	fprintf(d->logfile, "# FAILED %" PRIu64 "\n", timestamp_get());
	makeflow_log_sync(d,1);
}

void makeflow_log_completed_event( struct dag *d )
{
	/* In the case where Makeflow exits prior to creating the DAG or opening log. */
	if(!d || !d->logfile) return;

	fprintf(d->logfile, "# COMPLETED %" PRIu64 "\n", timestamp_get());
	makeflow_log_sync(d,1);
}

void makeflow_log_mount_event( struct dag *d, const char *target, const char *source, const char *cache_name, dag_file_source_t type ) {
	fprintf(d->logfile, "# MOUNT %" PRIu64 " %s %s %s %d\n", timestamp_get(), target, source, cache_name, type);
	makeflow_log_sync(d,1);
}

void makeflow_log_cache_event( struct dag *d, const char *cache_dir ) {
	fprintf(d->logfile, "# CACHE %" PRIu64 " %s\n", timestamp_get(), cache_dir);
	makeflow_log_sync(d,1);
}

void makeflow_log_event( struct dag *d, char *name, uint64_t value)
{
	fprintf(d->logfile, "# EVENT\t%"PRIu64"\t%s\t%" PRIu64 "\n", timestamp_get(), name, value);
	makeflow_log_sync(d,1);
}

void makeflow_log_state_change( struct dag *d, struct dag_node *n, int newstate )
{
	debug(D_MAKEFLOW_RUN, "node %d %s -> %s\n", n->nodeid, dag_node_state_name(n->state), dag_node_state_name(newstate));

	if(d->node_states[n->state] > 0) {
		d->node_states[n->state]--;
	}
	n->state = newstate;
	d->node_states[n->state]++;

	fprintf(d->logfile, "%" PRIu64 " %d %d %" PRIbjid " %d %d %d %d %d %d\n", timestamp_get(), n->nodeid, newstate, n->jobid, d->node_states[0], d->node_states[1], d->node_states[2], d->node_states[3], d->node_states[4], d->nodeid_counter);

	makeflow_log_sync(d,0);
}

void makeflow_log_file_state_change( struct dag *d, struct dag_file *f, int newstate )
{
	debug(D_MAKEFLOW_RUN, "file %s %s -> %s\n", f->filename, dag_file_state_name(f->state), dag_file_state_name(newstate));

	f->state = newstate;

	/* If a file is a wrapper global file do not log to avoid cleaning floating global files. */
	if(f->type == DAG_FILE_TYPE_GLOBAL) return;

	timestamp_t time = timestamp_get();
	fprintf(d->logfile, "# FILE %" PRIu64 " %s %d %" PRIu64 "\n", time, f->filename, f->state, dag_file_size(f));
	if(f->state == DAG_FILE_STATE_EXISTS){
		d->completed_files += 1;
		f->creation_logged = (time_t) (time / 1000000);
	} else if(f->state == DAG_FILE_STATE_DELETE) {
		d->deleted_files += 1;
	}
	makeflow_log_sync(d,0);
}

void makeflow_log_batch_file_state_change( struct dag *d, struct batch_file *f, int newstate )
{
	makeflow_log_file_state_change(d, dag_file_lookup_or_create(d, f->outer_name), newstate);
}

void makeflow_log_batch_file_list_state_change( struct dag *d, struct list *file_list, int newstate )
{
	struct batch_file *f;

	if(!d || !file_list) return;

	list_first_item(file_list);
	while((f=list_next_item(file_list))) {
		makeflow_log_batch_file_state_change(d,f,newstate);
	}
}


void makeflow_log_dag_file_list_state_change( struct dag *d, struct list *file_list, int newstate )
{
	struct dag_file *f;

	if(!d || !file_list) return;

	list_first_item(file_list);
	while((f=list_next_item(file_list))) {
		makeflow_log_file_state_change(d,f,newstate);
	}
}

void makeflow_log_alloc_event( struct dag *d, struct makeflow_alloc *a )
{
	fprintf(d->logfile, "# ALLOC %"PRIu64" %"PRIu64" %"PRIu64" %"PRIu64" %"PRIu64" %"PRIu64" %"PRIu64"\n", timestamp_get(), a->storage->total, a->storage->used, a->storage->greedy, a->storage->commit, a->storage->free, d->total_file_size);
	makeflow_log_sync(d,0);
}

void makeflow_log_gc_event( struct dag *d, int collected, timestamp_t elapsed, int total_collected )
{
	fprintf(d->logfile, "# GC %" PRIu64 " %d %" PRIu64 " %d\n", timestamp_get(), collected, elapsed, total_collected);
	makeflow_log_sync(d,0);
}

/*
Dump the dag structure into the log file in comment formats.
This is used by some tools (such as Weaver) for debugging
assistance.
*/

void makeflow_log_dag_structure( struct dag *d )
{
	struct dag_file *f;
	struct dag_node *n, *p;

	for(n = d->nodes; n; n = n->next) {
		/* Record node information to log */
		fprintf(d->logfile, "# NODE\t%d\t%s\n", n->nodeid, n->command);

		/* Record the node category to the log */
		fprintf(d->logfile, "# CATEGORY\t%d\t%s\n", n->nodeid, n->category->name);
		fprintf(d->logfile, "# SYMBOL\t%d\t%s\n", n->nodeid, n->category->name);   /* also write the SYMBOL as alias of CATEGORY, deprecated. */

		/* Record node parents to log */
		fprintf(d->logfile, "# PARENTS\t%d", n->nodeid);
		list_first_item(n->source_files);
		while( (f = list_next_item(n->source_files)) ) {
			p = f->created_by;
			if(p)
				fprintf(d->logfile, "\t%d", p->nodeid);
		}
		fputc('\n', d->logfile);

		/* Record node inputs to log */
		fprintf(d->logfile, "# SOURCES\t%d", n->nodeid);
		list_first_item(n->source_files);
		while( (f = list_next_item(n->source_files)) ) {
			fprintf(d->logfile, "\t%s", f->filename);
		}
		fputc('\n', d->logfile);

		/* Record node outputs to log */
		fprintf(d->logfile, "# TARGETS\t%d", n->nodeid);
		list_first_item(n->target_files);
		while( (f = list_next_item(n->target_files)) ) {
			fprintf(d->logfile, "\t%s", f->filename);
		}
		fputc('\n', d->logfile);

		/* Record translated command to log */
		fprintf(d->logfile, "# COMMAND\t%d\t%s\n", n->nodeid, n->command);
	}
}

/*
Recover the state of the workflow so far by reading back the state
from the log file, if it exists.  (If not, create a new log.)
*/

int makeflow_log_recover(struct dag *d, const char *filename, int verbose_mode, struct batch_queue *queue, makeflow_clean_depth clean_mode )
{
	char *line, file[MAX_BUFFER_SIZE];
	int nodeid, state, jobid, file_state;
	int first_run = 1;
	struct dag_node *n;
	struct dag_file *f;
	timestamp_t previous_completion_time;
	uint64_t size;

	d->logfile = fopen(filename, "r");
	if(d->logfile) {
		int linenum = 0;
		first_run = 0;

		printf("recovering from log file %s...\n",filename);

		while((line = get_line(d->logfile))) {
			char source[PATH_MAX], cache_dir[NAME_MAX], cache_name[NAME_MAX];
			int type;
			linenum++;

			if(sscanf(line, "# FILE %" SCNu64 " %s %d %" SCNu64 "", &previous_completion_time, file, &file_state, &size) == 4) {
				f = dag_file_lookup_or_create(d, file);
				f->state = file_state;
				if(file_state == DAG_FILE_STATE_EXISTS){
					d->completed_files += 1;
					f->creation_logged = (time_t) (previous_completion_time / 1000000);
				} else if(file_state == DAG_FILE_STATE_DELETE){
					d->deleted_files += 1;
				}
			} else if(sscanf(line, "# CACHE %" SCNu64 " %s", &previous_completion_time, cache_dir) == 2) {
				/* if the user specifies a cache dir using --cache dir, ignore the info from the log file */
				if(!d->cache_dir) {
					d->cache_dir = xxstrdup(cache_dir);
				} else {
					/* There are two possible reasons for the inconsistency:
					 * 1) the cache dir specified via the --cache opt and in the log file mismatch;
					 * 2) the log file includes multiple different CACHE entries.
					 */
					if(strcmp(cache_dir, d->cache_dir)) {
						fprintf(stderr, "The --cache option (%s) does not match the cache dir (%s) in the log file!\n", d->cache_dir, cache_dir);
						free(line);
						return -1;
					}
				}
			} else if(sscanf(line, "# MOUNT %" SCNu64 " %s %s %s %d", &previous_completion_time, file, source, cache_name, &type) == 5) {
				f = dag_file_lookup_or_create(d, file);

				if(!f->source) {
					f->source = xxstrdup(source);
					f->cache_name = xxstrdup(cache_name);
					f->type = type;
				} else {
					/* If a mount entry is specified in the mountfile and logged in a log file at the same time, they must not conflict with each other. */
					/* If a mount entry is logged in a log file multiple times deliberately or not, they must not conflict with each other. */
					if(makeflow_mount_check_consistency(file, f->source, source, d->cache_dir, cache_name)) {
						free(line);
						return -1;
					}
				}
			} else if(line[0] == '#') {
				/* Ignore any other comment lines */
			} else if(sscanf(line, "%" SCNu64 " %d %d %d", &previous_completion_time, &nodeid, &state, &jobid) == 4) {
				n = itable_lookup(d->node_table, nodeid);
				if(n) {
					n->state = state;
					n->jobid = jobid;
					/* Log timestamp is in microseconds, we need seconds for diff. */
					n->previous_completion = (time_t) (previous_completion_time / 1000000);
				}
			} else {
				fprintf(stderr, "makeflow: %s appears to be corrupted on line %d\n", filename, linenum);
				exit(1);
			}
			free(line);
		}
		fclose(d->logfile);
	} else {
		printf("creating new log file %s...\n",filename);
	}

	d->logfile = fopen(filename, "a");
	if(!d->logfile) {
		fprintf(stderr, "makeflow: couldn't open logfile %s: %s\n", filename, strerror(errno));
		exit(1);
	}
	if(setvbuf(d->logfile, NULL, _IOLBF, BUFSIZ) != 0) {
		fprintf(stderr, "makeflow: couldn't set line buffer on logfile %s: %s\n", filename, strerror(errno));
		exit(1);
	}

	if(first_run && verbose_mode) {
		makeflow_log_dag_structure(d);
	}

	/*
	Count up the current number of nodes in the WAITING, COMPLETED, etc, states.
	*/

	dag_count_states(d);


	/*
	If this is not the first attempt at running, then
	scan for nodes that are running, failed, or aborted,
	and reset them to a waiting state to be retried.
	*/

	if(!first_run) {
		printf("checking for old running or failed jobs...\n");
		int silent = clean_mode != MAKEFLOW_CLEAN_NONE;
		for(n = d->nodes; n; n = n->next) {
			makeflow_node_decide_reset(d, n, silent);
		}
	}

	/*
	To bring garbage collection up to date, decrement
	file reference counts for every node that is complete.
	*/

	for(n = d->nodes; n; n = n->next) {
		if(n->state == DAG_NODE_STATE_COMPLETE)
		{
			struct dag_file *f;
			list_first_item(n->source_files);
			while((f = list_next_item(n->source_files)))
				f->reference_count--;
		}
	}

	return 0;
}

/* vim: set noexpandtab tabstop=4: */
