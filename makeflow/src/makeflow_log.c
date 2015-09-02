/*
Copyright (C) 2014- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "makeflow_log.h"
#include "dag.h"
#include "get_line.h"

#include "timestamp.h"
#include "list.h"
#include "debug.h"

#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>

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
job_id - the job id of this node in the underline execution system (local or batch system). If the makeflow is executed locally, the job id would be the process id of the process that executes this node. If the underline execution system is a batch system, such as Condor or SGE, the job id would be the job id assigned by the batch system when the task was sent to the batch system for execution.
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

Line format: # STARTED timestamp
Line format: # ABORTED timestamp
Line format: # FAILED timestamp
Line format: # COMPLETED timestamp

Line format: # EXPECT filename timestamp
Line format: # RECIEVE filename timestamp
Line format: # COMPLETE filename timestamp
Line format: # DELETE filename timestamp


These event types indicate that the workflow as a whole has started or completed in the indicated manner.
*/

void makeflow_node_decide_rerun(struct itable *rerun_table, struct dag *d, struct dag_node *n );

/*
To balance between performance and consistency, we sync the log every 60 seconds
on ordinary events, but sync immediately on important events like a makeflow restart.
*/

static void makeflow_log_sync( struct dag *d, int force )
{
	static time_t last_fsync = 0;

	if(force || (time(NULL)-last_fsync) > 60) {
		fsync(fileno(d->logfile));
		last_fsync = time(NULL);
	}
}

void makeflow_log_started_event( struct dag *d )
{
	fprintf(d->logfile, "# STARTED\t%" PRIu64 "\n", timestamp_get());
	makeflow_log_sync(d,1);
}

void makeflow_log_aborted_event( struct dag *d )
{
	fprintf(d->logfile, "# ABORTED\t%" PRIu64 "\n", timestamp_get());
	makeflow_log_sync(d,1);
}

void makeflow_log_failed_event( struct dag *d )
{
	fprintf(d->logfile, "# FAILED\t%" PRIu64 "\n", timestamp_get());
	makeflow_log_sync(d,1);
}

void makeflow_log_completed_event( struct dag *d )
{
	fprintf(d->logfile, "# COMPLETED\t%" PRIu64 "\n", timestamp_get());
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

	fprintf(d->logfile, "# %d %s %" PRIu64 "\n", f->state, f->filename, timestamp_get());
	makeflow_log_sync(d,0);
}

void makeflow_log_gc_event( struct dag *d, int collected, timestamp_t elapsed, int total_collected )
{
	fprintf(d->logfile, "# GC\t%" PRIu64 "\t%d\t%" PRIu64 "\t%d\n", timestamp_get(), collected, elapsed, total_collected);
	makeflow_log_sync(d,0);
}

void makeflow_log_recover(struct dag *d, const char *filename, int verbose_mode )
{
	char *line, file[30];
	int nodeid, state, jobid, file_state;
	int first_run = 1;
	struct dag_node *n;
	struct dag_file *f;
	timestamp_t previous_completion_time;

	d->logfile = fopen(filename, "r");
	if(d->logfile) {
		int linenum = 0;
		first_run = 0;

		printf("recovering from log file %s...\n",filename);

		while((line = get_line(d->logfile))) {
			linenum++;

			if(sscanf(line, "# %d %s %" SCNu64 "", &file_state, file, &previous_completion_time) == 3) {

				f = dag_file_lookup_or_create(d, file);
				f->state = file_state;
				continue;
			}
			if(line[0] == '#')
				continue;
			if(sscanf(line, "%" SCNu64 " %d %d %d", &previous_completion_time, &nodeid, &state, &jobid) == 4) {
				n = itable_lookup(d->node_table, nodeid);
				if(n) {
					n->state = state;
					n->jobid = jobid;
					/* Log timestamp is in microseconds, we need seconds for diff. */
					n->previous_completion = (time_t) (previous_completion_time / 1000000);
					continue;
				}
			}

			fprintf(stderr, "makeflow: %s appears to be corrupted on line %d\n", filename, linenum);
			exit(1);
		}
		fclose(d->logfile);
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
		struct dag_file *f;
		struct dag_node *p;
		for(n = d->nodes; n; n = n->next) {
			/* Record node information to log */
			fprintf(d->logfile, "# NODE\t%d\t%s\n", n->nodeid, n->command);

			/* Record the node category to the log */
			fprintf(d->logfile, "# CATEGORY\t%d\t%s\n", n->nodeid, n->category->label);
			fprintf(d->logfile, "# SYMBOL\t%d\t%s\n", n->nodeid, n->category->label);   /* also write the SYMBOL as alias of CATEGORY, deprecated. */

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


	dag_count_states(d);

	// Decide rerun tasks
	if(!first_run) {
		struct itable *rerun_table = itable_create(0);
		for(n = d->nodes; n; n = n->next) {
			makeflow_node_decide_rerun(rerun_table, d, n);
		}
		itable_delete(rerun_table);
	}

	//Update file reference counts from nodes in log
	for(n = d->nodes; n; n = n->next) {
		if(n->state == DAG_NODE_STATE_COMPLETE)
		{
			struct dag_file *f;
			list_first_item(n->source_files);
			while((f = list_next_item(n->source_files)))
				f->ref_count += -1;
		}
	}
}

/* vim: set noexpandtab tabstop=4: */
