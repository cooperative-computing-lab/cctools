/*
Copyright (C) 2008,2013- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>

#include "debug.h"
#include "xxmalloc.h"

#include "itable.h"
#include "hash_table.h"
#include "list.h"

#include "dag.h"

struct dag *dag_create()
{
	struct dag *d = malloc(sizeof(*d));

	if(!d)
	{
		debug(D_DEBUG, "makeflow: could not allocate new dag : %s\n", strerror(errno));
		return NULL;
	}
	else
	{
		memset(d, 0, sizeof(*d));
		d->nodes = 0;
		d->filename = NULL;
		d->node_table = itable_create(0);
		d->local_job_table = itable_create(0);
		d->remote_job_table = itable_create(0);
		d->file_table = hash_table_create(0, 0);
		d->completed_files = hash_table_create(0, 0);
		d->symlinks_created = list_create();
		d->variables = hash_table_create(0, 0);
		d->local_jobs_running = 0;
		d->local_jobs_max = 1;
		d->remote_jobs_running = 0;
		d->remote_jobs_max = MAX_REMOTE_JOBS_DEFAULT;
		d->nodeid_counter = 0;
		d->filename_translation_rev = hash_table_create(0, 0);
		d->filename_translation_fwd = hash_table_create(0, 0);
		d->collect_table = hash_table_create(0, 0);
		d->export_list = list_create();

		/* Add _MAKEFLOW_COLLECT_LIST to variables table to ensure it is in
		 * global DAG scope. */
		hash_table_insert(d->variables, "_MAKEFLOW_COLLECT_LIST", xxstrdup(""));

		memset(d->node_states, 0, sizeof(int) * DAG_NODE_STATE_MAX);
		return d;
	}
}

struct dag_node *dag_node_create(struct dag *d, int linenum)
{
	struct dag_node *n;

	n = malloc(sizeof(struct dag_node));
	memset(n, 0, sizeof(struct dag_node));
	n->linenum = linenum;
	n->state = DAG_NODE_STATE_WAITING;
	n->nodeid = d->nodeid_counter++;
	n->variables = hash_table_create(0, 0);

	return n;
}

struct dag_file *dag_file_create(const char *filename, char *remotename, struct dag_file *next)
{
	struct dag_file *f = malloc(sizeof(*f));
	f->filename = xxstrdup(filename);
	if(remotename) {
		f->remotename = xxstrdup(remotename);
	} else {
		f->remotename = NULL;
	}
	f->next = next;
	return f;
}

struct hash_table *dag_input_files(struct dag *d)
{
	struct dag_node *n, *tmp;
	struct dag_file *f;
	struct hash_table *ih;

	ih = hash_table_create(0,0);
	for(n = d->nodes; n; n = n->next) {
		//for each source file, see if it is a target file of another node
		for(f = n->source_files; f; f = f->next) {
			// d->file_table contains all target files
			// get the node (tmp) that outputs current source file
			tmp = hash_table_lookup(d->file_table, f->filename);
			// if a source file is also a target file
			if(!tmp) {
				debug(D_DEBUG, "Found independent input file: %s", f->filename);
				hash_table_insert(ih, f->filename, (void *) f->remotename);
			}
		}
	}

	return ih;
}

char *dag_lookup(const char *name, void *arg)
{
	struct dag_lookup_set s = {(struct dag *)arg, NULL, NULL};
	return dag_lookup_set(name, &s);
}

char *dag_lookup_set(const char *name, void *arg)
{
	struct dag_lookup_set *s = (struct dag_lookup_set *)arg;
	const char *value;

	/* Try node variables table */
	if(s->node) {
		value = (const char *)hash_table_lookup(s->node->variables, name);
		if(value) {
			s->table = s->node->variables;
			return xxstrdup(value);
		}
	}

	/* Try dag variables table */
	if(s->dag) {
		value = (const char *)hash_table_lookup(s->dag->variables, name);
		if(value) {
			s->table = s->dag->variables;
			return xxstrdup(value);
		}
	}

	/* Try environment */
	value = getenv(name);
	if(value) {
		return xxstrdup(value);
	}

	return NULL;
}

const char *dag_node_state_name(dag_node_state_t state)
{
	switch (state) {
	case DAG_NODE_STATE_WAITING:
		return "waiting";
	case DAG_NODE_STATE_RUNNING:
		return "running";
	case DAG_NODE_STATE_COMPLETE:
		return "complete";
	case DAG_NODE_STATE_FAILED:
		return "failed";
	case DAG_NODE_STATE_ABORTED:
		return "aborted";
	default:
		return "unknown";
	}
}


void dag_node_add_source_file(struct dag_node *n, const char *filename, char *remotename)
{
	n->source_files = dag_file_create(filename, remotename, n->source_files);
}

void dag_node_add_target_file(struct dag_node *n, const char *filename, char *remotename)
{
	n->target_files = dag_file_create(filename, remotename, n->target_files);
}

void dag_count_states(struct dag *d)
{
	struct dag_node *n;
	int i;

	for(i = 0; i < DAG_NODE_STATE_MAX; i++) {
		d->node_states[i] = 0;
	}

	for(n = d->nodes; n; n = n->next) {
		d->node_states[n->state]++;
	}
}

void dag_node_state_change(struct dag *d, struct dag_node *n, int newstate)
{
	debug(D_DEBUG, "node %d %s -> %s\n", n->nodeid, dag_node_state_name(n->state), dag_node_state_name(newstate));

	if(d->node_states[n->state] > 0) {
		d->node_states[n->state]--;
	}
	n->state = newstate;
	d->node_states[n->state]++;

        /**
	 * Line format : timestamp node_id new_state job_id nodes_waiting nodes_running nodes_complete nodes_failed nodes_aborted node_id_counter
	 *
	 * timestamp - the unix time (in microseconds) when this line is written to the log file.
	 * node_id - the id of this node (task).
	 * new_state - a integer represents the new state this node (whose id is in the node_id column) has just entered. The value of the integer ranges from 0 to 4 and the states they are representing are:
	 *	0. waiting
	 *	1. running
	 *	2. complete
	 *	3. failed
	 *	4. aborted
	 * job_id - the job id of this node in the underline execution system (local or batch system). If the makeflow is executed locally, the job id would be the process id of the process that executes this node. If the underline execution system is a batch system, such as Condor or SGE, the job id would be the job id assigned by the batch system when the task was sent to the batch system for execution.
	 * nodes_waiting - the number of nodes are waiting to be executed.
	 * nodes_running - the number of nodes are being executed.
	 * nodes_complete - the number of nodes has been completed.
	 * nodes_failed - the number of nodes has failed.
	 * nodes_aborted - the number of nodes has been aborted.
	 * node_id_counter - total number of nodes in this makeflow.
	 *
	 */
	fprintf(d->logfile, "%" PRIu64 " %d %d %d %d %d %d %d %d %d\n", timestamp_get(), n->nodeid, newstate, n->jobid, d->node_states[0], d->node_states[1], d->node_states[2], d->node_states[3], d->node_states[4], d->nodeid_counter);
}

