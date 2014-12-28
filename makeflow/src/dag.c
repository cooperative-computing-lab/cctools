/*
Copyright (C) 2014 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <math.h>

#include "debug.h"
#include "xxmalloc.h"

#include "itable.h"
#include "hash_table.h"
#include "list.h"
#include "set.h"
#include "stringtools.h"
#include "rmsummary.h"

#include "dag.h"

struct dag_variable *dag_variable_create(const char *name, const char *initial_value);

struct dag *dag_create()
{
	struct dag *d = malloc(sizeof(*d));

	memset(d, 0, sizeof(*d));
	d->nodes = 0;
	d->filename = NULL;
	d->node_table = itable_create(0);
	d->local_job_table = itable_create(0);
	d->remote_job_table = itable_create(0);
	d->file_table = hash_table_create(0, 0);
	d->completed_files = hash_table_create(0, 0);
	d->variables = hash_table_create(0, 0);
	d->local_jobs_running = 0;
	d->local_jobs_max = 1;
	d->remote_jobs_running = 0;
	d->remote_jobs_max = MAX_REMOTE_JOBS_DEFAULT;
	d->nodeid_counter = 0;
	d->collect_table = set_create(0);
	d->export_vars  = set_create(0);
	d->special_vars = set_create(0);
	d->task_categories = hash_table_create(0, 0);

	/* Add GC_*_LIST to variables table to ensure it is in
	 * global DAG scope. */
	hash_table_insert(d->variables,"GC_COLLECT_LIST",  dag_variable_create(NULL, ""));
	hash_table_insert(d->variables,"GC_PRESERVE_LIST", dag_variable_create(NULL, ""));

	/* Declare special variables */
	set_insert(d->special_vars, "CATEGORY");
	set_insert(d->special_vars, RESOURCES_CORES);
	set_insert(d->special_vars, RESOURCES_MEMORY);
	set_insert(d->special_vars, RESOURCES_DISK);
	set_insert(d->special_vars, RESOURCES_GPUS);

	memset(d->node_states, 0, sizeof(int) * DAG_NODE_STATE_MAX);
	return d;
}

void dag_compile_ancestors(struct dag *d)
{
	struct dag_node *n, *m;
	struct dag_file *f;
	char *name;

	hash_table_firstkey(d->file_table);
	while(hash_table_nextkey(d->file_table, &name, (void **) &f)) {
		m = f->target_of;

		if(!m)
			continue;

		list_first_item(f->needed_by);
		while((n = list_next_item(f->needed_by))) {
			debug(D_MAKEFLOW_RUN, "rule %d ancestor of %d\n", m->nodeid, n->nodeid);
			set_insert(m->descendants, n);
			set_insert(n->ancestors, m);
		}
	}
}

static int get_ancestor_depth(struct dag_node *n)
{
	int group_number = -1;
	struct dag_node *ancestor = NULL;

	debug(D_MAKEFLOW_RUN, "n->ancestor_depth: %d", n->ancestor_depth);

	if(n->ancestor_depth >= 0) {
		return n->ancestor_depth;
	}

	set_first_element(n->ancestors);
	while((ancestor = set_next_element(n->ancestors))) {

		group_number = get_ancestor_depth(ancestor);
		debug(D_MAKEFLOW_RUN, "group: %d, n->ancestor_depth: %d", group_number, n->ancestor_depth);
		if(group_number > n->ancestor_depth) {
			n->ancestor_depth = group_number;
		}
	}

	n->ancestor_depth++;
	return n->ancestor_depth;
}

void dag_find_ancestor_depth(struct dag *d)
{
	UINT64_T key;
	struct dag_node *n;

	itable_firstkey(d->node_table);
	while(itable_nextkey(d->node_table, &key, (void **) &n)) {
		get_ancestor_depth(n);
	}
}

/* Return the dag_file associated with the local name filename.
 * If one does not exist, it is created. */
struct dag_file *dag_file_lookup_or_create(struct dag *d, const char *filename)
{
	struct dag_file *f;

	f = hash_table_lookup(d->file_table, filename);
	if(f) return f;

	f = dag_file_create(filename);

	hash_table_insert(d->file_table, f->filename, (void *) f);

	return f;
}

/* Returns the struct dag_file for the local filename */
struct dag_file *dag_file_from_name(struct dag *d, const char *filename)
{
	return (struct dag_file *) hash_table_lookup(d->file_table, filename);
}

/* Returns the list of dag_file's which are not the target of any
 * node */
struct list *dag_input_files(struct dag *d)
{
	struct dag_file *f;
	char *filename;
	struct list *il;

	il = list_create(0);

	hash_table_firstkey(d->file_table);
	while((hash_table_nextkey(d->file_table, &filename, (void **) &f)))
		if(!f->target_of) {
			debug(D_MAKEFLOW_RUN, "Found independent input file: %s", f->filename);
			list_push_tail(il, f);
		}

	return il;
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

struct dag_task_category *dag_task_category_lookup_or_create(struct dag *d, const char *label)
{
	struct dag_task_category *category;

	category = hash_table_lookup(d->task_categories, label);
	if(!category) {
		category = dag_task_category_create(label);
		hash_table_insert(d->task_categories, label, category);
	}

	return category;
}

/* vim: set noexpandtab tabstop=4: */
