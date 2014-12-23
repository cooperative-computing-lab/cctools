/*
Copyright (C) 2008,2013- The University of Notre Dame
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

/* Constructs the dictionary of environment variables for a dag
 * */
char *dag_lookup_set(const char *name, void *arg)
{
	struct dag_lookup_set s = { (struct dag *) arg, NULL, NULL, NULL };
	return dag_lookup_str(name, &s);
}

/* 'floor' of node_id */
int variable_binary_search(struct dag_variable_value **values, int nodeid, int min, int max)
{
	if(nodeid < 0)
		return max;

	struct dag_variable_value *v;
	int mid;

	while(max >= min)
	{
		mid = (max + min)/2;
		v = values[mid];

		if(v->nodeid < nodeid)
		{
			min = mid + 1;
		}
		else if(v->nodeid > nodeid)
		{
			max = mid - 1;
		}
		else
		{
			return mid;
		}
	}

	//here max =< min, thus v[max] < nodeid < v[min]
	return max;
}

struct dag_variable_value *dag_get_variable_value(const char *name, struct hash_table *t, int node_id)
{
	struct dag_variable *var;

	var = (struct dag_variable *) hash_table_lookup(t, name);

	if(!var)
		return NULL;

	if(node_id < 0)
		return var->values[var->count - 1];

	int index = variable_binary_search(var->values, node_id, 0, var->count - 1);
	if(index < 0)
		return NULL;

	return var->values[index];
}

struct dag_variable_value *dag_lookup(const char *name, void *arg)
{
	struct dag_lookup_set *s = (struct dag_lookup_set *) arg;
	struct dag_variable_value *v;

	int nodeid;
	if(s->node)
	{
		nodeid = s->node->nodeid;
	}
	else if(s->dag)
	{
		nodeid = s->dag->nodeid_counter;
	}

	if(s) {
		/* Try node variables table */
		if(s->node) {
			v = dag_get_variable_value(name, s->node->variables, nodeid);
			if(v) {
				s->table = s->node->variables; //why this line?
				return v;
			}
		}

		/* Try dag variables table */
		if(s->dag) {
			v = dag_get_variable_value(name, s->dag->variables, nodeid);
			if(v) {
				s->table = s->dag->variables;
				return v;
			}
		}
	}

	/* Try environment */
	char *value = getenv(name);
	if(value) {
		return dag_variable_value_create(value);
	}

	return NULL;
}

char *dag_lookup_str(const char *name, void *arg)
{
	struct dag_variable_value *v = dag_lookup(name, arg);

	if(v)
		return xxstrdup(v->value);
	else
		return NULL;
}

void dag_variable_add_value(const char *name, struct hash_table *current_table, int nodeid, const char *value)
{
	struct dag_variable *var = hash_table_lookup(current_table, name);
	if(!var)
	{
		char *value_env = getenv(name);
		var = dag_variable_create(name, value_env);
		hash_table_insert(current_table, name, var);
	}

	struct dag_variable_value *v = dag_variable_value_create(value);
	v->nodeid = nodeid;

	if(var->count < 1 || var->values[var->count - 1]->nodeid != v->nodeid)
	{
		var->count++;
		var->values = realloc(var->values, var->count * sizeof(struct dag_variable_value *));
	}

	//possible memory leak...
	var->values[var->count - 1] = v;
}

struct dag_variable *dag_variable_create(const char *name, const char *initial_value)
{
	struct dag_variable *var = malloc(sizeof(struct dag_variable));

	if(!initial_value && name)
	{
		initial_value = getenv(name);
	}

	if(initial_value)
	{
		var->count  = 1;
		var->values = malloc(sizeof(struct dag_variable_value *));
		var->values[0] = dag_variable_value_create(initial_value);
	}
	else
	{
		var->count  = 0;
		var->values = NULL;
	}

	return var;
}

struct dag_variable_value *dag_variable_value_create(const char *value)
{
	struct dag_variable_value *v = malloc(sizeof(struct dag_variable_value));

	v->nodeid = 0;
	v->len    = strlen(value);
	v->size   = v->len + 1;

	v->value = malloc(v->size * sizeof(char));

	strcpy(v->value, value);

	return v;
}

void dag_variable_value_free(struct dag_variable_value *v)
{
	free(v->value);
	free(v);
}

struct dag_variable_value *dag_variable_value_append_or_create(struct dag_variable_value *v, const char *value)
{
	if(!v)
		return dag_variable_value_create(value);

	int nlen = strlen(value);
	int req  = v->len + nlen + 2;   // + 2 for ' ' and '\0'

	if( req > v->size )
	{
		//make size for string to be appended, plus some more, so we do not
		//need to reallocate for a while.
		int nsize = req > 2*(v->size) ? 2*req : 2*(v->size);
		char *new_val = realloc(v->value, nsize*sizeof(char));
		if(!new_val)
			fatal("Could not allocate memory for makeflow variable value: %s\n", value);

		v->size  = nsize;
		v->value = new_val;
	}

	//add separating space
	*(v->value + v->len) = ' ';

	//append new string
	strcpy(v->value + v->len + 1, value);
	v->len = v->len + nlen + 1;

	return v;
}

/* Adds remotename to the local name filename in the namespace of
 * the given node. If remotename is NULL, then a new name is
 * found using dag_node_translate_filename. If the remotename
 * given is different from a previosly specified, a warning is
 * written to the debug output, but otherwise this is ignored. */
const char *dag_node_add_remote_name(struct dag_node *n, const char *filename, const char *remotename)
{
	char *oldname;
	struct dag_file *f = dag_file_from_name(n->d, filename);

	if(!f)
		fatal("trying to add remote name %s to unknown file %s.\n", remotename, filename);

	if(!remotename)
		remotename = dag_node_translate_filename(n, filename);
	else
		remotename = xxstrdup(remotename);

	oldname = hash_table_lookup(n->remote_names_inv, remotename);

	if(oldname && strcmp(oldname, filename) == 0)
		debug(D_MAKEFLOW_RUN, "Remote name %s for %s already in use for %s\n", remotename, filename, oldname);

	itable_insert(n->remote_names, (uintptr_t) f, remotename);
	hash_table_insert(n->remote_names_inv, remotename, (void *) f);

	return remotename;
}

/* Adds the local name to the list of source files of the node,
 * and adds the node as a dependant of the file. If remotename is
 * not NULL, it is added to the namespace of the node. */
void dag_node_add_source_file(struct dag_node *n, const char *filename, char *remotename)
{
	struct dag_file *source = dag_file_lookup_or_create(n->d, filename);

	if(remotename)
		dag_node_add_remote_name(n, filename, remotename);

	/* register this file as a source of the node */
	list_push_head(n->source_files, source);

	/* register this file as a requirement of the node */
	list_push_head(source->needed_by, n);

	source->ref_count++;
}

/* Adds the local name as a target of the node, and register the
 * node as the producer of the file. If remotename is not NULL,
 * it is added to the namespace of the node. */
void dag_node_add_target_file(struct dag_node *n, const char *filename, char *remotename)
{
	struct dag_file *target = dag_file_lookup_or_create(n->d, filename);

	if(target->target_of && target->target_of != n)
		fatal("%s is defined multiple times at %s:%d and %s:%d\n", filename, filename, target->target_of->linenum, filename, n->linenum);

	if(remotename)
		dag_node_add_remote_name(n, filename, remotename);

	/* register this file as a target of the node */
	list_push_head(n->target_files, target);

	/* register this node as the creator of the file */
	target->target_of = n;
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
		category = malloc(sizeof(struct dag_task_category));
		category->label = xxstrdup(label);
		category->nodes = list_create();
		hash_table_insert(d->task_categories, label, category);
	}

	return category;
}

/* vim: set noexpandtab tabstop=4: */
