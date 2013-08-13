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

struct dag *dag_create()
{
	struct dag *d = malloc(sizeof(*d));

	if(!d) {
		debug(D_DEBUG, "makeflow: could not allocate new dag : %s\n", strerror(errno));
		return NULL;
	} else {
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
		d->collect_table = set_create(0);
		d->export_list = list_create();

		d->task_categories = hash_table_create(0, 0);

		/* Add GC_*_LIST to variables table to ensure it is in
		 * global DAG scope. */
		hash_table_insert(d->variables, "GC_COLLECT_LIST", dag_variable_value_create(""));
		hash_table_insert(d->variables, "GC_PRESERVE_LIST", dag_variable_value_create(""));

		memset(d->node_states, 0, sizeof(int) * DAG_NODE_STATE_MAX);
		return d;
	}
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
			debug(D_DEBUG, "rule %d ancestor of %d\n", m->nodeid, n->nodeid);
			set_insert(m->descendants, n);
			set_insert(n->ancestors, m);
		}
	}
}

int get_ancestor_depth(struct dag_node *n)
{
	int group_number = -1;
	struct dag_node *ancestor = NULL;

	debug(D_DEBUG, "n->ancestor_depth: %d", n->ancestor_depth);

	if(n->ancestor_depth >= 0) {
		return n->ancestor_depth;
	}

	set_first_element(n->ancestors);
	while((ancestor = set_next_element(n->ancestors))) {

		group_number = get_ancestor_depth(ancestor);
		debug(D_DEBUG, "group: %d, n->ancestor_depth: %d", group_number, n->ancestor_depth);
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

struct dag_node *dag_node_create(struct dag *d, int linenum)
{
	struct dag_node *n;

	n = malloc(sizeof(struct dag_node));
	memset(n, 0, sizeof(struct dag_node));
	n->d = d;
	n->linenum = linenum;
	n->state = DAG_NODE_STATE_WAITING;
	n->nodeid = d->nodeid_counter++;
	n->variables = hash_table_create(0, 0);

	n->source_files = list_create(0);
	n->target_files = list_create(0);

	n->remote_names = itable_create(0);
	n->remote_names_inv = hash_table_create(0, 0);

	n->descendants = set_create(0);
	n->ancestors = set_create(0);

	n->ancestor_depth = -1;

	return n;
}

/* Returns the struct dag_file for the local filename */
struct dag_file *dag_file_from_name(struct dag *d, const char *filename)
{
	return (struct dag_file *) hash_table_lookup(d->file_table, filename);
}

/* Returns the remotename used in rule n for local name filename */
char *dag_file_remote_name(struct dag_node *n, const char *filename)
{
	struct dag_file *f;
	char *name;

	f = dag_file_from_name(n->d, filename);
	name = (char *) itable_lookup(n->remote_names, (uintptr_t) f);

	return name;
}

/* True if the local file is specified as an absolute path */
int dag_file_isabsolute(const struct dag_file *f)
{
	return f->filename[0] == '/';
}

/* Translate an absolute filename into a unique slash-less name to allow for the
   sending of any file to remote systems. The function allows for upto a million name collisions. */
char *dag_node_translate_filename(struct dag_node *n, const char *filename)
{
	int len;
	char *newname_ptr;

	len = strlen(filename);

	/* If there are no slashes in path, then we don't need to translate. */
	if(!strchr(filename, '/')) {
		newname_ptr = xxstrdup(filename);
		return newname_ptr;
	}

	/* If the filename is in the current directory and doesn't contain any
	 * additional slashes, then we can also skip translation.
	 *
	 * Note: this doesn't handle redundant ./'s such as ./././././foo/bar */
	if(!strncmp(filename, "./", 2) && !strchr(filename + 2, '/')) {
		newname_ptr = xxstrdup(filename);
		return newname_ptr;
	}

	/* Make space for the new filename + a hyphen + a number to
	 * handle upto a million name collisions */
	newname_ptr = calloc(len + 8, sizeof(char));
	strcpy(newname_ptr, filename);

	char *c;
	for(c = newname_ptr; *c; ++c) {
		switch (*c) {
		case '/':
		case '.':
			*c = '_';
			break;
		default:
			break;
		}
	}

	if(!n)
		return newname_ptr;

	int i = 0;
	char *newname_org = xxstrdup(newname_ptr);
	while(hash_table_lookup(n->remote_names_inv, newname_ptr)) {
		sprintf(newname_ptr, "%06d-%s", i, newname_org);
		i++;
	}

	free(newname_org);

	return newname_ptr;
}

/* Return the dag_file associated with the local name filename.
 * If one does not exist, it is created. */
struct dag_file *dag_file_lookup_or_create(struct dag *d, const char *filename)
{
	struct dag_file *f;

	f = hash_table_lookup(d->file_table, filename);

	if(f)
		return f;

	f = malloc(sizeof(struct dag_file));

	f->filename = xxstrdup(filename);
	f->needed_by = list_create(0);
	f->target_of = NULL;

	f->ref_count = 0;

	hash_table_insert(d->file_table, f->filename, (void *) f);

	return f;
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
			debug(D_DEBUG, "Found independent input file: %s", f->filename);
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

struct dag_variable_value *dag_lookup(const char *name, void *arg)
{
	struct dag_lookup_set *s = (struct dag_lookup_set *) arg;
	struct dag_variable_value *v;

	if(s) {
		/* Try node variables table */
		if(s->node) {
			v = (struct dag_variable_value *) hash_table_lookup(s->node->variables, name);
			if(v) {
				s->table = s->node->variables; //why this line?
				return v;
			}
		}

		/* Try variables from category */
		if(s->category) {
			v = (struct dag_variable_value *) hash_table_lookup(s->category->variables, name);
			if(v) {
				s->table = s->category->variables;
				return v;
			}
		}

		/* Try dag variables table */
		if(s->dag) {
			v = (struct dag_variable_value *) hash_table_lookup(s->dag->variables, name);
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

struct dag_variable_value *dag_variable_value_create(const char *value)
{
	struct dag_variable_value *v = malloc(sizeof(struct dag_variable_value));

	v->len  = strlen(value);
	v->size = v->len + 1;

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
		debug(D_DEBUG, "Remote name %s for %s already in use for %s\n", remotename, filename, oldname);

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

struct dag_task_category *dag_task_category_lookup_or_create(struct dag *d, const char *label)
{
	struct dag_task_category *category;

	category = hash_table_lookup(d->task_categories, label);
	if(!category) {
		category = malloc(sizeof(struct dag_task_category));
		category->label = xxstrdup(label);
		category->nodes = list_create();
		category->variables = hash_table_create(0, 0);
		category->resources = make_rmsummary(-1);
		
		hash_table_insert(d->task_categories, label, category);
	}

	return category;
}

void dag_task_category_get_env_resources(struct dag_task_category *category)
{
	if(category)
		rmsummary_read_env_vars(category->resources);
}

void dag_task_category_print_debug_resources(struct dag_task_category *category)
{
	if( category->resources->cores > -1 )
		debug(D_DEBUG, "cores:  %d.\n",    category->resources->cores);
	if( category->resources->resident_memory > -1 )
		debug(D_DEBUG, "memory:   %d MB.\n", category->resources->resident_memory);
	if( category->resources->workdir_footprint > -1 )
		debug(D_DEBUG, "disk:     %d MB.\n", category->resources->workdir_footprint);
}

char *dag_task_category_wrap_as_wq_options(struct dag_task_category *category, const char *default_options)
{
	struct rmsummary *s;

	s = category->resources;

	char *options = NULL;

	options = string_format("%s resources: cores: %" PRId64 ", resident_memory: %" PRId64 ", workdir_footprint: %" PRId64,
			default_options           ? default_options      : "",
			s->cores             > -1 ? s->cores             : -1,
			s->resident_memory   > -1 ? s->resident_memory   : -1,
			s->workdir_footprint > -1 ? s->workdir_footprint : -1);

	return options;
}

char *dag_task_category_wrap_as_rmonitor_options(struct dag_task_category *category)
{
	struct rmsummary *s;

	s = category->resources;

	char *options = NULL;
	char *opt;

	if( s->cores > -1 )
	{
		opt = string_format("%s -L'cores: %" PRId64 "' ", options ? options : "", s->cores ); 
		if(options)
			free(options);
		options = opt;
	}
	if( s->resident_memory > -1 )
	{
		opt = string_format("%s -L'resident_memory: %" PRId64 "' ", options ? options : "", s->resident_memory ); 
		if(options)
			free(options);
		options = opt;
	}
	if( s->workdir_footprint > -1 )
	{
		opt = string_format("%s -L'workdir_footprint: %" PRId64 "' ", options ? options : "", s->workdir_footprint ); 
		if(options)
			free(options);
		options = opt;
	}

	return options;
}

/* works as realloc for the first argument */
char *dag_task_category_add_condor_option(char *options, const char *expression, int64_t value)
{
	if(value < 0)
		return options;

	char *opt = NULL;
	if(options)
	{
		opt = string_format("%s && (%s%" PRId64 ")", options, expression, value); 
		free(options);
		options = opt;
	}
	else
	{
		options = string_format("(%s%" PRId64 ")", expression, value); 
	}

	return options;
}

char *dag_task_category_wrap_as_condor_options(struct dag_task_category *category, const char *default_options)
{
	struct rmsummary *s;

	s = category->resources;

	char *options = NULL;
	char *opt;

	options = dag_task_category_add_condor_option(options, "Cores>=", s->cores); 
	options = dag_task_category_add_condor_option(options, "Memory>=", s->resident_memory); 
	options = dag_task_category_add_condor_option(options, "Disk>=", s->workdir_footprint); 

	if(!options)
		return xxstrdup(default_options);

	if(!default_options)
	{
		opt = string_format("Requirements = %s\n", options);
		free(options);
		return opt;
	}

	/* else default_options && options */
	char *scratch = xxstrdup(default_options);
	char *req_pos = strstr(scratch, "Requirements");
	if(!req_pos)
		req_pos = strstr(scratch, "requirements");

	if(!req_pos)
	{
		opt = string_format("Requirements = %s\n%s", options, scratch);
		free(options);
		free(scratch);
		return opt;
	}

	/* else, requirements have been specified also at default_options*/
	char *equal_sign = strchr(req_pos, '='); 
	if(!equal_sign)
	{
	/* Possibly malformed, not much we can do. */
		free(options);
		free(scratch);
		return xxstrdup(default_options);
	}
	
	char *newline = strchr(scratch, '\n'); 
	if(!newline)
		newline = (scratch + strlen(default_options) - 1); /* end of string */

	*req_pos    = '\0';
	*equal_sign = '\0';
	*newline    = '\0';

	/* Now we have these different strings:
	   scratch: from beginning of default_options to the 'R' in Requirements
	   equal_sign: from the '=' after Requirements to a new line or end of default_options
	   newline: from the end of original requirements to end of default_options
	*/

	opt = string_format("%s\nRequirements = (%s) && (%s)\n%s", scratch, (equal_sign + 1), options, (newline + 1));
	free(scratch);
	free(options);

	return opt;
}

char *dag_task_category_wrap_options(struct dag_task_category *category, const char *default_options, batch_queue_type_t batch_type)
{
	switch(batch_type)
	{
		case BATCH_QUEUE_TYPE_WORK_QUEUE:
		case BATCH_QUEUE_TYPE_WORK_QUEUE_SHAREDFS:
			return dag_task_category_wrap_as_wq_options(category, default_options);
			break;
		case BATCH_QUEUE_TYPE_CONDOR:
			return dag_task_category_wrap_as_condor_options(category, default_options);
			break;
		default:
			if(default_options)
				return xxstrdup(default_options);
			else
				return NULL;
	}
}

int dag_file_is_source(struct dag_file *f)
{
	if(f->target_of)
		return 0;
	else
		return 1;
}

int dag_file_is_sink(struct dag_file *f)
{
	if(list_size(f->needed_by) > 0)
		return 0;
	else
		return 1;
}

int dag_node_is_source(struct dag_node *n)
{
	return (set_size(n->ancestors) == 0);
}

int dag_node_is_sink(struct dag_node *n)
{
	return (set_size(n->descendants) == 0);
}


