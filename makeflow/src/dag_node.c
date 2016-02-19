/*
Copyright (C) 2014- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "dag.h"
#include "dag_node.h"

#include "debug.h"
#include "rmsummary.h"
#include "list.h"
#include "stringtools.h"
#include "xxmalloc.h"
#include "jx.h"

#include <string.h>
#include <stdlib.h>
#include <unistd.h>

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

	n->resources_needed = rmsummary_create(-1);
	n->resources_measured = NULL;

	n->resource_request = CATEGORY_ALLOCATION_UNLABELED;

	return n;
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

/* Returns the remotename used in rule n for local name filename */
const char *dag_node_get_remote_name(struct dag_node *n, const char *filename)
{
	struct dag_file *f;
	char *name;

	f = dag_file_from_name(n->d, filename);
	name = (char *) itable_lookup(n->remote_names, (uintptr_t) f);

	return name;
}

/* Returns the local name of filename */
const char *dag_node_get_local_name(struct dag_node *n, const char *filename)
{
	struct dag_file *f;
	const char *name;

	f = hash_table_lookup(n->remote_names_inv, filename);

	if(!f)
	{
		name =  NULL;
	}
	else
	{
		name = f->filename;
	}

	return name;
}

/* Translate an absolute filename into a unique slash-less name to allow for the
   sending of any file to remote systems. The function allows for upto a million name collisions. */
static char *dag_node_translate_filename(struct dag_node *n, const char *filename)
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

/* Adds remotename to the local name filename in the namespace of
 * the given node. If remotename is NULL, then a new name is
 * found using dag_node_translate_filename. If the remotename
 * given is different from a previosly specified, a warning is
 * written to the debug output, but otherwise this is ignored. */
static const char *dag_node_add_remote_name(struct dag_node *n, const char *filename, const char *remotename)
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

	if(target->created_by && target->created_by != n)
		fatal("%s is defined multiple times at %s:%d and %s:%d\n", filename, filename, target->created_by->linenum, filename, n->linenum);

	if(remotename)
		dag_node_add_remote_name(n, filename, remotename);

	/* register this file as a target of the node */
	list_push_head(n->target_files, target);

	/* register this node as the creator of the file */
	target->created_by = n;
}

void dag_node_init_resources(struct dag_node *n)
{
	struct rmsummary *rs    = n->resources_needed;
	struct dag_variable_lookup_set s_node = { NULL, NULL, n, NULL };
	struct dag_variable_lookup_set s_all  = { n->d, n->category, n, NULL };

	struct dag_variable_value *val;

	/* first pass, only node variables. We only check if this node was individually labeled. */
	val = dag_variable_lookup(RESOURCES_CORES, &s_node);
	if(val)
		n->resource_request = CATEGORY_ALLOCATION_USER;

	val = dag_variable_lookup(RESOURCES_DISK, &s_node);
	if(val)
		n->resource_request = CATEGORY_ALLOCATION_USER;

	val = dag_variable_lookup(RESOURCES_MEMORY, &s_node);
	if(val)
		n->resource_request = CATEGORY_ALLOCATION_USER;

	val = dag_variable_lookup(RESOURCES_GPUS, &s_node);
	if(val)
		n->resource_request = CATEGORY_ALLOCATION_USER;


	int category_flag = 0;
	/* second pass: fill fall-back values if at least one resource was individually labeled. */
	/* if not, resources will come from the category when submitting. */
	val = dag_variable_lookup(RESOURCES_CORES, &s_all);
	if(val) {
		category_flag = 1;
		rs->cores = atoll(val->value);
	}

	val = dag_variable_lookup(RESOURCES_DISK, &s_all);
	if(val) {
		category_flag = 1;
		rs->disk = atoll(val->value);
	}

	val = dag_variable_lookup(RESOURCES_MEMORY, &s_all);
	if(val) {
		category_flag = 1;
		rs->memory = atoll(val->value);
	}

	val = dag_variable_lookup(RESOURCES_GPUS, &s_all);
	if(val) {
		category_flag = 1;
		rs->gpus = atoll(val->value);
	}

	if(n->resource_request != CATEGORY_ALLOCATION_USER && category_flag) {
		n->resource_request = CATEGORY_ALLOCATION_AUTO_ZERO;
	}
}

int dag_node_update_resources(struct dag_node *n, int overflow)
{
	if(overflow && (n->resource_request == CATEGORY_ALLOCATION_USER || n->resource_request == CATEGORY_ALLOCATION_AUTO_MAX || n->resource_request == CATEGORY_ALLOCATION_UNLABELED)) {
		return 0;
	}

	struct rmsummary *rs = n->resources_needed;
	struct rmsummary *rc = n->category->max_allocation;
	struct rmsummary *rd = n->d->default_category->max_allocation;

	if(overflow) {
		n->resource_request = CATEGORY_ALLOCATION_AUTO_MAX;
		rs->cores  = rc->cores  > -1 ? rc->cores  : rd->cores;
		rs->memory = rc->memory > -1 ? rc->memory : rd->memory;
		rs->disk   = rc->disk   > -1 ? rc->disk   : rd->disk;
		rs->gpus   = rc->gpus   > -1 ? rc->gpus   : rd->gpus;

		return 1;
	}

	if(n->category->first_allocation) {
		rc = n->category->first_allocation;

		n->resource_request = CATEGORY_ALLOCATION_AUTO_FIRST;
		rs->cores  = rc->cores  > -1 ? rc->cores  : rd->cores;
		rs->memory = rc->memory > -1 ? rc->memory : rd->memory;
		rs->disk   = rc->disk   > -1 ? rc->disk   : rd->disk;
		rs->gpus   = rc->gpus   > -1 ? rc->gpus   : rd->gpus;

		return 1;
	}

	/* else, no change possible, we keep the CATEGORY_ALLOCATION_AUTO_ZERO as is. */

	return 1;
}

void dag_node_print_debug_resources(struct dag_node *n)
{
	if( n->resources_needed->cores > -1 )
		debug(D_MAKEFLOW_RUN, "cores:  %"PRId64".\n",      n->resources_needed->cores);
	if( n->resources_needed->memory > -1 )
		debug(D_MAKEFLOW_RUN, "memory:   %"PRId64" MB.\n", n->resources_needed->memory);
	if( n->resources_needed->disk > -1 )
		debug(D_MAKEFLOW_RUN, "disk:     %"PRId64" MB.\n", n->resources_needed->disk);
	if( n->resources_needed->gpus > -1 )
		debug(D_MAKEFLOW_RUN, "gpus:  %"PRId64".\n",       n->resources_needed->gpus);
}

/*
Creates a jx object containing the explicit environment
strings for this given node.
*/

struct jx * dag_node_env_create( struct dag *d, struct dag_node *n )
{
	struct dag_variable_lookup_set s = { d, n->category, n, NULL };
	char *key;

	struct jx *object = jx_object(0);

	set_first_element(d->export_vars);
	while((key = set_next_element(d->export_vars))) {
		char *value = dag_variable_lookup_string(key, &s);
		if(value) {
			jx_insert(object,jx_string(key),jx_string(value));
			debug(D_MAKEFLOW_RUN, "export %s=%s", key, value);
		}
	}

	return object;
}

/* vim: set noexpandtab tabstop=4: */
