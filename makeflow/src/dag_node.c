/*
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "dag.h"
#include "dag_node_footprint.h"
#include "dag_node.h"

#include "debug.h"
#include "batch_job.h"
#include "rmsummary.h"
#include "list.h"
#include "stringtools.h"
#include "xxmalloc.h"
#include "jx.h"
#include "jx_print.h"

#include <assert.h>
#include <errno.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>

extern char **environ; 

struct dag_node *dag_node_create(struct dag *d, int linenum)
{
	struct dag_node *n = calloc(1, sizeof(*n));

	n->d = d;
	n->linenum = linenum;
	n->state = DAG_NODE_STATE_WAITING;
	n->nodeid = d->nodeid_counter++;
	n->variables = hash_table_create(0, 0);

	n->type = DAG_NODE_TYPE_COMMAND;
	n->source_files = list_create();
	n->target_files = list_create();

	n->remote_names = itable_create(0);
	n->remote_names_inv = hash_table_create(0, 0);

	n->descendants = set_create(0);
	n->ancestors = set_create(0);

	n->ancestor_depth = -1;

	// resources explicitely requested for only this node in the dag file.
	// PROBABLY not what you want. Most likely you want dag_node_dynamic_label(n)
	n->resources_requested = rmsummary_create(-1);

	// the value of dag_node_dynamic_label(n) when this node was submitted.
	n->resources_allocated  = rmsummary_create(-1);

	// resources used by the node, as measured by the resource_monitor (if
	// using monitoring).
	n->resources_measured  = NULL;

	n->resource_request = CATEGORY_ALLOCATION_FIRST;

	// arguments for subworkflow nodes
	n->workflow_args = NULL;

	return n;
}

void dag_node_delete(struct dag_node *n)
{
	hash_table_delete(n->variables);

	itable_delete(n->remote_names);
	hash_table_delete(n->remote_names_inv);

	set_delete(n->descendants);
	set_delete(n->ancestors);

	list_delete(n->source_files);
	list_delete(n->target_files);

	if(n->footprint)
		dag_node_footprint_delete(n->footprint);

	rmsummary_delete(n->resources_requested);
	if(n->resources_measured)
		rmsummary_delete(n->resources_measured);


	jx_delete(n->workflow_args);
	free(n);
}

void dag_node_set_command(struct dag_node *n, const char *cmd) {
	assert(n);
	assert(cmd);
	assert(!n->command);
	assert(!n->workflow_file);

	n->type = DAG_NODE_TYPE_COMMAND;
	n->command = xxstrdup(cmd);
}

const char *dag_node_nested_workflow_filename(struct dag_node *n, const char *which_file) {
	static char filename[PATH_MAX];
	snprintf(filename, PATH_MAX, "%s.%d.%s", n->workflow_file, n->nodeid, which_file);
	return filename;
}

void dag_node_set_workflow(struct dag_node *n, const char *dag, struct jx * args, int is_jx )
{
	assert(n);
	assert(dag);
	assert(!n->workflow_file);

	n->type = DAG_NODE_TYPE_WORKFLOW;
	n->workflow_file = xxstrdup(dag);
	n->workflow_is_jx = is_jx;

	n->workflow_args = jx_copy(args);

	if(n->workflow_args) { 
		const char *args_file = dag_node_nested_workflow_filename(n, "args");
		FILE *file = fopen(args_file,"w");
		jx_print_stream(n->workflow_args,file);
		fclose(file);
	}

	/* Record a placeholder in the command field */
	/* A usable command will be created at submit time. */

	n->command = xxstrdup(n->workflow_file);
}

void dag_node_insert(struct dag_node *n) {
	assert(n);
	assert(n->d);

	n->next = n->d->nodes;
	n->d->nodes = n;
	itable_insert(n->d->node_table, n->nodeid, n);
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

void dag_node_set_umbrella_spec(struct dag_node *n, const char *umbrella_spec)
{
	struct stat st;

	if(!n) return;

	if(lstat(umbrella_spec, &st) == -1) {
		fatal("lstat(`%s`) failed: %s\n", umbrella_spec, strerror(errno));
	}
	if((st.st_mode & S_IFMT) != S_IFREG) {
		fatal("the umbrella spec (`%s`) should specify a regular file\n", umbrella_spec);
	}

	n->umbrella_spec = umbrella_spec;
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
void dag_node_add_source_file(struct dag_node *n, const char *filename, const char *remotename)
{
	struct dag_file *source = dag_file_lookup_or_create(n->d, filename);

	if(remotename)
		dag_node_add_remote_name(n, filename, remotename);

	/* register this file as a source of the node */
	list_push_head(n->source_files, source);

	/* register this file as a requirement of the node */
	list_push_head(source->needed_by, n);

	source->reference_count++;
}

/* Adds the local name as a target of the node, and register the
 * node as the producer of the file. If remotename is not NULL,
 * it is added to the namespace of the node. */
void dag_node_add_target_file(struct dag_node *n, const char *filename, const char *remotename)
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

void dag_node_print_debug_resources(struct dag_node *n)
{
	const struct rmsummary *r = dag_node_dynamic_label(n);

	if(!r)
		return;

	if( r->cores > -1 )
		debug(D_MAKEFLOW_RUN, "cores:  %s\n", rmsummary_resource_to_str("cores", r->cores, 0));
	if( r->memory > -1 )
		debug(D_MAKEFLOW_RUN, "memory: %s\n", rmsummary_resource_to_str("memory", r->memory, 1));
	if( r->disk > -1 )
		debug(D_MAKEFLOW_RUN, "disk:   %s\n", rmsummary_resource_to_str("disk", r->disk, 0));
	if( r->gpus > -1 )
		debug(D_MAKEFLOW_RUN, "gpus:   %s\n", rmsummary_resource_to_str("gpus", r->gpus, 1));
}

void dag_node_add_local_environment(struct jx *j) {

		char **var;
		for(var = environ; *var; var++) {
			char *name   = xxstrdup(*var);
			char *value  = strchr(name, '=');

			if(value) {
				*value = '\0'; 
				value++;
			} else {
				value = "";
			}

			jx_insert(j, jx_string(name), jx_string(value));

			free(name);
		}
}

/*
Creates a jx object containing the explicit environment
strings for this given node.
*/

struct jx * dag_node_env_create( struct dag *d, struct dag_node *n, int should_send_all_local_environment )
{
	struct dag_variable_lookup_set s = { d, n->category, n, NULL };
	char *key;

	struct jx *object = jx_object(0);

	if(should_send_all_local_environment) {
		dag_node_add_local_environment(object);
	}

	char *num_cores = dag_variable_lookup_string(RESOURCES_CORES, &s);
	char *num_omp_threads = dag_variable_lookup_string("OMP_NUM_THREADS", &s);

	if (num_cores && !num_omp_threads) {
		// if number of cores is set, number of omp threads is not set,
		// then we set number of omp threads to number of cores
		jx_insert(object, jx_string("OMP_NUM_THREADS"), jx_string(num_cores));
	} else if (num_omp_threads) {
		// if number of omp threads is set, then we set number of cores
		// to the number of omp threads
		jx_insert(object, jx_string(RESOURCES_CORES), jx_string(num_omp_threads));
	} else {
		// if both number of cores and omp threads are not set, we
		// set them to 1
		jx_insert(object, jx_string("OMP_NUM_THREADS"), jx_string("1"));
		jx_insert(object, jx_string(RESOURCES_CORES), jx_string("1"));
	}

	string_set_first_element(d->export_vars);
	while(string_set_next_element(d->export_vars, &key)) {
		char *value = dag_variable_lookup_string(key, &s);
		if(value) {
			jx_insert(object,jx_string(key),jx_string(value));
			debug(D_MAKEFLOW_RUN, "export %s=%s", key, value);
			free(value);
		}
	}

	free(num_cores);
	free(num_omp_threads);

	return object;
}

/* Return resources according to request. */

const struct rmsummary *dag_node_dynamic_label(const struct dag_node *n) {
	return category_task_max_resources(n->category, n->resources_requested, n->resource_request, -1);
}

/* Return JX object containing cmd, inputs, outputs, env, and resources. */

struct jx * dag_node_to_jx( struct dag *d, struct dag_node *n, int send_all_local_env)
{
	struct jx *task = jx_object(0);

	jx_insert(task, jx_string("resources"), rmsummary_to_json(dag_node_dynamic_label(n), 1));
	jx_insert(task, jx_string("category"), jx_string(n->category->name));
	jx_insert(task, jx_string("environment"), dag_node_env_create(d, n, send_all_local_env));

	struct dag_file *f = NULL;

	struct jx *outputs = jx_array(0);
	list_first_item(n->target_files);
	while((f = list_next_item(n->target_files))) {
		jx_array_insert(outputs, dag_file_to_jx(f, n));
	}
	jx_insert(task, jx_string("outputs"), outputs);

	struct jx *inputs = jx_array(0);
	list_first_item(n->source_files);
	while((f = list_next_item(n->source_files))) {
		jx_array_insert(inputs, dag_file_to_jx(f, n));
	}
	jx_insert(task, jx_string("inputs"), inputs);

	jx_insert(task, jx_string("command"), jx_string(n->command));

	return task;
}

/* vim: set noexpandtab tabstop=4: */
