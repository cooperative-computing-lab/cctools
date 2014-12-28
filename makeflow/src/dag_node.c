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

#include <string.h>
#include <stdlib.h>
#include <unistd.h>

#define PARSING_RULE_MOD_COUNTER 250

extern int verbose_parsing;

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

	n->resources = make_rmsummary(-1);

	if(verbose_parsing && d->nodeid_counter % PARSING_RULE_MOD_COUNTER == 0)
	{
		fprintf(stdout, "\rRules parsed: %d", d->nodeid_counter + 1);
		fflush(stdout);
	}

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

void dag_node_state_change(struct dag *d, struct dag_node *n, int newstate)
{
	static time_t last_fsync = 0;

	debug(D_MAKEFLOW_RUN, "node %d %s -> %s\n", n->nodeid, dag_node_state_name(n->state), dag_node_state_name(newstate));

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
	fprintf(d->logfile, "%" PRIu64 " %d %d %" PRIbjid " %d %d %d %d %d %d\n", timestamp_get(), n->nodeid, newstate, n->jobid, d->node_states[0], d->node_states[1], d->node_states[2], d->node_states[3], d->node_states[4], d->nodeid_counter);

	if(time(NULL) - last_fsync > 60) {
		/* We use fsync here to gurantee that the log is syncronized in AFS,
		 * even if something goes wrong with the node running makeflow. Using
		 * fsync comes with an overhead, so we do not fsync more than once per
		 * minute. This avoids hammering AFS, and reduces the overhead for
		 * short running tasks, while having the desired effect for long
		 * running workflows. */

		fsync(fileno(d->logfile));
		last_fsync = time(NULL);
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

	if(target->target_of && target->target_of != n)
		fatal("%s is defined multiple times at %s:%d and %s:%d\n", filename, filename, target->target_of->linenum, filename, n->linenum);

	if(remotename)
		dag_node_add_remote_name(n, filename, remotename);

	/* register this file as a target of the node */
	list_push_head(n->target_files, target);

	/* register this node as the creator of the file */
	target->target_of = n;
}

void dag_node_fill_resources(struct dag_node *n)
{
	struct rmsummary *rs    = n->resources;
	struct dag_variable_lookup_set s = { n->d, n->category, n, NULL };

	char    *val_str;

	val_str = dag_variable_lookup_string(RESOURCES_CORES, &s);
	if(val_str)
		rs->cores = atoll(val_str);

	val_str = dag_variable_lookup_string(RESOURCES_DISK, &s);
	if(val_str)
		rs->workdir_footprint = atoll(val_str);

	val_str = dag_variable_lookup_string(RESOURCES_MEMORY, &s);
	if(val_str)
		rs->resident_memory = atoll(val_str);

	val_str = dag_variable_lookup_string(RESOURCES_GPUS, &s);
	if(val_str)
		rs->gpus = atoll(val_str);
}

void dag_node_print_debug_resources(struct dag_node *n)
{
	if( n->resources->cores > -1 )
		debug(D_MAKEFLOW_RUN, "cores:  %"PRId64".\n",      n->resources->cores);
	if( n->resources->resident_memory > -1 )
		debug(D_MAKEFLOW_RUN, "memory:   %"PRId64" MB.\n", n->resources->resident_memory);
	if( n->resources->workdir_footprint > -1 )
		debug(D_MAKEFLOW_RUN, "disk:     %"PRId64" MB.\n", n->resources->workdir_footprint);
	if( n->resources->gpus > -1 )
		debug(D_MAKEFLOW_RUN, "gpus:  %"PRId64".\n", n->resources->gpus);
}

char *dag_node_resources_wrap_as_wq_options(struct dag_node *n, const char *default_options)
{
	struct rmsummary *s;

	s = n->resources;

	char *options = NULL;

	options = string_format("%s resources: cores: %" PRId64 ", resident_memory: %" PRId64 ", workdir_footprint: %" PRId64,
			default_options           ? default_options      : "",
			s->cores             > -1 ? s->cores             : -1,
			s->resident_memory   > -1 ? s->resident_memory   : -1,
			s->workdir_footprint > -1 ? s->workdir_footprint : -1);

	return options;
}

#define add_monitor_field_int(options, s, field) \
	if(s->field > -1) \
	{ \
		char *opt = string_format("%s -L'%s: %" PRId64 "' ", options ? options : "", #field, s->field);\
		if(options)\
			free(options);\
		options = opt;\
	}

#define add_monitor_field_double(options, s, field) \
	if(s->field > -1) \
	{ \
		char *opt = string_format("%s -L'%s: %lf' ", options ? options : "", #field, s->field/1000000.0);\
		if(options)\
			free(options);\
		options = opt;\
	}

char *dag_node_resources_wrap_as_rmonitor_options(struct dag_node *n)
{
	struct rmsummary *s;

	s = n->resources;

	char *options = NULL;

	add_monitor_field_double(options, s, wall_time);
	add_monitor_field_int(options, s, max_concurrent_processes);
	add_monitor_field_int(options, s, total_processes);
	add_monitor_field_double(options, s, cpu_time);
	add_monitor_field_int(options, s, virtual_memory);
	add_monitor_field_int(options, s, resident_memory);
	add_monitor_field_int(options, s, swap_memory);
	add_monitor_field_int(options, s, bytes_read);
	add_monitor_field_int(options, s, bytes_written);
	add_monitor_field_int(options, s, workdir_num_files);
	add_monitor_field_int(options, s, workdir_footprint);

	return options;
}

/* works as realloc for the first argument */
char *dag_node_resources_add_condor_option(char *options, const char *expression, int64_t value)
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

char *dag_node_resources_wrap_as_condor_options(struct dag_node *n, const char *default_options)
{
	struct rmsummary *s;

	s = n->resources;

	char *options = NULL;
	char *opt;

	options = dag_node_resources_add_condor_option(options, "Cores>=", s->cores);
	options = dag_node_resources_add_condor_option(options, "Memory>=", s->resident_memory);
	options = dag_node_resources_add_condor_option(options, "Disk>=", s->workdir_footprint);

	if(!options)
	{
		if(default_options)
			return xxstrdup(default_options);
		return
			NULL;
	}

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

char *dag_node_resources_wrap_options(struct dag_node *n, const char *default_options, batch_queue_type_t batch_type)
{
	switch(batch_type)
	{
		case BATCH_QUEUE_TYPE_WORK_QUEUE:
			return dag_node_resources_wrap_as_wq_options(n, default_options);
			break;
		case BATCH_QUEUE_TYPE_CONDOR:
			return dag_node_resources_wrap_as_condor_options(n, default_options);
			break;
		default:
			if(default_options)
				return xxstrdup(default_options);
			else
				return NULL;
	}
}

int dag_node_is_source(struct dag_node *n)
{
	return (set_size(n->ancestors) == 0);
}

int dag_node_is_sink(struct dag_node *n)
{
	return (set_size(n->descendants) == 0);
}



