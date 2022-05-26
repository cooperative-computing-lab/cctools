/*
Copyright (C) 2019- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "dag.h"
#include "dag_node.h"
#include "dag_variable.h"
#include "dag_resources.h"
#include "parser_jx.h"
#include "xxmalloc.h"
#include "set.h"
#include "itable.h"
#include "stringtools.h"
#include "path.h"
#include "hash_table.h"
#include "debug.h"
#include "parser.h"
#include "rmsummary.h"
#include "jx_eval.h"
#include "jx_match.h"
#include "jx_print.h"

#include <assert.h>

/* Print error to stderr for the user, and with D_NOTICE, for logs. */
void report_error(int64_t line, const char *message, struct jx *actual)
{
	char *fmt;

	if(actual) {
		char *str = jx_print_string(actual);
		fmt = string_format("makeflow: line %" PRId64 ": expected %s but got %s instead\n", line, message, str);
		free(str);
	} else {
		fmt = string_format("makeflow: line %" PRId64 ": %s", line, message);
	}

	debug(D_MAKEFLOW_PARSER, "%s", fmt);
	fprintf(stderr, "%s", fmt);

	free(fmt);
}

static int environment_from_jx(struct dag *d, struct dag_node *n, struct hash_table *h, struct jx *env)
{
	int nodeid;

	if(!env) {
		debug(D_MAKEFLOW_PARSER, "No \"environment\" specified");
		return 1;
	}
	debug(D_MAKEFLOW_PARSER, "Line %u: Parsing \"environment\"", env->line);

	if(n) {
		nodeid = n->nodeid;
	} else {
		nodeid = 0;
	}

	if(jx_istype(env, JX_OBJECT)) {
		const char *key;
		void *i = NULL;
		while((key = jx_iterate_keys(env, &i))) {
			const char *value;
			debug(D_MAKEFLOW_PARSER, "export %s", key);
			if((value = jx_lookup_string(env, key))) {
				debug(D_MAKEFLOW_PARSER, "env %s=%s", key, value);
				dag_variable_add_value(key, h, nodeid, value);
			}
			string_set_insert(d->export_vars, key);
		}
	} else {
		report_error(env->line, "a JSON object", env);
		return 0;
	}
	return 1;
}

static int resources_from_jx(struct hash_table *h, struct jx *j, int nodeid)
{
	if(!j) {
		debug(D_MAKEFLOW_PARSER, "No \"resources\" specified");
		return 1;
	}
	debug(D_MAKEFLOW_PARSER, "Line %u: Parsing \"resources\"", j->line);

	const char *key;
	void *i = NULL;
	while((key = jx_iterate_keys(j, &i))) {
		if(!strcmp(key, "cores")) {
			int cores = jx_lookup_integer(j, "cores");
			if(cores) {
				debug(D_MAKEFLOW_PARSER, "%d core(s)", cores);
				dag_variable_add_value(RESOURCES_CORES, h, nodeid, string_format("%d", cores));
			}
		} else if(!strcmp(key, "disk")) {
			int disk = jx_lookup_integer(j, "disk");
			if(disk) {
				debug(D_MAKEFLOW_PARSER, "%d disk", disk);
				dag_variable_add_value(RESOURCES_DISK, h, nodeid, string_format("%d", disk));
			}
		} else if(!strcmp(key, "memory")) {
			int memory = jx_lookup_integer(j, "memory");
			if(memory) {
				debug(D_MAKEFLOW_PARSER, "%d memory", memory);
				dag_variable_add_value(RESOURCES_MEMORY, h, nodeid, string_format("%d", memory));
			}
		} else if(!strcmp(key, "gpus")) {
			int gpus = jx_lookup_integer(j, "gpus");
			if(gpus > -1) {  // Note that when "gpus" is missing, this defaults to 0
				debug(D_MAKEFLOW_PARSER, "%d gpus", gpus);
				dag_variable_add_value(RESOURCES_GPUS, h, nodeid, string_format("%d", gpus));
			}
		} else if(!strcmp(key, "mpi-processes")) {
			int procs = jx_lookup_integer(j, "mpi-processes");
			if(procs) {
				debug(D_MAKEFLOW_PARSER, "%d mpi-processes", procs);
				dag_variable_add_value(RESOURCES_MPI_PROCESSES, h, nodeid, string_format("%d", procs));
			}
		} else if(!strcmp(key, "wall-time")) {
			int wall_time = jx_lookup_integer(j, "wall-time");
			if(wall_time) {
				debug(D_MAKEFLOW_PARSER, "%d wall-time", wall_time);
				dag_variable_add_value(RESOURCES_WALL_TIME, h, nodeid, string_format("%d", wall_time));
			}
		} else {
			debug(D_MAKEFLOW_PARSER, "Line %u: Unknown resource %s", j->line, key);
			return 0;
		}
	}

	return 1;
}

static int file_from_jx(struct dag_node *n, int input, struct jx *j)
{
	assert(j);
	assert(n);
	const char *path = NULL;
	const char *remote = NULL;

	if(jx_istype(j, JX_STRING)) {
		path = j->u.string_value;
	} else if(jx_istype(j, JX_OBJECT)) {
		path = jx_lookup_string(j, "dag_name");
		remote = jx_lookup_string(j, "task_name");
		if(!path) {
			report_error(j->line, "missing a \"dag_name key\".", NULL);
			return 0;
		}
	} else {
		report_error(j->line, "expected a file specification as a string or object", j);
		return 0;
	}

	if(input) {
		debug(D_MAKEFLOW_PARSER, "Input %s, remote name %s", path, remote ? remote : "NULL");
		dag_node_add_source_file(n, path, remote);
	} else {
		debug(D_MAKEFLOW_PARSER, "Output %s, remote name %s", path, remote ? remote : "NULL");
		dag_node_add_target_file(n, path, remote);
	}
	return 1;
}

static int files_from_jx(struct dag_node *n, int inputs, struct jx *j)
{
	if(!j) {
		debug(D_MAKEFLOW_PARSER, "files missing");
		return 1;
	}
	if(!jx_istype(j, JX_ARRAY)) {
		report_error(j->line, "a list of files as JSON array", j);
		return 0;
	}
	struct jx *item;
	void *i = NULL;
	while((item = jx_iterate_array(j, &i))) {
		if(!file_from_jx(n, inputs, item)) {
			return 0;
		}
	}
	return 1;
}

static int rule_from_jx(struct dag *d, struct jx *j)
{
	assert(j);

	debug(D_MAKEFLOW_PARSER, "Line %u: Parsing rule", j->line);
	struct dag_node *n = dag_node_create(d, j->line);

	struct jx *inputs = jx_lookup(j, "inputs");
	debug(D_MAKEFLOW_PARSER, "Parsing inputs");
	if(!files_from_jx(n, 1, inputs)) {
		report_error(j->line, "could not parse rule inputs.", NULL);
		return 0;
	}
	struct jx *outputs = jx_lookup(j, "outputs");
	debug(D_MAKEFLOW_PARSER, "Parsing outputs");
	if(!files_from_jx(n, 0, outputs)) {
		report_error(j->line, "could not parse rule outputs.", NULL);
		return 0;
	}

	const char *command = jx_lookup_string(j, "command");
	const char *workflow = jx_lookup_string(j, "workflow");

	if(workflow && command) {
		report_error(j->line, "rule is invalid because it defines both a command and a workflow.", NULL);
		return 0;
	}

	if(command) {
		debug(D_MAKEFLOW_PARSER, "command: %s", command);
		dag_node_set_command(n,command);
	} else if(workflow) {
		struct jx *args = jx_lookup(j, "args");
		debug(D_MAKEFLOW_PARSER, "Line %u: sub-workflow at %s", j->line,workflow);
		dag_node_set_workflow(n, workflow, args, 1);
	} else {
		report_error(j->line, "rule neither defines a command nor a sub-workflow.", NULL);
		return 0;
	}

	dag_node_insert(n);

	n->local_job = jx_lookup_boolean(j, "local_job");
	if(n->local_job) {
		debug(D_MAKEFLOW_PARSER, "Rule at line %u: Local job", j->line);
	}

	const char *category = jx_lookup_string(j, "category");
	if(category) {
		debug(D_MAKEFLOW_PARSER, "Category %s", category);
		n->category = makeflow_category_lookup_or_create(d, category);
	} else {
		debug(D_MAKEFLOW_PARSER, "Rule at line %u: category malformed or missing, using default", j->line);
		n->category = makeflow_category_lookup_or_create(d, "default");
	}

	struct jx *resource = jx_lookup(j, "resources");
	if(resource && !resources_from_jx(n->variables, resource, n->nodeid)) {
		report_error(j->line, "a resource definition", resource);
		return 0;
	}

	const char *allocation = jx_lookup_string(j, "allocation");
	if(allocation) {
		if(!strcmp(allocation, "first") || !strcmp(allocation, "auto")) {	// "first" is deprecated
			debug(D_MAKEFLOW_PARSER, "Rule at line %u: auto allocation", j->line);
			n->resource_request = CATEGORY_ALLOCATION_FIRST;
		} else if(!strcmp(allocation, "max")) {
			debug(D_MAKEFLOW_PARSER, "Rule at line %u: max allocation", j->line);
			n->resource_request = CATEGORY_ALLOCATION_MAX;
		} else if(!strcmp(allocation, "error")) {
			debug(D_MAKEFLOW_PARSER, "Rule at line %u: error allocation", j->line);
			n->resource_request = CATEGORY_ALLOCATION_ERROR;
		} else {
			report_error(j->line, "one of \"max\", \"auto\", or \"error\"", j);
			return 0;
		}
	}

	environment_from_jx(d, n, n->variables, jx_lookup(j, "environment"));

	return 1;
}

static int category_from_jx(struct dag *d, const char *name, struct jx *j)
{
	assert(j);

	struct category *c = makeflow_category_lookup_or_create(d, name);
	struct jx *resource = jx_lookup(j, "resources");
	if(resource && !resources_from_jx(c->mf_variables, resource, 0)) {
		report_error(resource->line, "a resources definition", j);
		return 0;
	}
	struct jx *environment = jx_lookup(j, "environment");
	if(environment && !environment_from_jx(d, NULL, c->mf_variables, environment)) {
		report_error(environment->line, "an environment definition", j);
		return 0;
	}
	return 1;
}

struct dag *dag_parse_jx(struct dag *d, struct jx *j)
{
	if(!j) {
		report_error(0, "a workflow definition is missing.", NULL);
		return NULL;
	}
	if(!jx_istype(j, JX_OBJECT)) {
		report_error(0, "a workflow definition as a JSON object", j);
		return NULL;
	}

	debug(D_MAKEFLOW_PARSER, "Parsing categories");
	struct jx *categories = jx_lookup(j, "categories");
	if(jx_istype(categories, JX_OBJECT)) {
		const char *key;
		void *i = NULL;
		while((key = jx_iterate_keys(categories, &i))) {
			struct jx *value = jx_lookup(categories, key);
			if(!category_from_jx(d, key, value)) {
				report_error(value->line, "a category definition as a JSON object", j);
				return NULL;
			}
		}
	} else {
		debug(D_MAKEFLOW_PARSER, "Workflow at line %u: categories malformed or missing", j->line);
	}

	const char *default_category = jx_lookup_string(j, "default_category");
	if(default_category) {
		debug(D_MAKEFLOW_PARSER, "Default category %s", default_category);
	} else {
		debug(D_MAKEFLOW_PARSER, "Workflow at line %u: default_category malformed or missing, using \"default\"", j->line);
		default_category = "default";
	}
	d->default_category = makeflow_category_lookup_or_create(d, default_category);

	struct jx *environment = jx_lookup(j, "environment");
	if(environment && !environment_from_jx(d, NULL, d->default_category->mf_variables, environment)) {
		report_error(environment->line, "an environment definition as a JSON object", environment);
		return NULL;
	} else {
		debug(D_MAKEFLOW_PARSER, "Workflow at line %u: Top-level environment malformed or missing", j->line);
	}

	struct jx *rules = jx_lookup(j, "rules");
	if(jx_istype(rules, JX_ARRAY)) {
		struct jx *item;
		void *i = NULL;
		while((item = jx_iterate_array(rules, &i))) {
			if(!rule_from_jx(d, item)) {
				report_error(item->line, "error parsing the rule.", NULL);
				return NULL;
			}
		}
	}

	dag_close_over_environment(d);
	dag_close_over_nodes(d);
	dag_close_over_categories(d);

	dag_compile_ancestors(d);

	return d;
}
