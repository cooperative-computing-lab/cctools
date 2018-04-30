/*
Copyright (C) 2016- The University of Notre Dame
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

static int environment_from_jx(struct dag *d, struct dag_node *n, struct hash_table *h, struct jx *env) {
	int nodeid;

	if (!env) {
		debug(D_MAKEFLOW_PARSER, "No \"environment\" specified");
		return 1;
	}
	debug(D_MAKEFLOW_PARSER, "Line %u: Parsing \"environment\"", env->line);

	if (n) {
		nodeid = n->nodeid;
	} else {
		nodeid = 0;
	}

	if (jx_istype(env, JX_OBJECT)) {
		const char *key;
		void *i = NULL;
		while ((key = jx_iterate_keys(env, &i))) {
			key = xxstrdup(key);
			const char *value;
			debug(D_MAKEFLOW_PARSER, "export %s", key);
			if ((value = jx_lookup_string(env, key))) {
				debug(D_MAKEFLOW_PARSER, "env %s=%s", key, value);
				dag_variable_add_value(key, h, nodeid, value);
			}
			string_set_insert(d->export_vars, key);
		}
	} else {
		debug(D_MAKEFLOW_PARSER|D_NOTICE,
			"Line %u: Expected environment to be an object",
			env->line);
		return 0;
	}
	return 1;
}

static int resources_from_jx(struct hash_table *h, struct jx *j, int nodeid) {
	if (!j) {
		debug(D_MAKEFLOW_PARSER, "No \"resources\" specified");
		return 1;
	}
	debug(D_MAKEFLOW_PARSER, "Line %u: Parsing \"resources\"", j->line);

	const char *key;
	void *i = NULL;
	while ((key = jx_iterate_keys(j, &i))) {
		if(!strcmp(key, "cores")){	
			int cores = jx_lookup_integer(j, "cores");
			if (cores) {
				debug(D_MAKEFLOW_PARSER, "%d core(s)", cores);
				dag_variable_add_value(RESOURCES_CORES, h, nodeid, string_format("%d", cores));
			}
		} else if(!strcmp(key, "disk")){	
			int disk = jx_lookup_integer(j, "disk");
			if (disk) {
				debug(D_MAKEFLOW_PARSER, "%d disk", disk);
				dag_variable_add_value(RESOURCES_DISK, h, nodeid, string_format("%d", disk));
			}
		} else if(!strcmp(key, "memory")){	
			int memory = jx_lookup_integer(j, "memory");
			if (memory) {
				debug(D_MAKEFLOW_PARSER, "%d memory", memory);
				dag_variable_add_value(RESOURCES_MEMORY, h, nodeid, string_format("%d", memory));
			}
		} else if(!strcmp(key, "memory")){	
			int gpus = jx_lookup_integer(j, "gpus");
			if (gpus) {
				debug(D_MAKEFLOW_PARSER, "%d gpus", gpus);
				dag_variable_add_value(RESOURCES_GPUS, h, nodeid, string_format("%d", gpus));
			}
		} else {
			debug(D_MAKEFLOW_PARSER, "Line %u: Unknown resource %s", j->line, key);
			return 0;
		}
	}

	return 1;
}

static int file_from_jx(struct dag_node *n, int input, struct jx *j) {
	assert(j);
	assert(n);
	const char *path = NULL;
	const char *remote = NULL;

	if (jx_istype(j, JX_STRING)) {
		path = j->u.string_value;
	} else if (jx_istype(j, JX_OBJECT)) {
		path = jx_lookup_string(j, "dag_name");
		remote = jx_lookup_string(j, "task_name");
		if (!path) {
			debug(D_MAKEFLOW_PARSER | D_NOTICE,
				"File at line %u: missing \"dag_name\" key",
				j->line);
			return 0;
		}
	} else {
		debug(D_MAKEFLOW_PARSER | D_NOTICE,
			"Line %u: File must be specified as a string or object",
			j->line);
		return 0;
	}

	if (input) {
		debug(D_MAKEFLOW_PARSER, "Input %s, remote name %s", path, remote ? remote : "NULL");
		dag_node_add_source_file(n, path, remote);
	} else {
		debug(D_MAKEFLOW_PARSER, "Output %s, remote name %s", path, remote ? remote : "NULL");
		dag_node_add_target_file(n, path, remote);
	}
	return 1;
}

static int files_from_jx(struct dag_node *n, int inputs, struct jx *j) {
	if (!j) {
		debug(D_MAKEFLOW_PARSER, "files missing");
		return 1;
	}
	if (!jx_istype(j, JX_ARRAY)) {
		debug(D_MAKEFLOW_PARSER|D_NOTICE,
			"Line %u: files must be in a JSON array",
			j->line);
		return 0;
	}
	struct jx *item;
	void *i = NULL;
	while ((item = jx_iterate_array(j, &i))) {
		if (!file_from_jx(n, inputs, item)) {
			return 0;
		}
	}
	return 1;
}

static int rule_from_jx(struct dag *d, struct jx *j) {
	assert(j);

	debug(D_MAKEFLOW_PARSER, "Line %u: Parsing rule", j->line);
	struct dag_node *n = dag_node_create(d, j->line);

	struct jx *inputs = jx_lookup(j, "inputs");
	debug(D_MAKEFLOW_PARSER, "Parsing inputs");
	if (!files_from_jx(n, 1, inputs)) {
		debug(D_MAKEFLOW_PARSER|D_NOTICE,
			"Rule at line %u: Failure parsing inputs",
			j->line);
		return 0;
	}
	struct jx *outputs = jx_lookup(j, "outputs");
	debug(D_MAKEFLOW_PARSER, "Parsing outputs");
	if (!files_from_jx(n, 0, outputs)) {
		debug(D_MAKEFLOW_PARSER|D_NOTICE,
			"Rule at line %u: Failure parsing outputs",
			j->line);
		return 0;
	}

	struct jx *makeflow = jx_lookup(j, "makeflow");
	struct jx *command = jx_lookup(j, "command");

	if (makeflow && command) {
		debug(D_MAKEFLOW_PARSER|D_NOTICE,
			"Rule at line %u: can not have both command and submakeflow",
			j->line);
		return 0;
	}

	if (jx_istype(command, JX_STRING)) {
		debug(D_MAKEFLOW_PARSER, "command: %s", command->u.string_value);
		dag_node_set_command(n, command->u.string_value);
	} else if (jx_istype(makeflow, JX_OBJECT)) {
		const char *path = jx_lookup_string(makeflow, "path");
		const char *cwd = jx_lookup_string(makeflow, "cwd");

		if (!path) {
			debug(D_MAKEFLOW_PARSER|D_NOTICE,
				"Sub-Makeflow at line %u: must specify a path",
				makeflow->line);
			return 0;
		}
		debug(D_MAKEFLOW_PARSER, "Line %u: Submakeflow at %s", makeflow->line, path);
		dag_node_set_submakeflow(n, path, cwd);
		if (cwd) {
			debug(D_MAKEFLOW_PARSER, "working directory %s", cwd);
		} else {
			debug(D_MAKEFLOW_PARSER,
				"Sub-Makeflow at line %u: cwd malformed or missing, using process cwd",
				makeflow->line);
		}
	} else {
		debug(D_MAKEFLOW_PARSER|D_NOTICE,
			"Rule at line %u: must have a command or submakeflow", j->line);
		return 0;
	}

	dag_node_insert(n);

	n->local_job = jx_lookup_boolean(j, "local_job");
	if (n->local_job) {
		debug(D_MAKEFLOW_PARSER, "Rule at line %u: Local job", j->line);
	}

	const char *category = jx_lookup_string(j, "category");
	if (category) {
		debug(D_MAKEFLOW_PARSER, "Category %s", category);
		n->category = makeflow_category_lookup_or_create(d, category);
	} else {
		debug(D_MAKEFLOW_PARSER,
			"Rule at line %u: category malformed or missing, using default",
			j->line);
		n->category = makeflow_category_lookup_or_create(d, "default");
	}

	struct jx *resource = jx_lookup(j, "resources");
	if (resource && !resources_from_jx(n->variables, resource, n->nodeid)) {
		debug(D_MAKEFLOW_PARSER|D_NOTICE,
			"Rule at line %u: Failure parsing resources",
			j->line);
		return 0;
	}

	const char *allocation = jx_lookup_string(j, "allocation");
	if (allocation) {
		if (!strcmp(allocation, "first")) {
			debug(D_MAKEFLOW_PARSER,
				"Rule at line %u: first allocation",
				j->line);
			n->resource_request = CATEGORY_ALLOCATION_FIRST;
		} else if (!strcmp(allocation, "max")) {
			debug(D_MAKEFLOW_PARSER,
				"Rule at line %u: max allocation",
				j->line);
			n->resource_request = CATEGORY_ALLOCATION_MAX;
		} else if (!strcmp(allocation, "error")) {
			debug(D_MAKEFLOW_PARSER,
				"Rule at line %u: error allocation",
				j->line);
			n->resource_request = CATEGORY_ALLOCATION_ERROR;
		} else {
			debug(D_MAKEFLOW_PARSER|D_NOTICE,
				"Rule at line %u: Unknown allocation type",
				j->line);
			return 0;
		}
	}

	environment_from_jx(d, n, n->variables, jx_lookup(j, "environment"));

	return 1;
}

static int category_from_jx(struct dag *d, const char *name, struct jx *j) {
	assert(j);

	struct category *c = makeflow_category_lookup_or_create(d, name);
	struct jx *resource = jx_lookup(j, "resources");
	if (resource && !resources_from_jx(c->mf_variables, resource, 0)) {
		debug(D_MAKEFLOW_PARSER|D_NOTICE,
			"Line %u: Failure parsing resources",
			j->line);
		return 0;
	}
	struct jx *environment = jx_lookup(j, "environment");
	if (environment && !environment_from_jx(d, NULL, c->mf_variables, environment)) {
		debug(D_MAKEFLOW_PARSER|D_NOTICE,
			"Line %u: Failure parsing environment",
			environment->line);
		return 0;
	}
	return 1;
}

struct dag *dag_parse_jx(struct dag *d, struct jx *j) {
	if (!j) {
		debug(D_MAKEFLOW_PARSER|D_NOTICE, "Missing DAG");
		return NULL;
	}
	if (!jx_istype(j, JX_OBJECT)) {
		char *s = jx_print_string(j);
		debug(D_MAKEFLOW_PARSER|D_NOTICE,
			"Line %u: Workflow must be an object, got %s",
			j->line,
			s);
		free(s);
		return NULL;
	}

	debug(D_MAKEFLOW_PARSER, "Parsing categories");
	struct jx *categories = jx_lookup(j, "categories");
	if (jx_istype(categories, JX_OBJECT)) {
		const char *key;
		void *i = NULL;
		while ((key = jx_iterate_keys(categories, &i))) {
			struct jx *value = jx_lookup(categories, key);
			if (!category_from_jx(d, key, value)) {
				debug(D_MAKEFLOW_PARSER|D_NOTICE,
					"Line %u: Failure parsing category",
					value->line);
				return NULL;
			}
		}
	} else {
		debug(D_MAKEFLOW_PARSER,
			"Workflow at line %u: categories malformed or missing",
			j->line);
	}

	const char *default_category = jx_lookup_string(j, "default_category");
	if (default_category) {
		debug(D_MAKEFLOW_PARSER, "Default category %s", default_category);
	} else {
		debug(D_MAKEFLOW_PARSER,
			"Workflow at line %u: default_category malformed or missing, using \"default\"",
			j->line);
		default_category = "default";
	}
	d->default_category = makeflow_category_lookup_or_create(d, default_category);

	struct jx *environment = jx_lookup(j, "environment");
	if (environment && !environment_from_jx(d, NULL, d->default_category->mf_variables, environment)) {
		debug(D_MAKEFLOW_PARSER|D_NOTICE,
			"Line %u: Failure parsing top-level environment",
			environment->line);
		return NULL;
	} else {
		debug(D_MAKEFLOW_PARSER,
			"Workflow at line %u: Top-level environment malformed or missing",
			j->line);
	}

	struct jx *rules = jx_lookup(j, "rules");
	if (jx_istype(rules, JX_ARRAY)) {
		struct jx *item;
		void *i = NULL;
		while ((item = jx_iterate_array(rules, &i))) {
			if (!rule_from_jx(d, item)) {
				debug(D_MAKEFLOW_PARSER|D_NOTICE,
					"Line %u: Failure parsing rule",
					item->line);
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

