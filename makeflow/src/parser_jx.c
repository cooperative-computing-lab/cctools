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
#include "jx_eval.h"

static int environment_from_jx(struct dag *d, struct dag_node *n, struct hash_table *h, struct jx *env) {
	debug(D_MAKEFLOW_PARSER, "Parsing environment");
	int nodeid;
	if (n) {
		nodeid = n->nodeid;
	} else {
		nodeid = 0;
	}

	if (jx_istype(env, JX_OBJECT)) {
		for (struct jx_pair *p = env->u.pairs; p; p = p->next) {
			if (jx_istype(p->key, JX_STRING)) {
				debug(D_MAKEFLOW_PARSER, "export %s", p->key->u.string_value);
				set_insert(d->export_vars, xxstrdup(p->key->u.string_value));
				if (jx_istype(p->value, JX_STRING)) {
					debug(D_MAKEFLOW_PARSER, "env %s=%s", p->key->u.string_value, p->value->u.string_value);
					dag_variable_add_value(p->key->u.string_value, h, nodeid, p->value->u.string_value);
				}
			} else {
				debug(D_MAKEFLOW_PARSER, "Environment key/value must be strings");
				return 0;
			}
		}
	} else {
		debug(D_MAKEFLOW_PARSER, "Expected environment to be an object");
		return 0;
	}
	return 1;
}

static int resources_from_jx(struct hash_table *h, struct jx *j) {
	debug(D_MAKEFLOW_PARSER, "Parsing resources");
	int cores = jx_lookup_integer(j, "cores");
	if (cores) {
		debug(D_MAKEFLOW_PARSER, "%d core(s)", cores);
		dag_variable_add_value(RESOURCES_CORES, h, 0, string_format("%d", cores));
	} else {
		debug(D_MAKEFLOW_PARSER, "cores malformed or missing");
	}

	int disk = jx_lookup_integer(j, "disk");
	if (disk) {
		debug(D_MAKEFLOW_PARSER, "%d disk(s)", disk);
		dag_variable_add_value(RESOURCES_DISK, h, 0, string_format("%d", disk));
	} else {
		debug(D_MAKEFLOW_PARSER, "disks malformed or missing");
	}

	int memory = jx_lookup_integer(j, "memory");
	if (memory) {
		debug(D_MAKEFLOW_PARSER, "%d memory", memory);
		dag_variable_add_value(RESOURCES_MEMORY, h, 0, string_format("%d", memory));
	} else {
		debug(D_MAKEFLOW_PARSER, "memory malformed or missing");
	}

	int gpus = jx_lookup_integer(j, "gpus");
	if (gpus) {
		debug(D_MAKEFLOW_PARSER, "%d gpus", gpus);
		dag_variable_add_value(RESOURCES_GPUS, h, 0, string_format("%d", gpus));
	} else {
		debug(D_MAKEFLOW_PARSER, "gpus malformed or missing");
	}

	return 1;
}

static int rule_from_jx(struct dag *d, struct jx *context, struct jx *j) {
	debug(D_MAKEFLOW_PARSER, "Parsing rule");
	struct dag_node *n;

	struct jx *makeflow = jx_eval(jx_lookup(j, "makeflow"), context);
	struct jx *command = jx_eval(jx_lookup(j, "command"), context);

	if (makeflow && command) {
		debug(D_MAKEFLOW_PARSER, "Rule must not have both command and submakeflow");
		return 0;
	}

	if (jx_istype(command, JX_STRING)) {
		n = dag_node_create(d, 0);
		n->command = xxstrdup(command->u.string_value);
		debug(D_MAKEFLOW_PARSER, "command: %s", command->u.string_value);
	} else if (jx_istype(makeflow, JX_OBJECT)) {
		const char *path = jx_lookup_string(makeflow, "path");
		if (path) {
			debug(D_MAKEFLOW_PARSER, "Submakeflow at %s", path);
			n = dag_node_create(d, 0);
			n->nested_job = 1;
			n->makeflow_dag = xxstrdup(path);
			const char *cwd = jx_lookup_string(makeflow, "cwd");
			if (cwd) {
				debug(D_MAKEFLOW_PARSER, "working directory %s", cwd);
				n->makeflow_cwd = xxstrdup(cwd);
			} else {
				debug(D_MAKEFLOW_PARSER, "cwd malformed or missing, using process cwd");
				n->makeflow_cwd = path_getcwd();
			}
		} else {
			debug(D_MAKEFLOW_PARSER, "Submakeflow must specify a path");
			return 0;
		}
	} else {
		debug(D_MAKEFLOW_PARSER, "Rule must have a command or submakeflow");
		return 0;
	}
	n->next = d->nodes;
	d->nodes = n;
	itable_insert(d->node_table, n->nodeid, n);

	// If we got this far, the rule specification is valid and the dag_node exists

	struct jx *local_job = jx_eval(jx_lookup(j, "local_job"), context);
	if (jx_istype(local_job, JX_BOOLEAN) && local_job->u.boolean_value) {
		debug(D_MAKEFLOW_PARSER, "Local job");
		n->local_job = 1;
	}

	struct jx *category = jx_eval(jx_lookup(j, "category"), context);
	if (jx_istype(category, JX_STRING)) {
		debug(D_MAKEFLOW_PARSER, "Category %s", category->u.string_value);
		n->category = makeflow_category_lookup_or_create(d, category->u.string_value);
	} else {
		debug(D_MAKEFLOW_PARSER, "category malformed or missing, using default");
		n->category = makeflow_category_lookup_or_create(d, "default");
	}

	if (!resources_from_jx(n->variables, jx_eval(jx_lookup(j, "resources"), context))) {
		debug(D_MAKEFLOW_PARSER, "Failure parsing resources");
		return 0;
	}

	struct jx *remotes = jx_eval(jx_lookup(j, "remote_names"), context);
	struct jx *inputs = jx_eval(jx_lookup(j, "inputs"), context);
	if (jx_istype(inputs, JX_ARRAY)) {
		for (struct jx_item *i = inputs->u.items; i; i = i->next) {
			if (jx_istype(i->value, JX_STRING)) {
				debug(D_MAKEFLOW_PARSER, "Input %s", i->value->u.string_value);
				const char *r = jx_lookup_string(remotes, i->value->u.string_value);
				if (r) {
					debug(D_MAKEFLOW_PARSER, "Remote name %s", r);
				} else {
					debug(D_MAKEFLOW_PARSER, "Remote name malformed or missing");
				}
				dag_node_add_source_file(n, i->value->u.string_value, r);
			} else {
				debug(D_MAKEFLOW_PARSER, "Input must be a string");
				return 0;
			}
		}
	} else {
		debug(D_MAKEFLOW_PARSER, "inputs malformed or missing");
	}

	struct jx *outputs = jx_eval(jx_lookup(j, "outputs"), context);
	if (jx_istype(outputs, JX_ARRAY)) {
		for (struct jx_item *i = outputs->u.items; i; i = i->next) {
			if (jx_istype(i->value, JX_STRING)) {
				debug(D_MAKEFLOW_PARSER, "Output %s", i->value->u.string_value);
				const char *r = jx_lookup_string(remotes, i->value->u.string_value);
				if (r) {
					debug(D_MAKEFLOW_PARSER, "Remote name %s", r);
				}
				dag_node_add_target_file(n, i->value->u.string_value, r);
			} else {
				debug(D_MAKEFLOW_PARSER, "Output must be a string");
				return 0;
			}
		}
	} else {
		debug(D_MAKEFLOW_PARSER, "Outputs malformed or missing");
	}

	struct jx *allocation = jx_eval(jx_lookup(j, "allocation"), context);
	if (jx_istype(allocation, JX_STRING)) {
		if (!strcmp(allocation->u.string_value, "first")) {
			debug(D_MAKEFLOW_PARSER, "first allocation");
			n->resource_request = CATEGORY_ALLOCATION_FIRST;
		} else if (!strcmp(allocation->u.string_value, "max")) {
			debug(D_MAKEFLOW_PARSER, "max allocation");
			n->resource_request = CATEGORY_ALLOCATION_MAX;
		} else if (!strcmp(allocation->u.string_value, "error")) {
			debug(D_MAKEFLOW_PARSER, "error allocation");
			n->resource_request = CATEGORY_ALLOCATION_ERROR;
		} else {
			debug(D_MAKEFLOW_PARSER, "Unknown allocation");
			return 0;
		}
	} else {
		debug(D_MAKEFLOW_PARSER, "Allocation malformed or missing");
	}

	struct jx *environment = jx_lookup(j, "environment");
	if (environment) {
		environment_from_jx(d, n, n->variables, environment);
	} else {
		debug(D_MAKEFLOW_PARSER, "environment malformed or missing");
	}

	jx_delete(command);
	return 1;
}

static int category_from_jx(struct dag *d, const char *name, struct jx *j) {
	struct category *c = makeflow_category_lookup_or_create(d, name);
	if (!resources_from_jx(c->mf_variables, j)) {
		debug(D_MAKEFLOW_PARSER, "Failure parsing resources");
		return 0;
	}
	struct jx *environment = jx_lookup(j, "environment");
	if (environment && !environment_from_jx(d, NULL, c->mf_variables, environment)) {
		debug(D_MAKEFLOW_PARSER, "Failure parsing environment");
		return 0;
	}
	return 1;
}

// This leaks memory on failure, but it's assumed that if the DAG can't be
// parsed, the program will be exiting soon anyway
struct dag *dag_from_jx(struct jx *j) {
	if (!j) {
		return NULL;
	}

	struct dag *d = dag_create();
	struct jx *dummy_context = jx_object(NULL);
	struct jx *variables = jx_lookup(j, "variables");
	if (variables && !jx_istype(variables, JX_OBJECT)) {
		debug(D_MAKEFLOW_PARSER, "Expected variables to be an object");
		return NULL;
	}

	debug(D_MAKEFLOW_PARSER, "Parsing categories");
	struct jx *categories = jx_lookup(j, "categories");
	if (jx_istype(categories, JX_OBJECT)) {
		for (struct jx_pair *p = categories->u.pairs; p; p = p->next) {
			if (jx_istype(p->key, JX_STRING)) {
				if (!category_from_jx(d, p->key->u.string_value, p->value)) {
					debug(D_MAKEFLOW_PARSER, "Failure parsing category");
					return NULL;
				}
			} else {
				debug(D_MAKEFLOW_PARSER, "Category names must be strings");
				return NULL;
			}
		}
	} else {
		debug(D_MAKEFLOW_PARSER, "categories malformed or missing");
	}

	const char *default_category = jx_lookup_string(j, "default_category");
	if (default_category) {
		debug(D_MAKEFLOW_PARSER, "Default category %s", default_category);
	} else {
		debug(D_MAKEFLOW_PARSER, "default_category malformed or missing, using default");
		default_category = "default";
	}
	d->default_category = makeflow_category_lookup_or_create(d, default_category);

	struct jx *environment = jx_lookup(j, "environment");
	if (j) {
		if (!environment_from_jx(d, NULL, d->default_category->mf_variables, environment)) {
			debug(D_MAKEFLOW_PARSER, "Failure parsing top-level category");
			return NULL;
		}
	} else {
		debug(D_MAKEFLOW_PARSER, "Top-level environment malformed or missing");
	}

	struct jx *rules = jx_lookup(j, "rules");
	if (jx_istype(rules, JX_ARRAY)) {
		for (struct jx_item *i = rules->u.items; i; i = i->next) {
			struct jx *rule_context = jx_merge(variables ? variables : dummy_context, jx_lookup(i->value, "variables"));
			if (!rule_from_jx(d, rule_context, i->value)) {
				debug(D_MAKEFLOW_PARSER, "Failure parsing rule");
				return NULL;
			}
			jx_delete(rule_context);
		}
	}

	dag_close_over_environment(d);
	dag_close_over_categories(d);
	jx_delete(dummy_context);
	return d;
}

