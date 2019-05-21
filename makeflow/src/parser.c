/*
Copyright (C) 2008- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <ctype.h>
#include <errno.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <signal.h>
#include <stdarg.h>
#include <fcntl.h>
#include <dirent.h>
#include <unistd.h>

#include "cctools.h"
#include "catalog_query.h"
#include "category.h"
#include "create_dir.h"
#include "copy_stream.h"
#include "datagram.h"
#include "host_disk_info.h"
#include "domain_name_cache.h"
#include "link.h"
#include "macros.h"
#include "hash_table.h"
#include "itable.h"
#include "debug.h"
#include "delete_dir.h"
#include "stringtools.h"
#include "load_average.h"
#include "get_line.h"
#include "int_sizes.h"
#include "list.h"
#include "xxmalloc.h"
#include "jx.h"
#include "jx_parse.h"
#include "jx_eval.h"
#include "jx_print.h"
#include "getopt_aux.h"
#include "rmonitor.h"
#include "path.h"

#include "dag.h"
#include "dag_visitors.h"
#include "dag_resources.h"
#include "lexer.h"
#include "buffer.h"

#include "parser_make.h"
#include "parser_jx.h"
#include "parser.h"


/* Returns a pointer to a new struct dag described by filename. Return NULL on
 * failure. */
struct dag *dag_from_file(const char *filename, dag_syntax_type format, struct jx *args)
{
	FILE *dagfile = NULL;
	struct jx *dag = NULL;
	struct jx *jx_tmp = NULL;
	struct dag *d = NULL;

	// Initial verification of file existence
	if(format == DAG_SYNTAX_MAKE){
		dagfile = fopen(filename, "r");
		if(dagfile == NULL){
			debug(D_MAKEFLOW_PARSER, "makeflow: unable to open file %s: %s\n", filename, strerror(errno));
			return d;
		}
	} else if (format == DAG_SYNTAX_JX || format == DAG_SYNTAX_JSON){
		dag = jx_parse_file(filename);
        if (!dag){
			debug(D_MAKEFLOW_PARSER, "makeflow: failed to parse jx from %s\n", filename);
			return d;
		}
	}
	
	//Create empty dag to be assigned during parse
	d = dag_create();
	
	// Actually parse file/data into DAG
	switch (format){
		case DAG_SYNTAX_MAKE:
			d->filename = xxstrdup(filename);
			if(!dag_parse_make(d, dagfile)) {
				free(d);
				d = NULL;
			}

			fclose(dagfile);
			break;
		case DAG_SYNTAX_JX: //Evaluates the pending JX Variables from args file
			jx_tmp = jx_eval_with_defines(dag,args);
			jx_delete(dag);
			jx_delete(args);
			dag = jx_tmp;
			 //Intentional fall-through as JX and JSON both use dag_parse_jx
			/* falls through */
		case DAG_SYNTAX_JSON:
			if(!dag_parse_jx(d, dag)){
				free(d);
				d = NULL;
			}
			jx_delete(dag);
			// JX doesn't really use errno, so give something generic
			errno = EINVAL;
			break;
	}

	dag_close_over_environment(d);
	dag_close_over_nodes(d);
	dag_close_over_categories(d);

	dag_compile_ancestors(d);

	return d;
}

void dag_close_over_environment(struct dag *d)
{
	//for each exported and special variable, if the variable does not have a
	//value assigned yet, we look for its value in the running environment

	char *name;
	struct dag_variable_value *v;

	if (!d) return;

	string_set_first_element(d->special_vars);
	while(string_set_next_element(d->special_vars, &name))
	{
		v = dag_variable_get_value(name, d->default_category->mf_variables, d->nodeid_counter);
		if(!v)
		{
			char *value_env = getenv(name);
			if(value_env)
			{
				dag_variable_add_value(name, d->default_category->mf_variables, 0, value_env);
			}

		}
	}

	string_set_first_element(d->export_vars);
	while(string_set_next_element(d->export_vars, &name))
	{
		v = dag_variable_get_value(name, d->default_category->mf_variables, d->nodeid_counter);
		if(!v)
		{
			char *value_env = getenv(name);
			if(value_env)
			{
				dag_variable_add_value(name, d->default_category->mf_variables, 0, value_env);
			}
		}
	}

}

void rmsummary_set_resources_from_env(struct rmsummary *rs, struct dag_variable_lookup_set s)
{
	struct dag_variable_value *val;

	val = dag_variable_lookup(RESOURCES_CORES, &s);
	if(val) {
		rs->cores = atoll(val->value);
	}

	val = dag_variable_lookup(RESOURCES_DISK, &s);
	if(val) {
		rs->disk = atoll(val->value);
	}

	val = dag_variable_lookup(RESOURCES_MEMORY, &s);
	if(val) {
		rs->memory = atoll(val->value);
	}

	val = dag_variable_lookup(RESOURCES_GPUS, &s);
	if(val) {
		rs->gpus = atoll(val->value);
	}

	val = dag_variable_lookup(RESOURCES_WALL_TIME, &s);
	if(val) {
		rs->wall_time = atoll(val->value);
	}
}

void dag_close_over_nodes(struct dag *d)
{
	struct dag_node *n;

	if (!d) return;

	for(n = d->nodes; n; n = n->next) {
		struct rmsummary *rs = n->resources_requested;

		struct dag_variable_lookup_set s = {NULL, NULL, n, NULL };
		
		rmsummary_set_resources_from_env(rs, s);
	}
}

void dag_close_over_categories(struct dag *d) {
	/* per category, we assign the values found for resources. */

	struct category *c;
	char *name;

	if (!d) return;

	hash_table_firstkey(d->categories);
	while(hash_table_nextkey(d->categories, &name, (void **) &c)) {
		struct rmsummary *rs = rmsummary_create(-1);

		struct dag_variable_lookup_set s = {d, c, NULL, NULL };

		rmsummary_set_resources_from_env(rs, s);

		char *resources = rmsummary_print_string(rs, 1);
		debug(D_MAKEFLOW_PARSER, "Category %s defined as: %s", name, resources);
		free(resources);

		c->max_allocation = rs;
	}
}

