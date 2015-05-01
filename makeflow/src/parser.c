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
#include "create_dir.h"
#include "copy_stream.h"
#include "work_queue_catalog.h"
#include "datagram.h"
#include "disk_info.h"
#include "domain_name_cache.h"
#include "link.h"
#include "macros.h"
#include "hash_table.h"
#include "itable.h"
#include "debug.h"
#include "work_queue.h"
#include "work_queue_internal.h"
#include "delete_dir.h"
#include "stringtools.h"
#include "load_average.h"
#include "get_line.h"
#include "int_sizes.h"
#include "list.h"
#include "xxmalloc.h"
#include "getopt_aux.h"
#include "rmonitor.h"
#include "path.h"

#include "dag.h"
#include "visitors.h"
#include "lexer.h"
#include "buffer.h"

#include "makeflow_common.h"

int dag_parse(struct dag *d, FILE * dag_stream);
int dag_parse_variable(struct lexer *bk, struct dag_node *n);
int dag_parse_node(struct lexer *bk);
int dag_parse_syntax(struct lexer *bk);
int dag_parse_node_filelist(struct lexer *bk, struct dag_node *n);
int dag_parse_node_command(struct lexer *bk, struct dag_node *n);
int dag_parse_node_regular_command(struct lexer *bk, struct dag_node *n);
int dag_parse_node_nested_makeflow(struct lexer *bk, struct dag_node *n);
int dag_parse_export(struct lexer *bk);

int dag_parse_node_regular_command(struct lexer *bk, struct dag_node *n)
{
	struct buffer b;

	buffer_init(&b);

	struct token *t;
	while((t = lexer_next_token(bk)) && t->type != TOKEN_NEWLINE)
	{
		switch(t->type)
		{
		case TOKEN_SPACE:
			buffer_printf(&b, " ");
			break;
		case TOKEN_LITERAL:
			dag_file_local_name(n, t->lexeme);
			buffer_printf(&b, "%s", t->lexeme);
			break;
		case TOKEN_IO_REDIRECT:
			buffer_printf(&b, "%s", t->lexeme);
			break;
		default:
			lexer_report_error(bk, "Unexpected command token: %s.\n", lexer_print_token(t));
			break;
		}

		lexer_free_token(t);
	}

	if(!t)
	{
		lexer_report_error(bk, "Command does not end with newline.\n");
	}

	n->command = xxstrdup(buffer_tostring(&b));

	buffer_free(&b);

	debug(D_MAKEFLOW_PARSER, "node command=%s", n->command);

	return 1;
}

/* Returns a pointer to a new struct dag described by filename. Return NULL on
 * failure. */
struct dag *dag_from_file(const char *filename)
{
	FILE *dagfile;
	struct dag *d = NULL;

	dagfile = fopen(filename, "r");
	if(dagfile == NULL)
		debug(D_MAKEFLOW_PARSER, "makeflow: unable to open file %s: %s\n", filename, strerror(errno));
	else {
		d = dag_create();
		d->filename = xxstrdup(filename);
		if(!dag_parse(d, dagfile)) {
			free(d);
			d = NULL;
		}

		fclose(dagfile);
	}

	return d;
}

void dag_close_over_environment(struct dag *d)
{
	//for each exported and special variable, if the variable does not have a
	//value assigned yet, we look for its value in the running environment

	char *name;
	struct dag_variable_value *v;

	set_first_element(d->special_vars);
	while((name = set_next_element(d->special_vars)))
	{
		v = dag_get_variable_value(name, d->variables, d->nodeid_counter);
		if(!v)
		{
			char *value_env = getenv(name);
			if(value_env)
			{
				dag_variable_add_value(name, d->variables, 0, value_env);
			}
		}
	}

	set_first_element(d->export_vars);
	while((name = set_next_element(d->export_vars)))
	{
		v = dag_get_variable_value(name, d->variables, d->nodeid_counter);
		if(!v)
		{
			char *value_env = getenv(name);
			if(value_env)
			{
				dag_variable_add_value(name, d->variables, 0, value_env);
			}
		}
	}

}

int dag_parse(struct dag *d, FILE *stream)
{
	struct lexer *bk = lexer_create(STREAM, stream, 1, 1);

	bk->d        = d;
	bk->stream   = stream;
	bk->category = dag_task_category_lookup_or_create(d, "default");

	struct dag_lookup_set s = { d, NULL, NULL, NULL };
	bk->environment = &s;

	struct token *t;

	while((t = lexer_peek_next_token(bk)))
	{
		s.category = bk->category;
		s.node     = NULL;
		s.table    = NULL;

		switch (t->type) {
		case TOKEN_NEWLINE:
		case TOKEN_SPACE:
			/* Skip newlines, spaces at top level. */
			lexer_free_token(lexer_next_token(bk));
			break;
		case TOKEN_SYNTAX:
			dag_parse_syntax(bk);
			break;
		case TOKEN_FILES:
			dag_parse_node(bk);
			break;
		case TOKEN_VARIABLE:
			dag_parse_variable(bk, NULL);
			break;
		default:
			lexer_report_error(bk, "Unexpected token. Expected one of NEWLINE, SPACE, SYNTAX, FILES, or VARIABLE, but got: %s\n:", lexer_print_token(t));
			break;
		}
	}

	dag_close_over_environment(d);
	dag_compile_ancestors(d);
	free(bk);

	return 1;
}

//return 1 if name was processed as special variable, 0 otherwise
int dag_parse_process_special_variable(struct lexer *bk, struct dag_node *n, int nodeid, char *name, const char *value)
{
	struct dag *d = bk->d;
	int   special = 0;

	if(strcmp("CATEGORY", name) == 0) {
		special = 1;
		/* If we have never seen this label, then create
		 * a new category, otherwise retrieve the category. */
		struct dag_task_category *category = dag_task_category_lookup_or_create(d, value);

		/* If we are parsing inside a node, make category
		 * the category of the node, but do not update
		 * the global task_category. Else, update the
		 * global task category. */
		if(n) {
			/* Remove node from previous category...*/
			list_pop_tail(n->category->nodes);
			n->category = category;
			/* and add it to the new one */
			list_push_tail(n->category->nodes, n);
			debug(D_MAKEFLOW_PARSER, "Updating category '%s' for rule %d.\n", value, n->nodeid);
		}
		else
			bk->category = category;
	}
	/* else if some other special variable .... */
	/* ... */

	return special;
}

void dag_parse_append_variable(struct lexer *bk, int nodeid, struct dag_node *n, const char *name, const char *value)
{
	struct dag_lookup_set      sd = { bk->d, NULL, NULL, NULL };
	struct dag_variable_value *vd = dag_lookup(name, &sd);

	struct dag_variable_value *v;
	if(n)
	{
		v = dag_get_variable_value(name, n->variables, nodeid);
		if(v)
		{
			dag_variable_value_append_or_create(v, value);
		}
		else
		{
			char *new_value;
			if(vd)
			{
				new_value = string_format("%s %s", vd->value, value);
			}
			else
			{
				new_value = xxstrdup(value);
			}
			dag_variable_add_value(name, n->variables, nodeid, new_value);
			free(new_value);
		}
	}
	else
	{
		if(vd)
		{
			dag_variable_value_append_or_create(vd, value);
		}
		else
		{
			dag_variable_add_value(name, bk->d->variables, nodeid, value);
		}
	}
}

int dag_parse_syntax(struct lexer *bk)
{
	struct token *t = lexer_next_token(bk);

	if(strcmp(t->lexeme, "export") == 0) {
		lexer_free_token(t);
		dag_parse_export(bk);
	} else {
		lexer_report_error(bk, "Unknown syntax keyboard.\n");
	}


	return 1;
}

int dag_parse_variable(struct lexer *bk, struct dag_node *n)
{
	struct token *t = lexer_next_token(bk);
	char mode       = t->lexeme[0];            //=, or + (assign or append)
	lexer_free_token(t);

	t = lexer_next_token(bk);
	if(t->type != TOKEN_LITERAL)
	{
		lexer_report_error(bk, "Literal variable name expected.");
	}

	char *name = xxstrdup(t->lexeme);
	lexer_free_token(t);

	t = lexer_next_token(bk);
	if(t->type != TOKEN_LITERAL)
	{
		lexer_report_error(bk, "Expected LITERAL token, got: %s\n", lexer_print_token(t));
	}

	char *value = xxstrdup(t->lexeme);
	lexer_free_token(t);

	struct hash_table *current_table;
	int nodeid;
	if(n)
	{
		current_table = n->variables;
		nodeid        = n->nodeid;
	}
	else
	{
		current_table = bk->d->variables;
		nodeid        = bk->d->nodeid_counter;
	}

	int result = 1;
	switch(mode)
	{
	case '=':
		dag_variable_add_value(name, current_table, nodeid, value);
		debug(D_MAKEFLOW_PARSER, "%s appending to variable name=%s, value=%s", (n ? "node" : "dag"), name, value);
		break;
	case '+':
		dag_parse_append_variable(bk, nodeid, n, name, value);
		debug(D_MAKEFLOW_PARSER, "%s variable name=%s, value=%s", (n ? "node" : "dag"), name, value);
		break;
	default:
		lexer_report_error(bk, "Unknown variable operator.");
		result = 0;
	}

	dag_parse_process_special_variable(bk, n, nodeid, name, value);

	free(name);
	free(value);

	return result;
}

int dag_parse_node_filelist(struct lexer *bk, struct dag_node *n)
{
	int before_colon = 1;

	char *filename;
	char *newname;

	struct token *t, *arrow, *rename;
	while((t = lexer_next_token(bk)))
	{
		filename = NULL;
		newname  = NULL;

		switch (t->type) {
		case TOKEN_COLON:
			before_colon = 0;
			lexer_free_token(t);
			break;
		case TOKEN_NEWLINE:
			/* Finished reading file list */
			lexer_free_token(t);
			return 1;
			break;
		case TOKEN_LITERAL:
			rename = NULL;
			arrow = lexer_peek_next_token(bk);
			if(!arrow)
			{
				lexer_report_error(bk, "Rule specification is incomplete.");
			}
			else if(arrow->type == TOKEN_REMOTE_RENAME)        //Is the arrow really an arrow?
			{
				lexer_free_token(lexer_next_token(bk));  //Jump arrow.
				rename = lexer_next_token(bk);
				if(!rename)
				{
					lexer_report_error(bk, "Remote name specification is incomplete.");
				}
			}

			filename = t->lexeme;
			newname  = rename ? rename->lexeme : NULL;

			if(before_colon)
				dag_node_add_target_file(n, filename, newname);
			else
				dag_node_add_source_file(n, filename, newname);

			lexer_free_token(t);

			if(rename)
			{
				lexer_free_token(rename);
			}

			break;
		default:
			lexer_report_error(bk, "Error reading file list. %s", lexer_print_token(t));
			break;
		}

	}

	return 0;
}

int dag_parse_node(struct lexer *bk)
{
	struct token *t = lexer_next_token(bk);
	if(t->type != TOKEN_FILES)
	{
		lexer_report_error(bk, "Error reading rule.");
	}
	lexer_free_token(t);

	struct dag_node *n;
	n = dag_node_create(bk->d, bk->line_number);
	n->category = bk->category;
	list_push_tail(n->category->nodes, n);

	dag_parse_node_filelist(bk, n);

	bk->environment->node = n;

	/* Read variables, if any */
	while((t = lexer_peek_next_token(bk)) && t->type != TOKEN_COMMAND)
	{
		switch (t->type) {
		case TOKEN_VARIABLE:
			dag_parse_variable(bk, n);
			break;
		default:
			lexer_report_error(bk, "Expected COMMAND or VARIABLE, got: %s", lexer_print_token(t));
			break;
		}
	}

	if(!t)
	{
		lexer_report_error(bk, "Rule does not have a command.\n");
	}

	dag_parse_node_command(bk, n);
	bk->environment->node = NULL;

	n->next = bk->d->nodes;
	bk->d->nodes = n;
	itable_insert(bk->d->node_table, n->nodeid, n);

	debug(D_MAKEFLOW_PARSER, "Setting resource category '%s' for rule %d.\n", n->category->label, n->nodeid);
	dag_task_fill_resources(n);
	dag_task_print_debug_resources(n);

	return 1;
}

int dag_parse_node_command(struct lexer *bk, struct dag_node *n)
{
	struct token *t;

	//Jump COMMAND token.
	t = lexer_next_token(bk);
	lexer_free_token(t);

	char *local = dag_lookup_str("BATCH_LOCAL", bk->environment);
	if(local) {
		if(string_istrue(local))
			n->local_job = 1;
		free(local);
	}

	/* Read command modifiers. */
	while((t = lexer_peek_next_token(bk)) && t->type != TOKEN_COMMAND_MOD_END)
	{
		t = lexer_next_token(bk);

		if(strcmp(t->lexeme, "LOCAL") == 0)
		{
			n->local_job = 1;
		}
		else if(strcmp(t->lexeme, "MAKEFLOW") == 0)
		{
			n->nested_job = 1;
		}
		else
		{
			lexer_report_error(bk, "Parser does not know about modifier: %s.\n", t->lexeme);
		}

		lexer_free_token(t);
	}

	if(!t)
	{
		lexer_report_error(bk, "Malformed command.");
	}

	//Free COMMAND_MOD_END token.
	t = lexer_next_token(bk);
	lexer_free_token(t);

	if(n->nested_job)
	{
		return dag_parse_node_nested_makeflow(bk, n);
	}
	else
	{
		return dag_parse_node_regular_command(bk, n);
	}
}

void dag_parse_drop_spaces(struct lexer *bk)
{
	struct token *t;

	while((t = lexer_peek_next_token(bk)) && t->type == TOKEN_SPACE) {
		t = lexer_next_token(bk);
		lexer_free_token(t);
	}
}

/* Support for recursive calls to makeflow. A recursive call is indicated in
 * the makeflow file with the following syntax:
 * \tMAKEFLOW some-makeflow-file [working-directory [wrapper]]
 *
 * If wrapper is not given, it defaults to an empty string.
 * If working-directory is not given, it defaults to ".".
 * If makeflow_exe is NULL, it defaults to makeflow
 *
 * The call is then as:
 *
 * cd working-directory && wrapper makeflow_exe some-makeflow-file
 *
 * */

int dag_parse_node_nested_makeflow(struct lexer *bk, struct dag_node *n)
{
	struct token *t, *start;

	dag_parse_drop_spaces(bk);

	//Get the dag's file name.
	t = lexer_next_token(bk);

	if(t->type == TOKEN_LITERAL) {
		n->makeflow_dag = xxstrdup(t->lexeme);
		start = t;
	} else {
		lexer_report_error(bk, "At least the name of the Makeflow file should be specified in a recursive call.\n");
		return 0; // not reached, silences warning
	}

	dag_parse_drop_spaces(bk);

	//Get dag's working directory.
	t = lexer_peek_next_token(bk);
	if(t->type == TOKEN_LITERAL) {
		t = lexer_next_token(bk);
		n->makeflow_cwd = xxstrdup(t->lexeme);
		lexer_free_token(t);
	} else {
		n->makeflow_cwd = xxstrdup(".");
	}

	dag_parse_drop_spaces(bk);

	//Get wrapper's name
	char *wrapper = NULL;
	t = lexer_peek_next_token(bk);
	if(t->type == TOKEN_LITERAL) {
		wrapper = xxstrdup(t->lexeme);
		lexer_free_token(t);
	} else {
		wrapper = xxstrdup("");
	}

	free(start->lexeme);
	start->lexeme = string_format("cd %s && %s %s %s",
							  n->makeflow_cwd,
							  wrapper,
							  get_makeflow_exe(),
							  n->makeflow_dag);
	free(wrapper);

	dag_parse_drop_spaces(bk);
	lexer_preppend_token(bk, start);

	return dag_parse_node_regular_command(bk, n);
}

int dag_parse_export(struct lexer *bk)
{
	struct token *t, *vtoken, *vname;

	const char *name;

	int count = 0;
	while((t = lexer_peek_next_token(bk)) && t->type != TOKEN_NEWLINE)
	{
		switch(t->type)
		{
		case TOKEN_VARIABLE:
			vtoken = lexer_next_token(bk);     //Save VARIABLE token.
			vname  = lexer_peek_next_token(bk);
			if(vname->type == TOKEN_LITERAL) {
				name = xxstrdup(vname->lexeme);
			} else {
				lexer_report_error(bk, "Variable definition has name missing.\n");
			}
			lexer_preppend_token(bk, vtoken);  //Restore VARIABLE token.
			dag_parse_variable(bk, NULL);

			break;
		case TOKEN_LITERAL:
			t = lexer_next_token(bk);
			name = xxstrdup(t->lexeme);
			lexer_free_token(t);

			break;
		default:
			lexer_report_error(bk, "Malformed export syntax.\n");
			break;
		}

		set_insert(bk->d->export_vars, name);
		count++;
		debug(D_MAKEFLOW_PARSER, "export variable: %s", name);
	}

	if(t) {
		//Free newline
		t = lexer_next_token(bk);
		lexer_free_token(t);
	}

	if(count < 1) {
		lexer_report_error(bk, "The export syntax needs the explicit name of the variables to be exported.\n");
	}

	return 1;
}

/* vim: set noexpandtab tabstop=4: */


