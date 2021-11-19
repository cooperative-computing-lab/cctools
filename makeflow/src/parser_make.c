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
#include "unlink_recursive.h"
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
#include "dag_visitors.h"
#include "dag_resources.h"
#include "lexer.h"
#include "buffer.h"

#include "parser_make.h"

int dag_parse_make(struct dag *d, FILE * dag_stream);
static int dag_parse_make_variable(struct lexer *bk, struct dag_node *n);
static int dag_parse_make_directive(struct lexer *bk, struct dag_node *n);
static int dag_parse_make_node(struct lexer *bk);
static int dag_parse_make_syntax(struct lexer *bk);
static int dag_parse_make_node_filelist(struct lexer *bk, struct dag_node *n);
static int dag_parse_make_node_command(struct lexer *bk, struct dag_node *n);
static int dag_parse_make_node_regular_command(struct lexer *bk, struct dag_node *n);
static int dag_parse_make_node_nested_makeflow(struct lexer *bk, struct dag_node *n);
static int dag_parse_make_export(struct lexer *bk);

int verbose_parsing=0;

static const int parsing_rule_mod_counter = 250;

static int dag_parse_make_node_regular_command(struct lexer *bk, struct dag_node *n)
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

	if(!t) {
		lexer_report_error(bk, "Command does not end with newline.\n");
	} else {
		lexer_free_token(t);
	}

	dag_node_set_command(n, buffer_tostring(&b));

	buffer_free(&b);

	debug(D_MAKEFLOW_PARSER, "node command=%s", n->command);

	return 1;
}

int dag_parse_make(struct dag *d, FILE * dag_stream)
{
	struct lexer *bk = lexer_create(STREAM, dag_stream, 1, 1);

	bk->d        = d;
	bk->stream   = dag_stream;
	bk->category = d->default_category;

	struct dag_variable_lookup_set s = { d, NULL, NULL, NULL };
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
			dag_parse_make_syntax(bk);
			break;
		case TOKEN_FILES:
			dag_parse_make_node(bk);
			break;
		case TOKEN_VARIABLE:
			dag_parse_make_variable(bk, NULL);
			break;
		case TOKEN_DIRECTIVE:
			dag_parse_make_directive(bk, NULL);
			break;
		default:
			lexer_report_error(bk, "Unexpected token. Expected one of NEWLINE, SPACE, SYNTAX, FILES, or VARIABLE, but got: %s\n:", lexer_print_token(t));
			break;
		}
	}
	lexer_delete(bk);

	return 1;
}

static void dag_parse_make_process_category(struct lexer *bk, struct dag_node *n, int nodeid, const char* value)
{
	/* If we have never seen this label, then create
	 * a new category, otherwise retrieve the category. */
	struct category *category = makeflow_category_lookup_or_create(bk->d, value);

	/* If we are parsing inside a node, make category
	 * the category of the node, but do not update
	 * the global task_category. Else, update the
	 * global task category. */
	if(n) {
		n->category = category;
		debug(D_MAKEFLOW_PARSER, "Updating category '%s' for rule %d.\n", value, n->nodeid);
	}
	else {
		/* set value of current category */
		bk->category = category;
	}
}

//return 1 if name was processed as special variable, 0 otherwise
static int dag_parse_make_process_special_variable(struct lexer *bk, struct dag_node *n, int nodeid, const char *name, const char *value)
{
	int   special = 0;

	if(strcmp("CATEGORY", name) == 0 || strcmp("SYMBOL", name) == 0) {
		special = 1;
		dag_parse_make_process_category(bk, n, nodeid, value);
	}
	/* else if some other special variable .... */
	/* ... */

	return special;
}

void dag_parse_make_append_variable(struct lexer *bk, int nodeid, struct dag_node *n, const char *name, const char *value)
{
	struct dag_variable_lookup_set      sd = { bk->d, NULL, NULL, NULL };
	struct dag_variable_value *vd = dag_variable_lookup(name, &sd);

	struct dag_variable_value *v;
	if(n)
	{
		v = dag_variable_get_value(name, n->variables, nodeid);
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
			dag_variable_add_value(name, bk->d->default_category->mf_variables, nodeid, value);
		}
	}
}

static int dag_parse_make_syntax(struct lexer *bk)
{
	struct token *t = lexer_next_token(bk);

	if(strcmp(t->lexeme, "export") == 0) {
		lexer_free_token(t);
		dag_parse_make_export(bk);
	} else {
		lexer_report_error(bk, "Unknown syntax keyboard.\n");
	}


	return 1;
}

static int dag_parse_make_set_variable(struct lexer *bk, struct dag_node *n, char mode, const char *name, const char *value)
{
	struct hash_table *current_table;
	int nodeid;
	if(n)
	{
		current_table = n->variables;
		nodeid        = n->nodeid;
	}
	else if(strcmp(name, "CATEGORY") == 0)
	{
		current_table = bk->d->default_category->mf_variables;
		nodeid        = bk->d->nodeid_counter;
	}
	else
	{
		current_table = bk->category->mf_variables;
		nodeid        = bk->d->nodeid_counter;
	}

	int result = 1;
	switch(mode)
	{
	case '=':
		dag_variable_add_value(name, current_table, nodeid, value);
		debug(D_MAKEFLOW_PARSER, "%s variable name=%s, value=%s", (n ? "node" : "dag"), name, value);
		break;
	case '+':
		dag_parse_make_append_variable(bk, nodeid, n, name, value);
		debug(D_MAKEFLOW_PARSER, "%s appending to variable name=%s, value=%s", (n ? "node" : "dag"), name, value);
		break;
	default:
		lexer_report_error(bk, "Unknown variable operator.");
		result = 0;
	}

	dag_parse_make_process_special_variable(bk, n, nodeid, name, value);

	return result;
}

static int dag_parse_make_variable(struct lexer *bk, struct dag_node *n)
{
	struct token *t = lexer_next_token(bk);
	char mode       = t->lexeme[0];            //=, or + (assign or append)
	lexer_free_token(t);

	t = lexer_next_token(bk);
	if(t->type != TOKEN_LITERAL)
	{
		lexer_report_error(bk, "Literal variable name expected. %s\n", lexer_print_token(t));
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

	int result = dag_parse_make_set_variable(bk, n, mode, name, value);

	free(name);
	free(value);

	return result;
}

static int dag_parse_make_directive_SIZE(struct lexer *bk, struct dag_node *n) {

	int result = 0;
	struct token *t = lexer_next_token(bk);

	if(t->type != TOKEN_LITERAL)
	{
		lexer_report_error(bk, "Expected LITERAL token (a filename), got: %s\n", lexer_print_token(t));
		return 0;
	}

	char *filename = xxstrdup(t->lexeme);
	lexer_free_token(t);

	t = lexer_next_token(bk);
	if(t->type != TOKEN_LITERAL)
	{
		lexer_report_error(bk, "Expected LITERAL token (a file size), got: %s\n", lexer_print_token(t));
		return 0;
	}

	char *size = xxstrdup(t->lexeme);
	lexer_free_token(t);

	struct dag_file *f = NULL;
	if(filename) {
		f = dag_file_lookup_or_create(bk->d, filename);

		if(f) {
			f->estimated_size = string_metric_parse(size);
			result = 1;
		}
	}

	free(filename);
	free(size);

	return result;
}

static int dag_parse_make_directive_MAKEFLOW(struct lexer *bk, struct dag_node *n) {

	int result = 0;
	int set_var = 1;

	struct token *t = lexer_next_token(bk);

	if(t->type != TOKEN_LITERAL)
	{
		lexer_report_error(bk, "Expected LITERAL token (CATEGORY|MODE|CORES|DISK|MEMORY|WALL_TIME|SIZE|MPI_PROCESSES), got: %s\n", lexer_print_token(t));
	}

	struct token *t2 = lexer_next_token(bk);
	if(t2->type != TOKEN_LITERAL)
	{
		lexer_report_error(bk, "Expected LITERAL token, got: %s\n", lexer_print_token(t2));
	}

	if(!strcmp("CATEGORY", t->lexeme))
	{
		if(!(t2->lexeme))
		{
			lexer_report_error(bk, "Expected name for CATEGORY");
		}

		result = 1;
	}
	else if((   !strcmp("CORES",  t->lexeme))
			|| (!strcmp("DISK",   t->lexeme))
			|| (!strcmp("MEMORY", t->lexeme))
			|| (!strcmp("WALL_TIME", t->lexeme))
			|| (!strcmp("MPI_PROCESSES", t->lexeme)))
	{
		if(!(string_metric_parse(t2->lexeme) >= 0))
		{
			lexer_report_error(bk, "Expected numeric value for %s, got: %s\n", t->lexeme, lexer_print_token(t2));
		}

		result = 1;
	}
	else if(!strcmp("MODE", t->lexeme))
	{
		set_var = 0;

		if(!(t2->lexeme))
		{
			lexer_report_error(bk, "Expected category allocation mode.");
		}
		else if(!strcmp("MAX_THROUGHPUT", t2->lexeme))
		{
			category_specify_allocation_mode(bk->category, CATEGORY_ALLOCATION_MODE_MAX_THROUGHPUT);
			result = 1;
		}
		else if(!strcmp("MIN_WASTE", t2->lexeme))
		{
			category_specify_allocation_mode(bk->category, CATEGORY_ALLOCATION_MODE_MIN_WASTE);
			result = 1;
		}
		else if(!strcmp("FIXED", t2->lexeme))
		{
			category_specify_allocation_mode(bk->category, CATEGORY_ALLOCATION_MODE_FIXED);
			result = 1;
		}
		else
		{
			lexer_report_error(bk, "Expected one of: MAX_THROUGHPUT, MIN_WASTE, FIXED.");
		}
	}
	else
	{
		lexer_report_error(bk, "Unsupported .MAKEFLOW directive, expected (CATEGORY|MODE|CORES|DISK|MEMORY|WALL_TIME|SIZE|MPI_PROCESSES), got: %s\n", t->lexeme);
	}

	if(set_var) {
		dag_parse_make_set_variable(bk, n, '=', /* name */ t->lexeme, /* value */ t2->lexeme);
	}

	lexer_free_token(t);
	lexer_free_token(t2);

	return result;
}


static int dag_parse_make_directive_UMBRELLA(struct lexer *bk, struct dag_node *n) {

	int result = 0;

	struct token *t = lexer_next_token(bk);
	if(t->type != TOKEN_LITERAL)
	{
		lexer_report_error(bk, "Expected LITERAL token, got: %s\n", lexer_print_token(t));
	}

	struct token *t2 = lexer_next_token(bk);
	if(t2->type != TOKEN_LITERAL)
	{
		lexer_report_error(bk, "Expected LITERAL token, got: %s\n", lexer_print_token(t2));
	}

	if(!strcmp("SPEC", t->lexeme))
	{
		dag_parse_make_set_variable(bk, n, '=', /* name */ t->lexeme, /* value */ t2->lexeme);
		result = 1;
	}
	else {
		lexer_report_error(bk, "Unsupported .UMBRELLA type, got: %s\n", t->lexeme);
	}

	return result;
}


static int dag_parse_make_directive(struct lexer *bk, struct dag_node *n)
{
	// Eat TOKEN_DIRECTIVE
	struct token *t = lexer_next_token(bk);
	if(t->type != TOKEN_DIRECTIVE)
	{
		lexer_report_error(bk, "Literal directive expected.");
	}
	lexer_free_token(t);

	t = lexer_next_token(bk);
	if(t->type != TOKEN_LITERAL)
	{
		lexer_report_error(bk, "Literal directive expected.");
	}

	char *name = xxstrdup(t->lexeme);
	lexer_free_token(t);

	int result = 0;
	if(!strcmp(".MAKEFLOW", name))
	{
		result = dag_parse_make_directive_MAKEFLOW(bk, n);
	}
	else if(!strcmp(".SIZE", name))
	{
		result = dag_parse_make_directive_SIZE(bk, n);
	}
	else if(!strcmp(".UMBRELLA", name))
	{
		result = dag_parse_make_directive_UMBRELLA(bk, n);
	}
	else
	{
		lexer_report_error(bk, "Unknown DIRECTIVE type, got: %s\n", name);
	}

	free(name);

	return result;
}



static int dag_parse_make_node_filelist(struct lexer *bk, struct dag_node *n)
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

static int dag_parse_make_node(struct lexer *bk)
{
	struct token *t = lexer_next_token(bk);
	if(t->type != TOKEN_FILES)
	{
		lexer_report_error(bk, "Error reading rule.");
	}
	lexer_free_token(t);

	struct dag_node *n;
	n = dag_node_create(bk->d, bk->line_number);

	if(verbose_parsing && bk->d->nodeid_counter % parsing_rule_mod_counter == 0)
	{
		fprintf(stdout, "\rRules parsed: %d", bk->d->nodeid_counter + 1);
		fflush(stdout);
	}

	n->category = bk->category;

	dag_parse_make_node_filelist(bk, n);

	bk->environment->node = n;

	/* Read variables, if any */
	while((t = lexer_peek_next_token(bk)) && t->type != TOKEN_COMMAND)
	{
		switch (t->type) {
		case TOKEN_VARIABLE:
			dag_parse_make_variable(bk, n);
			break;
		case TOKEN_DIRECTIVE:
			dag_parse_make_directive(bk, n);
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

	dag_parse_make_node_command(bk, n);
	bk->environment->node = NULL;

	dag_node_insert(n);

	return 1;
}

static int dag_parse_make_node_command(struct lexer *bk, struct dag_node *n)
{
	struct token *t;

	//Jump COMMAND token.
	t = lexer_next_token(bk);
	lexer_free_token(t);

	char *local = dag_variable_lookup_string("BATCH_LOCAL", bk->environment);
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
		else if(strcmp(t->lexeme, "MAKEFLOW") == 0 || strcmp(t->lexeme, "WORKFLOW") )
		{
			n->type = DAG_NODE_TYPE_WORKFLOW;
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

	if(n->type==DAG_NODE_TYPE_WORKFLOW) {
		return dag_parse_make_node_nested_makeflow(bk, n);
	} else {
		return dag_parse_make_node_regular_command(bk, n);
	}
}

void dag_parse_make_drop_spaces(struct lexer *bk)
{
	struct token *t;

	while((t = lexer_peek_next_token(bk)) && t->type == TOKEN_SPACE) {
		t = lexer_next_token(bk);
		lexer_free_token(t);
	}
}

static int dag_parse_make_node_nested_makeflow(struct lexer *bk, struct dag_node *n)
{
	struct token *t;
	struct token *makeflow_dag;

	dag_parse_make_drop_spaces(bk);

	//Get the dag's file name.
	t = lexer_next_token(bk);

	if(t->type == TOKEN_LITERAL) {
		makeflow_dag = t;
	} else {
		lexer_report_error(bk, "At least the name of the Makeflow file should be specified in a recursive call.\n");
		return 0; // not reached, silences warning
	}

	dag_parse_make_drop_spaces(bk);

	t = lexer_next_token(bk);
	if (!(t && t->type == TOKEN_NEWLINE)) {
		lexer_report_error(bk, "MAKEFLOW specification does not end with a newline.\n");
	}

	dag_node_set_workflow(n, makeflow_dag->lexeme, 0, 0);

	lexer_free_token(t);
	lexer_free_token(makeflow_dag);
	return 1;
}

static int dag_parse_make_export(struct lexer *bk)
{
	struct token *t, *vtoken, *vname;

	const char *name = NULL;

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
			dag_parse_make_variable(bk, NULL);

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

		string_set_insert(bk->d->export_vars, name);
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
