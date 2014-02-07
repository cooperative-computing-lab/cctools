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
#include "catalog_server.h"
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
#include "random_init.h"
#include "path.h"

#include "dag.h"
#include "visitors.h"

#include "makeflow_common.h"

char *makeflow_exe = NULL;

char *dag_parse_readline(struct lexer_book *bk, struct dag_node *n);
int dag_parse(struct dag *d, FILE * dag_stream);
int dag_parse_variable(struct lexer_book *bk, struct dag_node *n, char *line);
int dag_parse_node(struct lexer_book *bk, char *line);
int dag_parse_node_filelist(struct lexer_book *bk, struct dag_node *n, char *filelist, int source);
int dag_parse_node_command(struct lexer_book *bk, struct dag_node *n, char *line);
int dag_parse_node_makeflow_command(struct lexer_book *bk, struct dag_node *n, char *line);
int dag_parse_export(struct lexer_book *bk, char *line);

void set_makeflow_exe(const char *makeflow_name)
{
	makeflow_exe = xxstrdup(makeflow_name);
}

const char *get_makeflow_exe()
{
	return makeflow_exe;
}

/**
 * If the return value is x, a positive integer, that means at least x tasks
 * can be run in parallel during a certain point of the execution of the
 * workflow. The following algorithm counts the number of direct child nodes of
 * each node (a node represents a task). Node A is a direct child of Node B
 * only when Node B is the only parent node of Node A. Then it returns the
 * maximum among the direct children counts.
 */
int dag_width_guaranteed_max(struct dag *d)
{
	struct dag_node *n, *m, *tmp;
	struct dag_file *f;
	int nodeid;
	int depends_on_single_node = 1;
	int max = 0;

	for(n = d->nodes; n; n = n->next) {
		depends_on_single_node = 1;
		nodeid = -1;
		m = 0;
		// for each source file, see if it is a target file of another node
		list_first_item(n->source_files);
		while((f = list_next_item(n->source_files))) {
			// get the node (tmp) that outputs current source file
			tmp = f->target_of;
			// if a source file is also a target file
			if(tmp) {
				debug(D_DEBUG, "%d depends on %d", n->nodeid, tmp->nodeid);
				if(nodeid == -1) {
					m = tmp;	// m holds the parent node
					nodeid = m->nodeid;
					continue;
				}
				// if current node depends on multiple nodes, continue to process next node
				if(nodeid != tmp->nodeid) {
					depends_on_single_node = 0;
					break;
				}
			}
		}
		// m != 0 : current node depends on at least one exsisting node
		if(m && depends_on_single_node && nodeid != -1) {
			m->only_my_children++;
		}
	}

	// find out the maximum number of direct children that a single parent node has
	for(n = d->nodes; n; n = n->next) {
		max = max < n->only_my_children ? n->only_my_children : max;
	}

	return max;
}

/**
 * returns the depth of the given DAG.
 */
int dag_depth(struct dag *d)
{
	struct dag_node *n, *parent;
	struct dag_file *f;

	struct list *level_unsolved_nodes = list_create();
	for(n = d->nodes; n != NULL; n = n->next) {
		n->level = 0;
		list_first_item(n->source_files);
		while((f = list_next_item(n->source_files))) {
			if((parent = f->target_of) != NULL) {
				n->level = -1;
				list_push_tail(level_unsolved_nodes, n);
				break;
			}
		}
	}

	int max_level = 0;
	while((n = (struct dag_node *) list_pop_head(level_unsolved_nodes)) != NULL) {
		list_first_item(n->source_files);
		while((f = list_next_item(n->source_files))) {
			if((parent = f->target_of) != NULL) {
				if(parent->level == -1) {
					n->level = -1;
					list_push_tail(level_unsolved_nodes, n);
					break;
				} else {
					int tmp_level = parent->level + 1;
					n->level = n->level > tmp_level ? n->level : tmp_level;
					max_level = n->level > max_level ? n->level : max_level;
				}
			}
		}
	}
	list_delete(level_unsolved_nodes);

	return max_level + 1;
}

/**
 * This algorithm assumes all the tasks take the same amount of time to execute
 * and each task would be executed as early as possible. If the return value is
 * x, a positive integer, that means at least x tasks can be run in parallel
 * during a certain point of the execution of the workflow.
 *
 * The following algorithm first determines the level (depth) of each node by
 * calling the dag_depth() function and then counts how many nodes are there at
 * each level. Then it returns the maximum of the numbers of nodes at each
 * level.
 */
int dag_width_uniform_task(struct dag *d)
{
	struct dag_node *n;

	int depth = dag_depth(d);

	size_t level_count_array_size = (depth) * sizeof(int);
	int *level_count = malloc(level_count_array_size);
	if(!level_count) {
		return -1;
	}
	memset(level_count, 0, level_count_array_size);

	for(n = d->nodes; n != NULL; n = n->next) {
		level_count[n->level]++;
	}

	int i, max = 0;
	for(i = 0; i < depth; i++) {
		if(max < level_count[i]) {
			max = level_count[i];
		}
	}

	free(level_count);
	return max;
}

/**
 * Computes the width of the graph
 */
int dag_width(struct dag *d, int nested_jobs)
{
	struct dag_node *n, *parent;
	struct dag_file *f;

	/* 1. Find the number of immediate children for all nodes; also,
	   determine leaves by adding nodes with children==0 to list. */

	for(n = d->nodes; n != NULL; n = n->next) {
		n->level = 0;	// initialize 'level' value to 0 because other functions might have modified this value.
		list_first_item(n->source_files);
		while((f = list_next_item(n->source_files))) {
			parent = f->target_of;
			if(parent)
				parent->children++;
		}
	}

	struct list *leaves = list_create();

	for(n = d->nodes; n != NULL; n = n->next) {
		n->children_remaining = n->children;
		if(n->children == 0)
			list_push_tail(leaves, n);
	}

	/* 2. Assign every node a "reverse depth" level. Normally by depth,
	   I mean topologically sort and assign depth=0 to nodes with no
	   parents. However, I'm thinking I need to reverse this, with depth=0
	   corresponding to leaves. Also, we want to make sure that no node is
	   added to the queue without all its children "looking at it" first
	   (to determine its proper "depth level"). */

	int max_level = 0;

	while(list_size(leaves) > 0) {
		struct dag_node *n = (struct dag_node *) list_pop_head(leaves);

		list_first_item(n->source_files);
		while((f = list_next_item(n->source_files))) {
			parent = f->target_of;
			if(!parent)
				continue;

			if(parent->level < n->level + 1)
				parent->level = n->level + 1;

			if(parent->level > max_level)
				max_level = parent->level;

			parent->children_remaining--;
			if(parent->children_remaining == 0)
				list_push_tail(leaves, parent);
		}
	}
	list_delete(leaves);

	/* 3. Now that every node has a level, simply create an array and then
	   go through the list once more to count the number of nodes in each
	   level. */

	size_t level_count_size = (max_level + 1) * sizeof(int);
	int *level_count = malloc(level_count_size);

	memset(level_count, 0, level_count_size);

	for(n = d->nodes; n != NULL; n = n->next) {
		if(nested_jobs && !n->nested_job)
			continue;
		level_count[n->level]++;
	}

	int i, max = 0;
	for(i = 0; i <= max_level; i++) {
		if(max < level_count[i])
			max = level_count[i];
	}

	free(level_count);
	return max;
}


static char *translate_command(struct dag_node *n, char *old_command, int is_local)
{
	char *new_command;
	char *sp;
	char *token;
	int first = 1;
	int wait = 0;		/* Wait for next token before prepending "./"? */
	int padding = 3;
	char prefix;

	UPTRINT_T current_length = (UPTRINT_T) 0;

	for(sp = old_command; *sp; sp++)
		if(isspace((int) *sp))
			padding += 2;

	new_command = malloc((strlen(old_command) + padding) * sizeof(char));
	new_command[0] = '\0';

	token = strtok(old_command, " \t\n");

	while(token) {
		/* Remove (and store) the shell metacharacter prefix, if
		   there is one. */
		switch (token[0]) {
		case '<':
		case '>':
			prefix = token[0];
			++token;
			break;
		default:
			prefix = '\0';
		}

		if(prefix && !token) {
			/* Indicates "< input" or "> output", i.e., with
			   space after the shell metacharacter */
			wait = 1;
		}

		char *val = NULL;
		int len;

		if(!is_local)
			val = dag_file_remote_name(n, token);

		if(!first) {
			strncat(new_command + current_length, " ", 1);
			++current_length;
		} else {
			first = 0;
		}

		/* Append the shell metacharacter prefix, if there is one. */
		if(prefix) {
			strncat(new_command + current_length, &prefix, 1);
			++current_length;
		}

		if(val) {
			/* If the executable has a hashtable entry, then we
			   need to prepend "./" to the symlink name */
			if(wait) {
				wait = 0;
			} else {
				strncat(new_command + current_length, "./", 2);
				current_length += 2;
			}

			len = strlen(val);
			strncat(new_command + current_length, val, len);
			current_length += len;
		} else {
			len = strlen(token);
			strncat(new_command + current_length, token, len);
			current_length += len;
		}

		token = strtok(NULL, " \t\n");
	}

	return new_command;
}

#define dag_parse_error(bk, type) \
	fprintf(stderr, "makeflow: invalid " type " in file %s at line %ld, column %ld\n", (bk)->d->filename, (bk)->line_number, (bk)->column_number);

/* Returns a pointer to a new struct dag described by filename. Return NULL on
 * failure. */
struct dag *dag_from_file(const char *filename)
{
	FILE *dagfile;
	struct dag *d = NULL;

	dagfile = fopen(filename, "r");
	if(dagfile == NULL)
		debug(D_DEBUG, "makeflow: unable to open file %s: %s\n", filename, strerror(errno));
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

int dag_parse(struct dag *d, FILE * dag_stream)
{
	char *line = NULL;
	struct lexer_book *bk = calloc(1, sizeof(struct lexer_book));	//Taking advantage that calloc zeroes memory

	bk->d = d;
	bk->stream = dag_stream;

	bk->category = dag_task_category_lookup_or_create(d, "default");

	while((line = dag_parse_readline(bk, NULL)) != NULL) {

		if(strlen(line) == 0 || line[0] == '#') {
			/* Skip blank lines and comments */
			free(line);
			continue;
		}
		if(strncmp(line, "export ", 7) == 0) {
			if(!dag_parse_export(bk, line)) {
				dag_parse_error(bk, "export");
				goto failure;
			}
		} else if(strchr(line, '=')) {
			if(!dag_parse_variable(bk, NULL, line)) {
				dag_parse_error(bk, "variable");
				goto failure;
			}
		} else if(strstr(line, ":")) {
			if(!dag_parse_node(bk, line)) {
				dag_parse_error(bk, "node");
				goto failure;
			}
		} else {
			dag_parse_error(bk, "syntax");
			goto failure;
		}

		free(line);
	}

//ok
	dag_close_over_environment(d);
	dag_compile_ancestors(d);
	free(bk);
	return 1;

      failure:
	free(line);
	free(bk);
	return 0;
}

/** Read multiple lines connected by shell continuation symbol
 * (backslash). Return as a single line. Caller will own the
 * memory.
 * The continuation is detected when backslash is the last symbol
 * before the newline character, and the symbol last before last is
 * not the backslash (double backslash is an escape sequence for
 * backslash itself).
 */
char* get_continued_line(struct lexer_book *bk) {

	char *cont_line = NULL;
	char *raw_line = NULL;
	int cont = 1;

	while(cont && ((raw_line=get_line(bk->stream)) != NULL)) {

		bk->column_number = 1;
		bk->line_number++;
		if(bk->line_number % 1000 == 0) {
			debug(D_DEBUG, "read line %ld\n", bk->line_number);
		}

		/* Strip whitespace */
		string_chomp(raw_line);
		while(isspace(*raw_line)) {
			raw_line++;
			bk->column_number++;
		}

		size_t len = strlen(raw_line);

		if(len>0 && raw_line[len-1] == '\\' &&
				(len==1 || raw_line[len-2] != '\\')) {
			raw_line[len-1] = '\0';
			cont++;
		}
		else {
			cont = 0;
		}

		if(cont_line) {
			cont_line = string_combine(cont_line,raw_line);
		}
		else {
			cont_line = xxstrdup(raw_line);
		}
	}
	if(cont > 1) {
		dag_parse_error(bk, "No line found after line continuation backslash");
		free(cont_line);
		cont_line = NULL;
		fatal("Unable to parse the makeflow file");
	}
	return cont_line;
}

char *dag_parse_readline(struct lexer_book *bk, struct dag_node *n)
{
	struct dag *d = bk->d;
	struct dag_lookup_set s = { d, bk->category, n, NULL };

	char *line = get_continued_line(bk);

	if(line) {
		/* Chop off comments
		 * TODO: this will break if we use # in a string. */
		char *hash = strrchr(line, '#');
		if(hash && hash != line) {
			*hash = 0;
		}

		char *subst_line = string_subst(line, dag_lookup_str, &s);

		free(bk->linetext);
		bk->linetext = xxstrdup(subst_line);

		/* Expand backslash-escaped characters. */
		/* NOTE: This function call is responsible for translating escape
		   character sequences such as \n, \t, etc. which are found in the
		   makeflow file into their ASCII character equivalents. Such escape
		   sequences are necessary for assigning values to variables which
		   contain multiple lines of text, since the entire assignment
		   statement must be contained on one line. */
		string_replace_backslash_codes(subst_line, subst_line);

		return subst_line;
	}

	return NULL;
}

//return 1 if name was processed as special variable, 0 otherwise
int dag_parse_process_special_variable(struct lexer_book *bk, struct dag_node *n, int nodeid, char *name, const char *value)
{
	struct dag *d = bk->d;
	int   special = 0;

	if(strcmp(RESOURCES_CATEGORY, name) == 0) {
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
			debug(D_DEBUG, "Updating category '%s' for rule %d.\n", value, n->nodeid);
		}
		else
			bk->category = category;
	}
	/* else if some other special variable .... */
	/* ... */

	return special;
}

void dag_parse_append_variable(struct lexer_book *bk, int nodeid, struct dag_node *n, const char *name, const char *value)
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

int dag_parse_variable(struct lexer_book *bk, struct dag_node *n, char *line)
{
	struct dag *d = bk->d;
	char *name = line + (n ? 1 : 0);	/* Node variables require offset of 1 */
	char *value = NULL;
	char *equal = NULL;
	int append = 0;

	equal = strchr(line, '=');
	if((value = strstr(line, "+=")) && value < equal) {
		*value = 0;
		value = value + 2;
		append = 1;
	} else {
		value = equal;
		*value = 0;
		value = value + 1;
	}

	name = string_trim_spaces(name);
	value = string_trim_spaces(value);
	value = string_trim_quotes(value);

	if(strlen(name) == 0) {
		dag_parse_error(bk, "variable name");
		return 0;
	}

	struct hash_table *current_table;
	int nodeid;
	if(n)
	{
		current_table = n->variables;
		nodeid        = n->nodeid;
	}
	else
	{
		current_table = d->variables;
		nodeid        = bk->d->nodeid_counter;
	}

	if(append)
	{
		dag_parse_append_variable(bk, nodeid, n, name, value);
	}
	else
	{
		dag_variable_add_value(name, current_table, nodeid, value);
	}

	dag_parse_process_special_variable(bk, n, nodeid, name, value);

	if(append)
		debug(D_DEBUG, "%s appending to variable name=%s, value=%s", (n ? "node" : "dag"), name, value);
	else
		debug(D_DEBUG, "%s variable name=%s, value=%s", (n ? "node" : "dag"), name, value);

	return 1;
}

int dag_parse_node(struct lexer_book *bk, char *line_org)
{
	struct dag *d = bk->d;
	char *line;
	char *outputs = NULL;
	char *inputs = NULL;
	struct dag_node *n;

	n = dag_node_create(bk->d, bk->line_number);

	n->category = bk->category;
	list_push_tail(n->category->nodes, n);

	line = xxstrdup(line_org);

	outputs = line;

	inputs = strchr(line, ':');
	*inputs = 0;
	inputs = inputs + 1;

	inputs = string_trim_spaces(inputs);
	outputs = string_trim_spaces(outputs);

	dag_parse_node_filelist(bk, n, outputs, 0);
	dag_parse_node_filelist(bk, n, inputs, 1);

	int ok;
	char *comment;
	//parse variables and comments
	while((line = dag_parse_readline(bk, n)) != NULL) {
		if(line[0] == '@' && strchr(line, '=')) {
			ok = dag_parse_variable(bk, n, line);
			free(line);

			if(ok) {
				continue;
			} else {
				dag_parse_error(bk, "node variable");
				free(line);
				return 0;
			}
		}

		comment = strchr(line, '#');
		if(comment)
		{
			*comment = '\0';
			int n = strspn(line, " \t");
			int m = strlen(line);
			*comment = '#';


			/* make sure that only spaces and tabs appear before the hash */
			if(n == m) {
				continue;
			}
		}

		/* not a comment or a variable, so we break to parse the command */
		break;
	}

	ok = dag_parse_node_command(bk, n, line);
	free(line);

	if(ok) {
		n->next = d->nodes;
		d->nodes = n;
		itable_insert(d->node_table, n->nodeid, n);
	} else {
		dag_parse_error(bk, "node command");
		return 0;
	}


	debug(D_DEBUG, "Setting resource category '%s' for rule %d.\n", n->category->label, n->nodeid);
	dag_task_fill_resources(n);
	dag_task_print_debug_resources(n);

	return 1;
}

/** Parse a node's input or output filelist.
Parse through a list of input or output files, adding each as a source or target file to the provided node.
@param d The DAG being constructed
@param n The node that the files are being added to
@param filelist The list of files, separated by whitespace
@param source a flag for whether the files are source or target files.  1 indicates source files, 0 indicates targets
*/
int dag_parse_node_filelist(struct lexer_book *bk, struct dag_node *n, char *filelist, int source)
{
	char *filename;
	char *newname;
	char **argv;
	int i, argc;

	string_split_quotes(filelist, &argc, &argv);
	for(i = 0; i < argc; i++) {
		filename = argv[i];
		newname = NULL;
		debug(D_DEBUG, "node %s file=%s", (source ? "input" : "output"), filename);

		// remote renaming
		if((newname = strstr(filename, "->"))) {
			*newname = '\0';
			newname += 2;
		}

		if(source)
			dag_node_add_source_file(n, filename, newname);
		else
			dag_node_add_target_file(n, filename, newname);
	}
	free(argv);
	return 1;

}

void dag_parse_node_set_command(struct lexer_book *bk, struct dag_node *n, char *command)
{
	struct dag_lookup_set s = { bk->d, bk->category, n, NULL };
	char *local = dag_lookup_str("BATCH_LOCAL", &s);

	if(local) {
		if(string_istrue(local))
			n->local_job = 1;
		free(local);
	}

	n->original_command = xxstrdup(command);
	n->command = translate_command(n, command, n->local_job);
	debug(D_DEBUG, "node command=%s", n->command);
}

int dag_parse_node_command(struct lexer_book *bk, struct dag_node *n, char *line)
{
	char *command = line;

	while(*command && isspace(*command))
		command++;

	if(strncmp(command, "LOCAL ", 6) == 0) {
		n->local_job = 1;
		command += 6;
	}

	/* Is this node a recursive call to makeflow? */
	if(strncmp(command, "MAKEFLOW ", 9) == 0) {
		return dag_parse_node_makeflow_command(bk, n, command + 9);
	}

	dag_parse_node_set_command(bk, n, command);

	return 1;
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

int dag_parse_node_makeflow_command(struct lexer_book *bk, struct dag_node *n, char *line)
{
	int argc;
	char **argv;
	char *wrapper = NULL;
	char *command = NULL;

	n->nested_job = 1;
	string_split_quotes(line, &argc, &argv);
	switch (argc) {
	case 1:
		n->makeflow_dag = xxstrdup(argv[0]);
		n->makeflow_cwd = xxstrdup(".");
		break;
	case 2:
		n->makeflow_dag = xxstrdup(argv[0]);
		n->makeflow_cwd = xxstrdup(argv[1]);
		break;
	case 3:
		n->makeflow_dag = xxstrdup(argv[0]);
		n->makeflow_cwd = xxstrdup(argv[1]);
		wrapper = argv[2];
		break;
	default:
		dag_parse_error(bk, "node makeflow command");
		goto failure;
	}

	wrapper = wrapper ? wrapper : "";
	command = xxmalloc(sizeof(char) * (strlen(n->makeflow_cwd) + strlen(wrapper) + strlen(makeflow_exe) + strlen(n->makeflow_dag) + 20));
	sprintf(command, "cd %s && %s %s %s", n->makeflow_cwd, wrapper, makeflow_exe, n->makeflow_dag);

	dag_parse_node_filelist(bk, n, argv[0], 1);
	dag_parse_node_set_command(bk, n, command);

	free(argv);
	free(command);
	return 1;
      failure:
	free(argv);
	return 0;
}

int dag_parse_export(struct lexer_book *bk, char *line)
{
	int i, argc;
	char *end_export, *equal;
	char **argv;

	end_export = strstr(line, "export ");

	if(!end_export)
		return 0;
	else
		end_export += strlen("export ");

	while(isblank(*end_export))
		end_export++;

	if(end_export == '\0')
		return 0;

	string_split_quotes(end_export, &argc, &argv);
	for(i = 0; i < argc; i++) {
		equal = strchr(argv[i], '=');
		if(equal) {
			if(!dag_parse_variable(bk, NULL, argv[i])) {
				return 0;
			} else {
				*equal = '\0';
				setenv(argv[i], equal + 1, 1);	//this shouldn't be here...
			}
		}
		set_insert(bk->d->export_vars, xxstrdup(argv[i]));
		debug(D_DEBUG, "export variable=%s", argv[i]);
	}
	free(argv);
	return 1;
}

/* vim: set noexpandtab tabstop=4: */
