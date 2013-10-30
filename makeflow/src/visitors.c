/*
Copyright (C) 2013- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <inttypes.h>

#include <ctype.h>
#include <pwd.h>
#include <time.h>
#include <unistd.h>

#include "hash_table.h"
#include "xxmalloc.h"
#include "list.h"
#include "itable.h"
#include "debug.h"
#include "set.h"
#include "stringtools.h"

#include "rmsummary.h"
#include "visitors.h"

/*
 * BUG: Error handling is not very good.
 * BUG: Integrate more with dttools (use DEBUG, etc.)
 */

/* Writes 'var=value' pairs from the dag to the stream */
int dag_to_file_vars(struct hash_table *vars, FILE * dag_stream, const char *prefix)
{
	char *var;
	struct dag_variable_value *v;

	hash_table_firstkey(vars);
	while(hash_table_nextkey(vars, &var, (void *) &v)) {
		if(!string_null_or_empty(v->value) && (strcmp(var, "GC_PRESERVE_LIST") || strcmp(var, "GC_COLLECT_LIST")))
			fprintf(dag_stream, "%s%s=\"%s\"\n", prefix, var, (char *) v->value);
	}

	return 0;
}

/* Writes 'export var' tokens from the dag to the stream */
int dag_to_file_exports(const struct dag *d, FILE * dag_stream)
{
	char *var;

	struct list *vars = d->export_list;

	list_first_item(vars);
	for(var = list_next_item(vars); var; var = list_next_item(vars))
		fprintf(dag_stream, "export %s\n", var);

	return 0;

}

/* Writes a list of files to the the stream */
int dag_to_file_files(struct dag_node *n, struct list *fs, FILE * dag_stream, char *(*rename) (struct dag_node * n, const char *filename))
{
	//here we may want to call the linker renaming function,
	//instead of using f->remotename

	const struct dag_file *f;
	list_first_item(fs);
	while((f = list_next_item(fs)))
		if(rename)
			fprintf(dag_stream, "%s ", rename(n, f->filename));
		else {
			char *remotename = dag_file_remote_name(n, f->filename);
			if(remotename)
				fprintf(dag_stream, "%s->%s ", f->filename, remotename);
			else
				fprintf(dag_stream, "%s ", f->filename);
		}

	return 0;
}

/* Writes a production rule to the stream, using remotenames when
 * available.
 *
 * Eventually, we would like to pass a 'convert_name' function,
 * instead of using just the remotenames.
 *
 * BUG: Currently, expansions are writen instead of variables.
 *
 * The entry function is dag_to_file(dag, filename).
 * */
int dag_to_file_node(struct dag_node *n, FILE * dag_stream, char *(*rename) (struct dag_node * n, const char *filename))
{
	fprintf(dag_stream, "\n");
	dag_to_file_files(n, n->target_files, dag_stream, rename);
	fprintf(dag_stream, ": ");
	dag_to_file_files(n, n->source_files, dag_stream, rename);
	fprintf(dag_stream, "\n");

	dag_to_file_vars(n->variables, dag_stream, "@");

	if(n->local_job)
		fprintf(dag_stream, "\tLOCAL %s", n->command);
	else
		fprintf(dag_stream, "\t%s\n", n->command);
	fprintf(dag_stream, "\n");

	return 0;
}

int dag_to_file_category_variables(struct dag_task_category *c, FILE * dag_stream)
{
	struct rmsummary *s = c->resources;

	fprintf(dag_stream, "\n");
	fprintf(dag_stream, "CATEGORY=\"%s\"\n", c->label);

	if(s->cores > -1)
		fprintf(dag_stream, "CORES=%" PRId64 "\n", s->cores);
	if(s->resident_memory > -1)
		fprintf(dag_stream, "MEMORY=%" PRId64 "\n", s->resident_memory);
	if(s->workdir_footprint > -1)
		fprintf(dag_stream, "DISK=%" PRId64 "\n", s->workdir_footprint);

	return 0;
}

/* Writes all the rules to the stream, per category, plus any variables from the category */
int dag_to_file_category(struct dag_task_category *c, FILE * dag_stream, char *(*rename) (struct dag_node * n, const char *filename))
{
	struct dag_node *n;

	dag_to_file_category_variables(c, dag_stream);

	list_first_item(c->nodes);
	while((n = list_next_item(c->nodes)))
		dag_to_file_node(n, dag_stream, rename);

	return 0;
}

int dag_to_file_categories(const struct dag *d, FILE * dag_stream, char *(*rename) (struct dag_node * n, const char *filename))
{
	char *name;
	struct dag_task_category *c;

	hash_table_firstkey(d->task_categories);
	while(hash_table_nextkey(d->task_categories, &name, (void *) &c))
		dag_to_file_category(c, dag_stream, rename);

	return 0;
}

/* Entry point of the dag_to_file* functions. Writes a dag as an
 * equivalent makeflow file. */
int dag_to_file(const struct dag *d, const char *dag_file, char *(*rename) (struct dag_node * n, const char *filename))
{
	FILE *dag_stream = fopen(dag_file, "w");

	if(!dag_stream)
		return 1;

	dag_to_file_vars(d->variables, dag_stream, "");
	dag_to_file_exports(d, dag_stream);
	dag_to_file_categories(d, dag_stream, rename);

	fclose(dag_stream);

	return 0;
}

/* Writes the xml header incantation for DAX */
void dag_to_dax_header(const char *name)
{

	fprintf(stdout, "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");

	time_t current_raw_time;
	struct tm *time_info;
	char buffer[64];
	time(&current_raw_time);
	time_info = localtime(&current_raw_time);
	strftime(buffer, 64, "%Y-%m-%d %T", time_info);
	fprintf(stdout, "<!-- generated: %s -->\n", buffer);

	uid_t uid = getuid();
	struct passwd *current_user_info;
	current_user_info = getpwuid(uid);
	fprintf(stdout, "<!-- generated by: %s -->\n", current_user_info->pw_name);

	fprintf(stdout, "<!-- generator: Makeflow -->\n");

	fprintf(stdout, "<adag ");
	fprintf(stdout, "xmlns=\"http://pegasus.isi.edu/schema/DAX\" ");
	fprintf(stdout, "xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" ");
	fprintf(stdout, "xsi:schemaLocation=\"http://pegasus.isi.edu/schema/DAX http://pegasus.isi.edu/schema/dax-3.4.xsd\" ");
	fprintf(stdout, "version=\"3.4\" ");
	fprintf(stdout, "name=\"%s\">\n", name);
}

/* Write list of files in DAX format for a given node 
 * @param type 0 for input 1 for output
 */
void dag_to_dax_files(struct list *fs, int type)
{
	const struct dag_file *f;
	list_first_item(fs);
	while((f = list_next_item(fs))) {
		if(type == 0)
			fprintf(stdout, "\t\t<uses name=\"%s\" link=\"input\" />\n", f->filename);
		else
			fprintf(stdout, "\t\t<uses name=\"%s\" link=\"output\" register=\"false\" transfer=\"true\" />\n", f->filename);
	}
}

/* Extract the executable from a node */
const char *node_executable(const struct dag_node *n)
{
	int first_space = strpos(n->command, ' ');
	char *executable_path = string_front(n->command, first_space);
	int executable_path_length = strlen(executable_path);
	int last_slash = strrpos(executable_path, '/');
	return string_back(executable_path, executable_path_length - last_slash - 1);
}

const char *node_executable_arguments(const struct dag_node *n)
{
	int command_length = strlen(n->command);
	int first_space = strpos(n->command, ' ');
	const char *before_redirection = string_back(n->command, command_length - first_space - 1);
	int first_redirect = strpos(n->command, '>');
	if(first_redirect < 0) return before_redirection;
	return string_trim_spaces(string_front(before_redirection, first_redirect - first_space - 1));
}

const char *node_executable_redirect(const struct dag_node *n)
{
	int command_length = strlen(n->command);
	int last_redirect = strrpos(n->command, '>');
	int first_redirect = strpos(n->command, '>');
	if(last_redirect < 0) return NULL;
	if(last_redirect != first_redirect) fatal("makeflow: One of your tasks (%s) contains multiple redirects. Currently Makeflow does not support DAX export with multiple redirects.\n", n->command);
	char *raw_redirect = (char *) string_back(n->command, command_length - last_redirect - 1);
	return string_trim_spaces(raw_redirect);
}

/* Writes the DAX representation of a node */
void dag_to_dax_individual_node(const struct dag_node *n, UINT64_T node_id)
{
	fprintf(stdout, "\t<job id=\"ID%07" PRIu64 "\" name=\"%s\">\n", node_id, node_executable(n));
	fprintf(stdout, "\t\t<argument>%s</argument>\n", node_executable_arguments(n));

	const char *redirection = node_executable_redirect(n);
	if(redirection) fprintf(stdout, "\t\t<stdout name=\"%s\" link=\"output\" />\n", redirection);

	dag_to_dax_files(n->source_files, 0);
	dag_to_dax_files(n->target_files, 1);
	fprintf(stdout, "\t</job>\n");
}

/* Iterates over each node to output as DAX */
void dag_to_dax_nodes(const struct dag *d)
{
	struct dag_node *n;
	UINT64_T node_id;

	itable_firstkey(d->node_table);
	while(itable_nextkey(d->node_table, &node_id, (void *) &n))
		dag_to_dax_individual_node(n, node_id);
}

/* Writes the DAX for a node's parent relationships */
void dag_to_dax_parents(const struct dag_node *n)
{
	struct dag_node *p;
	
	if(set_size(n->ancestors) > 0){
		fprintf(stdout, "\t<child ref=\"ID%07d\">\n", n->nodeid);
		set_first_element(n->ancestors);
		while((p = set_next_element(n->ancestors)))
			fprintf(stdout, "\t\t<parent ref=\"ID%07d\" />\n", p->nodeid);
		fprintf(stdout, "\t</child>\n");
	}
}

/* Writes the DAX version of each relationship in the dag */
void dag_to_dax_relationships(const struct dag *d)
{
	struct dag_node *n;
	UINT64_T node_id;
	
	itable_firstkey(d->node_table);
	while(itable_nextkey(d->node_table, &node_id, (void *) &n))
		dag_to_dax_parents(n);
}

/* Writes the xml footer for DAX */
void dag_to_dax_footer()
{
	fprintf(stdout, "</adag>\n");
}

/* Entry Point of the dag_to_dax* functions.
 * Writes a dag in DAX format to stdout.
 * see: http://pegasus.isi.edu/wms/docs/schemas/dax-3.4/dax-3.4.html
 */
int dag_to_dax(const struct dag *d, const char *name)
{
	dag_to_dax_header(name);
	dag_to_dax_nodes(d);
	dag_to_dax_relationships(d);
	dag_to_dax_footer();
	return 0;
}


/* The following functions and structures are used to write a dot
 * file (graphviz) that shows the graphical presentation of the
 * workflow. */

struct dot_node {
	int id;
	int count;
	int print;
};

struct file_node {
	int id;
	char *name;
	double size;
};

void dag_to_dot(struct dag *d, int condense_display, int change_size)
{
	struct dag_node *n;
	struct dag_file *f;
	struct hash_table *h, *g;
	struct dot_node *t;

	struct file_node *e;

	struct stat st;
	const char *fn;

	char *name;
	char *label;

	double average = 0;
	double width = 0;

	fprintf(stdout, "digraph {\n");

	if(change_size) {
		hash_table_firstkey(d->completed_files);
		while(hash_table_nextkey(d->completed_files, &label, (void **) &name)) {
			stat(label, &st);
			average += ((double) st.st_size) / ((double) hash_table_size(d->completed_files));
		}
	}


	h = hash_table_create(0, 0);

	fprintf(stdout, "node [shape=ellipse,color = green,style = unfilled,fixedsize = false];\n");

	for(n = d->nodes; n; n = n->next) {
		name = xxstrdup(n->command);
		label = strtok(name, " \t\n");
		t = hash_table_lookup(h, label);
		if(!t) {
			t = malloc(sizeof(*t));
			t->id = n->nodeid;
			t->count = 1;
			t->print = 1;
			hash_table_insert(h, label, t);
		} else {
			t->count++;
		}

		free(name);
	}


	for(n = d->nodes; n; n = n->next) {
		name = xxstrdup(n->command);
		label = strtok(name, " \t\n");
		t = hash_table_lookup(h, label);
		if(!condense_display || t->print) {

			if((t->count == 1) || !condense_display)
				fprintf(stdout, "N%d [label=\"%s\"];\n", condense_display ? t->id : n->nodeid, label);
			else
				fprintf(stdout, "N%d [label=\"%s x%d\"];\n", t->id, label, t->count);
			t->print = 0;
		}
		free(name);
	}

	fprintf(stdout, "node [shape=box,color=blue,style=unfilled,fixedsize=false];\n");

	g = hash_table_create(0, 0);

	for(n = d->nodes; n; n = n->next) {
		list_first_item(n->source_files);
		while((f = list_next_item(n->source_files))) {
			fn = f->filename;
			e = hash_table_lookup(g, fn);
			if(!e) {
				e = malloc(sizeof(*e));
				e->id = hash_table_size(g);
				e->name = xxstrdup(fn);
				if(stat(fn, &st) == 0) {
					e->size = (double) (st.st_size);
				} else
					e->size = -1;
				hash_table_insert(g, fn, e);
			}
		}
		list_first_item(n->target_files);
		while((f = list_next_item(n->target_files))) {
			fn = f->filename;
			e = hash_table_lookup(g, fn);
			if(!e) {
				e = malloc(sizeof(*e));
				e->id = hash_table_size(g);
				e->name = xxstrdup(fn);
				if(stat(fn, &st) == 0) {
					e->size = (double) (st.st_size);
				} else
					e->size = -1;
				hash_table_insert(g, fn, e);
			}
		}
	}

	hash_table_firstkey(g);
	while(hash_table_nextkey(g, &label, (void **) &e)) {
		fn = e->name;
		fprintf(stdout, "F%d [label = \"%s", e->id, fn);

		if(change_size) {
			if(e->size >= 0) {
				width = 5 * (e->size / average);
				if(width < 2.5)
					width = 2.5;
				if(width > 25)
					width = 25;
				fprintf(stdout, "\\nsize:%.0lfkb\", style=filled, fillcolor=skyblue1, fixedsize=true, width=%lf, height=0.75", e->size / 1024, width);
			} else {
				fprintf(stdout, "\", fixedsize = false, style = unfilled, ");
			}
		} else
			fprintf(stdout, "\"");

		fprintf(stdout, "];\n");

	}

	fprintf(stdout, "\n");

	for(n = d->nodes; n; n = n->next) {

		name = xxstrdup(n->command);
		label = strtok(name, " \t\n");
		t = hash_table_lookup(h, label);


		list_first_item(n->source_files);
		while((f = list_next_item(n->source_files))) {
			e = hash_table_lookup(g, f->filename);
			fprintf(stdout, "F%d -> N%d;\n", e->id, condense_display ? t->id : n->nodeid);
		}

		list_first_item(n->target_files);
		while((f = list_next_item(n->target_files))) {
			e = hash_table_lookup(g, f->filename);
			fprintf(stdout, "N%d -> F%d;\n", condense_display ? t->id : n->nodeid, e->id);
		}

		free(name);
	}

	fprintf(stdout, "}\n");

	hash_table_firstkey(h);
	while(hash_table_nextkey(h, &label, (void **) &t)) {
		free(t);
		hash_table_remove(h, label);
	}

	hash_table_firstkey(g);
	while(hash_table_nextkey(g, &label, (void **) &e)) {
		free(e);
		hash_table_remove(g, label);
	}

	hash_table_delete(g);
	hash_table_delete(h);
}

void ppm_color_parser(struct dag_node *n, char *color_array, int ppm_mode, char (*ppm_option), int current_level, int whitespace_on)
{

	if(whitespace_on) {
		color_array[0] = 1;
		color_array[1] = 1;
		color_array[2] = 1;
		return;
	}

	struct dag_file *f;

	int ppm_option_int;
	char *name, *label;

	memset(color_array, 0, 3 * sizeof(char));

	if(ppm_mode == 1) {
		switch (n->state) {
		case DAG_NODE_STATE_WAITING:
			break;
		case DAG_NODE_STATE_RUNNING:
			color_array[0] = 1;
			color_array[1] = 1;
			color_array[2] = 0;
			break;
		case DAG_NODE_STATE_COMPLETE:
			color_array[0] = 0;
			color_array[1] = 1;
			color_array[2] = 0;
			break;
		case DAG_NODE_STATE_FAILED:
			color_array[0] = 1;
			color_array[1] = 0;
			color_array[2] = 0;
			break;
		case DAG_NODE_STATE_ABORTED:
			color_array[0] = 1;
			color_array[1] = 0;
			color_array[2] = 0;
			break;
		default:
			color_array[0] = 0;
			color_array[1] = 0;
			color_array[2] = 1;
			break;
		}
	}
	if(ppm_mode == 2) {
		name = xxstrdup(n->command);
		label = strtok(name, " \t\n");
		if(strcmp(label, ppm_option) == 0) {
			//node name is matched, set to yellow
			color_array[0] = 0;
			color_array[1] = 1;
			color_array[2] = 1;
		}
	}
	if(ppm_mode == 3) {
		//searches the files for a result file named such
		list_first_item(n->target_files);
		while((f = list_next_item(n->target_files))) {
			if(strcmp(f->filename, ppm_option) == 0) {
				//makes this file, set to purple
				color_array[0] = 1;
				color_array[1] = 0;
				color_array[2] = 1;
				break;
			}
		}
	}
	if(ppm_mode == 4) {
		ppm_option_int = atoi(ppm_option);
		if(current_level == ppm_option_int) {
			//sets everything at that level to yellow
			color_array[0] = 0;
			color_array[1] = 1;
			color_array[2] = 1;
		}
	}
	if(ppm_mode == 5) {
		color_array[current_level % 3] = 1;
	}
}

void dag_to_ppm(struct dag *d, int ppm_mode, char *ppm_option)
{

	int count, count_row, max_ancestor = 0, max_size = 0;
	UINT64_T key;
	struct dag_node *n;

	char *name;
	char *label;

	struct hash_table *h;

	dag_find_ancestor_depth(d);

	h = hash_table_create(0, 0);

	itable_firstkey(d->node_table);
	while(itable_nextkey(d->node_table, &key, (void **) &n)) {

		name = xxstrdup(n->command);
		label = strtok(name, " \t\n");


		if(max_ancestor < n->ancestor_depth)
			max_ancestor = n->ancestor_depth;

		sprintf(name, "%d", n->nodeid);

		hash_table_insert(h, name, n);

	}

	struct list **ancestor_count_list = malloc((max_ancestor + 1) * sizeof(struct list *));

	//initialize all of the lists
	for(count = 0; count <= max_ancestor; count++) {
		ancestor_count_list[count] = list_create();
	}

	hash_table_firstkey(h);
	while(hash_table_nextkey(h, &label, (void **) &n)) {
		list_push_tail(ancestor_count_list[n->ancestor_depth], n);
		if(list_size(ancestor_count_list[n->ancestor_depth]) > max_size)
			max_size = list_size(ancestor_count_list[n->ancestor_depth]);
	}

	int i;
	int node_num_rows = 0;

	int max_image_width = 1200;
	int node_width = max_image_width / max_size;
	if(node_width < 5)
		node_width = 5;

	for(i = 0; i <= max_ancestor; i++) {
		node_num_rows = node_num_rows + ((node_width * list_size(ancestor_count_list[i])) - 1) / (max_image_width) + 1;
	}

	int max_image_height = 800;
	int row_height = max_image_height / node_num_rows;
	if(row_height < 5)
		row_height = 5;

	//calculate the column size so that we can center the data

	int x_length = (max_image_width / node_width) * node_width;
	int y_length = row_height * (node_num_rows);

	int current_depth_width;
	int current_depth_nodesPrinted;
	int current_depth_pixel_nodesPrinted;
	int nodesCanBePrinted = x_length / node_width;
	int current_depth_nodesCanBePrinted;
	int current_depth_numRows;

	int numRows;

	int pixel_count_col;
	int pixel_count_height;

	int whitespace;
	int whitespace_left;
	int whitespace_right;
	int whitespace_on;

	fprintf(stdout, "P6\n");	//"Magic Number", don't change
	fprintf(stdout, "%d %d\n", x_length, y_length);	//Width and Height
	fprintf(stdout, "1\n");	//maximum color value

	char color_array[3];

	for(count_row = 0; count_row <= max_ancestor; count_row++) {	//each ancestor depth in the dag
		current_depth_width = list_size(ancestor_count_list[count_row]);	//the width of this particular level of the dag
		current_depth_numRows = (node_width * current_depth_width - 1) / (x_length) + 1;
		current_depth_nodesPrinted = 0;

		for(numRows = 0; numRows < current_depth_numRows; numRows++) {

			if((current_depth_width - current_depth_nodesPrinted) < nodesCanBePrinted)
				current_depth_nodesCanBePrinted = current_depth_width - current_depth_nodesPrinted;
			else
				current_depth_nodesCanBePrinted = nodesCanBePrinted;


			whitespace = x_length - (current_depth_nodesCanBePrinted * node_width);
			whitespace_left = whitespace / 2;
			whitespace_right = x_length - (whitespace - whitespace_left);

			for(pixel_count_height = 0; pixel_count_height < row_height; pixel_count_height++) {	//each pixel row of said ancestor height

				list_first_item(ancestor_count_list[count_row]);
				current_depth_pixel_nodesPrinted = 0;
				for(pixel_count_col = 0; pixel_count_col < x_length; pixel_count_col++) {	//for each node in the width
					if((pixel_count_col < whitespace_left) || (pixel_count_col >= whitespace_right)) {
						whitespace_on = 1;
					} else {
						whitespace_on = 0;
						if((pixel_count_col - whitespace_left - (current_depth_pixel_nodesPrinted * node_width)) == 0) {
							n = list_next_item(ancestor_count_list[count_row]);
							current_depth_pixel_nodesPrinted++;
							if(pixel_count_height == 0)
								current_depth_nodesPrinted++;
						}
					}
					ppm_color_parser(n, color_array, ppm_mode, ppm_option, count_row, whitespace_on);
					fprintf(stdout, "%c%c%c", color_array[0], color_array[1], color_array[2]);
				}
			}
		}

	}

	hash_table_delete(h);
	free(ancestor_count_list);

}

/* vim: set noexpandtab tabstop=4: */
