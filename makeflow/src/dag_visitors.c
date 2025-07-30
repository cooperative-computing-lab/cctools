/*
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/utsname.h>
#include <inttypes.h>
#include <errno.h>

#include <ctype.h>
#include <limits.h>
#include <pwd.h>
#include <time.h>
#include <unistd.h>

#include "hash_table.h"
#include "xxmalloc.h"
#include "list.h"
#include "itable.h"
#include "debug.h"
#include "path.h"
#include "set.h"
#include "string_set.h"
#include "stringtools.h"
#include "copy_stream.h"

#include "dag.h"
#include "dag_resources.h"
#include "dag_variable.h"
#include "dag_visitors.h"
#include "rmsummary.h"

/*
 * BUG: Error handling is not very good.
 * BUG: Integrate more with dttools (use DEBUG, etc.)
 */

/* Writes 'var=value' pairs for special vars to the stream */
int dag_to_file_var(const char *name, struct hash_table *vars, int nodeid, FILE * dag_stream, const char *prefix)
{
	struct dag_variable_value *v;
	v = dag_variable_get_value(name, vars, nodeid);
	if(v && !string_null_or_empty(v->value))
		fprintf(dag_stream, "%s%s=\"%s\"\n", prefix, name, (char *) v->value);

	return 0;
}

int dag_to_file_vars(struct string_set *var_names, struct hash_table *vars, int nodeid, FILE * dag_stream, const char *prefix)
{
	char *name;

	string_set_first_element(var_names);
	while(string_set_next_element(var_names, &name))
	{
		dag_to_file_var(name, vars, nodeid, dag_stream, prefix);
	}

	return 0;
}

/* Writes 'export var' tokens from the dag to the stream */
int dag_to_file_exports(const struct dag *d, FILE * dag_stream, const char *prefix)
{
	char *name;

	struct string_set *vars = d->export_vars;

	struct dag_variable_value *v;
	string_set_first_element(vars);
	while(string_set_next_element(vars, &name))
	{
		v = dag_variable_get_value(name, d->default_category->mf_variables, 0);
		if(v)
		{
			fprintf(dag_stream, "%s%s=", prefix, name);
			if(!string_null_or_empty(v->value))
					fprintf(dag_stream, "\"%s\"", (char *) v->value);
			fprintf(dag_stream, "\n");
			fprintf(dag_stream, "export %s\n", name);
		}
	}

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
			const char *remotename = dag_node_get_remote_name(n, f->filename);
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
	dag_to_file_files(n, n->target_files, dag_stream, rename);
	fprintf(dag_stream, ": ");
	dag_to_file_files(n, n->source_files, dag_stream, rename);
	fprintf(dag_stream, "\n");

	dag_to_file_vars(n->d->special_vars, n->variables, n->nodeid, dag_stream, "@");
	dag_to_file_vars(n->d->export_vars,  n->variables, n->nodeid, dag_stream, "@");

	if(n->local_job)
		fprintf(dag_stream, "\tLOCAL %s", n->command);
	else
		fprintf(dag_stream, "\t%s\n", n->command);
	fprintf(dag_stream, "\n");

	return 0;
}

/* Writes all the rules to the stream, per category, plus any variables from the category */
int dag_to_file_category(struct category *c, struct list *nodes, FILE * dag_stream, char *(*rename) (struct dag_node * n, const char *filename))
{
	struct dag_node *n;

	list_first_item(nodes);
	while((n = list_next_item(nodes)))
	{
		dag_to_file_vars(n->d->special_vars, n->d->default_category->mf_variables, n->nodeid, dag_stream, "");
		dag_to_file_vars(n->d->export_vars,  n->d->default_category->mf_variables, n->nodeid, dag_stream, "");
		dag_to_file_node(n, dag_stream, rename);
	}

	return 0;
}

int dag_to_file_categories(const struct dag *d, FILE * dag_stream, char *(*rename) (struct dag_node * n, const char *filename))
{

	//separate nodes per category
	struct hash_table *nodes_of_category = hash_table_create(2*hash_table_size(d->categories), 0);

	struct category *c;
	struct list *ns;
	struct dag_node *n = d->nodes;
	char *name;

	while(n) {
		name = n->category->name;
		ns = hash_table_lookup(nodes_of_category, name);
		if(!ns) {
			ns = list_create();
			hash_table_insert(nodes_of_category, name, (void *) ns);
		}
		list_push_tail(ns, n);
		n = n->next;
	}

	hash_table_firstkey(nodes_of_category);
	while(hash_table_nextkey(nodes_of_category, &name, (void **) &ns)) {
		c = makeflow_category_lookup_or_create(d, name);
		dag_to_file_category(c, ns, dag_stream, rename);
	}

	return 0;
}

/* Entry point of the dag_to_file* functions. Writes a dag as an
 * equivalent makeflow file. */
int dag_to_file(const struct dag *d, const char *dag_file, char *(*rename) (struct dag_node * n, const char *filename))
{
	FILE *dag_stream;

	if(dag_file)
		dag_stream = fopen(dag_file, "w");
	else
		dag_stream = stdout;

	if(!dag_stream)
		return 1;

	// For the collect list, use the their final value (the value at node with id nodeid_counter).
	dag_to_file_var("GC_COLLECT_LIST", d->default_category->mf_variables, d->nodeid_counter, dag_stream, "");
	dag_to_file_var("GC_PRESERVE_LIST", d->default_category->mf_variables, d->nodeid_counter, dag_stream, "");

	dag_to_file_exports(d, dag_stream, "");

	dag_to_file_categories(d, dag_stream, rename);

	if(dag_file)
		fclose(dag_stream);

	return 0;
}

/* Writes the xml header incantation for DAX */
static void dag_to_dax_header( const char *name, FILE *output )
{
	fprintf(output,"<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");

	time_t current_raw_time;
	struct tm *time_info;
	char buffer[64];
	time(&current_raw_time);
	time_info = localtime(&current_raw_time);
	strftime(buffer, 64, "%Y-%m-%d %T", time_info);
	fprintf(output,"<!-- generated: %s -->\n", buffer);

	uid_t uid = getuid();
	struct passwd *current_user_info;
	current_user_info = getpwuid(uid);
	fprintf(output,"<!-- generated by: %s -->\n", current_user_info->pw_name);

	fprintf(output,"<!-- generator: Makeflow -->\n");

	fprintf(output,"<adag ");
	fprintf(output,"xmlns=\"http://pegasus.isi.edu/schema/DAX\" ");
	fprintf(output,"xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" ");
	fprintf(output,"xsi:schemaLocation=\"http://pegasus.isi.edu/schema/DAX http://pegasus.isi.edu/schema/dax-3.4.xsd\" ");
	fprintf(output,"version=\"3.4\" ");
	fprintf(output,"name=\"%s\">\n", name);
}

/* Write list of files in DAX format for a given node
 * @param type 0 for input 1 for output
 */
static void dag_to_dax_files(struct list *fs, int type, FILE *output)
{
	const struct dag_file *f;
	list_first_item(fs);
	while((f = list_next_item(fs))) {
		if(type == 0)
			fprintf(output, "\t\t<uses name=\"%s\" link=\"input\" />\n", f->filename);
		else
			fprintf(output, "\t\t<uses name=\"%s\" link=\"output\" register=\"false\" transfer=\"true\" />\n", f->filename);
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
void dag_to_dax_individual_node(const struct dag_node *n, UINT64_T node_id, FILE *output)
{
	fprintf(output, "\t<job id=\"ID%07" PRIu64 "\" name=\"%s\">\n", node_id, node_executable(n));
	fprintf(output, "\t\t<argument>%s</argument>\n", node_executable_arguments(n));

	const char *redirection = node_executable_redirect(n);
	if(redirection) fprintf(output, "\t\t<stdout name=\"%s\" link=\"output\" />\n", redirection);

	dag_to_dax_files(n->source_files, 0, output);
	dag_to_dax_files(n->target_files, 1, output);
	fprintf(output, "\t</job>\n");
}

/* Iterates over each node to output as DAX */
void dag_to_dax_nodes(const struct dag *d, FILE *output)
{
	struct dag_node *n;
	UINT64_T node_id;

	itable_firstkey(d->node_table);
	while(itable_nextkey(d->node_table, &node_id, (void *) &n))
		dag_to_dax_individual_node(n, node_id, output);
}

/* Writes the DAX for a node's parent relationships */
void dag_to_dax_parents(const struct dag_node *n, FILE *output)
{
	struct dag_node *p;

	if(set_size(n->ancestors) > 0){
		fprintf(output, "\t<child ref=\"ID%07d\">\n", n->nodeid);
		set_first_element(n->ancestors);
		while((p = set_next_element(n->ancestors)))
			fprintf(output, "\t\t<parent ref=\"ID%07d\" />\n", p->nodeid);
		fprintf(output, "\t</child>\n");
	}
}

/* Writes the DAX version of each relationship in the dag */
void dag_to_dax_relationships(const struct dag *d, FILE *output)
{
	struct dag_node *n;
	UINT64_T node_id;

	itable_firstkey(d->node_table);
	while(itable_nextkey(d->node_table, &node_id, (void *) &n))
		dag_to_dax_parents(n, output);
}

/* Writes the xml footer for DAX */
void dag_to_dax_footer(FILE *output)
{
	fprintf(output, "</adag>\n");
}

/* Write replica catalog to file */
void dag_to_dax_replica_catalog(const struct dag *d, FILE *output)
{
	struct dag_file *f = NULL;
	char fn[PATH_MAX];
	struct list *input_files = dag_input_files((struct dag*) d);
	list_first_item(input_files);
	while((f = (struct dag_file*)list_next_item(input_files)))
	{
		realpath(f->filename, fn);
		fprintf(output, "%s\tfile://%s\t%s\n", path_basename(f->filename), fn, "pool=\"local\"");
	}
}

/* Write transform catalog to file */
void dag_to_dax_transform_catalog(const struct dag *d, FILE *output)
{
	struct dag_node *n;
	uint64_t id;
	char *fn, *pfn;
	char *type;
	pfn = (char *) malloc(PATH_MAX * sizeof(char));
	struct utsname *name = malloc(sizeof(struct utsname));
	uname(name);
	struct list *transforms = list_create();

	itable_firstkey(d->node_table);
	while(itable_nextkey(d->node_table, &id, (void *) &n))
	{
		fn = xxstrdup(node_executable(n));
		if(!list_find(transforms, (int (*)(void *, const void*)) string_equal, fn))
			list_push_tail(transforms, fn);
	}

	list_first_item(transforms);
	while((fn = list_next_item(transforms))) {
		if(path_lookup(getenv("PATH"), fn, pfn, PATH_MAX)){
			realpath(fn, pfn);
			type = "STAGEABLE";
		} else {
			type = "INSTALLED";
		}

		fprintf(output, "tr %s {\n", fn);
		fprintf(output, "  site local {\n");
		fprintf(output, "    pfn \"%s\"\n", pfn);
		fprintf(output, "    arch \"%s\"\n", name->machine);
		fprintf(output, "    os \"%s\"\n", name->sysname);
		fprintf(output, "    type \"%s\"\n", type);
		fprintf(output, "  }\n");
		fprintf(output, "}\n\n");
	}

	list_free(transforms);
	list_delete(transforms);
	free(pfn);
}

void dag_to_dax_print_usage(const char *name)
{
	printf( "To plan your workflow try:\n");
	printf( "\tpegasus-plan -Dpegasus.catalog.replica.file=%s.rc \\\n", name);
	printf( "\t             -Dpegasus.catalog.transformation.file=%s.rc \\\n", name);
	printf( "\t             -d %s.dax\n\n", name);
}

/* Entry Point of the dag_to_dax* functions.
 * Writes a dag in DAX format to file.
 * see: http://pegasus.isi.edu/wms/docs/schemas/dax-3.4/dax-3.4.html
 */
int dag_to_dax( const struct dag *d, const char *name )
{
	char dax_filename[PATH_MAX];

	sprintf(dax_filename, "%s.dax", name);
	FILE *dax = fopen(dax_filename, "w");
	dag_to_dax_header(name, dax);
	dag_to_dax_nodes(d, dax);
	dag_to_dax_relationships(d, dax);
	dag_to_dax_footer(dax);
	fclose(dax);

	sprintf(dax_filename, "%s.rc", name);
	dax = fopen(dax_filename, "w");
	dag_to_dax_replica_catalog(d, dax);
	fclose(dax);

	sprintf(dax_filename, "%s.tc", name);
	dax = fopen(dax_filename, "w");
	dag_to_dax_transform_catalog(d, dax);
	fclose(dax);

	dag_to_dax_print_usage(name);

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

void write_node_to_xgmml(FILE *f, char idheader, int id, char* nodename, int process)
{
	//file *f must already be open!

	fprintf(f,"\t<node id=\"%c%d\" label=\"%s\">\n", idheader, id, nodename);
	fprintf(f,"\t\t<att name=\"shared name\" value=\"%s\" type=\"string\"/>\n", nodename);
	fprintf(f,"\t\t<att name=\"name\" value=\"%s\" type=\"string\"/>\n", nodename);
	fprintf(f,"\t\t<att name=\"process\" value=\"%d\" type=\"boolean\"/>\n", process);
	fprintf(f,"\t</node>\n");
}

void write_edge_to_xgmml(FILE *f, char sourceheader, int sourceid, char targetheader, int targetid, int directed)
{
	//file *f must already be open!
	fprintf(f, "\t<edge id=\"%c%d-%c%d\" label=\"%c%d-%c%d\" source=\"%c%d\" target=\"%c%d\" cy:directed=\"%d\">\n", sourceheader, sourceid, targetheader, targetid, sourceheader, sourceid, targetheader, targetid, sourceheader, sourceid, targetheader, targetid, directed);
	fprintf(f,"\t\t<att name=\"shared name\" value=\"%c%d-%c%d\" type=\"string\"/>\n", sourceheader, sourceid, targetheader, targetid);
	fprintf(f,"\t\t<att name=\"shared interaction\" value=\"\" type=\"string\"/>\n");
	fprintf(f,"\t\t<att name=\"name\" value=\"%c%d-%c%d\" type=\"string\"/>\n", sourceheader, sourceid, targetheader, targetid);
	fprintf(f,"\t\t<att name=\"selected\" value=\"0\" type=\"boolean\"/>\n");
	fprintf(f,"\t\t<att name=\"interaction\" value=\"\" type=\"string\"/>\n");
	fprintf(f,"\t\t<att name=\"weight\" value=\"8\" type=\"integer\"/>\n");
	fprintf(f,"\t</edge>\n");
}

void dag_to_cyto(struct dag *d, int condense_display, int change_size)
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

	FILE *cytograph = stdout;
	//FILE *cytograph = fopen("cytoscape.xgmml", "w");


	fprintf(cytograph, "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n");
	fprintf(cytograph, "<graph id=\"1\" label=\"small example\" directed=\"1\" cy:documentVersion=\"3.0\" xmlns:xlink=\"http://www.w3.org/1999/xlink\" xmlns:rdf=\"http://www.w3.org/1999/02/22-rdf-syntax-ns#\" xmlns:cy=\"http://www.cytoscape.org/\" xmlns=\"http://www.cs.rpi.edu/XGMML\">\n");
	fprintf(cytograph, "\t<att name=\"networkMetadata\">\n");
	fprintf(cytograph, "\t\t<rdf:RDF>\n");
	fprintf(cytograph, "\t\t\t<rdf:Description rdf:about=\"http://ccl.cse.nd.edu/\">\n");
	fprintf(cytograph, "\t\t\t\t<dc:type>Makeflow Structure</dc:type>\n");
	fprintf(cytograph, "\t\t\t\t<dc:description>N/A</dc:description>\n");
	fprintf(cytograph, "\t\t\t\t<dc:identifier>N/A</dc:identifier>\n");

	time_t timer;
	time(&timer);
	struct tm* currenttime = localtime(&timer);
	char timestring[20];
	strftime(timestring, sizeof(timestring), "%Y-%m-%d %H:%M:%S", currenttime);
	fprintf(cytograph, "\t\t\t\t<dc:date>%s</dc:date>\n", timestring);

	fprintf(cytograph, "\t\t\t\t<dc:title>Makeflow Visualization</dc:title>\n");
	fprintf(cytograph, "\t\t\t\t<dc:source>http://ccl.cse.nd.edu/</dc:source>\n");
	fprintf(cytograph, "\t\t\t\t<dc:format>Cytoscape-XGMML</dc:format>\n");
	fprintf(cytograph, "\t\t\t</rdf:Description>\n");
	fprintf(cytograph, "\t\t</rdf:RDF>\n");
	fprintf(cytograph, "\t</att>\n");

	fprintf(cytograph, "\t<att name=\"shared name\" value=\"Makeflow Visualization\" type=\"string\"/>\n");
	fprintf(cytograph, "\t<att name=\"name\" value=\"Makeflow Visualization\" type=\"string\"/>\n");
	fprintf(cytograph, "\t<att name=\"selected\" value=\"1\" type=\"boolean\"/>\n");
	fprintf(cytograph, "\t<att name=\"__Annotations\" type=\"list\">\n");
	fprintf(cytograph, "\t</att>\n");
	fprintf(cytograph, "\t<att name = \"layoutAlgorithm\" value = \"Grid Layout\" type = \"string\" cy:hidden = \"1\"/>\n");

	if(change_size) {
		hash_table_firstkey(d->files);
		while(hash_table_nextkey(d->files, &name, (void **) &f) && dag_file_should_exist(f)) {
			if(stat(name,&st)==0) {
				average += ((double) st.st_size) / ((double) d->completed_files);
			}
		}
	}


	h = hash_table_create(0, 0);

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
			t->print = 0;
		}
		write_node_to_xgmml(cytograph, 'N', n->nodeid, label,1);
		free(name);
	}

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
		write_node_to_xgmml(cytograph, 'F', e->id, (char *)fn, 0);

		if(change_size) {
			if(e->size >= 0) {
				width = 5 * (e->size / average);
				if(width < 2.5)
					width = 2.5;
				if(width > 25)
					width = 25;
			}
		}
	}

	for(n = d->nodes; n; n = n->next) {

		name = xxstrdup(n->command);
		label = strtok(name, " \t\n");
		t = hash_table_lookup(h, label);


		list_first_item(n->source_files);
		while((f = list_next_item(n->source_files))) {
			e = hash_table_lookup(g, f->filename);
			write_edge_to_xgmml(cytograph, 'F', e->id, 'N', n->nodeid, 1);
		}

		list_first_item(n->target_files);
		while((f = list_next_item(n->target_files))) {
			e = hash_table_lookup(g, f->filename);
			write_edge_to_xgmml(cytograph, 'N', n->nodeid, 'F', e->id, 1);
		}

		free(name);
	}

	fprintf(cytograph, "</graph>\n");
	fclose(cytograph);

	if(copy_file_to_file(INSTALL_PATH "/share/cctools/makeflow-cytoscape-style.xml", "style.xml") < 0) {
		fprintf(stderr, "Unable to create ./style.xml: %s\n", strerror(errno));
	}

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


void dag_to_dot(struct dag *d, int condense_display, int change_size, int with_labels, int task_id, int with_details, char *graph_attr, char *node_attr, char *edge_attr, char *task_attr, char *file_attr )
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

	//Dot Details Variables
	struct file_node *src;
	struct file_node *tar;

	double average = 0;
	double width = 0;

	printf( "digraph {\n");

	if(change_size) {
		hash_table_firstkey(d->files);
		while(hash_table_nextkey(d->files, &name, (void**)&f )) {
			if(stat(name,&st)==0) {
				average += ((double) st.st_size) / ((double) d->completed_files);
			}
		}
	}

	if(graph_attr){
		printf( "graph [%s]\n", graph_attr);
	}
	if(node_attr){
		printf( "node [%s]\n", node_attr);
	}
	if(edge_attr){
		printf( "edge [%s]\n", edge_attr);
	}

	h = hash_table_create(0, 0);

	if(task_attr){
		printf( "\nnode [shape=ellipse,color = green,style = %s,%s];\n", with_labels ? "unfilled" : "filled", task_attr );
	} else {
		printf( "\nnode [shape=ellipse,color = green,style = %s,fixedsize = false];\n", with_labels ? "unfilled" : "filled" );
	}
  
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

	for(n = d->nodes; n; n = n->next) {
		name = xxstrdup(n->command);
		label = strtok(name, " \t\n");
		t = hash_table_lookup(h, label);
		if(!condense_display || t->print) {
			//Dot Details
			if(with_details) {
				printf("subgraph cluster_S%d { \n", condense_display ? t->id : n->nodeid);
				printf("\tstyle=unfilled;\n\tcolor=red\n");
				printf("\tcores%d [style=filled, color=white, label=\"cores: %s\"]\n", condense_display ? t->id : n->nodeid, rmsummary_resource_to_str("cores", n->resources_requested->cores, 0));
				printf("\tgpus%d [style=filled, color=white, label=\"gpus: %s\"]\n", condense_display ? t->id : n->nodeid, rmsummary_resource_to_str("gpus", n->resources_requested->cores, 0));
				printf("\tresMem%d [style=filled, color=white, label=\"memory: %s\"]\n", condense_display ? t->id : n->nodeid, rmsummary_resource_to_str("memory", n->resources_requested->memory, 1));
				printf("\tworkDirFtprnt%d [style=filled, color=white, label=\"footprint: %s\"]\n", condense_display ? t->id : n->nodeid, rmsummary_resource_to_str("disk", n->resources_requested->disk, 1));
				printf("\tcores%d -> resMem%d -> workDirFtprnt%d [color=white]", condense_display ? t->id : n->nodeid, condense_display ? t->id : n->nodeid, condense_display ? t->id : n->nodeid);

				//Source Files
				list_first_item(n->source_files);
				while((f = list_next_item(n->source_files))) {
					fn = f->filename;
					e = hash_table_lookup(g, fn);
					if(e) {
						printf("\tsrc_%d_%d [label=\"%s\", style=unfilled, color=purple, shape=box];\n", condense_display ? t->id : n->nodeid, e->id, e->name);
						printf("\tsrc_%d_%d -> N%d;\n", condense_display ? t->id : n->nodeid, e->id, condense_display ? t->id : n->nodeid);
					}
				}

				//Target Files
				list_first_item(n->target_files);
				while((f = list_next_item(n->target_files))) {
					fn = f->filename;
					e = hash_table_lookup(g, fn);
					if(e) {
						printf("\ttar_%d_%d [label=\"%s\", style=dotted, color=purple, shape=box];\n", condense_display ? t->id : n->nodeid, e->id, e->name);
						printf("\tN%d -> tar_%d_%d;\n", condense_display ? t->id : n->nodeid, condense_display ? t->id : n->nodeid, e->id);

					}
				}
			}

			if((t->count == 1) || !condense_display) {
				if(task_id && with_labels){
					printf( "N%d [label=\"%d\"];\n", condense_display ? t->id : n->nodeid, n->nodeid);
				} else {
					printf( "N%d [label=\"%s\"];\n", condense_display ? t->id : n->nodeid, with_labels ? label : "");
				}
			} else {
				if(task_id && with_labels){
					printf( "N%d [label=\"%d x%d\"];\n", t->id, n->nodeid, t->count);
				} else {
					printf( "N%d [label=\"%s x%d\"];\n", t->id, with_labels ? label : "", t->count);
				}
			}

			if(with_details) {
				printf( "}\n" );
			}

			t->print = 0;

		}
		free(name);
	}

	if(file_attr){
		printf( "\nnode [shape=box,color=blue,style=%s,%s];\n", with_labels ? "unfilled" : "filled", task_attr );
	} else {
		printf( "\nnode [shape=box,color=blue,style=%s,fixedsize=false];\n", with_labels ? "unfilled" : "filled" );
	}

	hash_table_firstkey(g);
	while(hash_table_nextkey(g, &label, (void **) &e)) {
		fn = e->name;
		printf( "F%d [label = \"%s", e->id, with_labels ? fn : "" );
		char cytoid[6];
		sprintf(cytoid, "F%d", e->id);
		if(change_size) {
			if(e->size >= 0) {
				width = 5 * (e->size / average);
				if(width < 2.5)
					width = 2.5;
				if(width > 25)
					width = 25;
				printf( "\\nsize:%.0lfkb\", style=filled, fillcolor=skyblue1, fixedsize=true, width=%lf, height=0.75", e->size / 1024, width);
			} else {
				printf( "\", fixedsize = false, style = unfilled, ");
			}
		} else
			printf( "\"");

		printf( "];\n");

	}

	printf( "\n");

	for(n = d->nodes; n; n = n->next) {

		name = xxstrdup(n->command);
		label = strtok(name, " \t\n");
		t = hash_table_lookup(h, label);


		list_first_item(n->source_files);
		while((f = list_next_item(n->source_files))) {
			e = hash_table_lookup(g, f->filename);

			if(with_details) {
				src = hash_table_lookup(g, f->filename);
				if(src) {
					printf( "F%d -> src_%d_%d;\n", e->id, condense_display ? t->id : n->nodeid, e->id );
				}
			}
			else {
			printf( "F%d -> N%d;\n", e->id, condense_display ? t->id : n->nodeid);
			}
		}

		list_first_item(n->target_files);
		while((f = list_next_item(n->target_files))) {
			e = hash_table_lookup(g, f->filename);

			if(with_details) {
				tar = hash_table_lookup(g, f->filename);
				if(tar) {
					printf( "tar_%d_%d -> F%d;\n", condense_display ? t->id : n->nodeid, e->id, e->id );
				}
			}
			else {
				printf( "N%d -> F%d;\n", condense_display ? t->id : n->nodeid, e->id);
			}
		}

		free(name);
	}

	printf( "}\n");


	hash_table_clear(g, free);
	hash_table_delete(g);
	hash_table_clear(h, free);
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

	printf( "P6\n");	//"Magic Number", don't change
	printf( "%d %d\n", x_length, y_length);	//Width and Height
	printf( "1\n");	//maximum color value

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
					printf( "%c%c%c", color_array[0], color_array[1], color_array[2]);
				}
			}
		}

	}

	hash_table_delete(h);
	free(ancestor_count_list);

}

struct jx *variables_to_json(struct hash_table *h) {
	char *key;
	struct dag_variable *value;
	struct jx *result = jx_object(NULL);

	hash_table_firstkey(h);
	while(hash_table_nextkey(h, &key, (void **) &value)) {
		jx_insert(result, jx_string(key), jx_string(value->values[value->count - 1]->value));
	}

	return result;
}

struct jx *category_allocation_to_json(category_allocation_t c) {
	switch(c) {
		case CATEGORY_ALLOCATION_FIRST:
			return jx_string("first");
			break;
		case CATEGORY_ALLOCATION_MAX:
			return jx_string("max");
			break;
		case CATEGORY_ALLOCATION_ERROR:
			return jx_string("error");
			break;
		case CATEGORY_ALLOCATION_GREEDY_BUCKETING:
			return jx_string("greedy bucketing");
			break;
		case CATEGORY_ALLOCATION_EXHAUSTIVE_BUCKETING:
			return jx_string("exhaustive bucketing");
			break;
	}
	return jx_null();
}

struct jx *resources_to_json(struct rmsummary *r) {
	struct jx *result = jx_object(NULL);
	if(r->cores > 0) {
		jx_insert(result, jx_string("cores"), jx_integer(r->cores));
	}
	if(r->disk > 0) {
		jx_insert(result, jx_string("disk"), jx_integer(r->disk));
	}
	if(r->memory > 0) {
		jx_insert(result, jx_string("memory"), jx_integer(r->memory));
	}
	if(r->gpus > -1) {
		jx_insert(result, jx_string("gpus"), jx_integer(r->gpus));
	}
	return result;
}

struct jx *category_to_json(struct category *c) {
	struct jx *result = resources_to_json(c->max_allocation);
	jx_insert_unless_empty(result, jx_string("environment"), variables_to_json(c->mf_variables));
	return result;
}

struct jx *files_to_json(struct list *files, struct itable *remote_names) {
	struct jx *result = jx_array(NULL);
	struct dag_file *file;

	list_first_item(files);
	while((file = list_next_item(files))) {

		const char *task_name = file->filename;
		const char *dag_name = itable_lookup(remote_names,(uintptr_t)file);

		if(dag_name) {
			struct jx *f = jx_object(NULL);
			jx_insert(f, jx_string("task_name"),jx_string(task_name));
			jx_insert(f, jx_string("dag_name"),jx_string(dag_name));
			jx_array_insert(result, f);
		} else {
			jx_array_insert(result,jx_string(task_name));
		}
	}
	return result;
}

struct jx *dag_nodes_to_json(struct dag_node *node) {
	struct jx *result = jx_array(NULL);
	struct dag_node *n = node;
	struct jx *rule;

	while(n) {
		rule = jx_object(NULL);
		if(n->resource_request!=CATEGORY_ALLOCATION_FIRST) {
			jx_insert(rule, jx_string("allocation"), category_allocation_to_json(n->resource_request));
		}
		if(strcmp(n->category->name,"default")) {
			jx_insert(rule, jx_string("category"), jx_string(n->category->name));
		}
		jx_insert_unless_empty(rule, jx_string("resources"), resources_to_json(n->resources_requested));
		jx_insert_unless_empty(rule, jx_string("environment"), variables_to_json(n->variables));
		jx_insert(rule, jx_string("outputs"), files_to_json(n->target_files, n->remote_names));
		jx_insert(rule, jx_string("inputs"), files_to_json(n->source_files, n->remote_names));
		if(n->local_job) {
			jx_insert(rule, jx_string("local_job"), jx_boolean(n->local_job));
		}
		if(n->type==DAG_NODE_TYPE_WORKFLOW) {
			jx_insert(rule, jx_string("workflow"), jx_string(n->workflow_file));
			jx_insert(rule, jx_string("args"), jx_copy(n->workflow_args));
		} else {
			jx_insert(rule, jx_string("command"), jx_string(n->command));
		}

		jx_array_insert(result, rule);
		n = n->next;
	}

	return result;
}

struct jx *dag_to_json(struct dag *d) {
	char *key;
	void *value;
	struct jx *result = jx_object(NULL);
	struct jx *categories = jx_object(NULL);
	jx_insert(result, jx_string("rules"), dag_nodes_to_json(d->nodes));
	hash_table_firstkey(d->categories);
	while(hash_table_nextkey(d->categories, &key,& value)) {
		jx_insert(categories, jx_string(key), category_to_json((struct category *) value));
	}
	jx_insert(result, jx_string("categories"), categories);
	jx_insert(result, jx_string("default_category"), jx_string(d->default_category->name));

	return result;
}

/* vim: set noexpandtab tabstop=4: */
