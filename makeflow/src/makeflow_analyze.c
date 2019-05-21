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
#include "getopt_aux.h"
#include "rmonitor.h"
#include "random.h"
#include "path.h"
#include "jx.h"
#include "jx_parse.h"

#include "dag.h"
#include "dag_visitors.h"
#include "parser.h"

/* Display options */
enum { SHOW_INPUT_FILES = 2,
	   SHOW_OUTPUT_FILES,
	   SHOW_MAKEFLOW_ANALYSIS,
	   SHOW_DAG_FILE
};

#define MAKEFLOW_AUTO_WIDTH 1
#define MAKEFLOW_AUTO_GROUP 2

void dag_show_analysis(struct dag *d)
{
	printf("num_of_tasks\t%d\n", itable_size(d->node_table));
	printf("depth\t%d\n", dag_depth(d));
	printf("width_uniform_task\t%d\n", dag_width_uniform_task(d));
	printf("width_guaranteed_max\t%d\n", dag_width_guaranteed_max(d));
}

void dag_show_input_files(struct dag *d)
{
	struct dag_file *f;
	struct list *il;

	il = dag_input_files(d);
	list_first_item(il);
	while((f = list_next_item(il)))
		printf("%s\n", f->filename);

	list_delete(il);
}

void collect_input_files(struct dag *d, char *bundle_dir, char *(*rename) (struct dag_node * d, const char *filename))
{
	char file_destination[PATH_MAX];
	char *new_name;

	struct list *il;
	il = dag_input_files(d);

	struct dag_file *f;

	list_first_item(il);
	while((f = list_next_item(il))) {
		new_name = rename(NULL, f->filename);
		char *dir = NULL;
		dir = xxstrdup(new_name);
		path_dirname(new_name, dir);
		if(dir){
			sprintf(file_destination, "%s/%s", bundle_dir, dir);
			if(!create_dir(file_destination, 0755)) {
				fprintf(stderr,  "Could not create %s. Check the permissions and try again.\n", file_destination);
				free(dir);
				exit(1);
			}
			free(dir);
		}

		sprintf(file_destination, "%s/%s", bundle_dir, new_name);
		fprintf(stdout, "%s\t%s\n", f->filename, new_name);
		free(new_name);
	}

	list_delete(il);
}

/* When a collision is detected (a file with an absolute path has the same base name as a relative file) a
 * counter is appended to the filename and the name translation is retried */
char *bundler_translate_name(const char *input_filename, int collision_counter)
{
	static struct hash_table *previous_names = NULL;
	static struct hash_table *reverse_names = NULL;
	if(!previous_names)
		previous_names = hash_table_create(0, NULL);
	if(!reverse_names)
		reverse_names = hash_table_create(0, NULL);

	char *filename = NULL;

	if(collision_counter){
		filename = string_format("%s%d", input_filename, collision_counter);
	}else{
		filename = xxstrdup(input_filename);
	}

	const char *new_filename;
	new_filename = hash_table_lookup(previous_names, filename);
	if(new_filename)
		return xxstrdup(new_filename);

	new_filename = hash_table_lookup(reverse_names, filename);
	if(new_filename) {
		collision_counter++;
		char *tmp = bundler_translate_name(filename, collision_counter);
		free(filename);
		return tmp;
	}
	if(filename[0] == '/') {
		new_filename = path_basename(filename);
		if(hash_table_lookup(previous_names, new_filename)) {
			collision_counter++;
			char *tmp = bundler_translate_name(filename, collision_counter);
			free(filename);
			return tmp;
		} else if(hash_table_lookup(reverse_names, new_filename)) {
			collision_counter++;
			char *tmp = bundler_translate_name(filename, collision_counter);
			free(filename);
			return tmp;
		} else {
			hash_table_insert(reverse_names, new_filename, filename);
			hash_table_insert(previous_names, filename, new_filename);
			return xxstrdup(new_filename);
		}
	} else {
		hash_table_insert(previous_names, filename, filename);
		hash_table_insert(reverse_names, filename, filename);
		return xxstrdup(filename);
	}
}

char *bundler_rename(struct dag_node *n, const char *filename)
{

	if(n) {
		struct list *input_files = dag_input_files(n->d);
		if(list_find(input_files, (int (*)(void *, const void *)) string_equal, (void *) filename)) {
			list_free(input_files);
			return xxstrdup(filename);
		}
	}
	return bundler_translate_name(filename, 0);	/* no collisions yet -> 0 */
}

void dag_show_output_files(struct dag *d)
{
	struct dag_file *f;
	char *filename;

	hash_table_firstkey(d->files);
	while(hash_table_nextkey(d->files, &filename, (void **) &f)) {
		if(f->created_by)
			fprintf(stdout, "%s\n", filename);
	}
}

static void show_help_analyze(const char *cmd)
{
	fprintf(stdout, "Use: %s [options] <dagfile>\n", cmd);
	fprintf(stdout, " %-30s Create portable bundle of workflow in <directory>\n", "-b,--bundle-dir=<directory>");
	fprintf(stdout, " %-30s Show this help screen.\n", "-h,--help");
	fprintf(stdout, " %-30s Show the pre-execution analysis of the Makeflow script - <dagfile>.\n", "-i,--analyze-exec");
	fprintf(stdout, " %-30s Show input files.\n", "-I,--show-input");
	fprintf(stdout, " %-30s Syntax check.\n", "-k,--syntax-check");
	fprintf(stdout, " %-30s Show output files.\n", "-O,--show-output");
	fprintf(stdout, " %-30s Show version string\n", "-v,--version");

	fprintf(stdout, "\nThe following options are for JX/JSON formatted DAG files:\n\n");
	fprintf(stdout, " %-30s Use JSON format for the workflow specification.\n", "--json");
	fprintf(stdout, " %-30s Use JX format for the workflow specification.\n", "--jx");
	fprintf(stdout, " %-30s Evaluate the JX input with keys and values in file defined as variables.\n", "--jx-args=<file>");
	fprintf(stdout, " %-30s Set the JX variable VAR to the JX expression EXPR.\n", "jx-define=<VAR>=<EXPR>");
	

}

enum {	LONG_OPT_JSON,
        LONG_OPT_JX,
        LONG_OPT_JX_ARGS,
        LONG_OPT_JX_DEFINE
};

int main(int argc, char *argv[])
{
	int c;
	random_init();
	debug_config(argv[0]);
	int display_mode = 0;

	cctools_version_debug(D_MAKEFLOW_RUN, argv[0]);
	const char *dagfile;

	char *bundle_directory = NULL;
	int syntax_check = 0;

	dag_syntax_type dag_syntax = DAG_SYNTAX_MAKE;
	struct jx *jx_args = jx_object(NULL);

	static const struct option long_options_analyze[] = {
		{"bundle-dir", required_argument, 0, 'b'},
		{"help", no_argument, 0, 'h'},
		{"analyze-exec", no_argument, 0, 'i'},
		{"show-input", no_argument, 0, 'I'},
		{"syntax-check", no_argument, 0, 'k'},
		{"show-output", no_argument, 0, 'O'},
		{"json", no_argument, 0, LONG_OPT_JSON},
		{"jx", no_argument, 0, LONG_OPT_JX},
		{"jx-context", required_argument, 0, LONG_OPT_JX_ARGS},
		{"jx-args", required_argument, 0, LONG_OPT_JX_ARGS},
		{"jx-define", required_argument, 0, LONG_OPT_JX_DEFINE},
		{"version", no_argument, 0, 'v'},
		{0, 0, 0, 0}
	};
	const char *option_string_analyze = "b:hiIkOd:v";

	while((c = getopt_long(argc, argv, option_string_analyze, long_options_analyze, NULL)) >= 0) {
		switch (c) {
			case 'b':
				bundle_directory = xxstrdup(optarg);
				break;
			case 'h':
				show_help_analyze(argv[0]);
				return 0;
			case 'i':
				display_mode = SHOW_MAKEFLOW_ANALYSIS;
				break;
			case 'I':
				display_mode = SHOW_INPUT_FILES;
				break;
			case 'k':
				syntax_check = 1;
				break;
			case 'O':
				display_mode = SHOW_OUTPUT_FILES;
				break;
			case 'd':
				debug_flags_set(optarg);
				break;
			case 'v':
				cctools_version_print(stdout, argv[0]);
				return 0;
			case LONG_OPT_JSON:
				dag_syntax = DAG_SYNTAX_JSON;
				break;
			case LONG_OPT_JX:
				dag_syntax = DAG_SYNTAX_JX;
				break;
			case LONG_OPT_JX_ARGS:
				dag_syntax = DAG_SYNTAX_JX;
				if(!jx_parse_cmd_args(jx_args, optarg))
					fatal("Failed to parse in JX Args File.\n");
				break;
			case LONG_OPT_JX_DEFINE:
				dag_syntax = DAG_SYNTAX_JX;
				if(!jx_parse_cmd_define(jx_args, optarg))
					fatal("Failed to parse in JX Define.\n");
				break;
			default:
				show_help_analyze(argv[0]);
				return 1;
		}
	}

	if((argc - optind) != 1) {
		int rv = access("./Makeflow", R_OK);
		if(rv < 0) {
			fprintf(stderr, "makeflow_analyze: No makeflow specified and file \"./Makeflow\" could not be found.\n");
			fprintf(stderr, "makeflow_analyze: Run \"%s -h\" for help with options.\n", argv[0]);
			return 1;
		}

		dagfile = "./Makeflow";
	} else {
		dagfile = argv[optind];
	}

	struct dag *d = dag_from_file(dagfile, dag_syntax, jx_args);
	if(!d) {
		fatal("makeflow_analyze: couldn't load %s: %s\n", dagfile, strerror(errno));
	}

	if(syntax_check) {
		fprintf(stdout, "%s: Syntax OK.\n", dagfile);
		return 0;
	}

	if(bundle_directory) {
		char expanded_path[PATH_MAX];

		collect_input_files(d, bundle_directory, bundler_rename);
		realpath(bundle_directory, expanded_path);

		char output_makeflow[PATH_MAX];
		string_nformat(output_makeflow, sizeof(output_makeflow), "%s/%s", expanded_path, path_basename(dagfile));
		if(strcmp(bundle_directory, "*")) {
			if(create_dir(expanded_path, 0755)) {
				dag_to_file(d, output_makeflow, bundler_rename);
			} else {
				fatal("Could not create directory '%s'.", bundler_rename);
			}
		}
		free(bundle_directory);
		exit(0);
	}

	if(display_mode)
	{
		switch(display_mode)
		{
			case SHOW_INPUT_FILES:
				dag_show_input_files(d);
				break;

			case SHOW_OUTPUT_FILES:
				dag_show_output_files(d);
				break;

			case SHOW_MAKEFLOW_ANALYSIS:
				dag_show_analysis(d);
				break;

			default:
				fatal("Unknown display option.");
				break;
		}
		exit(0);
	}

	return 0;
}

/* vim: set noexpandtab tabstop=4: */
