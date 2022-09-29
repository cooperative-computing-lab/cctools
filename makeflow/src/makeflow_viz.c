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
#include "unlink_recursive.h"
#include "stringtools.h"
#include "load_average.h"
#include "get_line.h"
#include "list.h"
#include "xxmalloc.h"
#include "getopt_aux.h"
#include "random.h"
#include "path.h"
#include "jx.h"
#include "jx_parse.h"
#include "jx_pretty_print.h"

#include "dag.h"
#include "dag_visitors.h"
#include "parser.h"

/* Display options */
enum {
	   SHOW_DAG_DOT,
	   SHOW_DAG_PPM,
	   SHOW_DAG_CYTO,
	   SHOW_DAG_JSON,
	   SHOW_DAG_DAX
};

/* Unique integers for long options. */

enum {	LONG_OPT_PPM_ROW,
		LONG_OPT_PPM_FILE,
		LONG_OPT_PPM_EXE,
		LONG_OPT_PPM_LEVELS,
		LONG_OPT_DOT_PROPORTIONAL,
		LONG_OPT_DOT_CONDENSE,
		LONG_OPT_DOT_LABELS,
		LONG_OPT_DOT_NO_LABELS,
		LONG_OPT_DOT_TASK_ID,
		LONG_OPT_DOT_DETAILS,
		LONG_OPT_DOT_NO_DETAILS,
		LONG_OPT_DOT_GRAPH,
		LONG_OPT_DOT_NODE,
		LONG_OPT_DOT_EDGE,
		LONG_OPT_DOT_TASK,
		LONG_OPT_DOT_FILE,
		LONG_OPT_JSON,
		LONG_OPT_JX,
		LONG_OPT_JX_ARGS,
		LONG_OPT_JX_DEFINE
};

static void show_help_viz(const char *cmd)
{
	fprintf(stdout, "Use: %s [options] <dagfile>\n", cmd);
	fprintf(stdout, " %-30s Show this help screen.\n", "-h,--help");
	fprintf(stdout, " %-30s Translate the makeflow to the desired visualization format:\n", "-D,--display=<format>");
	fprintf(stdout, " %-30s Where <format> is:\n", "");
	fprintf(stdout, " %-35s dot      DOT file format for precise graph drawing.\n", "");
	fprintf(stdout, " %-35s ppm      PPM file format for rapid iconic display\n","");
	fprintf(stdout, " %-35s cyto     Cytoscape format for browsing and customization.\n","");
	fprintf(stdout, " %-35s dax      DAX format for use by the Pegasus workflow manager.\n","");
	fprintf(stdout, " %-35s json     JSON representation of the DAG.\n","");
	fprintf(stdout, "\n");
	fprintf(stdout, " %-30s Condense similar boxes.\n", "--dot-merge-similar");
	fprintf(stdout, " %-30s Change the size of the boxes proportional to file size.\n", "--dot-proportional");
	fprintf(stdout, " %-30s Show only shapes with no text labels.\n","--dot-no-labels");
	fprintf(stdout, " %-30s Include extra details in graph.\n","--dot-details");
	fprintf(stdout, " %-30s Set task label to ID number instead of command.\n","--dot-task-id");
	fprintf(stdout, " %-30s Set graph attributes.\n","--dot-graph-attr");
	fprintf(stdout, " %-30s Set node attributes.\n","--dot-node-attr");
	fprintf(stdout, " %-30s Set edge attributes.\n","--dot-edge-attr");
	fprintf(stdout, " %-30s Set task attributes.\n","--dot-task-attr");
	fprintf(stdout, " %-30s Set file attributes.\n","--dot-file-attr");

	fprintf(stdout, "\nThe following options for ppm generation are mutually exclusive:\n\n");
	fprintf(stdout, " %-30s Highlight row <row> in completion grap\n", "--ppm-highlight-row=<row>");
	fprintf(stdout, " %-30s Highlight node that creates file <file> in completion graph\n", "--ppm-highlight-file=<file>");
	fprintf(stdout, " %-30s Highlight executable <exe> in completion grap\n", "--ppm-highlight-exe=<exe>");
	fprintf(stdout, " %-30s Display different levels of depth in completion graph\n", "--ppm-show-levels");


	fprintf(stdout, "\nThe following options are for JX/JSON formatted DAG files:\n\n");
	fprintf(stdout, " %-30s Use JSON format for the workflow specification.\n", "--json");
	fprintf(stdout, " %-30s Use JX format for the workflow specification.\n", "--jx");
	fprintf(stdout, " %-30s Evaluate the JX input with keys and values in file defined as variables.\n", "--jx-args=<file>");
	fprintf(stdout, " %-30s Set the JX variable VAR to the JX expression EXPR.\n", "jx-define=<VAR>=<EXPR>");
	
}

int main(int argc, char *argv[])
{
	int c;
	random_init();
	debug_config(argv[0]);
	int display_mode = 0;

	cctools_version_debug(D_MAKEFLOW_RUN, argv[0]);
	const char *dagfile;

	int condense_display = 0;
	int change_size = 0;
	int ppm_mode = 0;
	int dot_labels = 1;
	int dot_details = 0;
	int dot_task_id = 0;
	char *graph_attr = NULL;
	char *node_attr = NULL;
	char *edge_attr = NULL;
	char *task_attr = NULL;
	char *file_attr = NULL;
	char *ppm_option = NULL;

	dag_syntax_type dag_syntax = DAG_SYNTAX_MAKE;
	struct jx *jx_args = jx_object(NULL);
	

	static const struct option long_options_viz[] = {
		{"display-mode", required_argument, 0, 'D'},
		{"help", no_argument, 0, 'h'},
		{"dot-merge-similar", no_argument, 0,  LONG_OPT_DOT_CONDENSE},
		{"dot-proportional",  no_argument, 0,  LONG_OPT_DOT_PROPORTIONAL},
		{"dot-no-labels", no_argument, 0, LONG_OPT_DOT_NO_LABELS},
		{"dot-labels", no_argument, 0, LONG_OPT_DOT_LABELS},
		{"dot-task-id", no_argument, 0, LONG_OPT_DOT_TASK_ID},
		{"dot-details", no_argument, 0, LONG_OPT_DOT_DETAILS},
		{"dot-no-details", no_argument, 0, LONG_OPT_DOT_NO_DETAILS},
		{"dot-graph-attr", required_argument, 0, LONG_OPT_DOT_GRAPH},
		{"dot-node-attr", required_argument, 0, LONG_OPT_DOT_NODE},
		{"dot-edge-attr", required_argument, 0, LONG_OPT_DOT_EDGE},
		{"dot-task-attr", required_argument, 0, LONG_OPT_DOT_TASK},
		{"dot-file-attr", required_argument, 0, LONG_OPT_DOT_FILE},
		{"json", no_argument, 0, LONG_OPT_JSON},
		{"jx", no_argument, 0, LONG_OPT_JX},
		{"jx-context", required_argument, 0, LONG_OPT_JX_ARGS},
		{"jx-args", required_argument, 0, LONG_OPT_JX_ARGS},
		{"jx-define", required_argument, 0, LONG_OPT_JX_DEFINE},
		{"ppm-highlight-row", required_argument, 0, LONG_OPT_PPM_ROW},
		{"ppm-highlight-exe", required_argument, 0, LONG_OPT_PPM_EXE},
		{"ppm-highlight-file", required_argument, 0, LONG_OPT_PPM_FILE},
		{"ppm-show-levels", no_argument, 0, LONG_OPT_PPM_LEVELS},
		{"export-as-dax", no_argument, 0, 'e'},
		{"version", no_argument, 0, 'v'},
		{0, 0, 0, 0}
	};
	const char *option_string_viz = "b:D:hv";

	while((c = getopt_long(argc, argv, option_string_viz, long_options_viz, NULL)) >= 0) {
		switch (c) {
			case 'D':
				if(strcasecmp(optarg, "dot") == 0) {
					display_mode = SHOW_DAG_DOT;
				} else if (strcasecmp(optarg, "ppm") == 0) {
					display_mode = SHOW_DAG_PPM;
				} else if (strcasecmp(optarg, "cyto") == 0) {
					display_mode = SHOW_DAG_CYTO;
				} else if (strcasecmp(optarg, "dax") ==0 ) {
					display_mode = SHOW_DAG_DAX;
				} else if(strcasecmp(optarg, "json") == 0) {
					display_mode = SHOW_DAG_JSON;
				} else {
					fatal("Unknown display option: %s\n", optarg);
				}
				break;
			case LONG_OPT_DOT_CONDENSE:
				display_mode = SHOW_DAG_DOT;
				condense_display = 1;
				break;
			case LONG_OPT_DOT_PROPORTIONAL:
				display_mode = SHOW_DAG_DOT;
				change_size = 1;
				break;
			case LONG_OPT_DOT_LABELS:
				dot_labels = 1;
				break;
			case LONG_OPT_DOT_NO_LABELS:
				dot_labels = 0;
				break;
			case LONG_OPT_DOT_TASK_ID:
				dot_task_id = 1;
				break;
			case LONG_OPT_DOT_DETAILS:
				dot_details = 1;
				break;
			case LONG_OPT_DOT_NO_DETAILS:
				dot_details = 0;
				break;
			case LONG_OPT_DOT_GRAPH:
				graph_attr = xxstrdup(optarg);
				break;
			case LONG_OPT_DOT_NODE:
				node_attr = xxstrdup(optarg);
				break;
			case LONG_OPT_DOT_EDGE:
				edge_attr = xxstrdup(optarg);
				break;
			case LONG_OPT_DOT_TASK:
				task_attr = xxstrdup(optarg);
				break;
			case LONG_OPT_DOT_FILE:
				file_attr = xxstrdup(optarg);
				break;
			case LONG_OPT_JSON:
				dag_syntax = DAG_SYNTAX_JSON;
				break;
			case LONG_OPT_JX:
				dag_syntax = DAG_SYNTAX_JX;
				break;
			case LONG_OPT_JX_ARGS:
				dag_syntax = DAG_SYNTAX_JX;
				jx_args = jx_parse_cmd_args(jx_args, optarg);
				if(!jx_args)
					fatal("Failed to parse in JX Args File.\n");
				break;
			case LONG_OPT_JX_DEFINE:
				dag_syntax = DAG_SYNTAX_JX;
				if(!jx_parse_cmd_define(jx_args, optarg))
					fatal("Failed to parse in JX Define.\n");
				break;
			case LONG_OPT_PPM_EXE:
				display_mode = SHOW_DAG_PPM;
				ppm_option = optarg;
				ppm_mode = 2;
				break;
			case LONG_OPT_PPM_FILE:
				display_mode = SHOW_DAG_PPM;
				ppm_option = optarg;
				ppm_mode = 3;
				break;
			case LONG_OPT_PPM_ROW:
				display_mode = SHOW_DAG_PPM;
				ppm_option = optarg;
				ppm_mode = 4;
				break;
			case LONG_OPT_PPM_LEVELS:
				display_mode = SHOW_DAG_PPM;
				ppm_mode = 5;
				break;
			case 'h':
				show_help_viz(argv[0]);
				return 0;
			case 'v':
				cctools_version_print(stdout, argv[0]);
				return 0;
			default:
				show_help_viz(argv[0]);
				return 1;
		}
	}

	if((argc - optind) != 1) {
		int rv = access("./Makeflow", R_OK);
		if(rv < 0) {
			fprintf(stderr, "makeflow_viz: No makeflow specified and file \"./Makeflow\" could not be found.\n");
			fprintf(stderr, "makeflow_viz: Run \"%s -h\" for help with options.\n", argv[0]);
			return 1;
		}

		dagfile = "./Makeflow";
	} else {
		dagfile = argv[optind];
	}

	struct dag *d = dag_from_file(dagfile, dag_syntax, jx_args);
	if(!d) {
		fatal("makeflow_viz: couldn't load %s: %s\n", dagfile, strerror(errno));
	}

		switch(display_mode)
		{
			case SHOW_DAG_DOT:
				dag_to_dot(d, condense_display, change_size, dot_labels, dot_task_id, dot_details,
							graph_attr, node_attr, edge_attr, task_attr, file_attr );
				break;
			case SHOW_DAG_PPM:
				dag_to_ppm(d, ppm_mode, ppm_option);
				break;
			case SHOW_DAG_CYTO:
				dag_to_cyto(d, condense_display, change_size);
				break;
			case SHOW_DAG_DAX:
				dag_to_dax(d, path_basename(dagfile) );
				break;
			case SHOW_DAG_JSON:
				jx_pretty_print_stream(dag_to_json(d), stdout);
				break;
			default:
				fatal("Unknown display option.");
				break;
		}

	return 0;
}

/* vim: set noexpandtab tabstop=4: */
