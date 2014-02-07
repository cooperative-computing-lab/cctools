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
#include "random_init.h"
#include "path.h"

#include "dag.h"
#include "visitors.h"

#include "makeflow_common.h"

/* Display options */
enum { SHOW_INPUT_FILES = 2,
       SHOW_OUTPUT_FILES,
       SHOW_MAKEFLOW_ANALYSIS,
       SHOW_DAG_DOT,
       SHOW_DAG_PPM,
       SHOW_DAG_FILE
};

/* Unique integers for long options. */

enum { LONG_OPT_PPM_ROW,
       LONG_OPT_PPM_FILE,
       LONG_OPT_PPM_EXE,
       LONG_OPT_PPM_LEVELS,
       LONG_OPT_DOT_PROPORTIONAL,
       LONG_OPT_VERBOSE_PARSING,
       LONG_OPT_DOT_CONDENSE };

int verbose_parsing = 0;

static void show_help_viz(const char *cmd)
{
	fprintf(stdout, "Use: %s [options] <dagfile>\n", cmd);
	fprintf(stdout, " %-30s Show this help screen.\n", "-h,--help");
	fprintf(stdout, " %-30s Display the Makefile as a Dot graph, a PPM completion graph, or print the Workflow to stdout.\n", "-D,--display=<opt>");
	fprintf(stdout, " %-30s Where <opt> is:\n", "");
	fprintf(stdout, " %-35s dot      Standard Dot graph\n", "");
	fprintf(stdout, " %-35s ppm      Display a completion graph in PPM format\n", "");
	fprintf(stdout, " %-35s file     Display the file as interpreted by makeflow\n", "");

	fprintf(stdout, " %-30s Condense similar boxes.\n", "--dot-merge-similar");
	fprintf(stdout, " %-30s Change the size of the boxes proportional to file size.\n", "--dot-proportional");
	fprintf(stdout, "\nThe following options for ppm generation are mutually exclusive:\n\n");
	fprintf(stdout, " %-30s Highlight row <row> in completion grap\n", "--ppm-highlight-row=<row>");
	fprintf(stdout, " %-30s Highlight node that creates file <file> in completion graph\n", "--ppm-highlight-file=<file>");
	fprintf(stdout, " %-30s Highlight executable <exe> in completion grap\n", "--ppm-highlight-exe=<exe>");
	fprintf(stdout, " %-30s Display different levels of depth in completion graph\n", "--ppm-show-levels");
}

int main(int argc, char *argv[])
{
	typedef enum {MAKEFLOW_RUN, MAKEFLOW_VIZ, MAKEFLOW_ANALYZE} runtime_mode;

	int c;
	random_init();
	set_makeflow_exe(argv[0]);
	debug_config(get_makeflow_exe());
	int display_mode = 0;

	cctools_version_debug(D_DEBUG, get_makeflow_exe());
	const char *dagfile;

	int condense_display = 0;
	int change_size = 0;
	int export_as_dax = 0;
	int ppm_mode = 0;
	char *ppm_option = NULL;

	struct option long_options_viz[] = {
		{"display-mode", required_argument, 0, 'D'},
		{"help", no_argument, 0, 'h'},
		{"dot-merge-similar", no_argument, 0,  LONG_OPT_DOT_CONDENSE},
		{"dot-proportional",  no_argument, 0,  LONG_OPT_DOT_PROPORTIONAL},
		{"ppm-highlight-row", required_argument, 0, LONG_OPT_PPM_ROW},
		{"ppm-highlight-exe", required_argument, 0, LONG_OPT_PPM_EXE},
		{"ppm-highlight-file", required_argument, 0, LONG_OPT_PPM_FILE},
		{"ppm-show-levels", no_argument, 0, LONG_OPT_PPM_LEVELS},
		{"export-as-dax", no_argument, 0, 'e'},
		{"version", no_argument, 0, 'v'},
		{0, 0, 0, 0}
	};
	const char *option_string_viz = "b:D:ehv";

	while((c = getopt_long(argc, argv, option_string_viz, long_options_viz, NULL)) >= 0) {
		switch (c) {
			case 'D':
				if(strcasecmp(optarg, "dot") == 0)
					display_mode = SHOW_DAG_DOT;
				else if (strcasecmp(optarg, "ppm") == 0)
					display_mode = SHOW_DAG_PPM;
				else
					fatal("Unknown display option: %s\n", optarg);
				break;
			case LONG_OPT_DOT_CONDENSE:
				display_mode = SHOW_DAG_DOT;
				condense_display = 1;
				break;
			case LONG_OPT_DOT_PROPORTIONAL:
				display_mode = SHOW_DAG_DOT;
				change_size = 1;
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
			case 'e':
				export_as_dax = 1;
				break;
			case 'h':
				show_help_viz(get_makeflow_exe());
				return 0;
			case 'v':
				cctools_version_print(stdout, get_makeflow_exe());
				return 0;
			default:
				show_help_viz(get_makeflow_exe());
				return 1;
		}
	}

	if((argc - optind) != 1) {
		int rv = access("./Makeflow", R_OK);
		if(rv < 0) {
			fprintf(stderr, "makeflow: No makeflow specified and file \"./Makeflow\" could not be found.\n");
			fprintf(stderr, "makeflow: Run \"%s -h\" for help with options.\n", get_makeflow_exe());
			return 1;
		}

		dagfile = "./Makeflow";
	} else {
		dagfile = argv[optind];
	}

	struct dag *d = dag_from_file(dagfile);
	if(!d) {
		fatal("makeflow: couldn't load %s: %s\n", dagfile, strerror(errno));
	}

	if(export_as_dax) {
		dag_to_dax(d, path_basename(dagfile));
		return 0;
	}

	if(display_mode)
	{
		switch(display_mode)
		{
			case SHOW_DAG_DOT:
				dag_to_dot(d, condense_display, change_size);
				break;

			case SHOW_DAG_PPM:
				dag_to_ppm(d, ppm_mode, ppm_option);
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
