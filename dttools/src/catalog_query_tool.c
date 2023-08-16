/*
Copyright (C) 2016 Douglas Thain and the University of Wisconsin
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "catalog_query.h"
#include "jx_pretty_print.h"
#include "jx_print.h"
#include "jx_parse.h"
#include "jx_eval.h"
#include "cctools.h"
#include "debug.h"
#include "getopt_aux.h"
#include "stringtools.h"
#include "xxmalloc.h"
#include "list.h"

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static const struct option long_options[] = {
	{"where", required_argument, 0, 'w' },
	{"output", required_argument, 0, 'p'},
	{"catalog", required_argument, 0, 'c'},
	{"debug", required_argument, 0, 'd'},
	{"debug-file", required_argument, 0, 'o'},
	{"debug-rotate-max", required_argument, 0, 'O'},
	{"help", no_argument, 0, 'h'},
	{"timeout", required_argument, 0, 't'},
	{"verbose", no_argument, 0, 'l'},
	{"version", no_argument, 0, 'v'},
	{0, 0, 0, 0}
};

static void show_help(const char *cmd)
{
	fprintf(stdout, "catalog_query [options]\n");
	fprintf(stdout, "where options are:\n");
	fprintf(stdout, " %-30s Filter results by this expression.\n", "-w,--where=<expr>");
	fprintf(stdout, " %-30s Output this expression for each record.\n","-p,--output=<expr>");
	fprintf(stdout, " %-30s Query the catalog on this host.\n", "-c,--catalog=<host>");
	fprintf(stdout, " %-30s Enable debugging for this sybsystem\n", "-d,--debug=<flag>");
	fprintf(stdout, " %-30s Send debugging to this file. (can also be :stderr, or :stdout)\n", "-o,--debug-file=<file>");
	fprintf(stdout, " %-30s Rotate file once it reaches this size. (default 10M, 0 disables)\n", "-O,--debug-rotate-max=<bytes>");
	fprintf(stdout, " %-30s Timeout.\n", "-t,--timeout=<time>");
	fprintf(stdout, " %-30s This message.\n", "-h,--help");
}

typedef enum {
	DISPLAY_MODE_JX,
	DISPLAY_MODE_TABLE,
} display_mode_t;

int main(int argc, char *argv[])
{
	struct catalog_query *q;
	const char *catalog_host = 0;
	const char *where_expr = 0;
	time_t timeout = 60, stoptime;
	display_mode_t display_mode = DISPLAY_MODE_JX;
	struct jx *jexpr;


	struct list *output_exprs = list_create();

	int c;

	debug_config(argv[0]);

	while((c = getopt_long(argc, argv, "w:p:c:d:t:o:O:vh", long_options, NULL)) > -1) {
		switch (c) {
		case 'w':
			where_expr = optarg;
			break;
		case 'p':
			display_mode = DISPLAY_MODE_TABLE;
			jexpr = jx_parse_string(optarg);
			if(jexpr) {
				list_push_tail(output_exprs,jexpr);
			} else {
				fprintf(stderr,"couldn't parse output expression: %s\n",optarg);
				return 1;
			}
			break;
		case 'c':
			catalog_host = optarg;
			break;
		case 'd':
			debug_flags_set(optarg);
			break;
		case 't':
			timeout = string_time_parse(optarg);
			break;
		case 'o':
			debug_config_file(optarg);
			break;
		case 'O':
			debug_config_file_size(string_metric_parse(optarg));
			break;
		case 'v':
			cctools_version_print(stdout, argv[0]);
			return 1;
		case 'h':
		default:
			show_help(argv[0]);
			return 1;
		}
	}

	cctools_version_debug(D_DEBUG, argv[0]);

	if(argc!=optind) {
		show_help(argv[0]);
		return 1;
	}

	stoptime = time(0) + timeout;

	if(where_expr) {
		jexpr = jx_parse_string(where_expr);
		if(!jexpr) {
			fprintf(stderr,"invalid expression: %s\n",where_expr);
			return 1;
		}
	} else {
		jexpr = 0;
	}

	q = catalog_query_create(catalog_host, jexpr, stoptime);
	if(!q) {
		fprintf(stderr, "couldn't query catalog: %s\n", strerror(errno));
		return 1;
	}

	int first = 1;

	if(display_mode==DISPLAY_MODE_JX) {
		printf("[\n");
	} else if(display_mode==DISPLAY_MODE_TABLE) {
		/* Display a header with each output expression */
		struct jx *j;
		list_first_item(output_exprs);
		while( (j=list_next_item(output_exprs)) ) {
			jx_print_stream(j,stdout);
			printf("\t");
		}
		printf("\n");
	}

	struct jx *jobject;
	while((jobject = catalog_query_read(q, stoptime))) {
		if(jexpr) {
			struct jx *result = jx_eval(jexpr,jobject);
			if(jx_istrue(result)) {
				jx_delete(result);
				// keepgoing
			} else {
				jx_delete(result);
				continue;
			}
		}

		if(display_mode==DISPLAY_MODE_JX) {

			/* Display the raw JX data for this record. */

			if(first) {
				first = 0;
			} else {
				printf(",\n");
			}
			jx_pretty_print_stream(jobject,stdout);

		} else if(display_mode==DISPLAY_MODE_TABLE) {

			/* Evaluate each expression and display it. */

			struct jx *j;
			list_first_item(output_exprs);
			while( (j=list_next_item(output_exprs)) ) {
				struct jx *jvalue = jx_eval(j,jobject);
				jx_print_stream(jvalue,stdout);
				printf("\t");
				jx_delete(jvalue);
			}

			printf("\n");
		}

		jx_delete(jobject);
	}

	if(display_mode==DISPLAY_MODE_JX) {
		printf("\n]\n");
	}

	return 0;
}

/* vim: set noexpandtab tabstop=8: */
