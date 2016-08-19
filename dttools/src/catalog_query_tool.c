/*
Copyright (C) 2016 Douglas Thain and the University of Wisconsin
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "catalog_query.h"
#include "jx_pretty_print.h"
#include "jx_parse.h"
#include "jx_eval.h"
#include "cctools.h"
#include "debug.h"
#include "getopt_aux.h"
#include "stringtools.h"
#include "xxmalloc.h"

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static const struct option long_options[] = {
	{"where", required_argument, 0, 'w' },
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
	fprintf(stdout, " %-30s Query the catalog on this host.\n", "-c,--catalog=<host>");
	fprintf(stdout, " %-30s Enable debugging for this sybsystem\n", "-d,--debug=<flag>");
	fprintf(stdout, " %-30s Send debugging to this file. (can also be :stderr, :stdout, :syslog, or :journal)\n", "-o,--debug-file=<file>");
	fprintf(stdout, " %-30s Rotate file once it reaches this size. (default 10M, 0 disables)\n", "-O,--debug-rotate-max=<bytes>");
	fprintf(stdout, " %-30s Timeout.\n", "-t,--timeout=<time>");
	fprintf(stdout, " %-30s This message.\n", "-h,--help");
}

int main(int argc, char *argv[])
{
	struct catalog_query *q;
	struct jx *j;
	const char *catalog_host = 0;
	const char *where_expr = 0;
	time_t timeout = 60, stoptime;
	int c;

	debug_config(argv[0]);

	while((c = getopt_long(argc, argv, "w:c:d:t:o:O:vh", long_options, NULL)) > -1) {
		switch (c) {
		case 'w':
			where_expr = optarg;
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

	struct jx *jexpr;

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

	printf("[\n");

	while((j = catalog_query_read(q, stoptime))) {
		if(jexpr) {
			struct jx *result = jx_eval(jexpr,j);
			if(jx_istrue(result)) {
				jx_delete(result);
				// keepgoing
			} else {
				jx_delete(result);
				continue;
			}
		}
		if(first) {
			first = 0;
		} else {
			printf(",\n");
		}
		jx_pretty_print_stream(j,stdout);
		jx_delete(j);
	}

	printf("\n]\n");

	return 0;
}

/* vim: set noexpandtab tabstop=4: */
