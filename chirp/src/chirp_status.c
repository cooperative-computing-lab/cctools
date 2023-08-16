/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "catalog_query.h"
#include "jx_table.h"
#include "jx_print.h"
#include "jx_parse.h"
#include "cctools.h"
#include "debug.h"
#include "getopt_aux.h"
#include "link.h"
#include "stringtools.h"
#include "xxmalloc.h"

#include <errno.h>
#include <stdio.h>
#include <stdbool.h>
#include <stdlib.h>
#include <string.h>

enum {
	MODE_TABLE,
	MODE_SHORT,
	MODE_LONG,
	MODE_TOTAL,
};

static struct jx_table headers[] = {
	{"type", "TYPE", JX_TABLE_MODE_PLAIN, JX_TABLE_ALIGN_LEFT, 8},
	{"name", "NAME", JX_TABLE_MODE_PLAIN, JX_TABLE_ALIGN_LEFT, -25},
	{"port", "PORT", JX_TABLE_MODE_PLAIN, JX_TABLE_ALIGN_LEFT, 5},
	{"owner", "OWNER", JX_TABLE_MODE_PLAIN, JX_TABLE_ALIGN_LEFT, 10},
	{"version", "VERSION", JX_TABLE_MODE_PLAIN, JX_TABLE_ALIGN_LEFT, 8},
	{"total", "TOTAL", JX_TABLE_MODE_METRIC, JX_TABLE_ALIGN_RIGHT, 8},
	{"avail", "AVAIL", JX_TABLE_MODE_METRIC, JX_TABLE_ALIGN_RIGHT, 8},
	{0, 0, 0, 0, 0}
};

static void show_help(const char *cmd)
{
	fprintf(stdout, "chirp_status [options] [ <name> <value> ]\n");
	fprintf(stdout, "where options are:\n");
	fprintf(stdout, " %-30s Query the catalog on this host.\n", "-c,--catalog=<host>");
	fprintf(stdout, " %-30s Enable debugging for this sybsystem\n", "-d,--debug=<flag>");
	fprintf(stdout, " %-30s Send debugging to this file. (can also be :stderr, or :stdout)\n", "-o,--debug-file=<file>");
	fprintf(stdout, " %-30s Rotate file once it reaches this size. (default 10M, 0 disables)\n", "-O,--debug-rotate-max=<bytes>");
	fprintf(stdout, " %-30s Only show servers with this space available. (example: -A 100MB)\n", "-A,--server-space=<size>");
	fprintf(stdout, " %-30s Only show servers with this project.\n", "   --server-project=<name>");
	fprintf(stdout, " %-30s Show all records, not just chirps and catalogs.\n", "-a,--all");
	fprintf(stdout, " %-30s Timeout.\n", "-t,--timeout=<time>");
	fprintf(stdout, " %-30s Short output.\n", "-s,--brief");
	fprintf(stdout, " %-30s Long output.\n", "-l,--verbose");
	fprintf(stdout, " %-30s Totals output.\n", "-T,--totals");
	fprintf(stdout, " %-30s Show version info.\n", "-v,--version");
	fprintf(stdout, " %-30s Filter results by this expression.\n", "   --where=<expr>");
	fprintf(stdout, " %-30s This message.\n", "-h,--help");
}

int compare_entries(struct jx **a, struct jx **b)
{
	int result;
	const char *x, *y;

	x = jx_lookup_string(*a, "type");
	if(!x)
		x = "unknown";

	y = jx_lookup_string(*b, "type");
	if(!y)
		y = "unknown";

	result = strcasecmp(x, y);
	if(result != 0)
		return result;

	x = jx_lookup_string(*a, "name");
	if(!x)
		x = "unknown";

	y = jx_lookup_string(*b, "name");
	if(!y)
		y = "unknown";

	return strcasecmp(x, y);
}

static struct jx *table[10000];

int main(int argc, char *argv[])
{
	enum {
		LONGOPT_SERVER_LASTHEARDFROM = INT_MAX-0,
		LONGOPT_SERVER_PROJECT       = INT_MAX-1,
		LONGOPT_WHERE = INT_MAX-2,
	};

	static const struct option long_options[] = {
		{"all", no_argument, 0, 'a'},
		{"brief", no_argument, 0, 's'},
		{"catalog", required_argument, 0, 'c'},
		{"debug", required_argument, 0, 'd'},
		{"debug-file", required_argument, 0, 'o'},
		{"debug-rotate-max", required_argument, 0, 'O'},
		{"help", no_argument, 0, 'h'},
		{"server-lastheardfrom", required_argument, 0, LONGOPT_SERVER_LASTHEARDFROM},
		{"server-project", required_argument, 0, LONGOPT_SERVER_PROJECT},
		{"server-space", required_argument, 0, 'A'},
		{"timeout", required_argument, 0, 't'},
		{"totals", no_argument, 0, 'T'},
		{"verbose", no_argument, 0, 'l'},
		{"version", no_argument, 0, 'v'},
		{"where", required_argument, 0, LONGOPT_WHERE },
		{0, 0, 0, 0}
	};

	struct catalog_query *q;
	struct jx *j;
	time_t timeout = 60, stoptime;
	const char *catalog_host = 0;
	int i;
	int c;
	int count = 0;
	int mode = MODE_TABLE;
	INT64_T sum_total = 0, sum_avail = 0;
	const char *filter_name = 0;
	const char *filter_value = 0;
	int show_all_types = 0;

	const char *server_project = NULL;
	time_t      server_lastheardfrom = 0;
	uint64_t    server_avail = 0;
	struct jx *jexpr = jx_boolean(true);

	debug_config(argv[0]);

	while((c = getopt_long(argc, argv, "aA:c:d:t:o:O:sTlvh", long_options, NULL)) > -1) {
		switch (c) {
		case 'a':
			show_all_types = 1;
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
		case 'A':
			server_avail = string_metric_parse(optarg);
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
		case 's':
			mode = MODE_SHORT;
			break;
		case 'l':
			mode = MODE_LONG;
			break;
		case 'T':
			mode = MODE_TOTAL;
			break;
		case LONGOPT_SERVER_LASTHEARDFROM:
			server_lastheardfrom = time(0)-string_time_parse(optarg);
			break;
		case LONGOPT_SERVER_PROJECT:
			server_project = xxstrdup(optarg);
			break;
		case LONGOPT_WHERE:
			jexpr = jx_parse_string(optarg);
			if (!jexpr) {
				fprintf(stderr,"invalid expression: %s\n", optarg);
				return 1;
			}
			break;
		case 'h':
		default:
			show_help(argv[0]);
			return 1;
		}
	}

	cctools_version_debug(D_DEBUG, argv[0]);

	if(argc - optind == 0) {
		// fine, keep going
	} else if((argc - optind) == 1) {
		filter_name = "name";
		filter_value = argv[optind];
	} else if((argc - optind) == 2) {
		filter_name = argv[optind];
		filter_value = argv[optind + 1];
	} else {
		show_help(argv[0]);
		return 1;
	}

	int columns = 80;
	char *column_str = getenv("COLUMNS");
	if(column_str) {
		columns = atoi(column_str);
		columns = columns < 1 ? 80 : columns;
	}

	stoptime = time(0) + timeout;

	if (!show_all_types) {
		jexpr = jx_operator(
			JX_OP_AND,
			jexpr,
			jx_operator(
				JX_OP_OR,
				jx_operator(
					JX_OP_EQ,
					jx_symbol("type"),
					jx_string("chirp")
				),
				jx_operator(
					JX_OP_EQ,
					jx_symbol("type"),
					jx_string("catalog")
				)
			)
		);
	}

	q = catalog_query_create(catalog_host, jexpr, stoptime);
	if(!q) {
		fprintf(stderr, "couldn't query catalog: %s\n", strerror(errno));
		return 1;
	}

	if(mode == MODE_TABLE) {
		jx_table_print_header(headers,stdout,columns);
	} else if(mode==MODE_LONG) {
		printf("[\n");
	}

	while((j = catalog_query_read(q, stoptime))) {
		table[count++] = j;
	}

	catalog_query_delete(q);

	qsort(table, count, sizeof(*table), (int (*)(const void *, const void *)) compare_entries);

	for(i = 0; i < count; i++) {
		const char *lastheardfrom = jx_lookup_string(table[i], "lastheardfrom");
		if (lastheardfrom && (time_t)strtoul(lastheardfrom, NULL, 10) < server_lastheardfrom)
			continue;

		const char *avail = jx_lookup_string(table[i], "avail");
		if (avail && strtoul(avail, NULL, 10) < server_avail)
			continue;

		const char *project = jx_lookup_string(table[i], "project");
		if (server_project && (project == NULL || !(strcmp(project, server_project) == 0)))
			continue;

		if(filter_name) {
			const char *v = jx_lookup_string(table[i], filter_name);
			if(!v || strcmp(filter_value, v))
				continue;
		}

		if(mode == MODE_SHORT) {
			const char *t = jx_lookup_string(table[i], "type");
			if(t && !strcmp(t, "chirp")) {
				printf("%s:%d\n", jx_lookup_string(table[i], "name"), (int) jx_lookup_integer(table[i], "port"));
			}
		} else if(mode == MODE_LONG) {
			if(i!=0) printf(",\n");
			jx_print_stream(table[i],stdout);
		} else if(mode == MODE_TABLE) {
			jx_table_print(headers, table[i], stdout, columns);
		} else if(mode == MODE_TOTAL) {
			sum_avail += jx_lookup_integer(table[i], "avail");
			sum_total += jx_lookup_integer(table[i], "total");
		}
	}

	for(i=0;i<count;i++) {
		jx_delete(table[i]);
	}

	if(mode == MODE_TOTAL) {
		printf("NODES: %4d\n", count);
		printf("TOTAL: %6sB\n", string_metric(sum_total, -1, 0));
		printf("AVAIL: %6sB\n", string_metric(sum_avail, -1, 0));
		printf("INUSE: %6sB\n", string_metric(sum_total - sum_avail, -1, 0));
	} else if(mode == MODE_TABLE) {
		jx_table_print_footer(headers,stdout,columns);
	} else if(mode==MODE_LONG) {
		printf("\n]\n");
	}

	return 0;
}

/* vim: set noexpandtab tabstop=8: */
