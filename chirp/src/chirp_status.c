/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2005- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "catalog_query.h"
#include "cctools.h"
#include "nvpair.h"
#include "link.h"
#include "stringtools.h"
#include "debug.h"
#include "getopt_aux.h"

#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <string.h>

#define MODE_TABLE 1
#define MODE_SHORT 2
#define MODE_LONG  3
#define MODE_TOTAL 4

static int show_all_types = 0;
static INT64_T minavail = 0;

static struct nvpair_header headers[] = {
	{"type", "TYPE", NVPAIR_MODE_STRING, NVPAIR_ALIGN_LEFT, 8},
	{"name", "NAME", NVPAIR_MODE_STRING, NVPAIR_ALIGN_LEFT, 25},
	{"port", "PORT", NVPAIR_MODE_INTEGER, NVPAIR_ALIGN_LEFT, 5},
	{"owner", "OWNER", NVPAIR_MODE_STRING, NVPAIR_ALIGN_LEFT, 10},
	{"version", "VERSION", NVPAIR_MODE_STRING, NVPAIR_ALIGN_LEFT, 8},
	{"total", "TOTAL", NVPAIR_MODE_METRIC, NVPAIR_ALIGN_RIGHT, 8},
	{"avail", "AVAIL", NVPAIR_MODE_METRIC, NVPAIR_ALIGN_RIGHT, 8},
	{0, 0, 0, 0, 0}
};

static void show_help(const char *cmd)
{
	fprintf(stdout, "chirp_status [options] [ <name> <value> ]\n");
	fprintf(stdout, "where options are:\n");
	fprintf(stdout, " %-30s Query the catalog on this host.\n", "-c,--catalog=<host>");
	fprintf(stdout, " %-30s Enable debugging for this sybsystem\n", "-d,--debug=<flag>");
	fprintf(stdout, " %-30s Send debugging output to this file. (can also be :stderr, :syslog, or :journal)\n", "-o,--debug-file=<file>");
	fprintf(stdout, " %-30s Rotate file once it reaches this size. (default 10M, 0 disables)\n", "-O,--debug-rotate-max=<bytes>");
	fprintf(stdout, " %-30s Only show servers with this space available. (example: -A 100MB)\n", "-A,--server-space=<size>");
	fprintf(stdout, " %-30s Show all records, not just chirps and catalogs.\n", "-a,--all");
	fprintf(stdout, " %-30s Timeout.\n", "-t,--timeout=<time>");
	fprintf(stdout, " %-30s Short output.\n", "-s,--brief");
	fprintf(stdout, " %-30s Long output.\n", "-l,--verbose");
	fprintf(stdout, " %-30s Totals output.\n", "-T,--totals");
	fprintf(stdout, " %-30s Show version info.\n", "-v,--version");
	fprintf(stdout, " %-30s This message.\n", "-h,--help");
}

int compare_entries(struct nvpair **a, struct nvpair **b)
{
	int result;
	const char *x, *y;

	x = nvpair_lookup_string(*a, "type");
	if(!x)
		x = "unknown";

	y = nvpair_lookup_string(*b, "type");
	if(!y)
		y = "unknown";

	result = strcasecmp(x, y);
	if(result != 0)
		return result;

	x = nvpair_lookup_string(*a, "name");
	if(!x)
		x = "unknown";

	y = nvpair_lookup_string(*b, "name");
	if(!y)
		y = "unknown";

	return strcasecmp(x, y);
}

static struct nvpair *table[10000];

int main(int argc, char *argv[])
{
	struct catalog_query *q;
	struct nvpair *n;
	time_t timeout = 60, stoptime;
	const char *catalog_host = 0;
	signed char c;
	int i;
	int count = 0;
	int mode = MODE_TABLE;
	INT64_T total = 0, avail = 0;
	const char *filter_name = 0;
	const char *filter_value = 0;

	debug_config(argv[0]);


	static struct option long_options[] = {
		{"catalog", required_argument, 0, 'c'},
		{"debug", required_argument, 0, 'd'},
		{"debug-file", required_argument, 0, 'o'},
		{"debug-rotate-max", required_argument, 0, 'O'},
		{"server-space", required_argument, 0, 'A'},
		{"all", no_argument, 0, 'a'},
		{"timeout", required_argument, 0, 't'},
		{"brief", no_argument, 0, 's'},
		{"verbose", no_argument, 0, 'l'},
		{"totals", no_argument, 0, 'T'},
		{"version", no_argument, 0, 'v'},
		{"help", no_argument, 0, 'h'},
		{0, 0, 0, 0}
	};

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
			minavail = string_metric_parse(optarg);
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

	stoptime = time(0) + timeout;

	q = catalog_query_create(catalog_host, 0, stoptime);
	if(!q) {
		fprintf(stderr, "couldn't query catalog: %s\n", strerror(errno));
		return 1;
	}

	if(mode == MODE_TABLE) {
		nvpair_print_table_header(stdout, headers);
	}

	while((n = catalog_query_read(q, stoptime))) {
		table[count++] = n;
	}

	qsort(table, count, sizeof(*table), (int (*)(const void *, const void *)) compare_entries);

	for(i = 0; i < count; i++) {
		const char *etype = nvpair_lookup_string(table[i], "type");
		if(!show_all_types) {
			if(etype) {
				if(!strcmp(etype, "chirp") || !strcmp(etype, "catalog")) {
					/* ok, keep going */
				} else {
					continue;
				}
			} else {
				continue;
			}
		}

		if(minavail != 0) {
			if(minavail > nvpair_lookup_integer(table[i], "avail")) {
				continue;
			}
		}

		if(filter_name) {
			const char *v = nvpair_lookup_string(table[i], filter_name);
			if(!v || strcmp(filter_value, v))
				continue;
		}

		if(mode == MODE_SHORT) {
			const char *t = nvpair_lookup_string(table[i], "type");
			if(t && !strcmp(t, "chirp")) {
				printf("%s:%d\n", nvpair_lookup_string(table[i], "name"), (int) nvpair_lookup_integer(table[i], "port"));
			}
		} else if(mode == MODE_LONG) {
			nvpair_print_text(table[i], stdout);
		} else if(mode == MODE_TABLE) {
			nvpair_print_table(table[i], stdout, headers);
		} else if(mode == MODE_TOTAL) {
			avail += nvpair_lookup_integer(table[i], "avail");
			total += nvpair_lookup_integer(table[i], "total");
		}
	}

	if(mode == MODE_TOTAL) {
		printf("NODES: %4d\n", count);
		printf("TOTAL: %6sB\n", string_metric(total, -1, 0));
		printf("AVAIL: %6sB\n", string_metric(avail, -1, 0));
		printf("INUSE: %6sB\n", string_metric(total - avail, -1, 0));
	}

	if(mode == MODE_TABLE) {
		nvpair_print_table_footer(stdout, headers);
	}

	return 0;
}

/* vim: set noexpandtab tabstop=4: */
