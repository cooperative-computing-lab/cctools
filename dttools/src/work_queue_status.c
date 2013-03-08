/* 
Copyright (C) 2010- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "debug.h"
#include "catalog_query.h"
#include "catalog_server.h"
#include "work_queue.h"
#include "nvpair.h"
#include "link_nvpair.h"
#include "link.h"

#include <errno.h>
#include <string.h>
#include <stdlib.h>

typedef enum {
	FORMAT_TABLE,
	FORMAT_LONG
} format_t;

typedef enum {
	QUERY_QUEUE,
	QUERY_TASKS,
	QUERY_WORKERS,
} query_t;

static format_t format_mode = FORMAT_TABLE;
static query_t query_mode = QUERY_QUEUE;
static int work_queue_status_timeout = 300;
static char *catalog_host = NULL;
static int catalog_port = 0;

static struct nvpair_header queue_headers[] = {
	{"project",       "PROJECT", NVPAIR_MODE_STRING, NVPAIR_ALIGN_LEFT, 18},
	{"name",          "HOST",    NVPAIR_MODE_STRING, NVPAIR_ALIGN_LEFT, 22},
	{"port",          "PORT",    NVPAIR_MODE_INTEGER, NVPAIR_ALIGN_RIGHT, 5},
	{"tasks_waiting", "WAITING",    NVPAIR_MODE_INTEGER, NVPAIR_ALIGN_RIGHT, 8},
	{"workers_busy",  "BUSY",    NVPAIR_MODE_INTEGER, NVPAIR_ALIGN_RIGHT, 5},
	{"tasks_complete","COMPLETE",    NVPAIR_MODE_INTEGER, NVPAIR_ALIGN_RIGHT, 8},
	{"workers",       "WORKERS", NVPAIR_MODE_INTEGER, NVPAIR_ALIGN_RIGHT, 7},
	{NULL,}
};

static struct nvpair_header task_headers[] = {
	{"taskid",       "ID",      NVPAIR_MODE_INTEGER, NVPAIR_ALIGN_LEFT, 8},
	{"state",        "STATE",   NVPAIR_MODE_STRING, NVPAIR_ALIGN_LEFT,  8},
	{"host",         "HOST",    NVPAIR_MODE_STRING, NVPAIR_ALIGN_LEFT, 24},
	{"command",      "COMMAND", NVPAIR_MODE_STRING, NVPAIR_ALIGN_LEFT, 38},
	{NULL,}
};

static struct nvpair_header worker_headers[] = {
	{"hostname",             "HOST",    NVPAIR_MODE_STRING, NVPAIR_ALIGN_LEFT, 24},
	{"addrport",             "ADDRESS", NVPAIR_MODE_STRING, NVPAIR_ALIGN_LEFT,16},
	{"total_tasks_complete",       "TASKS",   NVPAIR_MODE_INTEGER, NVPAIR_ALIGN_RIGHT, 8 },
	{"state",                "STATE",   NVPAIR_MODE_STRING, NVPAIR_ALIGN_LEFT,8},
	{"current_task_command", "TASK",    NVPAIR_MODE_STRING, NVPAIR_ALIGN_LEFT, 28},
	{NULL,}
};

static void work_queue_status_show_help(const char *progname)
{
	printf("usage: %s [master] [port]\n", progname);
	printf("If a master and port are given, get data directly from that master.\n");
	printf("Otherwise, contact the catalog server for summary data.\n");
	printf("Options:\n");
	printf(" -Q             Show queue summary statistics. (default)\n");
	printf(" -W             List workers connected to the master.\n");
	printf(" -T             List tasks of a given master.\n");
	printf(" -l             Long text output.\n");
	printf(" -C <catalog>   Set catalog server to <catalog>. Format: HOSTNAME:PORT\n");
	printf(" -d <flag>      Enable debugging for this subsystem.\n");
	printf(" -t <time>      RPC timeout (default is %ds).\n", work_queue_status_timeout);
	printf(" -h             This message.\n");
}

int parse_catalog_server_description(char *server_string, char **host, int *port)
{
	char *colon;

	colon = strchr(server_string, ':');

	if(!colon) {
		*host = NULL;
		*port = 0;
		return 0;
	}

	*colon = '\0';

	*host = strdup(server_string);
	*port = atoi(colon + 1);

	return *port;
}

static void work_queue_status_parse_command_line_arguments(int argc, char *argv[])
{
	int c;

	while((c = getopt(argc, argv, "QTWC:d:lo:O:t:vh")) != (char) -1) {
		switch (c) {
		case 'C':
			if(!parse_catalog_server_description(optarg, &catalog_host, &catalog_port)) {
				fprintf(stderr, "Cannot parse catalog description: %s. \n", optarg);
				exit(EXIT_FAILURE);
			}
			break;
		case 'd':
			debug_flags_set(optarg);
			break;
		case 'Q':
			query_mode = QUERY_QUEUE;
			break;
		case 'T':
			query_mode = QUERY_TASKS;
			break;
		case 'W':
			query_mode = QUERY_WORKERS;
		  	break;
		case 'l':
			format_mode = FORMAT_LONG;
			break;
		case 'o':
			debug_config_file(optarg);
			break;
		case 'O':
			debug_config_file_size(string_metric_parse(optarg));
			break;
		case 't':
			work_queue_status_timeout = strtol(optarg, NULL, 10);
			break;
		case 'h':
			work_queue_status_show_help(argv[0]);
			exit(EXIT_SUCCESS);
			break;
		case 'v':
			cctools_version_print(stdout, argv[0]);
			exit(EXIT_SUCCESS);
		default:
			work_queue_status_show_help(argv[0]);
			exit(EXIT_FAILURE);
			break;
		}
	}
}

int do_catalog_query( time_t stoptime )
{
	struct catalog_query *cq;
	struct nvpair *nv;

	if(!catalog_host) {
		catalog_host = strdup(CATALOG_HOST);
		catalog_port = CATALOG_PORT;
	}

	cq = catalog_query_create(catalog_host, catalog_port, stoptime );
	if(!cq) {
		fprintf(stderr, "failed to query catalog server %s:%d: %s \n",catalog_host,catalog_port,strerror(errno));
		exit(EXIT_FAILURE);
	}

	if(format_mode == FORMAT_TABLE)
		nvpair_print_table_header(stdout, queue_headers);

	while((nv = catalog_query_read(cq,stoptime))) {
		if(strcmp(nvpair_lookup_string(nv, "type"), CATALOG_TYPE_WORK_QUEUE_MASTER) == 0) {
			if(format_mode == FORMAT_TABLE)
				nvpair_print_table(nv, stdout, queue_headers);
			else
				nvpair_print_text(nv, stdout);
		}
		nvpair_delete(nv);
	}

	if(format_mode == FORMAT_TABLE)
		nvpair_print_table_footer(stdout, queue_headers);

	return EXIT_SUCCESS;
}

int do_direct_query( const char *master_host, int master_port, time_t stoptime )
{
	static struct nvpair_header *query_headers[3] = { queue_headers, task_headers, worker_headers };
	static const char * query_strings[3] = {"queue_status","task_status","worker_status"};

	struct nvpair_header *query_header = query_headers[query_mode];
	const char * query_string = query_strings[query_mode];

	struct link *l;
	struct nvpair *nv;

	char master_addr[LINK_ADDRESS_MAX];

	if(!domain_name_cache_lookup(master_host,master_addr)) {
		fprintf(stderr,"couldn't find address of %s\n",master_host);
		return 1;
	}

	l = link_connect(master_addr,master_port,stoptime);
	if(!l) {
		fprintf(stderr,"couldn't connect to %s port %d: %s\n",master_host,master_port,strerror(errno));
		return 1;
	}

	link_putfstring(l,"%s\n",stoptime,query_string);

	if(format_mode==FORMAT_TABLE) {
		nvpair_print_table_header(stdout, query_header);
	}

	while((nv = link_nvpair_read(l,stoptime))) {
		if(format_mode == FORMAT_TABLE) {
			nvpair_print_table(nv, stdout, query_header);
		} else {
			nvpair_print_text(nv, stdout);
		}
		nvpair_delete(nv);
	}

	if(format_mode == FORMAT_TABLE) {
		nvpair_print_table_footer(stdout, query_header);
	}

	return EXIT_SUCCESS;
}

int main(int argc, char *argv[])
{
	const char *master_host = 0;
	int master_port = WORK_QUEUE_DEFAULT_PORT;

	debug_config(argv[0]);

	work_queue_status_parse_command_line_arguments(argc, argv);

	cctools_version_debug(D_DEBUG, argv[0]);

	if( optind < argc ) {
		master_host = argv[optind];
		optind++;
	}

	if( optind < argc ) {
		master_port = atoi(argv[optind]);
		optind++;
	}

	if(optind < argc) {
		fprintf(stderr,"work_queue_status: Too many arguments.  Try the -h option for help.\n");
		exit(EXIT_FAILURE);
	}

	time_t stoptime = time(0) + work_queue_status_timeout;

	if(master_host) {
		return do_direct_query(master_host,master_port,stoptime);
	} else {
		return do_catalog_query(stoptime);
	}
}

