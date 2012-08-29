/* 
Copyright (C) 2010- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "debug.h"
#include "catalog_query.h"
#include "catalog_server.h"
#include "nvpair.h"

#include <errno.h>
#include <string.h>
#include <stdlib.h>
#include <sys/stat.h>

enum {
	MODE_TABLE,
	MODE_LONG
};

static int work_queue_status_mode = MODE_TABLE;
static int work_queue_status_timeout = 30;
char *catalog_host = NULL;
int catalog_port = 0;

static struct nvpair_header headers[] = {
	{"project",       "PROJECT", NVPAIR_MODE_STRING, NVPAIR_ALIGN_LEFT, 18},
	{"name",          "NAME",    NVPAIR_MODE_STRING, NVPAIR_ALIGN_LEFT, 22},
	{"port",          "PORT",    NVPAIR_MODE_INTEGER, NVPAIR_ALIGN_RIGHT, 5},
	{"tasks_waiting", "WAITING",    NVPAIR_MODE_INTEGER, NVPAIR_ALIGN_RIGHT, 8},
	{"workers_busy",  "BUSY",    NVPAIR_MODE_INTEGER, NVPAIR_ALIGN_RIGHT, 5},
	{"tasks_complete","COMPLETE",    NVPAIR_MODE_INTEGER, NVPAIR_ALIGN_RIGHT, 8},
	{"workers",       "WORKERS", NVPAIR_MODE_INTEGER, NVPAIR_ALIGN_RIGHT, 7},
	{NULL,}
};

static void work_queue_status_show_help(const char *progname)
{
	printf("usage: %s\n", progname);
	printf("Options:\n");
	printf(" -C <catalog>   Set catalog server to <catalog>. Format: HOSTNAME:PORT\n");
	printf(" -d <flag>      Enable debugging for this subsystem.\n");
	printf(" -t <time>      RPC timeout (default is %ds).\n", work_queue_status_timeout);
	printf(" -l             Long output.\n");
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

	while((c = getopt(argc, argv, "C:d:lo:O:t:vh")) != (char) -1) {
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
		case 'l':
			work_queue_status_mode = MODE_LONG;
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

int main(int argc, char *argv[])
{
	struct catalog_query *cq;
	struct nvpair *nv;

    debug_config(argv[0]);

	work_queue_status_parse_command_line_arguments(argc, argv);

	cctools_version_debug(D_DEBUG, argv[0]);

	if(optind > argc) {
		work_queue_status_show_help("work_queue_status");
		exit(EXIT_FAILURE);
	}

	if(!catalog_host) {
		catalog_host = strdup(CATALOG_HOST);
		catalog_port = CATALOG_PORT;
	}

	cq = catalog_query_create(catalog_host, catalog_port, time(0) + work_queue_status_timeout);
	if(!cq) {
		fprintf(stderr, "Failed to query catalog server at %s:%d. \n", catalog_host, catalog_port);
		exit(EXIT_FAILURE);
	}
	if(!cq) {
		fprintf(stderr, "couldn't query catalog %s:%d: %s\n", CATALOG_HOST, CATALOG_PORT, strerror(errno));
		return 1;
	}

	if(work_queue_status_mode == MODE_TABLE)
		nvpair_print_table_header(stdout, headers);

	while((nv = catalog_query_read(cq, time(0) + work_queue_status_timeout))) {
		if(strcmp(nvpair_lookup_string(nv, "type"), CATALOG_TYPE_WORK_QUEUE_MASTER) == 0) {
			if(work_queue_status_mode == MODE_TABLE)
				nvpair_print_table(nv, stdout, headers);
			else
				nvpair_print_text(nv, stdout);
		}
		nvpair_delete(nv);
	}

	if(work_queue_status_mode == MODE_TABLE)
		nvpair_print_table_footer(stdout, headers);

	return EXIT_SUCCESS;
}
