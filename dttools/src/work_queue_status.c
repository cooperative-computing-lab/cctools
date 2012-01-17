/* 
Copyright (C) 2010- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "debug.h"
#include "catalog_query.h"
#include "catalog_server.h"
#include "nvpair.h"

#include <string.h>
#include <stdlib.h>
#include <sys/stat.h>

enum {
	MODE_TABLE,
	MODE_LONG
};

static int Work_Queue_Status_Mode = MODE_TABLE;
static int Work_Queue_Status_Timeout = 30;
char *catalog_host = NULL;
int catalog_port = 0;

static struct nvpair_header headers[] = {
    { "project",		NVPAIR_MODE_STRING,  NVPAIR_ALIGN_LEFT,		20},
    { "name",       	NVPAIR_MODE_STRING,  NVPAIR_ALIGN_LEFT,		25},
	{ "port",			NVPAIR_MODE_INTEGER, NVPAIR_ALIGN_RIGHT,   	8},
	{ "tasks_waiting",	NVPAIR_MODE_INTEGER, NVPAIR_ALIGN_RIGHT,	15},
	{ "tasks_complete",	NVPAIR_MODE_INTEGER, NVPAIR_ALIGN_RIGHT,	15},
    { "workers",		NVPAIR_MODE_INTEGER, NVPAIR_ALIGN_RIGHT,  	10},
    { "workers_busy",	NVPAIR_MODE_INTEGER, NVPAIR_ALIGN_RIGHT,	15},
    { "capacity",		NVPAIR_MODE_INTEGER, NVPAIR_ALIGN_RIGHT,  	10},
    { "lastheardfrom",	NVPAIR_MODE_INTEGER, NVPAIR_ALIGN_RIGHT,	15},
    { NULL, }
};

static void work_queue_status_show_help(const char *progname)
{
	printf("usage: %s\n", progname);
	printf("Options:\n");
	printf(" -d <flag>   Enable debugging for this subsystem.\n");
	printf(" -t <time>   RPC timeout (default is %ds).\n", Work_Queue_Status_Timeout);
	printf(" -l          Long output.\n");
	printf(" -h          This message.\n");
}

int parse_catalog_server_description(char* server_string, char **host, int *port) {
	char *colon;

	colon = strchr(server_string, ':');

	if(!colon) {
		*host = NULL;
		*port = 0;
		return 0;
	}

	*colon = '\0';

	*host = strdup(server_string);
	*port = atoi(colon+1);
	
	return *port;
}

static void work_queue_status_parse_command_line_arguments(int argc, char *argv[])
{
	int c;

	while((c = getopt(argc, argv, "C:d:lt:h")) != (char)-1) {
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
				Work_Queue_Status_Mode = MODE_LONG;
				break;
			case 't': 
				Work_Queue_Status_Timeout = strtol(optarg, NULL, 10);
				break;
			case 'h':
				work_queue_status_show_help(argv[0]); 
				exit(EXIT_SUCCESS);
				break;
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

	work_queue_status_parse_command_line_arguments(argc, argv);

    if(optind > argc) {
		work_queue_status_show_help("work_queue_status");
		exit(EXIT_FAILURE);
    }

	if(!catalog_host) {
		catalog_host = strdup(CATALOG_HOST);
		catalog_port = CATALOG_PORT;
	}

    cq = catalog_query_create(catalog_host, catalog_port, time(0) + Work_Queue_Status_Timeout);
	if(!cq) {
		fprintf(stderr, "Failed to query catalog server at %s:%d. \n", catalog_host, catalog_port);
		exit(EXIT_FAILURE);
	}

    if (Work_Queue_Status_Mode == MODE_TABLE) nvpair_print_table_header(stdout, headers);

	while((nv = catalog_query_read(cq, time(0) + Work_Queue_Status_Timeout))) {
		if(strcmp(nvpair_lookup_string(nv, "type"), CATALOG_TYPE_WORK_QUEUE_MASTER) == 0) {
			if(Work_Queue_Status_Mode == MODE_TABLE)
				nvpair_print_table(nv, stdout, headers);
			else
				nvpair_print_text(nv, stdout);
		}
		nvpair_delete(nv);
	}

	if(Work_Queue_Status_Mode == MODE_TABLE)
		nvpair_print_table_footer(stdout, headers);

	return EXIT_SUCCESS;
}

