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

static int Work_Queue_Status_Mode    = MODE_TABLE;
static int Work_Queue_Status_Timeout = 30;

static struct nvpair_header headers[] = {
    { "project",		NVPAIR_MODE_STRING,  NVPAIR_ALIGN_LEFT,		20},
    { "name",       	NVPAIR_MODE_STRING,  NVPAIR_ALIGN_LEFT,		25},
	{ "port",			NVPAIR_MODE_INTEGER, NVPAIR_ALIGN_RIGHT,   	8},
	{ "tasks_waiting",	NVPAIR_MODE_INTEGER, NVPAIR_ALIGN_RIGHT,	15},
	{ "tasks_complete",	NVPAIR_MODE_INTEGER, NVPAIR_ALIGN_RIGHT,	15},
    { "workers",		NVPAIR_MODE_INTEGER, NVPAIR_ALIGN_RIGHT,  	10},
    { "workers_busy",	NVPAIR_MODE_INTEGER, NVPAIR_ALIGN_RIGHT,	15},
    { "lastheardfrom",	NVPAIR_MODE_INTEGER, NVPAIR_ALIGN_RIGHT,	15},
    { NULL, }
};

static void
work_queue_status_show_help(const char *progname)
{
    printf("usage: %s\n", progname);
    printf("Options:\n");
    printf(" -d <flag>   Enable debugging for this subsystem.\n");
    printf(" -t <time>   RPC timeout (default is %ds).\n", Work_Queue_Status_Timeout);
    printf(" -l          Long output.\n");
    printf(" -h          This message.\n");
}

static void
work_queue_status_parse_command_line_arguments(int argc, char *argv[])
{
    int c;

    while ((c = getopt(argc, argv, "d:lt:h")) != -1) {
	switch (c) {
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

int
main(int argc, char *argv[])
{
    struct catalog_query *cq;
    struct nvpair *nv;

    work_queue_status_parse_command_line_arguments(argc, argv);

    if(optind > argc) {
	work_queue_status_show_help("work_queue_status");
	exit(EXIT_FAILURE);
    }

    cq = catalog_query_create(CATALOG_HOST, CATALOG_PORT, time(0) + Work_Queue_Status_Timeout);

    if (Work_Queue_Status_Mode == MODE_TABLE) nvpair_print_table_header(stdout, headers);

    while ((nv = catalog_query_read(cq, time(0) + Work_Queue_Status_Timeout))) {
	if (strcmp(nvpair_lookup_string(nv, "type"), CATALOG_TYPE_WORK_QUEUE_MASTER) == 0) {
	    if (Work_Queue_Status_Mode == MODE_TABLE)
		nvpair_print_table(nv, stdout, headers);
	    else
		nvpair_print_text(nv, stdout);
	}
	nvpair_delete(nv);
    }

    if (Work_Queue_Status_Mode == MODE_TABLE) nvpair_print_table_footer(stdout, headers);

    return EXIT_SUCCESS;
}
