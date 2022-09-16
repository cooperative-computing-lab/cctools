#include "dataswarm.h"

#include "debug.h"
#include "cctools.h"
#include "path.h"
#include "errno.h"
#include "unlink_recursive.h"

#include <getopt.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>

extern int ds_mainloop( struct ds_manager *q );

void show_help( const char *cmd )
{
	printf("Usage: %s [options]\n",cmd);
	printf("Where options are:\n");
	printf("-m         Enable resource monitoring.\n");
	printf("-Z <file>  Write listening port to this file.\n");
	printf("-p <port>  Listen on this port.\n");
	printf("-N <name>  Advertise this project name.\n");
	printf("-d <flag>  Enable debugging for this subsystem.\n");
	printf("-o <file>  Send debugging output to this file.\n");
	printf("-v         Show version information.\n");
	printf("-h         Show this help screen.\n");
}

int main(int argc, char *argv[])
{
	int port = DS_DEFAULT_PORT;
	const char *port_file=0;
	const char *project_name=0;
	int monitor_flag = 0;
	int c;

	while((c = getopt(argc, argv, "d:o:mN:p:Z:vh"))!=-1) {
		switch (c) {
		case 'd':
			debug_flags_set(optarg);
			break;
		case 'o':
			debug_config_file(optarg);
			break;
		case 'p':
			port = atoi(optarg);
			break;
		case 'm':
			monitor_flag = 1;
			break;
		case 'N':
			project_name = optarg;
			break;
		case 'Z':
			port_file = optarg;
			port = 0;
			break;
		case 'v':
			cctools_version_print(stdout, argv[0]);
			return 0;
			break;
		case 'h':
			show_help(path_basename(argv[0]));
			return 0;
		default:
			show_help(path_basename(argv[0]));
			return 1;
		}
	}

	struct ds_manager *q = ds_create(port);
	if(!q) fatal("couldn't listen on any port!");

	printf("listening on port %d...\n", ds_port(q));

	if(port_file) {
		FILE *file = fopen(port_file,"w");
		if(!file) fatal("couldn't open %s: %s",port_file,strerror(errno));
		fprintf(file,"%d\n",ds_port(q));
		fclose(file);
	}

	if(project_name) {
		ds_specify_name(q,project_name);
	}

	if(monitor_flag) {
		unlink_recursive("work-queue-test-monitor");
		ds_enable_monitoring(q, "work-queue-test-monitor", 1);
		ds_specify_category_mode(q, NULL, DS_ALLOCATION_MODE_MAX_THROUGHPUT);

		ds_specify_transactions_log(q, "work-queue-test-monitor/transactions.log");
	}


	int result = ds_mainloop(q);

	ds_delete(q);

	return result;
}

/* vim: set noexpandtab tabstop=4: */
