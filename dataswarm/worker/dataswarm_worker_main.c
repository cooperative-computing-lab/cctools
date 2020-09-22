#include "dataswarm_worker.h"
#include "dataswarm_blob_table.h"

#include "debug.h"
#include "stringtools.h"
#include "cctools.h"

#include <getopt.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>

static const struct option long_options[] = {
	{"manager-name", required_argument, 0, 'N'},
	{"manager-host", required_argument, 0, 'm'},
	{"manager-port", required_argument, 0, 'p'},
	{"debug", required_argument, 0, 'd'},
	{"debug-file", required_argument, 0, 'o'},
	{"help", no_argument, 0, 'h'},
	{"version", no_argument, 0, 'v' },
	{0, 0, 0, 0}
};

static void show_help(const char *cmd)
{
	printf("use: %s [options]\n", cmd);
	printf("where options are:\n");
	printf("-N,--manager-name=<name>  Manager project name.\n");
	printf("-m,--manager-host=<host>  Manager host or address.\n");
	printf("-p,--manager-port=<port>  Manager port number.\n");
	printf("-d,--debug=<subsys>       Enable debugging for this subsystem.\n");
	printf("-o,--debug-file=<file>    Send debugging output to this file.\n");
	printf("-h,--help                 Show this help string\n");
	printf("-v,--version              Show version string\n");
}

int main(int argc, char *argv[])
{
	const char *manager_name = 0;
	const char *manager_host = 0;
	int manager_port = 0;
	const char *workspace_dir = string_format("/tmp/dataswarm-worker-%d", getuid());

	int c;
	while((c = getopt_long(argc, argv, "w:N:m:p:d:o:hv", long_options, 0)) != -1) {

		switch (c) {
		case 'w':
			workspace_dir = optarg;
			break;
		case 'N':
			manager_name = optarg;
			break;
		case 'd':
			debug_flags_set(optarg);
			break;
		case 'o':
			debug_config_file(optarg);
			break;
		case 'm':
			manager_host = optarg;
			break;
		case 'p':
			manager_port = atoi(optarg);
			break;
		case 'v':
			cctools_version_print(stdout, argv[0]);
			return 0;
			break;
		default:
		case 'h':
			show_help(argv[0]);
			return 0;
			break;
		}
	}

	struct dataswarm_worker *w = dataswarm_worker_create(workspace_dir);

	dataswarm_blob_table_purge(w);

	if(!w) {
		fprintf(stderr, "%s: couldn't create workspace %s: %s\n", argv[0], workspace_dir, strerror(errno));
		return 1;
	}

	if(manager_name) {
		dataswarm_worker_connect_by_name(w, manager_name);
	} else if(manager_host && manager_port) {
		dataswarm_worker_connect_loop(w, manager_host, manager_port);
	} else {
		fprintf(stderr, "%s: must specify manager name (-N) or host (-m) and port (-p)\n", argv[0]);
	}

	dataswarm_worker_delete(w);

	return 0;
}
