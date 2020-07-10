/*
Copyright (C) 2020- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <getopt.h>
#include <errno.h>

#include "link.h"
#include "jx.h"
#include "jx_print.h"
#include "jx_parse.h"
#include "debug.h"
#include "stringtools.h"
#include "cctools.h"
#include "domain_name.h"

static const struct option long_options[] = 
{
	{"manager-host", required_argument, 0, 'm'},
	{"manager-port", required_argument, 0, 'p'},
	{"debug", required_argument, 0, 'd'},
	{"debug-file", required_argument, 0, 'o'},
	{"help", no_argument, 0, 'h' },
	{"version", no_argument, 0, 'v' }
};

static void show_help( const char *cmd )
{
	printf("use: %s [options]\n",cmd);
	printf("where options are:\n");
	printf("-m,--manager-host=<name>  Manager host or address.\n");
	printf("-p,--manager-port=<port>  Manager port number.\n");
	printf("-d,--debug=<subsys>       Enable debugging for this subsystem.\n");
	printf("-o,--debug-file=<file>    Send debugging output to this file.\n");
	printf("-h,--help                 Show this help string\n");
	printf("-v,--version              Show version string\n");	
}

int main(int argc, char *argv[])
{
	const char *manager_host;
	int manager_port = 0;
	int timeout = 30;

	int c;
        while((c = getopt_long(argc, argv, "m:p:d:o:hv", long_options, 0))!=-1) {

		switch(c) {
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

	char manager_addr[LINK_ADDRESS_MAX];

	if(!domain_name_lookup(manager_host,manager_addr)) {
		printf("couldn't look up host name %s: %s\n",manager_host,strerror(errno));
		return 1;
	}

	struct link *manager_link = link_connect(manager_addr,manager_port,time(0)+timeout);
	if(!manager_link) {
		printf("could not connect to %s:%d: %s\n",manager_host,manager_port,strerror(errno));
		return 1;
	}

	printf("worker shutting down.\n");

	return 0;
}
