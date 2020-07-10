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
#include "macros.h"

// Give up and reconnect if no message received after this time.
int idle_timeout = 300;

// Minimum time between connection attempts.
int min_connect_retry = 1;

// Maximum time between connection attempts.
int max_connect_retry = 60;

int send_string_message( struct link *l, const char *str, int length, time_t stoptime )
{
	char lenstr[16];
	sprintf(lenstr,"%d\n",length);
	int lenstrlen = strlen(lenstr);
	int result = link_write(l,lenstr,lenstrlen,stoptime);
	if(result!=lenstrlen) return 0;
	result = link_write(l,str,length,stoptime);
	return result==length;
}

char * recv_string_message( struct link *l, time_t stoptime )
{
	char lenstr[16];
	int result = link_readline(l,lenstr,sizeof(lenstr),stoptime);
	if(!result) return 0;

	int length = atoi(lenstr);
	char *str = malloc(length);
	result = link_read(l,str,length,stoptime);
	if(result!=length) {
		free(str);
		return 0;
	}
	return str;
}

int send_json_message( struct link *l, struct jx *j, time_t stoptime )
{
	char *str = jx_print_string(j);
	int result = send_string_message(l,str,strlen(str),stoptime);
	free(str);
	return result;
}

struct jx * recv_json_message( struct link *l, time_t stoptime )
{
	char *str = recv_string_message(l,stoptime);
	if(!str) return 0;
	struct jx *j = jx_parse_string(str);
	free(str);
	return j;
}

void process_json_message( struct link *manager_link, struct jx *msg )
{
	const char *action = jx_lookup_string(msg,"action");
	if(!strcmp(action,"blob_create")) {
		// blob_create(msg,...);
	} else if(!strcmp(action,"task_create")) {
		// task_create(msg,...);
	} else {
		debug(D_DEBUG,"unknown action: %s\n",action);
	}
}

int worker_main_loop( struct link * manager_link )
{
	while(1) {
		time_t stoptime = time(0) + idle_timeout;

		struct jx *msg = recv_json_message(manager_link,stoptime);
		if(!msg) return 0;

		process_json_message(manager_link,msg);
	}
}

void worker_connect_loop( const char *manager_host, int manager_port )
{
	char manager_addr[LINK_ADDRESS_MAX];
	int sleeptime = min_connect_retry;

	while(1) {
		if(!domain_name_lookup(manager_host,manager_addr)) {
			printf("couldn't look up host name %s: %s\n",manager_host,strerror(errno));
			break;
		}

		struct link *manager_link = link_connect(manager_addr,manager_port,time(0)+sleeptime);
		if(manager_link) {
			worker_main_loop(manager_link);
			sleeptime = min_connect_retry;
		} else {
			printf("could not connect to %s:%d: %s\n",manager_host,manager_port,strerror(errno));
			sleeptime = MIN(sleeptime*2,max_connect_retry);
		}
		sleep(sleeptime);
	}

	printf("worker shutting down.\n");
}

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
	const char *manager_host = 0;
	int manager_port = 0;

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

	worker_connect_loop(manager_host,manager_port);

	return 0;
}
