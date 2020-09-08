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
#include "catalog_query.h"
#include "create_dir.h"
#include "hash_table.h"

#include "dataswarm_message.h"
#include "dataswarm_task.h"
#include "dataswarm_process.h"

// Give up and reconnect if no message received after this time.
int idle_timeout = 300;

//
int long_timeout = 3600;

// Minimum time between connection attempts.
int min_connect_retry = 1;

// Maximum time between connection attempts.
int max_connect_retry = 60;

// Maximum time to wait for a catalog query
int catalog_timeout = 60;

// id msg counter
int message_id = 1;

// taskid -> dataswarm_task *
struct hash_table *task_table = 0;

// This dummy function translates UUIDs of blobs to path.
// Remove after integrating blob support.

char *UUID_TO_LOCAL_PATH( const char *uuid )
{
	return string_format("/the/path/to/blob/%s",uuid);
}

void send_response_message( struct link *l, struct jx *original, struct jx *params )
{
	struct jx *message = jx_object(0);
	
	jx_insert_string(message,"method","response");
	jx_insert_integer(message,"id",jx_lookup_integer(original,"id"));
	jx_insert(message,jx_string("params"),jx_copy(params));

	dataswarm_json_send(l,message,time(0)+long_timeout);

	jx_delete(message);
}

void handle_manager_message( struct link *manager_link, struct jx *msg )
{
	if(!msg) {
		return;
	}

	const char *method = jx_lookup_string(msg,"method");
	struct jx *params = jx_lookup(msg,"params");
	const char *id = jx_lookup_string(msg,"id");

	if(!method || !params) {
		/* dataswarm_json_send_error_result(l, msg, DS_MSG_MALFORMED_MESSAGE, stoptime); */
		/* should the worker add the manager to a banned list at least temporarily? */
		/* disconnect from manager */
		return;
	}

	const char *taskid = jx_lookup_string(params,"taskid");
	struct dataswarm_task *task = 0;

	if(!strcmp(method,"task-submit")) {
		task = dataswarm_task_create(params);
		hash_table_insert(task_table,taskid,task);
		// no response?
	} else if(!strcmp(method,"task-retrieve")) {
		task = hash_table_lookup(task_table,taskid);
		struct jx *jtask = dataswarm_task_to_jx(task);
		send_response_message(manager_link,msg,jtask);
	} else if(!strcmp(method,"task-reap")) {
		task = hash_table_remove(task_table,taskid);		
		if(task->process) dataswarm_process_kill(task->process);
		dataswarm_task_delete(task);
		// No response?
	} else if(!strcmp(method,"task-cancel")) {
		task = hash_table_lookup(task_table,taskid);
		if(task->process) dataswarm_process_kill(task->process);
		// No response?
	} else if(!strcmp(method,"status-request")) {
		/* */
	} else if(!strcmp(method,"blob-create")) {
		/* */
	} else if(!strcmp(method,"blob-put")) {
		/* */
	} else if(!strcmp(method,"blob-get")) {
		/* */
	} else if(!strcmp(method,"blob-delete")) {
		/* */
	} else if(!strcmp(method,"blob-commit")) {
		/* */
	} else if(!strcmp(method,"blob-copy")) {
		/* */
	} else {
		/* dataswarm_json_send_error_result(l, msg, DS_MSG_UNEXPECTED_METHOD, stoptime); */
	}
}

void send_status_report( struct link *manager_link, time_t stoptime ) {
    struct jx *msg = jx_object(NULL);
    struct jx *params = jx_object(NULL);

    jx_insert_string(msg, "method", "status-report");
    jx_insert(msg, jx_string("params"), params);
    jx_insert_string(params, "hello", "manager");

    debug(D_DATASWARM, "Sending status-report message");

    dataswarm_json_send(manager_link, msg, stoptime);

    jx_delete(msg);
}


int worker_main_loop( struct link *manager_link )
{
	while(1) {
		time_t stoptime = time(0) + 5;  /* read messages for at most 5 seconds. remove with Tim's library. */

        while(1) {
            if(link_sleep(manager_link, stoptime, stoptime, 0)) {
                debug(D_DATASWARM, "reading new message...");
                struct jx *msg = dataswarm_json_recv(manager_link, stoptime);
                if(msg) {
                    handle_manager_message(manager_link, msg);
                    jx_delete(msg);
                } else {
                    /* handle manager disconnection */
                    return;
                }
            } else {
                break;
            }
        }

        /* testing: send status report every cycle for now */
        send_status_report(manager_link, stoptime);

        //do not busy sleep more than stoptime
        //this will probably go away with Tim's library
        time_t sleeptime = stoptime - time(0);
        if(sleeptime > 0) {
            sleep(sleeptime);
        }
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
            struct jx *msg = jx_object(NULL);
            struct jx *params = jx_object(NULL);

            jx_insert_string(msg, "method", "handshake");
            jx_insert(msg, jx_string("params"), params);
            jx_insert_string(params, "type", "worker");
            jx_insert_integer(msg, "id", message_id++); /* need function to register msgs and their ids */

			dataswarm_json_send(manager_link, msg, time(0)+long_timeout);
            jx_delete(msg);

			worker_main_loop(manager_link);
            link_close(manager_link);

			sleeptime = min_connect_retry;
		} else {
			printf("could not connect to %s:%d: %s\n",manager_host,manager_port,strerror(errno));
			sleeptime = MIN(sleeptime*2,max_connect_retry);
		}
		sleep(sleeptime);
	}

	printf("worker shutting down.\n");
}

void worker_connect_by_name( const char *manager_name )
{
	char *expr = string_format("type==\"dataswarm_manager\" && project==\"%s\"",manager_name);
	int sleeptime = min_connect_retry;

	while(1) {
		int got_result = 0;

		struct jx *jexpr = jx_parse_string(expr);
		struct catalog_query *query = catalog_query_create(0,jexpr,time(0)+catalog_timeout);
		if(query) {
			struct jx *j = catalog_query_read(query,time(0)+catalog_timeout);
			if(j) {
				const char *host = jx_lookup_string(j,"name");
				int port = jx_lookup_integer(j,"port");
				worker_connect_loop(host,port);
				got_result = 1;
			}
			catalog_query_delete(query);
		}

		if(got_result) {
			sleeptime = min_connect_retry;
		} else {
			debug(D_DATASWARM,"could not find %s\n",expr);
			sleeptime = MIN(sleeptime*2,max_connect_retry);
		}

		sleep(sleeptime);
	}

	free(expr);
}

int workspace_init( const char *workspace )
{
	if(!create_dir(workspace,0777)) return 0;

	chdir(workspace);
	mkdir("task",0777);
	mkdir("data",0777);
	mkdir("data/deleting",0777);
	mkdir("data/ro",0777);
	mkdir("data/rw",0777);

	return 1;
}

static const struct option long_options[] = 
{
	{"manager-name", required_argument, 0, 'N'},
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
	const char *workspace_dir = "/tmp/dataswarm-worker";

	int c;
        while((c = getopt_long(argc, argv, "w:N:m:p:d:o:hv", long_options, 0))!=-1) {

		switch(c) {
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

	if(!workspace_init(workspace_dir)) {
		fprintf(stderr,"%s: couldn't create workspace %s: %s\n",argv[0],workspace_dir,strerror(errno));
		return 1;
	}

	task_table = hash_table_create(0,0);

	if(manager_name) {
		worker_connect_by_name(manager_name);
	} else if(manager_host && manager_port) {
		worker_connect_loop(manager_host,manager_port);
	} else {
		fprintf(stderr,"%s: must specify manager name (-N) or host (-m) and port (-p)\n",argv[0]);
	}

	return 0;
}
