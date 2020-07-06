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

#include "work_queue_json.h"

int timeout = 25;

void reply(struct link *client, char *method, char *message, int id)
{
	struct jx_pair *result;

	char buffer[BUFSIZ];
	memset(buffer, 0, BUFSIZ);

	struct jx_pair *idd = jx_pair(jx_string("id"), jx_integer(id), NULL);

	if(!strcmp(method, "error")) {
		result = jx_pair(jx_string("error"), jx_string(message), idd);
	} else {
		result = jx_pair(jx_string("result"), jx_string(message), idd);
	}

	struct jx_pair *jsonrpc = jx_pair(jx_string("jsonrpc"), jx_string("2.0"), result);

	struct jx *j = jx_object(jsonrpc);

	char *response = jx_print_string(j);

	strcat(buffer, response);

	int len = strlen(buffer);

	link_putfstring(client, "%d", time(NULL) + timeout, len);

	int total_written = 0;
	ssize_t written = 0;
	while(total_written < len) {
		written = link_write(client, buffer, strlen(buffer), time(NULL) + timeout);
		total_written += written;
	}

	jx_delete(j);

}

void mainloop(struct work_queue *queue, struct link *client)
{
	char message[BUFSIZ];
	char msg[BUFSIZ];

	while(true) {

		//reset
		char *error = NULL;
		int id = -1;

		//receive message
		char l[5];

		memset(l, 0, 5);
		memset(message, 0, BUFSIZ);
		memset(msg, 0, BUFSIZ);

		int i = 0;
		ssize_t read = link_read(client, message, 1, time(NULL) + timeout);
		while(message[0] != '{') {

			//printf("server number read: %c\n", message[0]);

			l[i] = message[0];
			i++;
			link_read(client, message, 1, time(NULL) + timeout);
		}

		int length = atoi(l);

		read = link_read(client, msg, length - 1, time(NULL) + timeout);

		//if server cannot read message from client, break connection
		if(!read) {
			error = "Error reading from client";
			reply(client, "error", error, id);
			link_close(client);
			break;
		}

		strcat(message, msg);

		struct jx *jsonrpc = jx_parse_string(message);

		//if server cannot parse JSON string, break connection
		if(!jsonrpc) {
			error = "Could not parse JSON string";
			reply(client, "error", error, id);
			link_close(client);
			break;
		}
		//iterate over the object: get method and task description
		void *k = NULL, *v = NULL;
		const char *key = jx_iterate_keys(jsonrpc, &k);
		struct jx *value = jx_iterate_values(jsonrpc, &v);

		char *method;
		struct jx *val;

		while(key) {

			if(!strcmp(key, "method")) {
				method = value->u.string_value;
			} else if(!strcmp(key, "params")) {
				val = value;
			} else if(!strcmp(key, "id")) {
				id = value->u.integer_value;
			} else if(strcmp(key, "jsonrpc")) {
				error = "unrecognized parameter";
				reply(client, "error", error, id);
				link_close(client);
				break;
			}

			key = jx_iterate_keys(jsonrpc, &k);
			value = jx_iterate_values(jsonrpc, &v);

		}

		//submit or wait
		if(!strcmp(method, "submit")) {

			char *task = val->u.string_value;

			int taskid = work_queue_json_submit(queue, task);

			if(taskid < 0) {
				error = "Could not submit task";
				reply(client, "error", error, id);
			} else {
				reply(client, method, "Task submitted successfully.", id);
			}

		} else if(!strcmp(method, "wait")) {

			int time_out = val->u.integer_value;

			char *task = work_queue_json_wait(queue, time_out);

			if(!task) {
				error = "timeout reached with no task returned";
				reply(client, "error", error, id);
			} else {
				reply(client, method, task, id);
			}

		} else if(!strcmp(method, "remove")) {
			int taskid = val->u.integer_value;

			char *task = work_queue_json_remove(queue, taskid);
			if(!task) {
				error = "task not able to be removed from queue";
				reply(client, "error", error, id);
			} else {
				reply(client, method, "Task removed successfully.", id);
			}

		} else if(!strcmp(method, "disconnect")) {
			reply(client, method, "Successfully disconnected.", id);
			break;
		} else if(!strcmp(method, "empty")) {
			int empty = work_queue_empty(queue);
			if(empty) {
				reply(client, method, "Empty", id);
			} else {
				reply(client, method, "Not Empty", id);
			}
		} else {
			error = "Method not recognized";
			reply(client, "error", error, id);
		}

		//clean up
		jx_delete(value);
		jx_delete(jsonrpc);

	}
}

static const struct option long_options[] = 
{
	{"port", required_argument, 0, 'p'},
	{"server-port", required_argument, 0, 's'},
	{"project-name", required_argument, 0, 'N'},
	{"debug", required_argument, 0, 'd'},
	{"debug-file", required_argument, 0, 'o'},
	{"help", no_argument, 0, 'h' },
	{"version", no_argument, 0, 'v' }
};

static void show_help( const char *cmd )
{
	printf("use: %s [options]\n",cmd);
	printf("where options are:\n");
	printf("-p,--port=<port>          Port number to listen on.\n");
	printf("-s,--server-port=<port>   Port number for server.\n");
	printf("-N,--project-name=<name>  Set project name.\n");
	printf("-d,--debug=<subsys>       Enable debugging for this subsystem.\n");
	printf("-o,--debug-file=<file>    Send debugging output to this file.\n");
	printf("-h,--help                 Show this help string\n");
	printf("-v,--version              Show version string\n");	
}

int main(int argc, char *argv[])
{
	int port = 0;
	int server_port = 0;
	char *project_name = "wq_server";

	int c;
        while((c = getopt_long(argc, argv, "p:N:s:d:o:hv", long_options, 0))!=-1) {

		switch(c) {
			case 'd':
				debug_flags_set(optarg);
				break;
			case 'o':
				debug_config_file(optarg);
				break;
			case 'p':
				port = atoi(optarg);
				break;
			case 's':
				server_port = atoi(optarg);
				break;
			case 'N':
				project_name = strdup(optarg);
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


	char * config = string_format("{ \"name\":\"%s\", \"port\":%d }",project_name,port);
	
	struct work_queue *queue = work_queue_json_create(config);
	if(!queue) {
		printf("could not listen on port %d: %s\n",port,strerror(errno));
		return 1;
	}

	struct link *server_link = link_serve(server_port);
	if(!server_link) {
		printf("could not serve on port %d: %s\n", server_port,strerror(errno));
		return 1;
	}

	struct link *client = link_accept(server_link, time(NULL) + timeout);
	if(!client) {
		printf("could not accept connection: %s\n",strerror(errno));
		return 1;
	}

	printf("Connected to client. Waiting for messages..\n");

	mainloop(queue, client);

	link_close(client);

	return 0;
}
