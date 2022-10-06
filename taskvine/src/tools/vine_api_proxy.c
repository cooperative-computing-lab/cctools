/*
Copyright (C) 2022- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

/*
This is a work in progress, and is not yet complete.

To facilitate binding to languages and situations that are not
well supported by SWIG, the vine_api_proxy provides a translation between
JSON messages and operations on the manager API.  A simple library
written in any language sends JSON messages over a pipe to the
vine_api_proxy, which then invokes the appropriate functions
and returns a JSON result.

See clients/python3/vine_proxy.py for an example in Python.
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

#include "taskvine_json.h"
#include "taskvine.h"

int timeout = 25;

void reply(struct link *output_link, const char *method, const char *message, int id)
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

	link_printf(output_link, time(NULL) + timeout, "%d\n", len);

	int total_written = 0;
	ssize_t written = 0;
	while(total_written < len) {
		written = link_write(output_link, buffer, strlen(buffer), time(NULL) + timeout);
		total_written += written;
	}

	jx_delete(j);

}

void mainloop(struct vine_manager *queue, struct link *input_link, struct link *output_link )
{
	char line[BUFSIZ];
	char *msg = 0;

	while(true) {
		ssize_t nread = link_readline(input_link, line, sizeof(line), time(NULL) + timeout);
		int length = atoi(line);

		msg = malloc(length+1);

		nread = link_read(input_link, msg, length, time(NULL) + timeout);
		if(nread!=length) {
			free(msg);
			break;
		}

		msg[length] = 0;

		struct jx *jsonrpc = jx_parse_string(msg);
		if(!jsonrpc) {
			free(msg);
			reply(output_link, "error", "Could not parse JSON string",0);
			break;
		}

		const char *method = jx_lookup_string(jsonrpc,"method");
		int id = jx_lookup_integer(jsonrpc,"id");

		if(!strcmp(method, "submit")) {
			const char *task = jx_lookup_string(jsonrpc,"params");
			int taskid = vine_json_submit(queue, task);
			if(taskid < 0) {
				reply(output_link, "error", "Could not submit task", id);
			} else {
				reply(output_link, method, "Task submitted successfully.", id);
			}
		} else if(!strcmp(method, "wait")) {
			int time_out = jx_lookup_integer(jsonrpc,"params");
			char *task = vine_json_wait(queue, time_out);
			if(!task) {
				reply(output_link, "error", "timeout reached with no task returned", id);
			} else {
				reply(output_link, method, task, id);
			}
		} else if(!strcmp(method, "remove")) {
			int taskid = jx_lookup_integer(jsonrpc,"params");
			char *task = vine_json_remove(queue, taskid);
			if(!task) {
				reply(output_link, "error", "unable to remove task", id);
			} else {
				reply(output_link, method, "Task removed successfully.", id);
			}
		} else if(!strcmp(method, "disconnect")) {
			reply(output_link, method, "Successfully disconnected.", id);
			break;
		} else if(!strcmp(method, "empty")) {
			int empty = vine_empty(queue);
			if(empty) {
				reply(output_link, method, "Empty", id);
			} else {
				reply(output_link, method, "Not Empty", id);
			}
		} else if(!strcmp(method, "status")) {
			char *status = vine_json_get_status(queue);
			reply(output_link, method, status, id);
		} else {
			reply(output_link, "error", "method not recognized", id);
		}

		if(jsonrpc) {
			jx_delete(jsonrpc);
			jsonrpc = 0;
		}

		if(msg) {
			free(msg);
			msg = 0;
		}
	}
}

static const struct option long_options[] = {
	{"port", required_argument, 0, 'p'},
	{"server-port", required_argument, 0, 's'},
	{"project-name", required_argument, 0, 'N'},
	{"debug", required_argument, 0, 'd'},
	{"debug-file", required_argument, 0, 'o'},
	{"help", no_argument, 0, 'h'},
	{"version", no_argument, 0, 'v'}
};

static void show_help(const char *cmd)
{
	printf("use: %s [options]\n", cmd);
	printf("where options are:\n");
	printf("-p,--port=<port>          Port number to listen on.\n");
	printf("-N,--project-name=<name>  Set project name.\n");
	printf("-d,--debug=<subsys>       Enable debugging for this subsystem.\n");
	printf("-o,--debug-file=<file>    Send debugging output to this file.\n");
	printf("-h,--help                 Show this help string\n");
	printf("-v,--version              Show version string\n");
}

int main(int argc, char *argv[])
{
	int port = 0;
	char *project_name = "vine_server";

	int c;
	while((c = getopt_long(argc, argv, "p:N:s:d:o:hv", long_options, 0)) != -1) {

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


	char *config = string_format("{ \"name\":\"%s\", \"port\":%d }", project_name, port);

	struct vine_manager *queue = vine_json_create(config);
	if(!queue) {
		fprintf(stderr, "could not listen on port %d: %s\n", port, strerror(errno));
		return 1;
	}

	port = vine_port(queue);

	printf("vine_api_proxy ready port %d\n",port);
	fflush(stdout);

	struct link *input_link = link_attach_to_fd(0);
	struct link *output_link = link_attach_to_fd(1);

	mainloop(queue,input_link,output_link);

	link_close(input_link);
	link_close(output_link);

	vine_json_delete(queue);

	return 0;
}
