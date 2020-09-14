/*
Copyright (C) 2020- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <errno.h>

#include "link.h"
#include "jx.h"
#include "jx_print.h"
#include "jx_parse.h"
#include "debug.h"
#include "stringtools.h"
#include "domain_name.h"
#include "macros.h"
#include "catalog_query.h"
#include "create_dir.h"
#include "hash_table.h"

#include "dataswarm_worker.h"
#include "dataswarm_message.h"
#include "dataswarm_task.h"
#include "dataswarm_task_table.h"
#include "dataswarm_process.h"
#include "dataswarm_blob.h"

void dataswarm_worker_status_report(struct dataswarm_worker *w, time_t stoptime)
{
	struct jx *msg = jx_object(NULL);
	struct jx *params = jx_object(NULL);

	jx_insert_string(msg, "method", "status-report");
	jx_insert(msg, jx_string("params"), params);
	jx_insert_string(params, "hello", "manager");

	dataswarm_json_send(w->manager_link, msg, stoptime);

	jx_delete(msg);
}

struct jx * dataswarm_worker_handshake( struct dataswarm_worker *w )
{
	struct jx *msg = jx_object(NULL);
	struct jx *params = jx_object(NULL);

	jx_insert_string(msg, "method", "handshake");
	jx_insert(msg, jx_string("params"), params);
	jx_insert_string(params, "type", "worker");
	jx_insert_integer(msg, "id", w->message_id++);	/* need function to register msgs and their ids */

	return msg;
}


void dataswarm_worker_handle_message(struct dataswarm_worker *w, struct jx *msg)
{
	const char *method = jx_lookup_string(msg, "method");
	struct jx *params = jx_lookup(msg, "params");
	int64_t id = jx_lookup_integer(msg, "id");

	dataswarm_result_t result = DS_RESULT_SUCCESS;
	struct jx *result_params = 0;

	if(!method) {
		result = DS_RESULT_BAD_METHOD;
		goto done;
	} else if(!id) {
		result = DS_RESULT_BAD_ID;
		goto done;
	} else if(!params) {
		result = DS_RESULT_BAD_PARAMS;
		goto done;
	}

	const char *taskid = jx_lookup_string(params,"task-id");
	const char *blobid = jx_lookup_string(params,"blob-id");

	if(!strcmp(method, "task-submit")) {
		result = dataswarm_task_table_submit(w,taskid,params);
	} else if(!strcmp(method, "task-get")) {
		result = dataswarm_task_table_get(w,taskid,&result_params);
	} else if(!strcmp(method, "task-remove")) {
		result = dataswarm_task_table_remove(w,taskid);
	} else if(!strcmp(method, "status-request")) {
		result = DS_RESULT_SUCCESS;
	} else if(!strcmp(method, "blob-create")) {
		result = dataswarm_blob_create(w,blobid, jx_lookup_integer(params, "size"), jx_lookup(params, "metadata"));
	} else if(!strcmp(method, "blob-put")) {
		result = dataswarm_blob_put(w,blobid, w->manager_link);
	} else if(!strcmp(method, "blob-get")) {
		result = dataswarm_blob_get(w,blobid, w->manager_link);
	} else if(!strcmp(method, "blob-delete")) {
		result = dataswarm_blob_delete(w,blobid);
	} else if(!strcmp(method, "blob-commit")) {
		result = dataswarm_blob_commit(w,blobid);
	} else if(!strcmp(method, "blob-copy")) {
		result = dataswarm_blob_copy(w,blobid, jx_lookup_string(params, "blob-id-source"));
	} else {
		result = DS_RESULT_BAD_METHOD;
	}

	struct jx *response;

	done:
	response = dataswarm_message_standard_response(id,result,result_params);
	dataswarm_json_send(w->manager_link, response, time(0) + w->long_timeout);
	jx_delete(response);
	jx_delete(result_params);
}

int dataswarm_worker_main_loop(struct dataswarm_worker *w)
{
	while(1) {
		time_t stoptime = time(0) + 5;	/* read messages for at most 5 seconds. remove with Tim's library. */

		while(1) {
			if(link_sleep(w->manager_link, stoptime, stoptime, 0)) {
				struct jx *msg = dataswarm_json_recv(w->manager_link, stoptime);
				if(msg) {
					dataswarm_worker_handle_message(w, msg);
					jx_delete(msg);
				} else {
					/* handle manager disconnection */
					return 0;
				}
			} else {
				break;
			}
		}

		/* after processing all messages, work on tasks. */
		dataswarm_task_table_advance(w);

		/* testing: send status report every cycle for now */
		dataswarm_worker_status_report(w, stoptime);

		//do not busy sleep more than stoptime
		//this will probably go away with Tim's library
		time_t sleeptime = stoptime - time(0);
		if(sleeptime > 0) {
			// XXX make sure this is interrupted at the completion of a task. 
			sleep(sleeptime);
		}
	}
}

void dataswarm_worker_connect_loop(struct dataswarm_worker *w, const char *manager_host, int manager_port)
{
	char manager_addr[LINK_ADDRESS_MAX];
	int sleeptime = w->min_connect_retry;

	while(1) {

		if(!domain_name_lookup(manager_host, manager_addr)) {
			printf("couldn't look up host name %s: %s\n", manager_host, strerror(errno));
			break;
		}

		w->manager_link = link_connect(manager_addr, manager_port, time(0) + sleeptime);
		if(w->manager_link) {

			struct jx *msg = dataswarm_worker_handshake(w);
			dataswarm_json_send(w->manager_link, msg, time(0) + w->long_timeout);
			jx_delete(msg);

			dataswarm_worker_main_loop(w);
			link_close(w->manager_link);
			w->manager_link = 0;
			sleeptime = w->min_connect_retry;
		} else {
			printf("could not connect to %s:%d: %s\n", manager_host, manager_port, strerror(errno));
			sleeptime = MIN(sleeptime * 2, w->max_connect_retry);
		}
		sleep(sleeptime);
	}

	printf("worker shutting down.\n");
}

void dataswarm_worker_connect_by_name(struct dataswarm_worker *w, const char *manager_name)
{
	char *expr = string_format("type==\"dataswarm_manager\" && project==\"%s\"", manager_name);
	int sleeptime = w->min_connect_retry;

	while(1) {
		int got_result = 0;

		struct jx *jexpr = jx_parse_string(expr);
		struct catalog_query *query = catalog_query_create(0, jexpr, time(0) + w->catalog_timeout);
		if(query) {
			struct jx *j = catalog_query_read(query, time(0) + w->catalog_timeout);
			if(j) {
				const char *host = jx_lookup_string(j, "name");
				int port = jx_lookup_integer(j, "port");
				dataswarm_worker_connect_loop(w, host, port);
				got_result = 1;
			}
			catalog_query_delete(query);
		}

		if(got_result) {
			sleeptime = w->min_connect_retry;
		} else {
			debug(D_DATASWARM, "could not find %s\n", expr);
			sleeptime = MIN(sleeptime * 2, w->max_connect_retry);
		}

		sleep(sleeptime);
	}

	free(expr);
}

struct dataswarm_worker *dataswarm_worker_create(const char *workspace)
{
	struct dataswarm_worker *w = malloc(sizeof(*w));
	memset(w, 0, sizeof(*w));

	w->task_table = hash_table_create(0, 0);
	w->workspace = strdup(workspace);

	w->idle_timeout = 300;
	w->long_timeout = 3600;
	w->min_connect_retry = 1;
	w->max_connect_retry = 60;
	w->catalog_timeout = 60;
	w->message_id = 1;

	if(!create_dir(w->workspace, 0777)) {
		dataswarm_worker_delete(w);
		return 0;
	}

	chdir(w->workspace);

	mkdir("task", 0777);
	mkdir("task/deleting", 0777);

	mkdir("blob", 0777);
	mkdir("blob/deleting", 0777);
	mkdir("blob/ro", 0777);
	mkdir("blob/rw", 0777);

	return w;
}

void dataswarm_worker_delete(struct dataswarm_worker *w)
{
	if(!w)
		return;
	hash_table_delete(w->task_table);
	free(w->workspace);
	free(w);
}

