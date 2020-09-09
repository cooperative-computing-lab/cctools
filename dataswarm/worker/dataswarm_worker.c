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
#include "dataswarm_process.h"
#include "dataswarm_blob.h"

// This dummy function translates UUIDs of blobs to path.
// Remove after integrating blob support.

char *UUID_TO_LOCAL_PATH(const char *uuid)
{
	return string_format("/the/path/to/blob/%s", uuid);
}

/*
Every time a task changes state, send an async update message.
*/

void update_task_state( struct dataswarm_worker *w, struct dataswarm_task *task, dataswarm_task_state_t state )
{
	task->state = state;

	char *str = string_format("{\"method\":\"task-update\",\"params\":{\"taskid\":\"%s\",\"state\":\"%s\"}}",
		task->taskid,dataswarm_task_state_string(state));
	dataswarm_message_send(w->manager_link,str,strlen(str),time(0)+w->long_timeout);
	free(str);
}


/*
Consider each task currently in possession of the worker,
and act according to it's current state.
*/

void dataswarm_worker_advance_tasks( struct dataswarm_worker *w )
{
	struct dataswarm_task *task;
	char *taskid;

	hash_table_firstkey(w->task_table);
	while(hash_table_nextkey(w->task_table,&taskid,(void**)&task)) {

		switch(task->state) {
			case DATASWARM_TASK_READY:
				// XXX only start tasks when resources available.
				task->process = dataswarm_process_create(task,w);
				if(task->process) {
					// XXX check for invalid mounts?
					if(dataswarm_process_start(task->process)) {
						update_task_state(w,task,DATASWARM_TASK_RUNNING);
					} else {
						update_task_state(w,task,DATASWARM_TASK_FAILED);
					}
				} else {
					update_task_state(w,task,DATASWARM_TASK_FAILED);
				}
				break;
			case DATASWARM_TASK_RUNNING:
				if(dataswarm_process_isdone(task->process)) {
					update_task_state(w,task,DATASWARM_TASK_DONE);
				}
				break;
			case DATASWARM_TASK_DONE:
				// Do nothing until removed.
				break;
			case DATASWARM_TASK_FAILED:
				// Do nothing until removed.
				break;
			case DATASWARM_TASK_DELETING:
				// XXX Is this safe to do during an iteration?
				hash_table_remove(w->task_table,taskid);
				dataswarm_process_delete(task->process);
				dataswarm_task_delete(task);
				break;
		}
	}

}

void dataswarm_worker_send_response(struct dataswarm_worker *w, struct jx *original, struct jx *params)
{
	struct jx *message = jx_object(0);

	jx_insert_string(message, "method", "response");
	jx_insert_integer(message, "id", jx_lookup_integer(original, "id"));
	jx_insert(message, jx_string("params"), jx_copy(params));

	dataswarm_json_send(w->manager_link, message, time(0) + w->long_timeout);

	jx_delete(message);
}

struct jx *dataswarm_worker_handle_message(struct dataswarm_worker *w, struct jx *msg)
{
	struct jx *response = NULL;
	if(!msg) {
		return response;
	}

	const char *method = jx_lookup_string(msg, "method");
	struct jx *params = jx_lookup(msg, "params");
	const char *id = jx_lookup_string(msg, "id");

	if(!method || !params) {
		/* dataswarm_json_send_error_result(l, msg, DS_MSG_MALFORMED_MESSAGE, stoptime); */
		/* should the worker add the manager to a banned list at least temporarily? */
		/* disconnect from manager */
		return response;
	}

	const char *taskid = jx_lookup_string(params,"taskid");
	const char *blobid = jx_lookup_string(params,"blobid");
	struct dataswarm_task *task = 0;

	if(!strcmp(method, "task-submit")) {
		task = dataswarm_task_create(params);
		hash_table_insert(w->task_table, taskid, task);
		update_task_state(w,task,DATASWARM_TASK_READY);

	} else if(!strcmp(method, "task-get")) {
		task = hash_table_lookup(w->task_table, taskid);
		struct jx *jtask = dataswarm_task_to_jx(task);
		dataswarm_worker_send_response(w, msg, jtask);

	} else if(!strcmp(method, "task-remove")) {
		task = hash_table_lookup(w->task_table, taskid);
		update_task_state(w,task,DATASWARM_TASK_DELETING);

	} else if(!strcmp(method, "status-request")) {
		/* */
	} else if(!strcmp(method, "blob-create")) {
		response = dataswarm_blob_create(w,blobid, jx_lookup_integer(params, "size"), jx_lookup(params, "metadata"));
	} else if(!strcmp(method, "blob-put")) {
		response = dataswarm_blob_put(w,blobid, w->manager_link);
	} else if(!strcmp(method, "blob-get")) {
		response = dataswarm_blob_get(w,blobid, w->manager_link);
	} else if(!strcmp(method, "blob-delete")) {
		response = dataswarm_blob_delete(w,blobid);
	} else if(!strcmp(method, "blob-commit")) {
		response = dataswarm_blob_commit(w,blobid);
	} else if(!strcmp(method, "blob-copy")) {
		response = dataswarm_blob_copy(w,blobid, jx_lookup_string(params, "blob-id-source"));
	} else {
		response = dataswarm_message_error_response(DS_MSG_UNEXPECTED_METHOD, msg);
	}

	return response;
}

void dataswarm_worker_status_report(struct dataswarm_worker *w, time_t stoptime)
{
	struct jx *msg = jx_object(NULL);
	struct jx *params = jx_object(NULL);

	jx_insert_string(msg, "method", "status-report");
	jx_insert(msg, jx_string("params"), params);
	jx_insert_string(params, "hello", "manager");

	debug(D_DATASWARM, "Sending status-report message");

	dataswarm_json_send(w->manager_link, msg, stoptime);

	jx_delete(msg);
}


int dataswarm_worker_main_loop(struct dataswarm_worker *w)
{
	while(1) {
		time_t stoptime = time(0) + 5;	/* read messages for at most 5 seconds. remove with Tim's library. */

		while(1) {
			if(link_sleep(w->manager_link, stoptime, stoptime, 0)) {
				debug(D_DATASWARM, "reading new message...");
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
		dataswarm_worker_advance_tasks(w);

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
			struct jx *msg = jx_object(NULL);
			struct jx *params = jx_object(NULL);

			jx_insert_string(msg, "method", "handshake");
			jx_insert(msg, jx_string("params"), params);
			jx_insert_string(params, "type", "worker");
			jx_insert_integer(msg, "id", w->message_id++);	/* need function to register msgs and their ids */

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

