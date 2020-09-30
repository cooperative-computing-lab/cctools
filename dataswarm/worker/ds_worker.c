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

#include "mq.h"
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
#include "xxmalloc.h"

#include "ds_worker.h"
#include "common/ds_message.h"
#include "common/ds_task.h"
#include "ds_process.h"
#include "ds_task_table.h"
#include "ds_blob_table.h"

void ds_worker_status_report(struct ds_worker *w)
{
	struct jx *msg = jx_object(NULL);
	struct jx *params = jx_object(NULL);

	jx_insert_string(msg, "method", "status-report");
	jx_insert(msg, jx_string("params"), params);
	jx_insert_string(params, "hello", "manager");

	ds_json_send(w->manager_connection, msg);

	jx_delete(msg);
}

struct jx * ds_worker_handshake( struct ds_worker *w )
{
	struct jx *msg = jx_object(NULL);
	struct jx *params = jx_object(NULL);

	jx_insert_string(msg, "method", "handshake");
	jx_insert(msg, jx_string("params"), params);
	jx_insert_string(params, "type", "worker");
	jx_insert_integer(msg, "id", w->message_id++);	/* need function to register msgs and their ids */

	return msg;
}


void ds_worker_handle_message(struct ds_worker *w)
{
	int set_storage = 0;
	struct jx *msg = ds_parse_message(&w->recv_buffer);
	if (!msg) {
		printf("malformed message!\n");
		//XXX disconnect?
		return;
	}
	const char *method = jx_lookup_string(msg, "method");
	struct jx *params = jx_lookup(msg, "params");
	int64_t id = jx_lookup_integer(msg, "id");

	ds_result_t result = DS_RESULT_SUCCESS;
	struct jx *result_params = 0;

	/* Whether to send a response for the rpc. Used to turn off the blob-get
	 * response in this function, as blob-get manages its own response when
	 * succesfully sending a file. */
	int should_send_response = 1;

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
		result = ds_task_table_submit(w,taskid,params);
	} else if(!strcmp(method, "task-get")) {
		result = ds_task_table_get(w,taskid,&result_params);
	} else if(!strcmp(method, "task-remove")) {
		result = ds_task_table_remove(w,taskid);
	} else if(!strcmp(method, "task-list")) {
		result = ds_task_table_list(w,&result_params);
	} else if(!strcmp(method, "status-request")) {
		result = DS_RESULT_SUCCESS;
	} else if(!strcmp(method, "blob-create")) {
		result = ds_blob_table_create(w,blobid, jx_lookup_integer(params, "size"), jx_lookup(params, "metadata"));
	} else if(!strcmp(method, "blob-put")) {
		result = ds_blob_table_put(w,blobid);
		set_storage = 1;
	} else if(!strcmp(method, "blob-get")) {
		result = ds_blob_table_get(w,blobid,id,&should_send_response);
	} else if(!strcmp(method, "blob-delete")) {
		result = ds_blob_table_delete(w,blobid);
	} else if(!strcmp(method, "blob-commit")) {
		result = ds_blob_table_commit(w,blobid);
	} else if(!strcmp(method, "blob-copy")) {
		result = ds_blob_table_copy(w,blobid, jx_lookup_string(params, "blob-id-source"));
	} else if(!strcmp(method, "blob-list")) {
		result = ds_blob_table_list(w,&result_params);
	} else if(!strcmp(method, "response")) {
        /* only from handshake */
        should_send_response = 0;
	} else {
		result = DS_RESULT_BAD_METHOD;
	}

	struct jx *response = NULL;

done:
	if (!set_storage) {
		mq_store_buffer(w->manager_connection, &w->recv_buffer, 0);
	}

	if(should_send_response) {
		response = ds_message_standard_response(id,result,result_params);
		ds_json_send(w->manager_connection, response);
	}
	jx_delete(response);
	jx_delete(result_params);
}

int ds_worker_main_loop(struct ds_worker *w)
{
	while(1) {
		switch (mq_recv(w->manager_connection, NULL)) {
			case MQ_MSG_NONE:
				break;
			case MQ_MSG_FD:
				//XXX handle received files
				mq_store_buffer(w->manager_connection, &w->recv_buffer, 0);
				break;
			case MQ_MSG_BUFFER:
				ds_worker_handle_message(w);
				break;
		}

		errno = mq_geterror(w->manager_connection);
		if (errno != 0) {
			break;
		}

		/* after processing any messages, work on tasks. */
		ds_task_table_advance(w);

		time_t current = time(0);

		if(current > (w->last_status_report+w->status_report_interval) ) {
			w->last_status_report = current;
			ds_worker_status_report(w);
		}

		if (mq_wait(w->manager_connection, time(0) + 300) == -1 && errno != EINTR) {
			break;
		}
	}

	perror("ds_worker_main_loop");
	return -1;
}

void ds_worker_connect_loop(struct ds_worker *w, const char *manager_host, int manager_port)
{
	char manager_addr[LINK_ADDRESS_MAX];
	int sleeptime = w->min_connect_retry;

	while(1) {

		if(!domain_name_lookup(manager_host, manager_addr)) {
			printf("couldn't look up host name %s: %s\n", manager_host, strerror(errno));
			break;
		}

		w->manager_connection = mq_connect(manager_addr, manager_port);
		struct jx *msg = ds_worker_handshake(w);
		mq_store_buffer(w->manager_connection, &w->recv_buffer, 0);
		ds_json_send(w->manager_connection, msg);
		jx_delete(msg);

		ds_worker_main_loop(w);
		mq_close(w->manager_connection);
		w->manager_connection = 0;
		sleep(sleeptime);
	}

	printf("worker shutting down.\n");
}

void ds_worker_connect_by_name(struct ds_worker *w, const char *manager_name)
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
				ds_worker_connect_loop(w, host, port);
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

char * ds_worker_task_dir( struct ds_worker *w, const char *taskid )
{
	return string_format("%s/task/%s",w->workspace,taskid);
}

char * ds_worker_task_deleting( struct ds_worker *w )
{
	return string_format("%s/task/deleting",w->workspace);
}

char * ds_worker_task_sandbox( struct ds_worker *w, const char *taskid )
{
	return string_format("%s/task/%s/sandbox",w->workspace,taskid);
}

char * ds_worker_task_meta( struct ds_worker *w, const char *taskid )
{
	return string_format("%s/task/%s/meta",w->workspace,taskid);
}

char * ds_worker_blob_dir( struct ds_worker *w, const char *blobid )
{
	return string_format("%s/blob/%s",w->workspace,blobid);
}

char * ds_worker_blob_data( struct ds_worker *w, const char *blobid )
{
	return string_format("%s/blob/%s/data",w->workspace,blobid);
}

char * ds_worker_blob_meta( struct ds_worker *w, const char *blobid )
{
	return string_format("%s/blob/%s/meta",w->workspace,blobid);
}

char * ds_worker_blob_deleting( struct ds_worker *w )
{
	return string_format("%s/blob/deleting",w->workspace);
}

struct ds_worker *ds_worker_create(const char *workspace)
{
	struct ds_worker *w = xxcalloc(1, sizeof(*w));

	buffer_init(&w->recv_buffer);
	w->task_table = hash_table_create(0, 0);
	w->process_table = hash_table_create(0,0);
	w->blob_table = hash_table_create(0,0);
	w->workspace = strdup(workspace);

	w->idle_timeout = 300;
	w->long_timeout = 3600;
	w->min_connect_retry = 1;
	w->max_connect_retry = 60;
	w->catalog_timeout = 60;
	w->message_id = 1;
	w->last_status_report = 0;
	w->status_report_interval = 60;

	if(!create_dir(w->workspace, 0777)) {
		ds_worker_delete(w);
		return 0;
	}

	chdir(w->workspace);

	mkdir("task", 0777);
	mkdir("task/deleting", 0777);

	mkdir("blob", 0777);
	mkdir("blob/deleting", 0777);

	return w;
}

void ds_worker_delete(struct ds_worker *w)
{
	if(!w)
		return;
	hash_table_delete(w->task_table);
	hash_table_delete(w->process_table);
	hash_table_delete(w->blob_table);
	buffer_free(&w->recv_buffer);
	free(w->workspace);
	free(w);
}

