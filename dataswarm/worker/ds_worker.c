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
#include "host_disk_info.h"
#include "host_memory_info.h"
#include "load_average.h"

#include "ds_worker.h"
#include "common/ds_message.h"
#include "common/ds_task.h"
#include "ds_process.h"
#include "ds_task_table.h"
#include "ds_blob_table.h"

void ds_worker_status_report(struct ds_worker *w, time_t stoptime)
{
	struct jx *msg = jx_object(NULL);
	struct jx *params = jx_object(NULL);

	jx_insert_string(msg, "method", "status-report");
	jx_insert(msg, jx_string("params"), params);
	jx_insert_string(params, "hello", "manager");

	ds_json_send(w->manager_link, msg, stoptime);

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


void ds_worker_handle_message(struct ds_worker *w, struct jx *msg)
{
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
	} else {
		result = DS_RESULT_BAD_METHOD;
	}


	struct jx *response = NULL;

done:
    if(should_send_response) {
	    response = ds_message_standard_response(id,result,result_params);
	    ds_json_send(w->manager_link, response, time(0) + w->long_timeout);
    }
	jx_delete(response);
	jx_delete(result_params);
}

int ds_worker_main_loop(struct ds_worker *w)
{
	while(1) {
		time_t stoptime = time(0) + 5;	/* read messages for at most 5 seconds. remove with Tim's library. */

		while(1) {
			if(link_sleep(w->manager_link, stoptime, stoptime, 0)) {
				struct jx *msg = ds_json_recv(w->manager_link, stoptime + 60);
				if(msg) {
					ds_worker_handle_message(w, msg);
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
		ds_task_table_advance(w);

		time_t current = time(0);

		if(current > (w->last_status_report+w->status_report_interval) ) {
			w->last_status_report = current;
			ds_worker_status_report(w, stoptime);
		}

		//do not busy sleep more than stoptime
		//this will probably go away with Tim's library
		time_t sleeptime = stoptime - time(0);
		if(sleeptime > 0) {
			// XXX make sure this is interrupted at the completion of a task.
			sleep(sleeptime);
		}
	}
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

		w->manager_link = link_connect(manager_addr, manager_port, time(0) + sleeptime);
		if(w->manager_link) {

			struct jx *msg = ds_worker_handshake(w);
			ds_json_send(w->manager_link, msg, time(0) + w->long_timeout);
			jx_delete(msg);

			ds_worker_main_loop(w);
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

void ds_worker_measure_resources( struct ds_worker *w )
{
	uint64_t avail, total;

	host_memory_info_get(&avail,&total);
	w->memory_total = total;

	/*
	Note the use of avail is deliberate here: the worker's total
	space is the sum of what's free + the size of blobs already
	stored, which we work out later in ds_blob_table_recover.
	*/
 
	host_disk_info_get(w->workspace,&avail,&total);
	w->disk_total = avail;

	w->cores_total = load_average_get_cpus();
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
	struct ds_worker *w = malloc(sizeof(*w));
	memset(w, 0, sizeof(*w));

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
	free(w->workspace);
	free(w);
}

