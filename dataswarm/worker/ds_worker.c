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
#include "host_disk_info.h"
#include "host_memory_info.h"
#include "load_average.h"
#include "xxmalloc.h"

#include "ds_message.h"
#include "ds_task.h"
#include "ds_resources.h"
#include "ds_worker.h"
#include "ds_process.h"
#include "ds_task_table.h"
#include "ds_blob_table.h"

void ds_worker_status_report(struct ds_worker *w)
{
	struct jx *params = jx_object(NULL);
	jx_insert_string(params, "hello", "manager");
	struct jx *msg = ds_message_notification("status-report", params);
	ds_json_send(w->manager_connection, msg);
	jx_delete(msg);
}

struct jx * ds_worker_handshake( struct ds_worker *w )
{
	struct jx *params = jx_object(NULL);
	jx_insert_string(params, "type", "worker");
	return ds_message_notification("handshake", params);
}

void ds_worker_handle_notification(struct ds_worker *w, const char *method, struct jx *params) {
	if(!strcmp(method, "status-request")) {
		// do something
	} else {
		fatal("bad rpc!\n");
	}
	mq_store_buffer(w->manager_connection, &w->recv_buffer, 0);
}

void ds_worker_handle_request(struct ds_worker *w, const char *method, uint64_t id, struct jx *params) {
	struct jx *result_params = 0;
	ds_result_t result;

	/* Whether to send a response for the rpc. Used to turn off the blob-get
	 * response in this function, as blob-get manages its own response when
	 * succesfully sending a file. */
	int should_send_response = 1;
	int set_storage = 0;

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
	} else if(!strcmp(method, "blob-create")) {
		result = ds_blob_table_create(w,blobid, jx_lookup_integer(params, "size"), jx_lookup(params, "metadata"));
	} else if(!strcmp(method, "blob-put")) {
		result = ds_blob_table_put(w,blobid);
		set_storage = 1;
	} else if(!strcmp(method, "blob-get")) {
		result = ds_blob_table_get(w,blobid,id,&should_send_response);
	} else if(!strcmp(method, "blob-delete")) {
		result = ds_blob_table_deleting(w,blobid);
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

	if (!set_storage) {
		mq_store_buffer(w->manager_connection, &w->recv_buffer, 0);
	}

	if(should_send_response) {
		response = ds_message_response(id,result,result_params);
		ds_json_send(w->manager_connection, response);
	}
	jx_delete(response);
}

void ds_worker_handle_message(struct ds_worker *w)
{
	struct jx *msg = ds_parse_message(&w->recv_buffer);
	if (!msg) {
		fatal("malformed message!\n");
	}

	const char *method = NULL;
	struct jx *params = NULL;
	jx_int_t id = 0;

	if (ds_unpack_request(msg, &method, &id, &params) == DS_RESULT_SUCCESS) {
		ds_worker_handle_request(w, method, id, params);
	} else if (ds_unpack_notification(msg, &method, &params) == DS_RESULT_SUCCESS) {
		ds_worker_handle_notification(w, method, params);
	} else {
		fatal("invalid rpc!\n");
	}

	jx_delete(msg);
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

		/* process any pending blob deletes, etc. */
		ds_blob_table_advance(w);

		time_t current = time(0);

		if(current > (w->last_status_report+w->status_report_interval) ) {
			w->last_status_report = current;
			ds_worker_status_report(w);
		}

		if (mq_wait(w->manager_connection, time(0) + 10) == -1 && errno != EINTR) {
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

void ds_worker_resources_debug( struct ds_worker *w )
{
	debug(D_DATASWARM,"inuse: %lld cores, %lld MB memory, %lld MB disk\n",
		(long long) w->resources_inuse->cores,
		(long long) w->resources_inuse->memory/MEGA,
		(long long) w->resources_inuse->disk/MEGA
	);
}

void ds_worker_measure_resources( struct ds_worker *w )
{
	uint64_t avail, total;

	host_memory_info_get(&avail,&total);
	w->resources_total->memory = total;

	/*
	Note the use of avail is deliberate here: the worker's total
	space is the sum of what's free + the size of blobs already
	stored, which we work out later in ds_blob_table_recover.
	*/

	host_disk_info_get(w->workspace,&avail,&total);
	w->resources_total->disk = avail;

	w->resources_total->cores = load_average_get_cpus();
}

/*
ds_worker_resources_avail/alloc/free work on the resource triples
of cores/memory/disk that are needed by tasks.  However, note that
when tasks complete, they no longer need cores/memory, but they
still occupy disk until deleted.
*/

int ds_worker_resources_avail( struct ds_worker *w, struct ds_resources *r )
{
	return r->cores+w->resources_inuse->cores <= w->resources_total->cores
		&& r->memory + w->resources_inuse->memory <= w->resources_total->memory
		&& r->disk + w->resources_inuse->disk <= w->resources_total->disk;
}

void ds_worker_resources_alloc( struct ds_worker *w, struct ds_resources *r )
{
	ds_resources_add(w->resources_inuse,r);
	ds_worker_resources_debug(w);
}

void ds_worker_resources_free_except_disk( struct ds_worker *w, struct ds_resources *r )
{
	ds_resources_sub(w->resources_inuse,r);
	w->resources_inuse->disk += r->disk;
	ds_worker_resources_debug(w);
}

int ds_worker_disk_avail( struct ds_worker *w, int64_t size )
{
	return size<=(w->resources_total->disk-w->resources_inuse->disk);
}

void ds_worker_disk_alloc( struct ds_worker *w, int64_t size )
{
	w->resources_inuse->disk += size;
	ds_worker_resources_debug(w);
}

void ds_worker_disk_free( struct ds_worker *w, int64_t size )
{
	w->resources_inuse->disk -= size;
	ds_worker_resources_debug(w);
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

	w->resources_total = ds_resources_create(0,0,0);
	w->resources_inuse = ds_resources_create(0,0,0);

	w->idle_timeout = 300;
	w->long_timeout = 3600;
	w->min_connect_retry = 1;
	w->max_connect_retry = 60;
	w->catalog_timeout = 60;
	w->last_status_report = 0;
	w->status_report_interval = 60;

	if(!create_dir(w->workspace, 0777)) {
		ds_worker_delete(w);
		return 0;
	}

	chdir(w->workspace);

	mkdir("task", 0777);
	mkdir("blob", 0777);

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
