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
#include <assert.h>

#include "jx.h"
#include "jx_print.h"
#include "jx_parse.h"
#include "debug.h"
#include "stringtools.h"
#include "xxmalloc.h"
#include "cctools.h"
#include "hash_table.h"
#include "username.h"
#include "catalog_query.h"

#include "common/ds_message.h"
#include "common/ds_task.h"
#include "ds_worker_rep.h"
#include "ds_client_rep.h"
#include "ds_blob_rep.h"
#include "ds_task_rep.h"
#include "ds_manager.h"
#include "ds_client_ops.h"
#include "ds_file.h"

#include "ds_test.h"

struct jx * manager_status_jx( struct ds_manager *m )
{
	char owner[USERNAME_MAX];
	username_get(owner);

	struct jx * j = jx_object(0);
	jx_insert_string(j,"type","ds_manager");
	jx_insert_string(j,"project",m->project_name);
	jx_insert_integer(j,"starttime",(m->start_time/1000000));
	jx_insert_string(j,"owner",owner);
	jx_insert_string(j,"version",CCTOOLS_VERSION);
	jx_insert_integer(j,"port",m->server_port);

	return j;
}

void update_catalog( struct ds_manager *m, int force_update )
{
	if(!m->force_update && (time(0) - m->catalog_last_update_time) < m->update_interval)
		return;

	if(!m->catalog_hosts) m->catalog_hosts = strdup(CATALOG_HOST);

	struct jx *j = manager_status_jx(m);
	char *str = jx_print_string(j);

	debug(D_DATASWARM, "advertising to the catalog server(s) at %s ...", m->catalog_hosts);
	catalog_query_send_update_conditional(m->catalog_hosts, str);

	free(str);
	jx_delete(j);
	m->catalog_last_update_time = time(0);
}

void process_files( struct ds_manager *m )
{
}

void process_tasks( struct ds_manager *m )
{
}

/* declares a blob in a worker so that it can be manipulated via blob rpcs. */
struct ds_blob_rep *ds_manager_add_blob_to_worker( struct ds_manager *m, struct ds_worker_rep *r, const char *blobid) {
	struct ds_blob_rep *b = hash_table_lookup(r->blobs, blobid);
	if(b) {
		/* cannot create an already declared blob. This could only happen with
		 * a bug, as we have control of the create messages.*/
		fatal("blob-id %s already created at worker.", blobid);
	}

	b = calloc(1,sizeof(struct ds_blob_rep));
	b->state = DS_BLOB_WORKER_STATE_NEW;
	b->in_transition = b->state;
	b->result = DS_RESULT_SUCCESS;

	b->blobid = xxstrdup(blobid);

	hash_table_insert(r->blobs, blobid, b);

	return b;
}

/* declares a task in a worker so that it can be manipulated via task rpcs. */
struct ds_task_rep *ds_manager_add_task_to_worker( struct ds_manager *m, struct ds_worker_rep *r, const char *taskid) {
	struct ds_task_rep *t = hash_table_lookup(r->tasks, taskid);
	if(t) {
		/* cannot create an already declared task. This could only happen with
		 * a bug, as we have control of the create messages.*/
		fatal("task-id %s already created at worker.", taskid);
	}

	/* this should be a proper struct ds_task. */
	struct jx *description = hash_table_lookup(m->task_table, taskid);
	if(!description) {
		/* could not find task with taskid. This could only happen with a bug,
		 * as we have control of the create messages.*/
		fatal("task-id %s does not exist.", taskid);
	}

	t = calloc(1,sizeof(struct ds_task_rep));
	t->state = DS_TASK_WORKER_STATE_NEW;
	t->in_transition = t->state;
	t->result = DS_RESULT_SUCCESS;

	t->taskid = xxstrdup(taskid);
	t->description = description;

	hash_table_insert(r->tasks, taskid, t);

	return t;
}

char *ds_manager_submit_task( struct ds_manager *m, struct jx *description ) {
	char *taskid = string_format("task-%d", m->task_id++);

	/* do validation */
	/* convert to proper struct ds_task */

	jx_insert_string(description, "task-id", taskid);

	hash_table_insert(m->task_table, taskid, description);

	return taskid;
}

int handle_handshake(struct ds_manager *m, struct mq *conn) {
		switch (mq_recv(conn, NULL)) {
			case MQ_MSG_NONE:
			case MQ_MSG_FD:
				abort();
			case MQ_MSG_BUFFER:
				break;
		}

		buffer_t *buf = mq_get_tag(conn);
		assert(buf);
		mq_set_tag(conn, NULL);

		char *manager_key = NULL;
		struct jx *msg = ds_parse_message(buf);
		buffer_free(buf);
		free(buf);
		const char *method = jx_lookup_string(msg,"method");
		struct jx *params = jx_lookup(msg,"params");

		if(!method || !params) {
			/* ds_json_send_error_result(l, msg, DS_MSG_MALFORMED_MESSAGE, stoptime); */
			mq_close(conn);
			goto DONE;
		}

		if(strcmp(method, "handshake")) {
			/* ds_json_send_error_result(l, msg, DS_MSG_UNEXPECTED_METHOD, stoptime); */
			mq_close(conn);
			goto DONE;
		}

		jx_int_t id = jx_lookup_integer(msg, "id");
		if(id < 1) {
			/* ds_json_send_error_result(l, msg, DS_MSG_MALFORMED_ID, stoptime); */
			mq_close(conn);
			goto DONE;
		}

		manager_key = string_format("%p",conn);
		//const char *msg_key = jx_lookup_string(msg, "uuid");
		/* todo: replace manager_key when msg_key not null */
		const char *conn_type = jx_lookup_string(params, "type");

		struct jx *response = NULL;
		if(!strcmp(conn_type,"worker")) {
			struct ds_worker_rep *w = ds_worker_rep_create(conn);
			mq_address_remote(conn,w->addr,&w->port);
			debug(D_DATASWARM,"new worker from %s:%d\n",w->addr,w->port);
			hash_table_insert(m->worker_table,manager_key,w);
			response = ds_message_standard_response(id, DS_RESULT_SUCCESS, NULL);
			mq_store_buffer(conn, &w->recv_buffer, 0);
		} else if(!strcmp(conn_type,"client")) {
			struct ds_client_rep *c = ds_client_rep_create(conn);
			mq_address_remote(conn,c->addr,&c->port);
			debug(D_DATASWARM,"new client from %s:%d\n",c->addr,c->port);
			hash_table_insert(m->client_table,manager_key,c);
			response = ds_message_standard_response(id,DS_RESULT_SUCCESS,NULL);
			mq_store_buffer(conn, &c->recv_buffer, 0);
		} else {
			/* ds_json_send_error_result(l, {"result": ["params.type"] }, DS_MSG_MALFORMED_PARAMETERS, stoptime); */
			mq_close(conn);
		}

DONE:
		if(response) {
			/* this response probably shouldn't be here */
			ds_json_send(conn, response);
			jx_delete(response);
		}

		free(manager_key);
		return 0;
}

void handle_client_message( struct ds_manager *m, struct ds_client_rep *c, time_t stoptime )
{
	int set_storage = 0;
	struct jx *msg;
	switch (mq_recv(c->connection, NULL)) {
		case MQ_MSG_NONE:
			abort(); //XXX ??
			break;
		case MQ_MSG_FD:
			//XXX handle received files
			mq_store_buffer(c->connection, &c->recv_buffer, 0);
			return;
		case MQ_MSG_BUFFER:
			msg = ds_parse_message(&c->recv_buffer);
			break;
	}

	if (!msg) {
		// malformed message
		abort();
	}

	const char *method = jx_lookup_string(msg,"method");
	struct jx *params = jx_lookup(msg,"params");
	if(!method || !params) {
		/* ds_json_send_error_result(l, msg, DS_MSG_MALFORMED_MESSAGE, stoptime); */
		/* should we disconnect the client on a message error? */
    	return;
	}

	if(!strcmp(method,"task-submit")) {
		ds_submit_task(params, m); 
	} else if(!strcmp(method,"task-delete")) {
        const char *uuid = jx_lookup_string(params, "task-id");
		ds_delete_task(uuid, m); 
	} else if(!strcmp(method,"task-retrieve")) {
        const char *uuid = jx_lookup_string(params, "task-id");
		ds_retrieve_task(uuid, m); 
	} else if(!strcmp(method,"file-submit")) {
		ds_declare_file(params, m);
		set_storage = 1;
	} else if(!strcmp(method,"file-commit")) {
        const char *uuid = jx_lookup_string(params, "file-id");
		ds_commit_file(uuid, m);
	} else if(!strcmp(method,"file-delete")) {
        const char *uuid = jx_lookup_string(params, "file-id");
		ds_delete_file(uuid, m);
	} else if(!strcmp(method,"file-copy")) {
        const char *uuid = jx_lookup_string(params, "file-id");
		ds_copy_file(uuid, m);
	} else if(!strcmp(method,"service-submit")) {
		/* ds_submit_service(); */
	} else if(!strcmp(method,"service-delete")) {
		/* ds_delete_service(); */
	} else if(!strcmp(method,"project-create")) {
		/* ds_create_project(); */
	} else if(!strcmp(method,"project-delete")) {
		/* ds_delete_project(); */
	} else if(!strcmp(method,"wait")) {
		/* ds_wait(); */
	} else if(!strcmp(method,"queue-empty")) {
		/* ds_queue_empty(); */
	} else if(!strcmp(method,"status")) {
		/* ds_status(); */
	} else {
		/* ds_json_send_error_result(l, msg, DS_MSG_UNEXPECTED_METHOD, stoptime); */
	}

	if (!set_storage) {
			mq_store_buffer(c->connection, &c->recv_buffer, 0);
	}
}

void handle_worker_message( struct ds_manager *m, struct ds_worker_rep *w, time_t stoptime )
{
	int set_storage = 0;
	struct jx *msg;
	switch (mq_recv(w->connection, NULL)) {
		case MQ_MSG_NONE:
			abort(); //XXX ??
			break;
		case MQ_MSG_FD:
			//XXX handle received files
			mq_store_buffer(w->connection, &w->recv_buffer, 0);
			return;
		case MQ_MSG_BUFFER:
			msg = ds_parse_message(&w->recv_buffer);
			break;
	}

	if (!msg) {
		//XXX malformed message
		abort();
	}

	const char *method = jx_lookup_string(msg,"method");
	const char *params = jx_lookup_string(msg,"params");
	if(!method || !params) {
		/* ds_json_send_error_result(l, msg, DS_MSG_MALFORMED_MESSAGE, stoptime); */
		/* disconnect worker */
		mq_close(w->connection);
		return;
	}

	debug(D_DATASWARM, "worker %s:%d rx: %s", w->addr, w->port, method);


	if(!strcmp(method,"task-change")) {
		/* */
	} else if(!strcmp(method,"blob-change")) {
		/* */
	} else if(!strcmp(method,"status-report")) {
		/* */
	} else if(!strcmp(method,"blob-get")) {
		/* */
		set_storage = 1;
	} else {
		/* ds_json_send_error_result(l, msg, DS_MSG_UNEXPECTED_METHOD, stoptime); */
	}

	if (!set_storage) {
		mq_store_buffer(w->connection, &w->recv_buffer, 0);
	}
}

int handle_messages( struct ds_manager *m )
{
	struct mq *conn;
	struct ds_client_rep *c;
	struct ds_worker_rep *w;
	while ((conn = mq_poll_readable(m->polling_group))) {
		char *key = string_format("%p", conn);

		if((c=hash_table_lookup(m->client_table,key))) {
			handle_client_message(m,c,time(0)+m->stall_timeout);
		} else if((w=hash_table_lookup(m->worker_table,key))) {
			handle_worker_message(m,w,time(0)+m->stall_timeout);
		} else {
			handle_handshake(m, conn);
		}

		free(key);
	}

	return 0;
}

int handle_connections(struct ds_manager *m) {
	for (struct mq *conn; (conn = mq_poll_acceptable(m->polling_group));) {
		assert(conn == m->manager_socket);

		conn = mq_accept(m->manager_socket);
		assert(conn);

		char addr[LINK_ADDRESS_MAX];
		int port;
		mq_address_remote(conn,addr,&port);
		debug(D_DATASWARM,"new connection from %s:%d\n",addr,port);

		mq_poll_add(m->polling_group, conn);

		buffer_t *buf = xxmalloc(sizeof(*buf));
		buffer_init(buf);
		mq_set_tag(conn, buf);
		mq_store_buffer(conn, buf, 0);
	}

	return 0;
}

int handle_errors(struct ds_manager *m) {
		for (struct mq *conn; (conn = mq_poll_error(m->polling_group));) {
			char *key = string_format("%p", conn);
			//XXX handle disconnects/errors, clean up
			mq_close(conn);
			hash_table_remove(m->worker_table, key);
			hash_table_remove(m->client_table, key);
		}

		return 0;
}

void server_main_loop( struct ds_manager *m )
{
	while(1) {
		update_catalog(m,0);
		handle_connections(m);
		handle_messages(m);
		handle_errors(m);
		process_files(m);
		process_tasks(m);

		if (mq_poll_wait(m->polling_group, time(0) + 300) == -1 && errno != EINTR) {
				perror("server_main_loop");
				break;
		}
	}
}

static const struct option long_options[] =
{
	{"name", required_argument, 0, 'N'},
	{"port", required_argument, 0, 'p'},
	{"debug", required_argument, 0, 'd'},
	{"debug-file", required_argument, 0, 'o'},
	{"help", no_argument, 0, 'h' },
	{"version", no_argument, 0, 'v' },
	{0, 0, 0, 0}
};

static void show_help( const char *cmd )
{
	printf("use: %s [options]\n",cmd);
	printf("where options are:\n");
	printf("-N --name=<name>          Set project name for catalog update.\n");
	printf("-p,--port=<port>          Port number to listen on.\n");
	printf("-d,--debug=<subsys>       Enable debugging for this subsystem.\n");
	printf("-o,--debug-file=<file>    Send debugging output to this file.\n");
	printf("-h,--help                 Show this help string\n");
	printf("-v,--version              Show version string\n");
}

int main(int argc, char *argv[])
{
	struct ds_manager * m = ds_manager_create();

	int c;
	while((c = getopt_long(argc, argv, "p:N:s:d:o:hv", long_options, 0))!=-1) {

		switch(c) {
			case 'N':
				m->project_name = optarg;
				break;
			case 'd':
				debug_flags_set(optarg);
				break;
			case 'o':
				debug_config_file(optarg);
				break;
			case 'p':
				m->server_port = atoi(optarg);
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

	m->manager_socket = mq_serve(NULL, m->server_port);
	if(!m->manager_socket) {
		printf("could not serve on port %d: %s\n", m->server_port,strerror(errno));
		return 1;
	}
	mq_poll_add(m->polling_group, m->manager_socket);

	char addr[LINK_ADDRESS_MAX];
	mq_address_local(m->manager_socket,addr,&m->server_port);
	debug(D_DATASWARM,"listening on port %d...\n",m->server_port);

	server_main_loop(m);

	debug(D_DATASWARM,"server shutting down.\n");

	return 0;
}

struct ds_manager * ds_manager_create()
{
	struct ds_manager *m = malloc(sizeof(*m));

	memset(m,0,sizeof(*m));

	m->worker_table = hash_table_create(0,0);
	m->client_table = hash_table_create(0,0);
	m->task_table   = hash_table_create(0,0);

    m->task_table = hash_table_create(0,0);
    m->file_table = hash_table_create(0,0);

	m->polling_group = mq_poll_create();

	m->connect_timeout = 5;
	m->stall_timeout = 30;
	m->update_interval = 60;
	m->message_id = 1000;
	m->project_name = "dataswarm";

	return m;
}


/* vim: set noexpandtab tabstop=4: */
