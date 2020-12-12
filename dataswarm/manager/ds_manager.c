/*
Copyright (C) 2020- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
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
#include "ppoll_compat.h"

#include "ds_message.h"
#include "ds_task.h"
#include "ds_worker_rep.h"
#include "ds_client_rep.h"
#include "ds_blob_rep.h"
#include "ds_task_attempt.h"
#include "ds_manager.h"
#include "ds_client_ops.h"
#include "ds_file.h"
#include "ds_catalog_update.h"
#include "ds_scheduler.h"

#include "ds_test.h"

/* declares a blob in a worker so that it can be manipulated via blob rpcs. */
struct ds_blob_rep *ds_manager_add_blob_to_worker( struct ds_manager *m, struct ds_worker_rep *r, const char *blobid) {
	struct ds_blob_rep *b = hash_table_lookup(r->blobs, blobid);
	if(b) {
		/* cannot create an already declared blob. This could only happen with
		 * a bug, as we have control of the create messages.*/
		fatal("blob-id %s already created at worker.", blobid);
	}

	b = calloc(1,sizeof(struct ds_blob_rep));
	b->state = DS_BLOB_NEW;
	b->in_transition = b->state;
	b->result = DS_RESULT_SUCCESS;

	b->blobid = xxstrdup(blobid);

	hash_table_insert(r->blobs, blobid, b);

	return b;
}

/* declares a task in a worker so that it can be manipulated via task rpcs. */
struct ds_task_attempt *ds_manager_add_task_to_worker( struct ds_manager *m, struct ds_worker_rep *r, const char *taskid) {
	struct ds_task_attempt *t = hash_table_lookup(r->tasks, taskid);
	if(t) {
		/* cannot create an already declared task. This could only happen with
		 * a bug, as we have control of the create messages.*/
		fatal("task-id %s already created at worker.", taskid);
	}

	struct ds_task *task = hash_table_lookup(m->task_table, taskid);
	if(!task) {
		/* could not find task with taskid. This could only happen with a bug,
		 * as we have control of the create messages.*/
		fatal("task-id %s does not exist.", taskid);
	}

	t = ds_task_attempt_create(task);
	t->worker = task->worker;
	hash_table_insert(r->tasks, taskid, t);
	t->in_transition = DS_TASK_TRY_PENDING;

	ds_rpc_task_submit(m, r, taskid);

	return t;
}

void ds_manager_task_notify( struct ds_manager *m, struct ds_task *t, struct jx *msg) {
	assert(t);
	assert(msg);

	struct set *dead = set_create(0);

	struct ds_client_rep *c;
	set_first_element(t->subscribers);
	while ((c = set_next_element(t->subscribers))) {
		if (set_lookup(m->client_table, c)) {
			ds_client_rep_notify(c, jx_copy(msg));
		} else {
			set_insert(dead, c);
		}
	}

	jx_delete(msg);

	set_first_element(dead);
	while ((c = set_next_element(dead))) {
		set_remove(t->subscribers, c);
	}
	set_delete(dead);
}

int handle_handshake(struct ds_manager *m, struct mq *conn) {
	char addr[LINK_ADDRESS_MAX];
	int port;

	const char *method = NULL;
	struct jx *params = NULL;

	switch (mq_recv(conn, NULL)) {
		case MQ_MSG_NONE:
			return 0;
		case MQ_MSG_FD:
			abort(); // should never happen
		case MQ_MSG_BUFFER:
			break;
	}

	mq_address_remote(conn, addr, &port);

	buffer_t *buf = mq_get_tag(conn);
	assert(buf);
	mq_set_tag(conn, NULL);

	struct jx *msg = ds_parse_message(buf);
	buffer_free(buf);
	free(buf);

	if (!msg) {
		debug(D_DATASWARM, "malformed handshake from %s:%d, disconnecting", addr, port);
		mq_close(conn);
		goto DONE;
	}

	if (ds_unpack_notification(msg, &method, &params) != DS_RESULT_SUCCESS
			|| strcmp(method, "handshake")
			|| !jx_istype(params, JX_OBJECT)) {
		debug(D_DATASWARM, "invalid handshake from connection %s:%d, disconnecting", addr, port);
		mq_close(conn);
		goto DONE;
	}

	const char *conn_type = jx_lookup_string(params, "type");

	if (conn_type && !strcmp(conn_type, "worker")) {
		struct ds_worker_rep *w = ds_worker_rep_create(conn);
		mq_address_remote(conn,w->addr,&w->port);
		debug(D_DATASWARM,"new worker from %s:%d\n",w->addr,w->port);
		set_insert(m->worker_table, w);
		mq_set_tag(conn, w);
		mq_store_buffer(conn, &w->recv_buffer, 0);
	} else if (conn_type && !strcmp(conn_type,"client")) {
		struct ds_client_rep *c = ds_client_rep_create(conn);
		c->nowait = jx_lookup_boolean(params, "nowait");
		mq_address_remote(conn,c->addr,&c->port);
		debug(D_DATASWARM,"new client from %s:%d\n",c->addr,c->port);
		set_insert(m->client_table, c);
		mq_set_tag(conn, c);
		mq_store_buffer(conn, &c->recv_buffer, 0);
	} else {
		debug(D_DATASWARM, "invalid handshake parameters from connection %s:%d, disconnecting", addr, port);
		mq_close(conn);
	}

DONE:
	jx_delete(msg);
	return 0;
}

void handle_client_message( struct ds_manager *m, struct ds_client_rep *c )
{
	int set_storage = 0;
	int should_send_response = 1;
	ds_result_t result = DS_RESULT_SUCCESS;
	struct jx *msg = NULL;
	switch (mq_recv(c->connection, NULL)) {
		case MQ_MSG_NONE:
			return;
		case MQ_MSG_FD:
			//XXX handle received files
			mq_store_buffer(c->connection, &c->recv_buffer, 0);
			return;
		case MQ_MSG_BUFFER:
			msg = ds_parse_message(&c->recv_buffer);
			break;
	}

	if (!msg) {
		debug(D_DATASWARM, "malformed message from client %s:%d, disconnecting", c->addr, c->port);
		goto ERROR;
	}

	const char *method = NULL;
	struct jx *params = NULL;
	jx_int_t id = 0;

	// Client shouldn't send notifications, and the manager never issues requests
	// to the client, so the only valid RPC form to get is request.
	if(ds_unpack_request(msg, &method, &id, &params) != DS_RESULT_SUCCESS) {
		debug(D_DATASWARM, "invalid message from client %s:%d, disconnecting", c->addr, c->port);
		goto ERROR;
	}

	struct jx *response_data = NULL;

	if(!strcmp(method,"task-submit")) {
		result = ds_client_task_submit(m, c, params, &response_data);
	} else if(!strcmp(method,"task-delete")) {
        const char *uuid = jx_lookup_string(params, "task-id");
		ds_client_task_delete(m, uuid);
	} else if(!strcmp(method,"task-retrieve")) {
        const char *uuid = jx_lookup_string(params, "task-id");
		ds_client_task_retrieve(m, uuid);
	} else if(!strcmp(method,"file-create")) {
		struct ds_file *f = ds_client_file_declare(m, params);
		if(f) {
			response_data = jx_objectv("file-id", jx_string(f->fileid), NULL);
		} else {
			result = DS_RESULT_UNABLE;
		}
	} else if(!strcmp(method,"file-put")) {
		/* fix */
		set_storage = 1;
	} else if(!strcmp(method,"file-submit")) {
		ds_client_file_declare(m, params);
		set_storage = 1;
	} else if(!strcmp(method,"file-commit")) {
        const char *uuid = jx_lookup_string(params, "file-id");
		ds_client_file_commit(m, uuid);
	} else if(!strcmp(method,"file-delete")) {
        const char *uuid = jx_lookup_string(params, "file-id");
		ds_client_file_delete(m, uuid);
	} else if(!strcmp(method,"file-copy")) {
        const char *uuid = jx_lookup_string(params, "file-id");
		ds_client_file_copy(m, uuid);
	} else if(!strcmp(method,"service-submit")) {
		ds_client_service_submit(m, params);
	} else if(!strcmp(method,"service-delete")) {
		ds_client_service_delete(m, params);
	} else if(!strcmp(method,"project-create")) {
		ds_client_project_create(m, params);
	} else if(!strcmp(method,"project-delete")) {
		ds_client_project_delete(m, params);
	} else if(!strcmp(method,"wait")) {
		if (c->nowait) {
			result = DS_RESULT_BAD_METHOD;
		} else {
			should_send_response = 0;
			ds_client_wait(m, c, id, params);
		}
	} else if(!strcmp(method,"queue-empty")) {
		ds_client_queue_empty(m, params);
	} else if(!strcmp(method,"status")) {
		ds_client_status(m, params);
	} else {
		result = DS_RESULT_BAD_METHOD;
	}

	if (!set_storage) {
		mq_store_buffer(c->connection, &c->recv_buffer, 0);
	}

	if (should_send_response) {
		struct jx *response = ds_message_response(id, result, response_data);
		ds_json_send(c->connection, response);
		jx_delete(response);
	}

	jx_delete(msg);
	return;

ERROR:
	ds_client_rep_disconnect(c);
	set_remove(m->client_table, c);
	jx_delete(msg);
}

int handle_messages( struct ds_manager *m )
{
	struct mq *conn;
	while ((conn = mq_poll_readable(m->polling_group))) {
		void *key = mq_get_tag(conn);
		if (set_lookup(m->client_table, key)) {
			handle_client_message(m, key);
		} else if (set_lookup(m->worker_table, key)) {
			ds_rpc_handle_message(m, key);
		} else {
			handle_handshake(m, conn);
		}
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
		void *key = mq_get_tag(conn);

		if (set_remove(m->worker_table, key)) {
			struct ds_worker_rep *w = key;
			debug(D_DATASWARM, "worker disconnect (%s:%d): %s",
				w->addr, w->port, strerror(mq_geterror(conn)));
			ds_worker_rep_disconnect(w);
		} else if (set_remove(m->client_table, key)) {
			struct ds_client_rep *c = key;
			debug(D_DATASWARM, "client disconnect (%s:%d): %s",
				c->addr, c->port, strerror(mq_geterror(conn)));
			ds_client_rep_disconnect(c);
		} else {
			char addr[LINK_ADDRESS_MAX];
			int port;
			mq_address_remote(conn, addr, &port);
			debug(D_DATASWARM, "disconnect (%s:%d): %s",
				addr, port, strerror(mq_geterror(conn)));
			buffer_t *buf = mq_get_tag(conn);
			assert(buf);
			buffer_free(buf);
			free(buf);
			mq_close(conn);
		}
	}

	return 0;
}

void server_main_loop( struct ds_manager *m )
{
	while(1) {
		ds_catalog_update(m,0);
		handle_connections(m);
		handle_messages(m);
		handle_errors(m);
		ds_scheduler(m);

		if (mq_poll_wait(m->polling_group, time(0) + 10) == -1 && errno != EINTR) {
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

	ppoll_compat_set_up_sigchld();

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

	m->worker_table = set_create(0);
	m->client_table = set_create(0);
    m->task_table = hash_table_create(0,0);
    m->file_table = hash_table_create(0,0);

	m->polling_group = mq_poll_create();

	m->connect_timeout = 5;
	m->stall_timeout = 30;
	m->update_interval = 60;
	m->project_name = "dataswarm";

	return m;
}


/* vim: set noexpandtab tabstop=4 shiftwidth=4: */
