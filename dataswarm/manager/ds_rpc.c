
#include "ds_rpc.h"
#include "ds_blob_rep.h"
#include "ds_task_attempt.h"
#include "ds_message.h"

#include "debug.h"
#include "itable.h"
#include "stringtools.h"
#include "xxmalloc.h"
#include "jx.h"

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

/* not an rpc. writes the file to disk for a corresponding blob-get get request. */
ds_result_t blob_get_aux( struct ds_manager *m, struct ds_worker_rep *r, const char *blobid );

//XXX This is just a placeholder
static ds_result_t handle_result_blob_(struct ds_manager *m, struct ds_worker_rep *w, struct ds_rpc *rpc, struct jx *data)
{
	struct ds_blob_rep *b = rpc->blob;
	assert(b);
	b->result = DS_RESULT_SUCCESS;
	return b->result;
}

//XXX This is just a placeholder
static ds_result_t handle_result_task_(struct ds_manager *m, struct ds_worker_rep *w, struct ds_rpc *rpc, struct jx *data)
{
	struct ds_task_attempt *t = rpc->task;
	assert(t);
	t->result = DS_RESULT_SUCCESS;
	t->state = rpc->task->in_transition;
	return t->result;
}

static ds_result_t handle_result_blob_get(struct ds_manager *m, struct ds_worker_rep *w, struct ds_rpc *rpc, struct jx *data, int *set_storage)
{
	struct ds_blob_rep *b = rpc->blob;
	assert(b);
	assert(b->state == DS_BLOB_GET);
	b->result = blob_get_aux(m,w,b->blobid);
	*set_storage = 1;
	return b->result;
}

ds_result_t ds_rpc_handle_result( struct ds_manager *m, struct ds_worker_rep *w, jx_int_t msgid, struct jx *data )
{
	int set_storage = 0;
	ds_result_t result = DS_RESULT_SUCCESS;

	debug(D_DATASWARM, "worker %s:%d rx: ok (%" PRIiJX ")", w->addr, w->port, msgid);

	struct ds_rpc *rpc = itable_remove(w->rpcs, msgid);
	assert(rpc);

	switch (rpc->operation) {
		case DS_RPC_OP_TASK_SUBMIT:
		result = handle_result_task_(m, w, rpc, data);
		break;
		case DS_RPC_OP_TASK_GET:
		result = handle_result_task_(m, w, rpc, data);
		break;
		case DS_RPC_OP_TASK_REMOVE:
		result = handle_result_task_(m, w, rpc, data);
		break;
		case DS_RPC_OP_TASK_LIST:
		//TODO
		break;
		case DS_RPC_OP_BLOB_CREATE:
		result = handle_result_blob_(m, w, rpc, data);
		break;
		case DS_RPC_OP_BLOB_PUT:
		result = handle_result_blob_(m, w, rpc, data);
		break;
		case DS_RPC_OP_BLOB_GET:
		result = handle_result_blob_get(m, w, rpc, data, &set_storage);
		break;
		case DS_RPC_OP_BLOB_DELETE:
		result = handle_result_blob_(m, w, rpc, data);
		break;
		case DS_RPC_OP_BLOB_COMMIT:
		result = handle_result_blob_(m, w, rpc, data);
		break;
		case DS_RPC_OP_BLOB_COPY:
		result = handle_result_blob_(m, w, rpc, data);
		break;
		case DS_RPC_OP_BLOB_LIST:
		//TODO
		break;
		default:
		fatal("missing rpc handler!");
	}

	if (!set_storage) {
		mq_store_buffer(w->connection, &w->recv_buffer, 0);
	}
	return result;
}

ds_result_t ds_rpc_handle_notification( struct ds_worker_rep *w, const char *method, struct jx *params )
{
	ds_result_t result = DS_RESULT_SUCCESS;

	debug(D_DATASWARM, "worker %s:%d rx: %s", w->addr, w->port, method);

	if(!method) {
		result = DS_RESULT_BAD_METHOD;
	} else if(!strcmp(method, "task-update")) {
		result = ds_worker_rep_update_task(w, params);
	} else if(!strcmp(method, "blob-update")) {
		result = ds_worker_rep_update_blob(w, params);
	} else if(!strcmp(method, "status-report")) {
		// update stats
	} else {
		result = DS_RESULT_BAD_METHOD;
	}

	mq_store_buffer(w->connection, &w->recv_buffer, 0);
	return result;
}

ds_result_t ds_rpc_handle_error(struct ds_worker_rep *w, jx_int_t msgid, jx_int_t code, const char *message, struct jx *data) {
	ds_result_t result = code;

	debug(D_DATASWARM, "worker %s:%d rx: error (%" PRIiJX ") <%" PRIiJX ": %s>", w->addr, w->port, msgid, code, message);
	itable_remove(w->rpcs, msgid);
	//XXX do something about errors

	mq_store_buffer(w->connection, &w->recv_buffer, 0);
	return result;
}

ds_result_t ds_rpc_handle_message( struct ds_manager *m, struct ds_worker_rep *w)
{
	ds_result_t result = DS_RESULT_SUCCESS;
	struct jx *msg = NULL;
	switch (mq_recv(w->connection, NULL)) {
		case MQ_MSG_NONE:
			return DS_RESULT_SUCCESS;
		case MQ_MSG_BUFFER:
			msg = ds_parse_message(&w->recv_buffer);
			break;
		case MQ_MSG_FD:
			mq_store_buffer(w->connection, &w->recv_buffer, 0);
			return 0;
	}

	if (!msg) {
		debug(D_DATASWARM, "malformed message from worker %s:%d, disconnecting", w->addr, w->port);
		result = DS_RESULT_BAD_MESSAGE;
		goto ERROR;
	}

	jx_int_t msgid = 0;
	const char *method = NULL;
	struct jx *params = NULL;
	struct jx *data = NULL;
	jx_int_t err_code = 0;
	const char *err_message = NULL;
	struct jx *err_data = NULL;

	if (ds_unpack_notification(msg, &method, &params) == DS_RESULT_SUCCESS) {
		result = ds_rpc_handle_notification(w, method, params);
	} else if (ds_unpack_result(msg, &msgid, &data) == DS_RESULT_SUCCESS) {
		result = ds_rpc_handle_result(m, w, msgid, data);
	} else if (ds_unpack_error(msg, &msgid, &err_code, &err_message, &err_data) == DS_RESULT_SUCCESS) {
		result = ds_rpc_handle_error(w, msgid, err_code, err_message, err_data);
	} else {
		// workers should never issue requests
		abort();
	}

	jx_delete(msg);
	return result;

ERROR:
	ds_worker_rep_disconnect(w);
	set_remove(m->worker_table, w);
	jx_delete(msg);
	return result;
}

/*
   Send a remote procedure call, freeing it, and returning the message id
   associated with the future response.
   */

jx_int_t ds_rpc( struct ds_manager *m, struct ds_worker_rep *r, struct jx *rpc, struct ds_rpc *state)
{
	struct jx *i = jx_lookup(rpc, "id");
	assert(jx_istype(i, JX_INTEGER));
	jx_int_t msgid = i->u.integer_value;
	itable_insert(r->rpcs, msgid, state);

	ds_json_send(r->connection,rpc);

	jx_delete(rpc);
	return msgid;
}

jx_int_t ds_rpc_for_blob( struct ds_manager *m, struct ds_worker_rep *r, struct ds_blob_rep *b, struct jx *rpc, ds_blob_state_t in_transition, ds_rpc_op_t op )
{
	b->in_transition = in_transition;
	b->result = DS_RESULT_PENDING;
	struct ds_rpc *state = xxcalloc(1, sizeof(*state));
	state->operation = op;
	state->blob = b;

	return ds_rpc(m, r, rpc, state);
}

jx_int_t ds_rpc_for_task( struct ds_manager *m, struct ds_worker_rep *r, struct ds_task_attempt *t, struct jx *rpc, ds_task_try_state_t in_transition, ds_rpc_op_t op )
{
	t->in_transition = in_transition;
	t->result = DS_RESULT_PENDING;
	struct ds_rpc *state = xxcalloc(1, sizeof(*state));
	state->operation = op;
	state->task = t;

	return ds_rpc(m, r, rpc, state);
}

jx_int_t ds_rpc_blob_create( struct ds_manager *m, struct ds_worker_rep *r, const char *blobid, int64_t size, struct jx *metadata )
{
	struct ds_blob_rep *b = hash_table_lookup(r->blobs, blobid);
	if(!b) {
		fatal("No blob with id %s exist at the worker.", blobid);
	}

	//define method and params of blob-create.
	//msg id will be added by ds_rpc_blob_queue
	struct jx *msg = ds_message_request("blob-create",
								jx_objectv("blob-id", jx_string(blobid),
											"size",    jx_integer(size),
											"metadata", metadata ? metadata : jx_null(),
											NULL));

	return ds_rpc_for_blob(m, r, b, msg, DS_BLOB_RO, DS_RPC_OP_BLOB_CREATE);
}

jx_int_t ds_rpc_blob_commit( struct ds_manager *m, struct ds_worker_rep *r, const char *blobid )
{
	struct ds_blob_rep *b = hash_table_lookup(r->blobs, blobid);
	if(!b) {
		fatal("No blob with id %s exist at the worker.", blobid);
	}

	//define method and params of blob-commit.
	//msg id will be added by ds_rpc_blob_queue
	struct jx *msg = ds_message_request("blob-commit",
								jx_objectv("blob-id", jx_string(blobid),
											NULL));

	return ds_rpc_for_blob(m, r, b, msg, DS_BLOB_RO, DS_RPC_OP_BLOB_COMMIT);
}

jx_int_t ds_rpc_blob_delete( struct ds_manager *m, struct ds_worker_rep *r, const char *blobid )
{
	struct ds_blob_rep *b = hash_table_lookup(r->blobs, blobid);
	if(!b) {
		fatal("No blob with id %s exist at the worker.", blobid);
	}

	//define method and params of blob-delete.
	//msg id will be added by ds_rpc_blob_queue
	struct jx *msg = ds_message_request("blob-delete",
								jx_objectv("blob-id", jx_string(blobid),
											NULL));

	return ds_rpc_for_blob(m, r, b, msg, DS_BLOB_DELETING, DS_RPC_OP_BLOB_DELETE);
}

jx_int_t ds_rpc_blob_copy( struct ds_manager *m, struct ds_worker_rep *r, const char *blobid_source, const char *blobid_target )
{
	struct ds_blob_rep *b = hash_table_lookup(r->blobs, blobid_target);
	if(!b) {
		fatal("No blob with id %s exist at the worker.", blobid_target);
	}

	//define method and params of blob-copy.
	//msg id will be added by ds_rpc_blob_queue
	struct jx *msg = ds_message_request("blob-copy",
								jx_objectv("blob-id", jx_string(blobid_target),
											"blob-id-source", jx_string(blobid_source),
											NULL));

	return ds_rpc_for_blob(m, r, b, msg, DS_BLOB_COPIED, DS_RPC_OP_BLOB_COPY);
}


jx_int_t ds_rpc_blob_put( struct ds_manager *m, struct ds_worker_rep *r, const char *blobid, const char *filename )
{
	struct ds_blob_rep *b = hash_table_lookup(r->blobs, blobid);
	if(!b) {
		fatal("No blob with id %s exist at the worker.", blobid);
	}

	b->put_get_path = xxstrdup(filename);

	//define method and params of blob-put.
	//msg id will be added by ds_rpc_blob_queue
	struct jx *msg = ds_message_request("blob-put",
								jx_objectv("blob-id", jx_string(blobid),
											NULL));


	jx_int_t msgid = ds_rpc_for_blob(m, r, b, msg, DS_BLOB_PUT, DS_RPC_OP_BLOB_PUT);

	int file = open(filename, O_RDONLY);
	if (file < 0) {
			fatal("couldd't open %s", filename);
	}

	mq_send_fd(r->connection, file, 0);

	return msgid;
}

/* not an rpc, but its state behaves likes one. GETs a file for a corresponding REQ_GET get request. */
jx_int_t ds_rpc_blob_get( struct ds_manager *m, struct ds_worker_rep *r, const char *blobid, const char *filename )
{
	struct ds_blob_rep *b = hash_table_lookup(r->blobs, blobid);
	if(!b) {
		fatal("No blob with id %s exist at the worker.", blobid);
	}

	b->put_get_path = xxstrdup(filename);

	//define method and params of blob-put.
	//msg id will be added by ds_rpc_blob_queue
	struct jx *msg = ds_message_request("blob-get",
								jx_objectv("blob-id", jx_string(blobid),
											NULL));

	jx_int_t msgid = ds_rpc_for_blob(m, r, b, msg, DS_BLOB_GET, DS_RPC_OP_BLOB_GET);

	//This rpc does not modify the state of the blob at the worker:
	b->state = b->in_transition;

	return msgid;
}

ds_result_t blob_get_aux( struct ds_manager *m, struct ds_worker_rep *r, const char *blobid )
{
	struct ds_blob_rep *b = hash_table_lookup(r->blobs, blobid);
	if(!b) {
		fatal("No blob with id %s exist at the worker.", blobid);
	}

	debug(D_DATASWARM, "Getting contents of blob: %s", blobid);

	ds_result_t result = DS_RESULT_UNABLE;

	//XXX use O_EXCL once we're past the test script
	int file = open(b->put_get_path, O_WRONLY|O_CREAT, 0777);
	if(file < 0) {
		mq_store_buffer(r->connection, &r->recv_buffer, 0);
		debug(D_DATASWARM, "unable to open %s to receive blob %s", b->put_get_path, b->blobid);
		/* If this open fails, we don't have a way to stop the worker from
		 * proceeding to send the file contents. The only thing we can do here
		 * is close the connection.
		 */
		 ds_worker_rep_disconnect(r);
		 set_remove(m->worker_table, r);
		 result = DS_RESULT_UNABLE;
	} else {
		mq_store_fd(r->connection, file, 0);
		result = DS_RESULT_SUCCESS;
	}

	return result;
}

jx_int_t ds_rpc_task_submit( struct ds_manager *m, struct ds_worker_rep *r, const char *taskid )
{
	struct ds_task_attempt *t = hash_table_lookup(r->tasks, taskid);
	assert(t);

	struct jx *rpc = ds_message_request("task-submit", ds_task_to_jx(t->task));

	return ds_rpc_for_task(m, r, t, rpc, DS_TASK_TRY_PENDING, DS_RPC_OP_TASK_SUBMIT);
}

jx_int_t ds_rpc_task_remove( struct ds_manager *m, struct ds_worker_rep *r, const char *taskid )
{
	struct ds_task_attempt *t = hash_table_lookup(r->tasks, taskid);
	assert(t);

	struct jx *rpc = ds_message_request("task-remove",
								jx_objectv("task-id", jx_string(taskid),
											NULL));

	return ds_rpc_for_task(m, r, t, rpc, DS_TASK_TRY_DELETED, DS_RPC_OP_TASK_REMOVE);
}


jx_int_t ds_rpc_task_list( struct ds_manager *m, struct ds_worker_rep *r )
{
	struct jx *rpc = ds_message_request("task-list", NULL);
	struct ds_rpc *state = xxcalloc(1, sizeof(*state));
	state->operation = DS_RPC_OP_TASK_LIST;
	return ds_rpc(m, r, rpc, state);
}

jx_int_t ds_rpc_blob_list( struct ds_manager *m, struct ds_worker_rep *r )
{
	struct jx *rpc = ds_message_request("blob-list", NULL);
	struct ds_rpc *state = xxcalloc(1, sizeof(*state));
	state->operation = DS_RPC_OP_BLOB_LIST;
	return ds_rpc(m, r, rpc, state);
}

/* vim: set noexpandtab tabstop=4: */
