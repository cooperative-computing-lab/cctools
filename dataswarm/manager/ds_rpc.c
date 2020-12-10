
#include "ds_rpc.h"
#include "ds_blob_rep.h"
#include "ds_task_rep.h"
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

/* test read responses from workers. */
ds_result_t ds_rpc_get_response( struct ds_manager *m, struct ds_worker_rep *r)
{
	ds_result_t result = DS_RESULT_SUCCESS;
	int set_storage = 0;
	struct jx *msg = NULL;
	switch (mq_recv(r->connection, NULL)) {
		case MQ_MSG_NONE:
			return 0;
		case MQ_MSG_BUFFER:
			msg = ds_parse_message(&r->recv_buffer);
			assert(msg);
			break;
		case MQ_MSG_FD:
			mq_store_buffer(r->connection, &r->recv_buffer, 0);
			return 0;
	}

	jx_int_t msgid = 0;
	const char *method = NULL;
	struct jx *params = NULL;
	struct jx *data = NULL;
	jx_int_t err_code = 0;
	const char *err_message = NULL;
	struct jx *err_data = NULL;

	if (ds_unpack_notification(msg, &method, &params) == DS_RESULT_SUCCESS) {
		ds_worker_rep_async_update(r, method, params);
	} else if (ds_unpack_result(msg, &msgid, &data) == DS_RESULT_SUCCESS) {
		/* it could be an rpc for a blob or a task, but we don't know yet. */
		struct ds_blob_rep *b = (struct ds_blob_rep *) itable_lookup(r->blob_of_rpc, msgid);
		struct ds_task_rep *t = (struct ds_task_rep *) itable_lookup(r->task_of_rpc, msgid);

		if(b) {
			b->result = DS_RESULT_SUCCESS;
			if(b->state == DS_BLOB_GET) {
				b->result = blob_get_aux(m,r,b->blobid);
				set_storage = 1;
				result = b->result;
			}
			itable_remove(r->blob_of_rpc, msgid);
		} else if(t) {
			/* may be a response to a task */
			t->result = DS_RESULT_SUCCESS;
			t->state = t->in_transition;
			itable_remove(r->task_of_rpc, msgid);
		} else {
			debug(D_DATASWARM, "worker does not know about message id: %" PRId64, msgid);
		}
	} else if (ds_unpack_error(msg, &msgid, &err_code, &err_message, &err_data) == DS_RESULT_SUCCESS) {
		result = err_code;
		struct ds_blob_rep *b = (struct ds_blob_rep *) itable_lookup(r->blob_of_rpc, msgid);
		struct ds_task_rep *t = (struct ds_task_rep *) itable_lookup(r->task_of_rpc, msgid);

		if(b) {
			b->result = result;
			debug(D_DATASWARM, "blob rpc %" PRId64 " failed", msgid);
			//XXX print detailed info, clean up, etc.
			itable_remove(r->blob_of_rpc, msgid);
		} else if (t) {
			t->result = result;
			debug(D_DATASWARM, "task rpc %" PRId64 " failed", msgid);
			//XXX print detailed info, clean up, etc.
			itable_remove(r->task_of_rpc, msgid);
		} else {
			debug(D_DATASWARM, "worker does not know about message id: %" PRId64, msgid);
		}
	} else {
		abort();
	}

	if (!set_storage) {
		mq_store_buffer(r->connection, &r->recv_buffer, 0);
	}

	jx_delete(msg);

	return result;
}

/*
   Send a remote procedure call, freeing it, and returning the message id
   associated with the future response.
   */

jx_int_t ds_rpc( struct ds_manager *m, struct ds_worker_rep *r, struct jx *rpc)
{
	assert(jx_lookup(rpc, "id"));
	jx_int_t msgid = jx_lookup_integer(rpc, "id");

	ds_json_send(r->connection,rpc);
	jx_delete(rpc);

	return msgid;
}

jx_int_t ds_rpc_for_blob( struct ds_manager *m, struct ds_worker_rep *r, struct ds_blob_rep *b, struct jx *rpc, ds_blob_state_t in_transition )
{
	jx_int_t msgid = ds_rpc(m, r, rpc);

	b->in_transition = in_transition;
	b->result = DS_RESULT_PENDING;;

	itable_insert(r->blob_of_rpc, msgid, b);

	return msgid;
}

jx_int_t ds_rpc_for_task( struct ds_manager *m, struct ds_worker_rep *r, struct ds_task_rep *t, struct jx *rpc, ds_task_state_t in_transition )
{
	jx_int_t msgid = ds_rpc(m, r, rpc);

	t->in_transition = in_transition;
	t->result = DS_RESULT_PENDING;;

	itable_insert(r->task_of_rpc, msgid, t);

	return msgid;
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

	return ds_rpc_for_blob(m, r, b, msg, DS_BLOB_RO);
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

	return ds_rpc_for_blob(m, r, b, msg, DS_BLOB_RO);
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

	return ds_rpc_for_blob(m, r, b, msg, DS_BLOB_DELETING);
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

	return ds_rpc_for_blob(m, r, b, msg, DS_BLOB_COPIED);
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


	jx_int_t msgid = ds_rpc_for_blob(m, r, b, msg, DS_BLOB_PUT);

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

	jx_int_t msgid = ds_rpc_for_blob(m, r, b, msg, DS_BLOB_GET);

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
		 //XXX we need a worker_die() function?
		 mq_close(r->connection);
	} else {
		mq_store_fd(r->connection, file, 0);
		result = DS_RESULT_SUCCESS;
	}

	return result;
}

jx_int_t ds_rpc_task_submit( struct ds_manager *m, struct ds_worker_rep *r, const char *taskid )
{
	struct ds_task_rep *t = hash_table_lookup(r->tasks, taskid);
	assert(t);

	struct jx *rpc = ds_message_request("task-submit", ds_task_to_jx(t->task));

	return ds_rpc_for_task(m, r, t, rpc, DS_TASK_ACTIVE);
}

jx_int_t ds_rpc_task_remove( struct ds_manager *m, struct ds_worker_rep *r, const char *taskid )
{
	struct ds_task_rep *t = hash_table_lookup(r->tasks, taskid);
	assert(t);

	struct jx *rpc = ds_message_request("task-remove",
								jx_objectv("task-id", jx_string(taskid),
											NULL));

	return ds_rpc_for_task(m, r, t, rpc, DS_TASK_DELETING);
}


jx_int_t ds_rpc_task_list( struct ds_manager *m, struct ds_worker_rep *r )
{
	struct jx *rpc = ds_message_request("task-list", NULL);
	return ds_rpc(m, r, rpc);
}

jx_int_t ds_rpc_blob_list( struct ds_manager *m, struct ds_worker_rep *r )
{
	struct jx *rpc = ds_message_request("blob-list", NULL);
	return ds_rpc(m, r, rpc);
}

/* vim: set noexpandtab tabstop=4: */
