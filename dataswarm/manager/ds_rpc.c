
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
	ds_result_t result = -1;
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

	jx_int_t msgid = jx_lookup_integer(msg,"id");

	if(msgid == 0) {
		ds_worker_rep_async_update(r,msg);
		goto DONE;
	}

	result = jx_lookup_integer(msg,"result");

	/* it could be an rpc for a blob or a task, but we don't know yet. */
	struct ds_blob_rep *b = (struct ds_blob_rep *) itable_lookup(r->blob_of_rpc, msgid);
	struct ds_task_rep *t = (struct ds_task_rep *) itable_lookup(r->task_of_rpc, msgid);

	if(b) {
		b->result = result;
		if(b->result == DS_RESULT_SUCCESS) {
			if(b->state == DS_BLOB_GET) {
				b->result = blob_get_aux(m,r,b->blobid);
				set_storage = 1;
			}
		}
		itable_remove(r->blob_of_rpc, msgid);
	} else if(t) {
		/* may be a response to a task */
		t->result = result;
		if(t->result == DS_RESULT_SUCCESS) {
			t->state = t->in_transition;
		}
		itable_remove(r->task_of_rpc, msgid);
	} else {
		debug(D_DATASWARM, "worker does not know about message id: %" PRId64, msgid);
	}

DONE:
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
	jx_int_t msgid = m->message_id++;
	jx_insert_integer(rpc, "id", msgid);

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
	struct jx *msg = jx_objectv("method", jx_string("blob-create"),
								"params", jx_objectv("blob-id", jx_string(blobid),
													 "size",    jx_integer(size),
													 "metadata", metadata ? metadata : jx_null(),
													 NULL),
								NULL);

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
	struct jx *msg = jx_objectv("method", jx_string("blob-commit"),
								"params", jx_objectv("blob-id", jx_string(blobid),
													 NULL),
								NULL);

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
	struct jx *msg = jx_objectv("method", jx_string("blob-delete"),
								"params", jx_objectv("blob-id", jx_string(blobid),
													 NULL),
								NULL);

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
	struct jx *msg = jx_objectv("method", jx_string("blob-copy"),
								"params", jx_objectv("blob-id", jx_string(blobid_target),
													 "blob-id-source", jx_string(blobid_source),
													 NULL),
								NULL);

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
	struct jx *msg = jx_objectv("method", jx_string("blob-put"),
								"params", jx_objectv("blob-id", jx_string(blobid),
													 NULL),
								NULL);


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
	struct jx *msg = jx_objectv("method", jx_string("blob-get"),
								"params", jx_objectv("blob-id", jx_string(blobid),
													 NULL),
								NULL);

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

	int file = open(b->put_get_path, O_WRONLY|O_CREAT|O_EXCL, 0777);
	if(file < 0) {
		mq_store_buffer(r->connection, &r->recv_buffer, 0);
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

	struct jx *rpc = jx_objectv("method", jx_string("task-submit"),
								"params", ds_task_to_jx(t->task),
								NULL);

	return ds_rpc_for_task(m, r, t, rpc, DS_TASK_ACTIVE);
}

jx_int_t ds_rpc_task_remove( struct ds_manager *m, struct ds_worker_rep *r, const char *taskid )
{
	struct ds_task_rep *t = hash_table_lookup(r->tasks, taskid);
	assert(t);

	struct jx *rpc = jx_objectv("method", jx_string("task-remove"),
								"params", jx_objectv("task-id", jx_string(taskid),
													 NULL),
								NULL);

	return ds_rpc_for_task(m, r, t, rpc, DS_TASK_DELETING);
}


jx_int_t ds_rpc_task_list( struct ds_manager *m, struct ds_worker_rep *r )
{
	struct jx *rpc = jx_objectv("method", jx_string("task-list"),"params",jx_object(0),0);
	return ds_rpc(m, r, rpc);
}

jx_int_t ds_rpc_blob_list( struct ds_manager *m, struct ds_worker_rep *r )
{
	struct jx *rpc = jx_objectv("method", jx_string("blob-list"),"params",jx_object(0),0);
	return ds_rpc(m, r, rpc);
}

/* vim: set noexpandtab tabstop=4: */
