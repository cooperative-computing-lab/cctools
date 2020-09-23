
#include "dataswarm_rpc.h"
#include "comm/ds_message.h"
#include "dataswarm_blob_rep.h"
#include "dataswarm_task_rep.h"

#include "debug.h"
#include "itable.h"
#include "stringtools.h"
#include "xxmalloc.h"
#include "json.h"
#include "jx.h"

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* not an rpc. writes the file to disk for a corresponding blob-get get request. */
ds_result_t blob_get_aux( struct dataswarm_manager *m, struct dataswarm_worker_rep *r, const char *blobid );

/* test read responses from workers. */
ds_result_t dataswarm_rpc_get_response( struct dataswarm_manager *m, struct dataswarm_worker_rep *r)
{
	struct jx * msg = ds_json_recv(r->link,time(0)+m->connect_timeout);

	jx_int_t msgid = jx_lookup_integer(msg,"id");

	if(msgid == 0) {
		dataswarm_worker_rep_async_update(r,msg);
		jx_delete(msg);
		return -1;
	}

	ds_result_t result = jx_lookup_integer(msg,"result");

	/* it could be an rpc for a blob or a task, but we don't know yet. */
	struct dataswarm_blob_rep *b = (struct dataswarm_blob_rep *) itable_lookup(r->blob_of_rpc, msgid);
	struct dataswarm_task_rep *t = (struct dataswarm_task_rep *) itable_lookup(r->task_of_rpc, msgid);

	if(b) {
		b->result = result;
		if(b->result == DS_RESULT_SUCCESS) {
			if(b->state == DS_BLOB_WORKER_STATE_GET) {
				b->result = blob_get_aux(m,r,b->blobid);
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

	jx_delete(msg);

	return result;
}

/*
   Send a remote procedure call, freeing it, and returning the message id
   associated with the future response.
   */

jx_int_t dataswarm_rpc( struct dataswarm_manager *m, struct dataswarm_worker_rep *r, struct jx *rpc)
{
	jx_int_t msgid = m->message_id++;
	jx_insert_integer(rpc, "id", msgid);

	ds_json_send(r->link,rpc,time(0)+m->stall_timeout);
	jx_delete(rpc);

	return msgid;
}

jx_int_t dataswarm_rpc_for_blob( struct dataswarm_manager *m, struct dataswarm_worker_rep *r, struct dataswarm_blob_rep *b, struct jx *rpc, dataswarm_blob_worker_state_t in_transition )
{
	jx_int_t msgid = dataswarm_rpc(m, r, rpc);

	b->in_transition = in_transition;
	b->result = DS_RESULT_PENDING;;

	itable_insert(r->blob_of_rpc, msgid, b);

	return msgid;
}

jx_int_t dataswarm_rpc_for_task( struct dataswarm_manager *m, struct dataswarm_worker_rep *r, struct dataswarm_task_rep *t, struct jx *rpc, dataswarm_task_worker_state_t in_transition )
{
	jx_int_t msgid = dataswarm_rpc(m, r, rpc);

	t->in_transition = in_transition;
	t->result = DS_RESULT_PENDING;;

	itable_insert(r->task_of_rpc, msgid, t);

	return msgid;
}

jx_int_t dataswarm_rpc_blob_create( struct dataswarm_manager *m, struct dataswarm_worker_rep *r, const char *blobid, int64_t size, struct jx *metadata )
{
	struct dataswarm_blob_rep *b = hash_table_lookup(r->blobs, blobid);
	if(!b) {
		fatal("No blob with id %s exist at the worker.", blobid);
	}

	//define method and params of blob-create.
	//msg id will be added by dataswarm_rpc_blob_queue
	struct jx *msg = jx_objectv("method", jx_string("blob-create"),
								"params", jx_objectv("blob-id", jx_string(blobid),
													 "size",    jx_integer(size),
													 "metadata", metadata ? metadata : jx_null(),
													 NULL),
								NULL);

	return dataswarm_rpc_for_blob(m, r, b, msg, DS_BLOB_WORKER_STATE_CREATED);
}

jx_int_t dataswarm_rpc_blob_commit( struct dataswarm_manager *m, struct dataswarm_worker_rep *r, const char *blobid )
{
	struct dataswarm_blob_rep *b = hash_table_lookup(r->blobs, blobid);
	if(!b) {
		fatal("No blob with id %s exist at the worker.", blobid);
	}

	//define method and params of blob-commit.
	//msg id will be added by dataswarm_rpc_blob_queue
	struct jx *msg = jx_objectv("method", jx_string("blob-commit"),
								"params", jx_objectv("blob-id", jx_string(blobid),
													 NULL),
								NULL);

	return dataswarm_rpc_for_blob(m, r, b, msg, DS_BLOB_WORKER_STATE_COMMITTED);
}

jx_int_t dataswarm_rpc_blob_delete( struct dataswarm_manager *m, struct dataswarm_worker_rep *r, const char *blobid )
{
	struct dataswarm_blob_rep *b = hash_table_lookup(r->blobs, blobid);
	if(!b) {
		fatal("No blob with id %s exist at the worker.", blobid);
	}

	//define method and params of blob-delete.
	//msg id will be added by dataswarm_rpc_blob_queue
	struct jx *msg = jx_objectv("method", jx_string("blob-delete"),
								"params", jx_objectv("blob-id", jx_string(blobid),
													 NULL),
								NULL);

	return dataswarm_rpc_for_blob(m, r, b, msg, DS_BLOB_WORKER_STATE_DELETED);
}

jx_int_t dataswarm_rpc_blob_copy( struct dataswarm_manager *m, struct dataswarm_worker_rep *r, const char *blobid_source, const char *blobid_target )
{
	struct dataswarm_blob_rep *b = hash_table_lookup(r->blobs, blobid_target);
	if(!b) {
		fatal("No blob with id %s exist at the worker.", blobid_target);
	}

	//define method and params of blob-copy.
	//msg id will be added by dataswarm_rpc_blob_queue
	struct jx *msg = jx_objectv("method", jx_string("blob-copy"),
								"params", jx_objectv("blob-id", jx_string(blobid_target),
													 "blob-id-source", jx_string(blobid_source),
													 NULL),
								NULL);

	return dataswarm_rpc_for_blob(m, r, b, msg, DS_BLOB_WORKER_STATE_COPIED);
}


jx_int_t dataswarm_rpc_blob_put( struct dataswarm_manager *m, struct dataswarm_worker_rep *r, const char *blobid, const char *filename )
{
	struct dataswarm_blob_rep *b = hash_table_lookup(r->blobs, blobid);
	if(!b) {
		fatal("No blob with id %s exist at the worker.", blobid);
	}

	b->put_get_path = xxstrdup(filename);

	//define method and params of blob-put.
	//msg id will be added by dataswarm_rpc_blob_queue
	struct jx *msg = jx_objectv("method", jx_string("blob-put"),
								"params", jx_objectv("blob-id", jx_string(blobid),
													 NULL),
								NULL);


	jx_int_t msgid = dataswarm_rpc_for_blob(m, r, b, msg, DS_BLOB_WORKER_STATE_PUT);

	FILE *file = fopen(filename,"r");

	fseek(file,0,SEEK_END);
	int64_t length = ftell(file);
	fseek(file,0,SEEK_SET);
	char *filesize = string_format("%" PRId64 "\n",length);
	link_write(r->link,filesize,strlen(filesize),time(0)+m->stall_timeout);
	free(filesize);
	link_stream_from_file(r->link,file,length,time(0)+m->stall_timeout);
	fclose(file);

	return msgid;
}

/* not an rpc, but its state behaves likes one. GETs a file for a corresponding REQ_GET get request. */
jx_int_t dataswarm_rpc_blob_get( struct dataswarm_manager *m, struct dataswarm_worker_rep *r, const char *blobid, const char *filename )
{
	struct dataswarm_blob_rep *b = hash_table_lookup(r->blobs, blobid);
	if(!b) {
		fatal("No blob with id %s exist at the worker.", blobid);
	}

	b->put_get_path = xxstrdup(filename);

	//define method and params of blob-put.
	//msg id will be added by dataswarm_rpc_blob_queue
	struct jx *msg = jx_objectv("method", jx_string("blob-get"),
								"params", jx_objectv("blob-id", jx_string(blobid),
													 NULL),
								NULL);

	jx_int_t msgid = dataswarm_rpc_for_blob(m, r, b, msg, DS_BLOB_WORKER_STATE_GET);

	//This rpc does not modify the state of the blob at the worker:
	b->state = b->in_transition;

	return msgid;
}

ds_result_t blob_get_aux( struct dataswarm_manager *m, struct dataswarm_worker_rep *r, const char *blobid )
{
	struct dataswarm_blob_rep *b = hash_table_lookup(r->blobs, blobid);
	if(!b) {
		fatal("No blob with id %s exist at the worker.", blobid);
	}

	debug(D_DATASWARM, "Getting contents of blob: %s", blobid);

	ds_result_t result = DS_RESULT_UNABLE;

	int64_t actual;
	char line[16];
	actual = link_readline(r->link,line,sizeof(line),time(0)+m->stall_timeout);
	if(actual>0) {
		int64_t length = atoll(line);
		FILE *file = fopen(b->put_get_path,"w");
		if(file) {
			actual = link_stream_to_file(r->link,file,length,time(0)+m->stall_timeout);
			fclose(file);
			if(actual==length) {
				result = DS_RESULT_SUCCESS;
			}
		}

	}

	return result;
}

jx_int_t dataswarm_rpc_task_submit( struct dataswarm_manager *m, struct dataswarm_worker_rep *r, const char *taskid )
{
	struct dataswarm_task_rep *t = hash_table_lookup(r->tasks, taskid);
	assert(t);

	struct jx *rpc = jx_objectv("method", jx_string("task-submit"),
								"params", jx_copy(t->description),
								NULL);

	return dataswarm_rpc_for_task(m, r, t, rpc, DS_TASK_WORKER_STATE_SUBMITTED);
}

jx_int_t dataswarm_rpc_task_remove( struct dataswarm_manager *m, struct dataswarm_worker_rep *r, const char *taskid )
{
	struct dataswarm_task_rep *t = hash_table_lookup(r->tasks, taskid);
	assert(t);

	struct jx *rpc = jx_objectv("method", jx_string("task-remove"),
								"params", jx_objectv("task-id", jx_string(taskid),
													 NULL),
								NULL);

	return dataswarm_rpc_for_task(m, r, t, rpc, DS_TASK_WORKER_STATE_REMOVED);
}


/* vim: set noexpandtab tabstop=4: */
