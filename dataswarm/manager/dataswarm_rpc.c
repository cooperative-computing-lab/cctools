
#include "dataswarm_rpc.h"
#include "dataswarm_message.h"
#include "dataswarm_blob_rep.h"

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

/* not an rpc, but its state behaves likes one. GETs a file for a corresponding REQ_GET get request. */
dataswarm_result_t dataswarm_blob_get( struct dataswarm_manager *m, struct dataswarm_worker_rep *r, const char *blobid );

/* test read responses from workers. */
dataswarm_result_t dataswarm_rpc_get_response( struct dataswarm_manager *m, struct dataswarm_worker_rep *r)
{
    struct jx * msg = dataswarm_json_recv(r->link,time(0)+m->connect_timeout);

    jx_int_t msgid = jx_lookup_integer(msg,"id");

    if(msgid == 0) {
        dataswarm_worker_rep_async_update(r,msg);
        jx_delete(msg);
        return -1;
    }

    dataswarm_result_t result = jx_lookup_integer(msg,"result");

    struct dataswarm_blob_rep *b = (struct dataswarm_blob_rep *) itable_lookup(r->blob_of_rpc, msgid);

    if(b) {
        b->result = result;
        if(b->result == DS_RESULT_SUCCESS) {
            b->action = b->in_transition;
            /* this get should not be here... */
            if(b->action == DS_BLOB_ACTION_REQ_GET) {
                result = dataswarm_blob_get(m,r,b->blobid);
            }
        }
    } else {
        /* may be a task */
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

	dataswarm_json_send(r->link,rpc,time(0)+m->stall_timeout);
	jx_delete(rpc);

	return msgid;
}

jx_int_t dataswarm_rpc_for_blob( struct dataswarm_manager *m, struct dataswarm_worker_rep *r, struct dataswarm_blob_rep *b, struct jx *rpc, dataswarm_blob_action_t in_transition )
{

    jx_int_t msgid = dataswarm_rpc(m, r, rpc);

    b->in_transition = in_transition;
    b->result = DS_RESULT_PENDING;;

    itable_insert(r->blob_of_rpc, msgid, b);

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
    struct jx *msg = jx_object(0);
    jx_insert_string(msg, "method", "blob-create");

    struct jx *params = jx_object(0);
    jx_insert(msg, jx_string("params"), params);
    jx_insert_string(params, "blob-id", blobid);
    jx_insert_integer(params, "size", size);
    if(metadata) {
        jx_insert(msg, jx_string("metadata"), metadata);
    }

    return dataswarm_rpc_for_blob(m, r, b, msg, DS_BLOB_ACTION_CREATE);
}

jx_int_t dataswarm_rpc_blob_commit( struct dataswarm_manager *m, struct dataswarm_worker_rep *r, const char *blobid )
{
    struct dataswarm_blob_rep *b = hash_table_lookup(r->blobs, blobid);
    if(!b) {
        fatal("No blob with id %s exist at the worker.", blobid);
    }

    //define method and params of blob-commit.
    //msg id will be added by dataswarm_rpc_blob_queue
    struct jx *msg = jx_object(0);
    jx_insert_string(msg, "method", "blob-commit");

    struct jx *params = jx_object(0);
    jx_insert(msg, jx_string("params"), params);
    jx_insert_string(params, "blob-id", blobid);

    return dataswarm_rpc_for_blob(m, r, b, msg, DS_BLOB_ACTION_COMMIT);
}

jx_int_t dataswarm_rpc_blob_delete( struct dataswarm_manager *m, struct dataswarm_worker_rep *r, const char *blobid )
{
    struct dataswarm_blob_rep *b = hash_table_lookup(r->blobs, blobid);
    if(!b) {
        fatal("No blob with id %s exist at the worker.", blobid);
    }

    //define method and params of blob-delete.
    //msg id will be added by dataswarm_rpc_blob_queue
    struct jx *msg = jx_object(0);
    jx_insert_string(msg, "method", "blob-delete");

    struct jx *params = jx_object(0);
    jx_insert(msg, jx_string("params"), params);
    jx_insert_string(params, "blob-id", blobid);

    return dataswarm_rpc_for_blob(m, r, b, msg, DS_BLOB_ACTION_DELETE);
}

jx_int_t dataswarm_rpc_blob_copy( struct dataswarm_manager *m, struct dataswarm_worker_rep *r, const char *blobid_source, const char *blobid_target )
{
    struct dataswarm_blob_rep *b = hash_table_lookup(r->blobs, blobid_target);
    if(!b) {
        fatal("No blob with id %s exist at the worker.", blobid_target);
    }

    //define method and params of blob-copy.
    //msg id will be added by dataswarm_rpc_blob_queue
    struct jx *msg = jx_object(0);
    jx_insert_string(msg, "method", "blob-copy");

    struct jx *params = jx_object(0);
    jx_insert(msg, jx_string("params"), params);
    jx_insert_string(params, "blob-id-source", blobid_source);
    jx_insert_string(params, "blob-id", blobid_source);

    return dataswarm_rpc_for_blob(m, r, b, msg, DS_BLOB_ACTION_COPY);
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
    struct jx *msg = jx_object(0);
    jx_insert_string(msg, "method", "blob-put");

    struct jx *params = jx_object(0);
    jx_insert(msg, jx_string("params"), params);
    jx_insert_string(params, "blob-id", blobid);

    jx_int_t msgid = dataswarm_rpc_for_blob(m, r, b, msg, DS_BLOB_ACTION_PUT);


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
    struct jx *msg = jx_object(0);
    jx_insert_string(msg, "method", "blob-get");

    struct jx *params = jx_object(0);
    jx_insert(msg, jx_string("params"), params);
    jx_insert_string(params, "blob-id", blobid);

    return dataswarm_rpc_for_blob(m, r, b, msg, DS_BLOB_ACTION_REQ_GET);
}

dataswarm_result_t dataswarm_blob_get( struct dataswarm_manager *m, struct dataswarm_worker_rep *r, const char *blobid )
{
    struct dataswarm_blob_rep *b = hash_table_lookup(r->blobs, blobid);
    if(!b) {
        fatal("No blob with id %s exist at the worker.", blobid);
    }

    b->in_transition = DS_BLOB_ACTION_GET;
    b->result = DS_RESULT_PENDING;

	dataswarm_result_t result = DS_RESULT_UNABLE;

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

    b->result = result;
    if(result == DS_RESULT_SUCCESS) {
        b->action = b->in_transition;
    }

	return result;
}

jx_int_t dataswarm_rpc_task_submit( struct dataswarm_manager *m, struct dataswarm_worker_rep *r, const char *taskinfo )
{
	char *msg = string_format("{\"id\": %d, \"method\" : \"task-submit\", \"params\" : %s }",m->message_id++,taskinfo);

	dataswarm_message_send(r->link,msg,strlen(msg),time(0)+m->stall_timeout);
    free(msg);

    return m->message_id;
}

jx_int_t dataswarm_rpc_task_remove( struct dataswarm_manager *m, struct dataswarm_worker_rep *r, const char *taskid )
{
	char *msg = string_format("{\"id\": %d, \"method\" : \"task-remove\", \"params\" : {  \"task-id\" : \"%s\" } }",m->message_id++,taskid);
	dataswarm_message_send(r->link,msg,strlen(msg),time(0)+m->stall_timeout);

    return m->message_id;
}
