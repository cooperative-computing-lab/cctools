
#include "dataswarm_rpc.h"
#include "dataswarm_message.h"

#include "jx.h"
#include "debug.h"
#include "stringtools.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* Receive the response from a specific RPC.  If an asynchronous update is received instead, process that and then return to looking for the expected response. */

dataswarm_result_t dataswarm_rpc_get_response( struct dataswarm_manager *m, struct dataswarm_worker_rep *w, jx_int_t id )
{
	while(1) {
		struct jx * msg = dataswarm_json_recv(w->link,time(0)+m->stall_timeout);

		jx_int_t msgid = jx_lookup_integer(msg,"id");
		if(msgid!=0) {
			if(id==msgid) {
				dataswarm_result_t result = jx_lookup_integer(msg,"result");
				jx_delete(msg);
				return result;
			} else {
				debug(D_NOTICE|D_DATASWARM,"out-of-order message from worker: %d",(int)msgid);
				jx_delete(msg);
				// keep going?
			}
		} else {
			dataswarm_worker_rep_async_update(w,msg);
			jx_delete(msg);
		}
	}
}

/*
Perform a remote procedure call by sending a message, freeing it, and waiting for the matching response.
*/

dataswarm_result_t dataswarm_rpc( struct dataswarm_manager *m, struct dataswarm_worker_rep *w, char *msg )
{
	dataswarm_message_send(w->link,msg,strlen(msg),time(0)+m->stall_timeout);
	free(msg);
	return dataswarm_rpc_get_response(m,w,m->message_id++);
}

dataswarm_result_t dataswarm_rpc_blob_create( struct dataswarm_manager *m, struct dataswarm_worker_rep *w, const char *blobid, int64_t size )
{
	char *msg = string_format("{\"id\": %d, \"method\" : \"blob-create\", \"params\" : {  \"blob-id\" : \"%s\", \"size\" : %lld, \"metadata\": {} } }",m->message_id,blobid,(long long)size);
	return dataswarm_rpc(m,w,msg);
}

dataswarm_result_t dataswarm_rpc_blob_put( struct dataswarm_manager *m, struct dataswarm_worker_rep *w, const char *blobid, const char *filename )
{
	char *msg = string_format("{\"id\": %d, \"method\" : \"blob-put\", \"params\" : {  \"blob-id\" : \"%s\" } }",m->message_id,blobid);
	dataswarm_message_send(w->link,msg,strlen(msg),time(0)+m->stall_timeout);
	free(msg);

	FILE *file = fopen(filename,"r");
	fseek(file,0,SEEK_END);
	int64_t length = ftell(file);
	fseek(file,0,SEEK_SET);
	msg = string_format("%lld\n",(long long)length);
	link_write(w->link,msg,strlen(msg),time(0)+m->stall_timeout);
	link_stream_from_file(w->link,file,length,time(0)+m->stall_timeout);
	fclose(file);

	return dataswarm_rpc_get_response(m,w,m->message_id++);
}

dataswarm_result_t dataswarm_rpc_blob_get( struct dataswarm_manager *m, struct dataswarm_worker_rep *w, const char *blobid, const char *filename )
{
	dataswarm_result_t result;

	char *msg = string_format("{\"id\": %d, \"method\" : \"blob-get\", \"params\" : {  \"blob-id\" : \"%s\" } }",m->message_id,blobid);
	result = dataswarm_rpc(m,w,msg);
	if(result!=DS_RESULT_SUCCESS) return result;

	result = DS_RESULT_UNABLE;

	int64_t actual;
	char line[16];
	actual = link_readline(w->link,line,sizeof(line),time(0)+m->stall_timeout);
	if(actual>0) {
		int64_t length = atoll(line);
		FILE *file = fopen(filename,"w");
		if(file) {
			actual = link_stream_to_file(w->link,file,length,time(0)+m->stall_timeout);
			fclose(file);
			if(actual==length) {
				result = DS_RESULT_SUCCESS;
			} 
		}

	}

	return result;
}

dataswarm_result_t dataswarm_rpc_blob_commit( struct dataswarm_manager *m, struct dataswarm_worker_rep *w, const char *blobid )
{
	char *msg = string_format("{\"id\": %d, \"method\" : \"blob-commit\", \"params\" : {  \"blob-id\" : \"%s\" } }",m->message_id,blobid);
	return dataswarm_rpc(m,w,msg);
}

dataswarm_result_t dataswarm_rpc_blob_delete( struct dataswarm_manager *m, struct dataswarm_worker_rep *w, const char *blobid )
{
	char *msg = string_format("{\"id\": %d, \"method\" : \"blob-delete\", \"params\" : {  \"blob-id\" : \"%s\" } }",m->message_id,blobid);
	return dataswarm_rpc(m,w,msg);
}

dataswarm_result_t dataswarm_rpc_blob_copy( struct dataswarm_manager *m, struct dataswarm_worker_rep *w, const char *blobid_source, const char *blobid_target )
{
	char *msg = string_format("{\"id\": %d, \"method\" : \"blob-copy\", \"params\" : {  \"blob-id-source\" : \"%s\" \"blob-id\" : \"%s\" } }",m->message_id,blobid_source,blobid_target);
	return dataswarm_rpc(m,w,msg);
}

dataswarm_result_t dataswarm_rpc_task_submit( struct dataswarm_manager *m, struct dataswarm_worker_rep *w, const char *taskid, const char *bloba, const char *blobb )
{
	char *msg = string_format("{\"id\": %d, \"method\" : \"task-submit\", \"params\" : {  \"task-id\": \"%s\",\"command\" : \"ls -la; cat myinput\", \"namespace\" : { \"%s\" : {\"type\" : \"path\", \"path\" : \"myinput\", \"mode\" : \"R\" }, \"%s\" : {\"type\" : \"stdout\" } } } }",m->message_id,taskid,bloba,blobb);
	return dataswarm_rpc(m,w,msg);
}

dataswarm_result_t dataswarm_rpc_task_remove( struct dataswarm_manager *m, struct dataswarm_worker_rep *w, const char *taskid )
{
	char *msg = string_format("{\"id\": %d, \"method\" : \"task-remove\", \"params\" : {  \"task-id\" : \"%s\" } }",m->message_id,taskid);
	return dataswarm_rpc(m,w,msg);
}

