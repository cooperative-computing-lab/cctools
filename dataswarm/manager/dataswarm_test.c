
#include "dataswarm_manager.h"
#include "dataswarm_worker_rep.h"
#include "dataswarm_message.h"

#include "link.h"
#include "debug.h"
#include "stringtools.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

int long_timeout = 60;


void do_blob_create( struct dataswarm_manager *m, struct dataswarm_worker_rep *r, const char *blobid, int64_t size )
{
	char *msg = string_format("{\"method\" : \"blob-create\", \"params\" : {  \"blob-id\" : \"%s\", \"size\" : %lld, \"metadata\": {} } }",blobid,(long long)size);
	dataswarm_message_send(r->link,msg,strlen(msg),time(0)+long_timeout);
	free(msg);
}

void do_blob_put( struct dataswarm_manager *m, struct dataswarm_worker_rep *r, const char *blobid, const char *filename )
{
	char *msg = string_format("{\"method\" : \"blob-put\", \"params\" : {  \"blob-id\" : \"%s\" } }",blobid);
	dataswarm_message_send(r->link,msg,strlen(msg),time(0)+long_timeout);
	free(msg);


	FILE *file = fopen(filename,"r");
	fseek(file,0,SEEK_END);
	int64_t length = ftell(file);
	fseek(file,0,SEEK_SET);
	msg = string_format("%lld\n",(long long)length);
	link_write(r->link,msg,strlen(msg),time(0)+long_timeout);
	link_stream_from_file(r->link,file,length,time(0)+long_timeout);
	fclose(file);
}

void do_blob_commit( struct dataswarm_manager *m, struct dataswarm_worker_rep *r, const char *blobid )
{
	char *msg = string_format("{\"method\" : \"blob-commit\", \"params\" : {  \"blob-id\" : \"%s\" } }",blobid);
	dataswarm_message_send(r->link,msg,strlen(msg),time(0)+long_timeout);
	free(msg);
}

void do_blob_delete( struct dataswarm_manager *m, struct dataswarm_worker_rep *r, const char *blobid )
{
	char *msg = string_format("{\"method\" : \"blob-delete\", \"params\" : {  \"blob-id\" : \"%s\" } }",blobid);
	dataswarm_message_send(r->link,msg,strlen(msg),time(0)+long_timeout);
	free(msg);
}


void do_task_submit( struct dataswarm_manager *m, struct dataswarm_worker_rep *r, const char *taskid, const char *bloba, const char *blobb )
{
  char *msg = string_format("{\"method\" : \"task-submit\", \"params\" : {  \"task-id\": \"%s\",\"command\" : \"ls -la; cat myinput\", \"namespace\" : { \"%s\" : {\"type\" : \"path\", \"path\" : \"myinput\", \"mode\" : \"R\" }, \"%s\" : {\"type\" : \"stdout\" } } } }",taskid,bloba,blobb);
	dataswarm_message_send(r->link,msg,strlen(msg),time(0)+long_timeout);
	free(msg);
}

void do_task_remove( struct dataswarm_manager *m, struct dataswarm_worker_rep *r, const char *taskid )
{
	char *msg = string_format("{\"method\" : \"task-remove\", \"params\" : {  \"task-id\" : \"%s\" } }",taskid);
	dataswarm_message_send(r->link,msg,strlen(msg),time(0)+long_timeout);
	free(msg);
}


void dataswarm_test_script( struct dataswarm_manager *m, struct dataswarm_worker_rep *r )
{
	const char *bloba = "abc123";
	const char *blobb = "xyz456";

	do_blob_delete(m,r,bloba);
	do_blob_delete(m,r,blobb);

	sleep(1);

	do_blob_create(m,r,bloba,100000);
	do_blob_put(m,r,bloba,"/usr/share/dict/words");
	do_blob_commit(m,r,bloba);

	sleep(1);

	do_blob_create(m,r,blobb,100000);
	do_blob_put(m,r,blobb,"/usr/share/dict/words");

	sleep(1);

	do_task_submit(m,r,"t93",bloba,blobb);

	sleep(10);

	do_task_remove(m,r,"t93");

	sleep(1);

	do_blob_delete(m,r,bloba);
	do_blob_delete(m,r,blobb);

}

