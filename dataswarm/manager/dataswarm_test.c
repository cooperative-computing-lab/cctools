
#include "dataswarm_manager.h"
#include "dataswarm_worker_rep.h"
#include "dataswarm_rpc.h"

#include "debug.h"
#include "stringtools.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

void dataswarm_test_script( struct dataswarm_manager *m, struct dataswarm_worker_rep *r )
{
	const char *bloba = "abc123";
	const char *blobb = "xyz456";

	dataswarm_rpc_blob_delete(m,r,bloba);
	dataswarm_rpc_blob_delete(m,r,blobb);

	sleep(1);

	dataswarm_rpc_blob_create(m,r,bloba,100000);
	dataswarm_rpc_blob_put(m,r,bloba,"/usr/share/dict/words");
	dataswarm_rpc_blob_commit(m,r,bloba);

	sleep(1);

	dataswarm_rpc_blob_create(m,r,blobb,100000);

	sleep(1);

	/* Create a simple task that reads from bloba mounted as myinput and writes to blob mounted as stdout. */
	char *taskinfo = string_format("{ \"task-id\": \"%s\",\"command\" : \"wc -l myinput\", \"namespace\" : { \"%s\" : {\"type\" : \"path\", \"path\" : \"myinput\", \"mode\" : \"R\" }, \"%s\" : {\"type\" : \"stdout\" } } }","t93",bloba,blobb);
	dataswarm_rpc_task_submit(m,r,taskinfo);
	free(taskinfo);

	sleep(5);

	// need to wait for task to complete
	dataswarm_rpc_blob_get(m,r,blobb,"/dev/stdout");

	sleep(1);

	dataswarm_rpc_task_remove(m,r,"t93");

	sleep(1);


	dataswarm_rpc_blob_delete(m,r,bloba);
	dataswarm_rpc_blob_delete(m,r,blobb);

}

