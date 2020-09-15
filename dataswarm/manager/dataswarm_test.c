
#include "dataswarm_manager.h"
#include "dataswarm_worker_rep.h"
#include "dataswarm_rpc.h"

#include "debug.h"

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
	dataswarm_rpc_blob_put(m,r,blobb,"/usr/share/dict/words");

	sleep(1);

	dataswarm_rpc_task_submit(m,r,"t93",bloba,blobb);

	sleep(10);

	dataswarm_rpc_task_remove(m,r,"t93");

	sleep(1);

	dataswarm_rpc_blob_delete(m,r,bloba);
	dataswarm_rpc_blob_delete(m,r,blobb);

}

