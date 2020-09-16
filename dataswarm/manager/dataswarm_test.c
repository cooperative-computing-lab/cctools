
#include "dataswarm_manager.h"
#include "dataswarm_worker_rep.h"
#include "dataswarm_blob_rep.h"
#include "dataswarm_rpc.h"

#include "debug.h"
#include "stringtools.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

int wait_for_rpcs(struct dataswarm_manager *m, struct dataswarm_worker_rep *r, struct dataswarm_blob_rep **blob_reps) {
    struct dataswarm_blob_rep *b;

    int done   = 0;
    int all_ok = 1;
    while(1) {
        done = 1;

        int count = 0;
        for(count = 0; ;count++) {
            b = blob_reps[count];
            if(!b) break;
            if(b->result == DS_RESULT_PENDING) {
                done = 0;
            } else if (b->result != DS_RESULT_SUCCESS) {
                debug(D_DATASWARM, "rpc for %s failed with: %d", b->blobid, b->result);
                all_ok = 0;
            }
        }

        if(done) break;
        dataswarm_rpc_get_response(m,r);
        sleep(1);
    }

    return all_ok;
}

void dataswarm_test_script( struct dataswarm_manager *m, struct dataswarm_worker_rep *r )
{
	const char *bloba = "abc123";
	const char *blobb = "xyz456";

    struct dataswarm_blob_rep *blob_reps[] = {
        dataswarm_manager_add_blob_to_worker(m, r, bloba),
        dataswarm_manager_add_blob_to_worker(m, r, blobb),
        NULL
    };


	dataswarm_rpc_blob_delete(m,r,bloba);
	dataswarm_rpc_blob_delete(m,r,blobb);

    if(!wait_for_rpcs(m, r, blob_reps)) {
        debug(D_DATASWARM, "There was an error with rpc delete. But that may be ok.");
        return;
    }

	dataswarm_rpc_blob_create(m,r,bloba,100000,NULL);
	dataswarm_rpc_blob_create(m,r,blobb,100000,NULL);

    if(!wait_for_rpcs(m, r, blob_reps)) {
        debug(D_DATASWARM, "There was an error with an rpc. Cannot continue.");
        return;
    }

	//dataswarm_rpc_blob_put(m,r,bloba,"/usr/share/dict/words");
	dataswarm_rpc_blob_commit(m,r,bloba);
	dataswarm_rpc_blob_commit(m,r,blobb);

    if(!wait_for_rpcs(m, r, blob_reps)) {
        debug(D_DATASWARM, "There was an error with an rpc. Cannot continue.");
        return;
    }

	dataswarm_rpc_blob_delete(m,r,bloba);
	dataswarm_rpc_blob_delete(m,r,blobb);

    if(!wait_for_rpcs(m, r, blob_reps)) {
        debug(D_DATASWARM, "There was an error with an rpc. Cannot continue.");
        return;
    }

    debug(D_DATASWARM, "Done testing this worker.");
}

void dataswarm_test_script_old_sync( struct dataswarm_manager *m, struct dataswarm_worker_rep *r )
{
	const char *bloba = "abc123";
	const char *blobb = "xyz456";

	dataswarm_rpc_blob_delete(m,r,bloba);
	dataswarm_rpc_blob_delete(m,r,blobb);

	sleep(1);

	dataswarm_rpc_blob_create(m,r,bloba,100000,NULL);
	dataswarm_rpc_blob_put(m,r,bloba,"/usr/share/dict/words");
	dataswarm_rpc_blob_commit(m,r,bloba);

	sleep(1);

	dataswarm_rpc_blob_create(m,r,blobb,100000,NULL);

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

