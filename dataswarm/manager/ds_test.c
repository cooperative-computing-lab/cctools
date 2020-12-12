
#include "ds_manager.h"
#include "ds_worker_rep.h"
#include "ds_blob_rep.h"
#include "ds_task_attempt.h"
#include "ds_rpc.h"

#include "debug.h"
#include "stringtools.h"
#include "hash_table.h"
#include "itable.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>

int wait_for_rpcs(struct ds_manager *m, struct ds_worker_rep *r) {
	int done   = 0;
	int all_ok = 1;
	char *key;

	while(1) {
		ds_rpc_handle_message(m,r);

		done = 1;

		if (itable_size(r->rpcs) > 0) {
			done = 0;
		}

		/* check blob responses */
		struct ds_blob_rep *b;
		hash_table_firstkey(r->blobs);
		while((hash_table_nextkey(r->blobs, &key, (void **) &b))) {
			if(b->result == DS_RESULT_PENDING) {
				done = 0;
			} else if (b->result != DS_RESULT_SUCCESS) {
				debug(D_DATASWARM, "rpc for blob %s failed with: %d", b->blobid, b->result);
				all_ok = 0;
			}
		}

		struct ds_task_attempt *t;
		hash_table_firstkey(r->tasks);
		while((hash_table_nextkey(r->tasks, &key, (void **) &t))) {
			if(t->state == DS_TASK_TRY_PENDING) {
				/* task has not reached completed state after submission */
				done = 0;
			}

			if(t->result == DS_RESULT_PENDING) {
				done = 0;
			} else if (t->result != DS_RESULT_SUCCESS) {
				debug(D_DATASWARM, "rpc for task %s failed with: %d", t->task->taskid, t->result);
				all_ok = 0;
			}
		}

		if(done) break;

		if (mq_poll_wait(m->polling_group, time(0) + 1) == -1 && errno != EINTR) {
				perror("server_main_loop");
				break;
		}
	}

	return all_ok;
}

static char *submit_task( struct ds_manager *m, struct jx *description ) {
	char *taskid = string_format("task-%d", m->task_id++);
	jx_insert_string(description, "task-id", taskid);

	struct ds_task *t = ds_task_create(description);
	if (!t) {
		//XXX task failed validation
		struct jx *j = jx_string("task-id");
		jx_remove(description, j);
		jx_delete(j);
		return NULL;
	}
	hash_table_insert(m->task_table, taskid, t);

	return taskid;
}

void dataswarm_test_script( struct ds_manager *m, struct ds_worker_rep *r )
{
	const char *bloba = "abc123";
	const char *blobb = "xyz456";

	ds_rpc_task_list(m,r);
	ds_rpc_blob_list(m,r);

	if(!wait_for_rpcs(m, r)) {
		debug(D_DATASWARM, "There was an error with getting blobs/tasks.");
		return;
	}

	ds_manager_add_blob_to_worker(m, r, bloba);
	ds_manager_add_blob_to_worker(m, r, blobb);

	ds_rpc_blob_create(m,r,bloba,2000000,NULL);
	ds_rpc_blob_create(m,r,blobb,4000000,NULL);
	if(!wait_for_rpcs(m, r)) {
		debug(D_DATASWARM, "There was an error with an rpc. Cannot continue.");
		return;
	}

	ds_rpc_blob_put(m,r,bloba,"/usr/share/dict/words");
	if(!wait_for_rpcs(m, r)) {
		debug(D_DATASWARM, "There was an error with an rpc. Cannot continue.");
		return;
	}

	ds_rpc_blob_commit(m,r,bloba);
	if(!wait_for_rpcs(m, r)) {
		debug(D_DATASWARM, "There was an error with an rpc. Cannot continue.");
		return;
	}

	/* Create a simple task that reads from bloba mounted as myinput and writes to blob mounted as stdout. */
	struct jx *taskinfo = jx_objectv("command",   jx_string("wc -l myinput"),
			"namespace", jx_objectv(bloba, jx_objectv("type", jx_string("path"),
					"path", jx_string("myinput"),
					"mode", jx_string("R"),
					NULL),
				blobb, jx_objectv("type", jx_string("stdout"),
					NULL),
				NULL),
				 "resources", jx_objectv("cores",jx_integer(1),"memory",jx_integer(4096000),"disk",jx_integer(16000000),NULL),
			NULL);


	/* submit task to manager */
	char *taskid = submit_task(m, taskinfo);

	/* declare task in worker */
	ds_manager_add_task_to_worker(m,r,taskid);

	/* send task to worker */
	ds_rpc_task_submit(m,r,taskid);
	if(!wait_for_rpcs(m, r)) {
		debug(D_DATASWARM, "There was an error sending task to worker.");
		return;
	}

	ds_rpc_blob_get(m,r,blobb,"/dev/stdout");
	if(!wait_for_rpcs(m, r)) {
		debug(D_DATASWARM, "There was an error with an the get rpc.");
		return;
	}

	ds_rpc_task_remove(m,r,taskid);
	free(taskid);

	ds_rpc_blob_delete(m,r,bloba);
	//ds_rpc_blob_delete(m,r,blobb);
	if(!wait_for_rpcs(m, r)) {
		debug(D_DATASWARM, "There was an error with an rpc. Cannot continue.");
		return;
	}


	debug(D_DATASWARM, "Done testing this worker.");
}

void dataswarm_test_script_old_sync( struct ds_manager *m, struct ds_worker_rep *r )
{
	const char *bloba = "abc123";
	const char *blobb = "xyz456";

	ds_rpc_blob_delete(m,r,bloba);
	ds_rpc_blob_delete(m,r,blobb);

	sleep(1);

	ds_rpc_blob_create(m,r,bloba,100000,NULL);
	ds_rpc_blob_put(m,r,bloba,"/usr/share/dict/words");
	ds_rpc_blob_commit(m,r,bloba);


	ds_rpc_blob_create(m,r,blobb,100000,NULL);

	sleep(1);

	/* Create a simple task that reads from bloba mounted as myinput and writes to blob mounted as stdout. */
	char *taskinfo = string_format("{ \"task-id\": \"%s\",\"command\" : \"wc -l myinput\", \"namespace\" : { \"%s\" : {\"type\" : \"path\", \"path\" : \"myinput\", \"mode\" : \"R\" }, \"%s\" : {\"type\" : \"stdout\" } } }","t93",bloba,blobb);
	ds_rpc_task_submit(m,r,taskinfo);
	free(taskinfo);

	sleep(5);

	// need to wait for task to complete
	ds_rpc_blob_get(m,r,blobb,"/dev/stdout");

	sleep(1);

	ds_rpc_task_remove(m,r,"t93");

	sleep(1);


	ds_rpc_blob_delete(m,r,bloba);
	ds_rpc_blob_delete(m,r,blobb);

}

/* vim: set noexpandtab tabstop=4: */
