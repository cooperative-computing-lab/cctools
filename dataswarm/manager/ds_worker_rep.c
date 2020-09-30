#include <stdlib.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <getopt.h>
#include <errno.h>

#include "mq.h"
#include "jx.h"
#include "jx_print.h"
#include "jx_parse.h"
#include "debug.h"
#include "stringtools.h"
#include "cctools.h"
#include "hash_table.h"
#include "itable.h"
#include "username.h"
#include "catalog_query.h"

#include "common/ds_message.h"
#include "ds_worker_rep.h"
#include "ds_task_rep.h"
#include "ds_manager.h"

struct ds_worker_rep * ds_worker_rep_create( struct mq *conn )
{
	struct ds_worker_rep *w = malloc(sizeof(*w));
	w->connection = conn;
	mq_address_remote(conn,w->addr,&w->port);

	w->blobs = hash_table_create(0,0);
	w->tasks = hash_table_create(0,0);

	w->blob_of_rpc = itable_create(0);
	w->task_of_rpc = itable_create(0);

	buffer_init(&w->recv_buffer);

	return w;
}

ds_result_t ds_worker_rep_update_task( struct ds_worker_rep *r, struct jx *params ) {
	if(!params) {
		debug(D_DATASWARM, "message does not contain any parameters. Ignoring task update.");
		return DS_RESULT_BAD_PARAMS;
	}

	const char *state  = jx_lookup_string(params, "state");
	const char *taskid = jx_lookup_string(params, "task-id");

	if(!state || !taskid) {
		debug(D_DATASWARM, "message does not contain state or taskid. Ignoring task update.");
		return DS_RESULT_BAD_PARAMS;
	}

	struct ds_task_rep *t = hash_table_lookup(r->tasks, taskid);
	if(!t) {
		debug(D_DATASWARM, "morker does not know about taskid: %s", taskid);
		return DS_RESULT_BAD_PARAMS;
	}

	debug(D_DATASWARM, "task %s is %s at worker", taskid, state);
	if(!strcmp(state, "done")) {
		t->in_transition = DS_TASK_WORKER_STATE_COMPLETED;
		t->state = t->in_transition;
		t->result = DS_RESULT_SUCCESS;
	} else if(!strcmp(state, "running")) {
		/* ... */
	} // else if(...)

	return DS_RESULT_SUCCESS;
}

ds_result_t ds_worker_rep_async_update( struct ds_worker_rep *w, struct jx *msg )
{
	const char *method = jx_lookup_string(msg, "method");
	struct jx *params = jx_lookup(msg, "params");

	ds_result_t result;
	if(!method) {
		result = DS_RESULT_BAD_METHOD;
	} else if(!strcmp(method, "task-update")) {
		result = ds_worker_rep_update_task(w, params);
	} else if(!strcmp(method, "status-report")) {
		// update stats
	} else {
		result = DS_RESULT_BAD_METHOD;
	}

	return result;
}


/* vim: set noexpandtab tabstop=4: */
