#include <stdlib.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <getopt.h>
#include <errno.h>

#include "link.h"
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

#include "dataswarm_message.h"
#include "dataswarm_worker_rep.h"
#include "dataswarm_task_rep.h"
#include "dataswarm_manager.h"

struct dataswarm_worker_rep * dataswarm_worker_rep_create( struct link *l )
{
	struct dataswarm_worker_rep *w = malloc(sizeof(*w));
	w->link = l;
	link_address_remote(w->link,w->addr,&w->port);

	w->blobs = hash_table_create(0,0);
	w->tasks = hash_table_create(0,0);

	w->blob_of_rpc = itable_create(0);
	w->task_of_rpc = itable_create(0);

	return w;
}

dataswarm_result_t dataswarm_worker_rep_update_task( struct dataswarm_worker_rep *r, struct jx *params ) {
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

	struct dataswarm_task_rep *t = hash_table_lookup(r->tasks, taskid);
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
		t->in_transition = DS_TASK_WORKER_STATE_COMPLETED;
		t->result = DS_RESULT_PENDING;
	}

	return DS_RESULT_SUCCESS;
}

dataswarm_result_t dataswarm_worker_rep_async_update( struct dataswarm_worker_rep *w, struct jx *msg )
{
	const char *method = jx_lookup_string(msg, "method");
	struct jx *params = jx_lookup(msg, "params");

	dataswarm_result_t result;
	if(!method) {
		result = DS_RESULT_BAD_METHOD;
	} else if(!strcmp(method, "task-update")) {
		result = dataswarm_worker_rep_update_task(w, params);
	} else if(!strcmp(method, "status-report")) {
		// update stats
	} else {
		result = DS_RESULT_BAD_METHOD;
	}

	return result;
}


/* vim: set noexpandtab tabstop=4: */
