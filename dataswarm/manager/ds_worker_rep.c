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

#include "ds_message.h"
#include "ds_worker_rep.h"
#include "ds_task_attempt.h"
#include "ds_blob_rep.h"
#include "ds_manager.h"

struct ds_worker_rep * ds_worker_rep_create( struct mq *conn )
{
	struct ds_worker_rep *w = malloc(sizeof(*w));
	w->connection = conn;
	mq_address_remote(conn,w->addr,&w->port);

	w->blobs = hash_table_create(0,0);
	w->tasks = hash_table_create(0,0);

	w->rpcs = itable_create(0);

	buffer_init(&w->recv_buffer);

	return w;
}

void ds_worker_rep_disconnect(struct ds_worker_rep *w) {
	if (!w) return;
	mq_close(w->connection);
	buffer_free(&w->recv_buffer);
	//XXX clean up tables
	free(w);
}

ds_result_t ds_worker_rep_update_task( struct ds_worker_rep *r, struct jx *params ) {
	if(!params) {
		debug(D_DATASWARM, "message does not contain any parameters. Ignoring task update.");
		return DS_RESULT_BAD_PARAMS;
	}

	struct jx *s = jx_lookup(params, "state");
	const char *taskid = jx_lookup_string(params, "task-id");

	if(!jx_istype(s, JX_INTEGER) || !taskid) {
		debug(D_DATASWARM, "message does not contain state or taskid. Ignoring task update.");
		return DS_RESULT_BAD_PARAMS;
	}
	jx_int_t state = s->u.integer_value;

	struct ds_task_attempt *t = hash_table_lookup(r->tasks, taskid);
	if(!t) {
		debug(D_DATASWARM, "worker does not know about taskid: %s", taskid);
		return DS_RESULT_BAD_PARAMS;
	}

	debug(D_DATASWARM, "task %s is %s at worker", taskid, ds_task_state_string(state));

	switch(state) {
		case DS_TASK_ACTIVE:
			/* can't really happen from an update from the worker. */
			break;
		case DS_TASK_DONE:
			t->in_transition = DS_TASK_TRY_SUCCESS;
			t->state = t->in_transition;
			t->result = DS_RESULT_SUCCESS;
			break;
		case DS_TASK_DELETING:
			/* do nothing until task deleted at worker. */
			break;
		case DS_TASK_DELETED:
			/* FIX: do some book-keeping now that the task is deleted. */
			break;
		default:
				/* ... */
				break;
	}

	return DS_RESULT_SUCCESS;
}

ds_result_t ds_worker_rep_update_blob( struct ds_worker_rep *r, struct jx *params ) {
	if(!params) {
		debug(D_DATASWARM, "message does not contain any parameters. Ignoring task update.");
		return DS_RESULT_BAD_PARAMS;
	}

	ds_blob_state_t state  = jx_lookup_integer(params, "state");
	const char *blobid = jx_lookup_string(params, "blob-id");

	if(!state || !blobid) { //FIX: state may be 0
		debug(D_DATASWARM, "message does not contain state or blob-id. Ignoring blob update.");
		return DS_RESULT_BAD_PARAMS;
	}

	struct ds_blob_rep *b = hash_table_lookup(r->blobs, blobid);
	if(!b) {
		debug(D_DATASWARM, "worker does not know about blob-id: %s", blobid);
		return DS_RESULT_BAD_PARAMS;
	}

	debug(D_DATASWARM, "blob %s is %s at worker", blobid, ds_blob_state_string(state));

	if(state == DS_BLOB_DELETED) {
		b->state = DS_BLOB_DELETED;
	} else {
		/* ... */
	}

	return DS_RESULT_SUCCESS;
}

/* vim: set noexpandtab tabstop=4: */
