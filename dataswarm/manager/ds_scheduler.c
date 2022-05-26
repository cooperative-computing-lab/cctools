#include "ds_scheduler.h"

#include <stdlib.h>
#include <stdbool.h>
#include <string.h>
#include <errno.h>
#include <assert.h>

#include "jx.h"
#include "jx_print.h"
#include "jx_parse.h"
#include "debug.h"
#include "stringtools.h"
#include "xxmalloc.h"
#include "hash_table.h"

#include "ds_message.h"
#include "ds_task.h"
#include "ds_worker_rep.h"
#include "ds_client_rep.h"
#include "ds_blob_rep.h"
#include "ds_task_attempt.h"
#include "ds_client_ops.h"
#include "ds_file.h"

static int blobs_reached_state( struct ds_file *f, ds_blob_state_t state )
{
	uint64_t key;
	struct ds_blob_rep *b;

	itable_firstkey(f->blobs);
	while (itable_nextkey(f->blobs, &key, (void **) &b)) {
		if (b->state != state) return false;
		if (b->result != DS_RESULT_SUCCESS) return false;
		assert(b->in_transition == b->state);
	}
	return true;
}

static void ds_advance_file( struct ds_manager *m, struct ds_file *f )
{
	switch (f->state) {
		case DS_FILE_ALLOCATING:
			if (blobs_reached_state(f, DS_BLOB_RW)) {
				f->state = DS_FILE_MUTABLE;
			} else {
				//XXX talk to workers to advance replica states
			}
			break;
		case DS_FILE_COMMITTING:
			if (blobs_reached_state(f, DS_BLOB_RO)) {
				f->state = DS_FILE_IMMUTABLE;
			} else {
				//XXX talk to workers to advance replica states
			}
			break;
		case DS_FILE_DELETING:
			if (blobs_reached_state(f, DS_BLOB_DELETED)) {
				f->state = DS_FILE_DELETED;
			} else {
				//XXX talk to workers to advance replica states
			}
			break;
		case DS_FILE_PENDING:
		case DS_FILE_MUTABLE:
		case DS_FILE_IMMUTABLE:
		case DS_FILE_DELETED:
			// nothing to do, wait for the client
			break;
	}
}

static void advance_all_files( struct ds_manager *m )
{
	char *key;
	struct ds_file *f;

	hash_table_firstkey(m->file_table);
	while (hash_table_nextkey(m->file_table, &key, (void **) &f)) {
		ds_advance_file(m,f);
	}
}

static bool prepare_worker(struct ds_manager *m, struct ds_task *t) {
	for (struct ds_mount *u = t->mounts; u; u = u->next) {
		struct ds_file *f = hash_table_lookup(m->file_table, u->uuid);
		assert(f);
		struct ds_blob_rep *b = itable_lookup(f->blobs, (uintptr_t) t->worker);
		if (!b) {
			char *blobid = string_format("blob-%d", m->blob_id++);
			itable_insert(f->blobs, (uintptr_t) (void *) t->worker, ds_manager_add_blob_to_worker(m, t->worker, blobid));
			return false;
		}

		//XXX match mount options to file/blob state
		// or return if not ready
	}

	return true;
}

struct ds_worker_rep * choose_worker_for_task( struct ds_manager *m, struct ds_task *t )
{
	if (set_size(m->worker_table) == 0) {
		debug(D_DATASWARM, "no workers available for task %s", t->taskid);
		return NULL;
	}

	//XXX do some matching of tasks to workers
	int r = rand() % set_size(m->worker_table);
	struct ds_worker_rep *w = NULL;
	set_first_element(m->worker_table);
	while (r >= 0) {
		w = set_next_element(m->worker_table);
		r--;
	}
	return w;
}

static void schedule_task( struct ds_manager *m, struct ds_task *t )
{
	if (t->state != DS_TASK_ACTIVE) {
		// nothing to do, we're either waiting on a client or worker message
		return;
	}

	if (t->worker) {
		return;
	}

	t->worker = choose_worker_for_task(m,t);
	if (!t->worker) {
		// couldn't/didn't want to schedule the task this time around
		return;
	}

	//if (!prepare_worker(m, t)) return;
	ds_manager_add_task_to_worker(m, t->worker, t->taskid);
}

static bool free_task_resources( struct ds_manager *m, struct ds_task *t )
{
	//XXX decrease refcount on files, etc.
	return true;
}

static void attempt_task( struct ds_manager *m, struct ds_task *t )
{
	if (!t->worker) return;

	if (!prepare_worker(m, t)) return;

	if (!t->attempts) {
		ds_manager_add_task_to_worker(m, t->worker, t->taskid);
	}

	struct ds_task_attempt *try = t->attempts;
	if (try->state != try->in_transition) {
		// waiting on RPC response from the worker
		return;
	}

	switch (try->state) {
		case DS_TASK_TRY_NEW:
			// we already sent the task to a worker, this is a bug
			abort();
		case DS_TASK_TRY_PENDING:
			// waiting for task to finish on worker
			break;
		case DS_TASK_TRY_SUCCESS:
			assert(try->result == DS_RESULT_SUCCESS);
			t->state = DS_TASK_DONE;
			t->result = DS_TASK_RESULT_SUCCESS;
			ds_manager_task_notify(m, t, ds_message_task_update(t));
			break;
		case DS_TASK_TRY_FIX:
		case DS_TASK_TRY_AGAIN:
			//XXX do some reties, change resources, etc.
			// falls through
		case DS_TASK_TRY_ERROR:
			t->state = DS_TASK_DONE;
			t->result = DS_TASK_RESULT_ERROR;
			ds_manager_task_notify(m, t, ds_message_task_update(t));
			break;
		case DS_TASK_TRY_DELETED:
			break;
	}
}

static void advance_task( struct ds_manager *m, struct ds_task *t )
{
	switch (t->state) {
		case DS_TASK_DONE:
		case DS_TASK_DELETED:
			// nothing to do
			break;
		case DS_TASK_DELETING:
			if (free_task_resources(m, t)) {
				t->state = DS_TASK_DELETED;
			}
			break;
		case DS_TASK_ACTIVE:
			attempt_task(m, t);
			break;
	}
}

static void advance_all_tasks( struct ds_manager *m )
{
	char *key;
	struct ds_task *t;

	hash_table_firstkey(m->task_table);
	while (hash_table_nextkey(m->task_table, &key, (void **) &t)) {
		schedule_task(m,t);
		advance_task(m, t);

	}
}

void ds_scheduler( struct ds_manager *m )
{
	advance_all_tasks(m);
	advance_all_files(m);
}
