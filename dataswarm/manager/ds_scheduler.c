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
#include "ds_task_rep.h"
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

static void ds_schedule_file( struct ds_manager *m, struct ds_file *f )
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

static void schedule_all_files( struct ds_manager *m )
{
	char *key;
	struct ds_file *f;

	hash_table_firstkey(m->file_table);
	while (hash_table_nextkey(m->file_table, &key, (void **) &f)) {
		ds_schedule_file(m,f);
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
	//XXX do some matching of tasks to workers

	int r = rand() % set_size(m->worker_table);
	struct ds_worker_rep *w;
	set_first_element(m->worker_table);
	while (r >= 0) {
		w = set_next_element(m->worker_table);
		r--;
	}
	return w;
}

static void schedule_task( struct ds_manager *m, struct ds_task *t )
{
	switch (t->state) {
		case DS_TASK_ACTIVE:
			// schedule/advance below
			break;
		case DS_TASK_DELETING:
			// nothing to do, wait for the worker
			return;
		case DS_TASK_DONE:
		case DS_TASK_DELETED:
			// nothing to do, wait for the client
			return;
	}

	if (!t->worker) {
		t->worker = choose_worker_for_task(m,t);
	}

	if (!t->worker) {
		// couldn't/didn't want to schedule the task this time around
		return;
	}

	if (!prepare_worker(m, t)) return;
	ds_manager_add_task_to_worker(m, t->worker, t->taskid);
}

static void schedule_all_tasks( struct ds_manager *m )
{
	char *key;
	struct ds_task *t;

	hash_table_firstkey(m->task_table);
	while (hash_table_nextkey(m->task_table, &key, (void **) &t)) {
		schedule_task(m,t);
	}
}

void ds_scheduler( struct ds_manager *m )
{
	schedule_all_tasks(m);
	schedule_all_files(m);
}
