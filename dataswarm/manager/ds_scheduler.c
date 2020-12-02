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
	char *key;
	struct ds_blob_rep *b;

	hash_table_firstkey(f->blobs);
	while (hash_table_nextkey(f->blobs, &key, (void **) &b)) {
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
	struct ds_worker_rep *w = hash_table_lookup(m->worker_table, t->worker);

	for (struct ds_mount *u = t->mounts; u; u = u->next) {
		struct ds_file *f = hash_table_lookup(m->file_table, u->uuid);
		assert(f);
		struct ds_blob_rep *b = hash_table_lookup(f->blobs, t->worker);
		if (!b) {
			char *blobid = string_format("blob-%d", m->blob_id++);
			hash_table_insert(f->blobs, t->worker, ds_manager_add_blob_to_worker(m, w, blobid));
			return false;
		}

		//XXX match mount options to file/blob state
		// or return if not ready
	}

	return true;
}

static char * choose_worker_for_task( struct ds_manager *m, struct ds_task *t )
{
	//XXX do some matching of tasks to workers

	int r = rand() % hash_table_size(m->worker_table);
	char *key;
	void *w;
	hash_table_firstkey(m->worker_table);
	while (r >= 0) {
		hash_table_nextkey(m->worker_table, &key, &w);
		r--;
	}
	return xxstrdup(key);
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

	struct ds_worker_rep *w = hash_table_lookup(m->worker_table, t->worker);
	if (!prepare_worker(m, t)) return;
	ds_manager_add_task_to_worker(m, w, t->taskid);
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
