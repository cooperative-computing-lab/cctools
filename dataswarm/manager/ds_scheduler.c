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

//XXX change table setups?
static bool check_replicas(struct ds_file *f, ds_blob_state_t state) {
	char *key;
	struct ds_blob_rep *b;

	hash_table_firstkey(f->replicas);
	while (hash_table_nextkey(f->replicas, &key, (void **) &b)) {
		if (b->state != state) return false;
		if (b->result != DS_RESULT_SUCCESS) return false;
		assert(b->in_transition == b->state);
	}
	return true;
}

static void process_files(struct ds_manager *m) {
	char *key;
	struct ds_file *f;

	hash_table_firstkey(m->file_table);
	while (hash_table_nextkey(m->file_table, &key, (void **) &f)) {
		switch (f->state) {
			case DS_FILE_ALLOCATING:
				if (check_replicas(f, DS_BLOB_RW)) {
					f->state = DS_FILE_MUTABLE;
				} else {
					//XXX talk to workers to advance replica states
				}
				break;
			case DS_FILE_COMMITTING:
				if (check_replicas(f, DS_BLOB_RO)) {
					f->state = DS_FILE_IMMUTABLE;
				} else {
					//XXX talk to workers to advance replica states
				}
				break;
			case DS_FILE_DELETING:
				if (check_replicas(f, DS_BLOB_DELETED)) {
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
}

static void retry_task(struct ds_task *t) {
	//XXX inspect task, check retry count, etc.
	// for now, just switch to a hard failure
	t->result = DS_TASK_RESULT_ERROR;
}

static void fix_task(struct ds_task *t) {
	//XXX inspect task, check retry count, etc.
	// for now, just switch to a hard failure
	t->result = DS_TASK_RESULT_ERROR;
}

static void schedule_task(struct ds_manager *m, struct ds_task *t) {
	//XXX do some matching of tasks to workers

	int r = rand() % hash_table_size(m->worker_table);
	char *key;
	void *w;
	hash_table_firstkey(m->worker_table);
	while (r >= 0) {
		hash_table_nextkey(m->worker_table, &key, &w);
		r--;
	}
	t->worker = xxstrdup(key);
}

static bool prepare_worker(struct ds_manager *m, struct ds_task *t) {
	struct ds_worker_rep *w = hash_table_lookup(m->worker_table, t->worker);

	for (struct ds_mount *u = t->mounts; u; u = u->next) {
		struct ds_file *f = hash_table_lookup(m->file_table, u->uuid);
		assert(f);
		struct ds_blob_rep *b = hash_table_lookup(f->replicas, t->worker);
		if (!b) {
			char *blobid = string_format("blob-%d", m->blob_id++);
			hash_table_insert(f->replicas, t->worker, ds_manager_add_blob_to_worker(m, w, blobid));
			return false;
		}

		//XXX match mount options to file/blob state
		// or return if not ready
	}

	return true;
}

void ds_scheduler( struct ds_manager *m )
{
	char *key;
	struct ds_task *t;

	hash_table_firstkey(m->task_table);
	while (hash_table_nextkey(m->task_table, &key, (void **) &t)) {
		switch (t->state) {
			case DS_TASK_ACTIVE:
				// schedule/advance below
				break;
			case DS_TASK_DONE:
			case DS_TASK_DELETED:
				// nothing to do, wait for the client
				continue;
			case DS_TASK_RUNNING:
			case DS_TASK_DELETING:
				// nothing to do, wait for the worker
				continue;
		}

		switch (t->result) {
			case DS_TASK_RESULT_SUCCESS:
				break;
			case DS_TASK_RESULT_ERROR:
			case DS_TASK_RESULT_UNDEFINED:
				// nothing to do
				continue;
			case DS_TASK_RESULT_FIX:
				fix_task(t);
				continue;
			case DS_TASK_RESULT_AGAIN:
				retry_task(t);
				continue;
		}

		if (!t->worker) {
			schedule_task(m, t);
		}
		if (!t->worker) {
			// couldn't/didn't want to schedule the task this time around
			return;
		}

		struct ds_worker_rep *w = hash_table_lookup(m->worker_table, t->worker);
		if (!prepare_worker(m, t)) return;
		ds_manager_add_task_to_worker(m, w, key);
	}
}

