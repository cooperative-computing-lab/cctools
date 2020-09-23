#ifndef DS_BLOB_REP_H
#define DS_BLOB_REP_H

#include "ds_rpc.h"  /* needed for ds_result_t */

typedef enum {
	DS_BLOB_WORKER_STATE_NEW = 0,
	DS_BLOB_WORKER_STATE_CREATED,
	DS_BLOB_WORKER_STATE_PUT, DS_BLOB_WORKER_STATE_COPIED,
	DS_BLOB_WORKER_STATE_COMMITTED,
	DS_BLOB_WORKER_STATE_GET,
	DS_BLOB_WORKER_STATE_DELETED,
} ds_blob_worker_state_t;

struct ds_blob_rep {
	/* Records the lifetime of a blob in a worker.
	 *
	 * state, in_transition, and result represent the state of the blob in the
	 * worker according to the manager according to the following invariants:
	 *
	 * 1) state always records the latest rpc successfully completed.
	 * 2) result always records the result of the latest rpc, whether it has
	 *    completed. If it has not completed, then result == DS_RESULT_PENDING.
	 * 3) result == DS_RESULT_SUCCESS implies state == in_transition.
	 * 4) If result is not DS_RESULT_SUCCESS nor DS_RESULT_PENDING, the
	 *    in_transition records the blob's lifetime stage that could not been
	 *    reached because of the error in result.
	 * 5) state and in_transition are strictly monotonically increasing
	 *    according to DS_BLOB_WORKER_STATE: NEW, CREATED, ((PUT or COPIED), COMMITTED) or
	 *    (COMMITTED, GET) or GET. DELETED may occur at any time after create.
	 *
	 * With GET, state and in_transition are immediately set to GET, as the
	 * state of the blob in the worker does not change. result is set to
	 * DS_RESULT_PENDING, and changed to DS_RESULT_SUCCESS once the file is
	 * retrieved.
	 *
	 */

	ds_blob_worker_state_t state;
	ds_blob_worker_state_t in_transition;
	ds_result_t result;

	/* this blob id */
	char *blobid;

	/* defined for rpc blob-put or blob-get only. */
	char *put_get_path;
};

#endif

/* vim: set noexpandtab tabstop=4: */
