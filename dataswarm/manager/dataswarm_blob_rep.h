#ifndef DATASWARM_BLOB_REP_H
#define DATASWARM_BLOB_REP_H

#include "dataswarm_rpc.h"  /* needed for dataswarm_result_t */

typedef enum {
	DS_BLOB_WORKER_STATE_NEW = 0,
	DS_BLOB_WORKER_STATE_CREATED,
	DS_BLOB_WORKER_STATE_PUT, DS_BLOB_WORKER_STATE_COPIED,
	DS_BLOB_WORKER_STATE_GET,
	DS_BLOB_WORKER_STATE_RETRIEVED,
	DS_BLOB_WORKER_STATE_COMMITTED,
	DS_BLOB_WORKER_STATE_DELETED,
} dataswarm_blob_worker_state_t;

struct dataswarm_blob_rep {
	/* Records the lifetime of a blob in a worker.
	 *
	 * state, in_transition, and result represent the state of the blob in the
	 * worker according to the manager according to the following invariants:
	 *
	 * 1) state always records the latest rpc succesfully completed.
	 * 2) result always records the result of the latest rpc, whether it has
	 *    completed. If it has not completed, then result == DS_RESULT_PENDING.
	 * 3) result == DS_RESULT_SUCCESS implies state == in_transition.
	 * 4) If result is not DS_RESULT_SUCCESS nor DS_RESULT_PENDING, the
	 *    in_transition records the blob's lifetime stage that could not been
	 *    reached because of the error in result.
	 * 5) state and in_transition are strictly monotonically increasing
	 *    according to DS_BLOB_WORKER_STATE: NEW, CREATED, ((PUT or COPIED), COMMITTED) or
	 *    (GET, RETRIEVED). DELETED may occur at any time after create.
	 *
	 * To get a blob there are two stages: GET which prompts the worker to
	 * start sending the blob. The manager is free to do other things while
	 * an in_transition GET has a result of DS_RESULT_PENDING. When it
	 * becomes DS_RESULT_SUCCESS, then the in_transition becomes RETRIEVED.
	 *
	 * Note that RETRIEVED does not really represent an rpc, but the inflight
	 * contents of the buffer. This is necessary, as the GET may succeed,
	 * but the overall transfer may fail.
	 *
	 */

	dataswarm_blob_worker_state_t state;
	dataswarm_blob_worker_state_t in_transition;
	dataswarm_result_t result;

	/* this blob id */
	char *blobid;

	/* defined for rpc blob-put or blob-get only. */
	char *put_get_path;
};

#endif

/* vim: set noexpandtab tabstop=4: */
