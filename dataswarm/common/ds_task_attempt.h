#ifndef DATASWARM_TASK_ATTEMPT_H
#define DATASWARM_TASK_ATTEMPT_H

#include "ds_message.h"  /* needed for ds_result_t */

typedef enum {
	DS_TASK_TRY_NEW,		/* Attempt has been defined, but it is not executing. */
	DS_TASK_TRY_PENDING,	/* Attempt is currently executing. */
	DS_TASK_TRY_SUCCESS,	/* Attempt executed to completion. */
	DS_TASK_TRY_ERROR,		/* Attempt has a permanent error (e.g., missing inputs) */
	DS_TASK_TRY_FIX,		/* Attempt could not be completed as defined, but can be fixed and retried with another attempt without
							 user intervention. (e.g., increase resource allocation) */
	DS_TASK_TRY_AGAIN,		/* Attempt could not be completed for no fault of its own (e.g., graceful disconnection, change of worker
							 resources) */
	DS_TASK_TRY_DELETED,	/* Task is removed from worker */
} ds_task_try_state_t;

struct ds_task_attempt {
	/* Records the lifetime of a task in a worker.
	 *
	 * As with blobs, state, in_transition, and result represent the state of
	 * the task in the worker according to the manager according to the
	 * following invariants:
	 *
	 * 1) state always records the latest rpc successfully completed.
	 * 2) result always records the result of the latest rpc, whether it has
	 *    completed. If it has not completed, then result == DS_RESULT_PENDING.
	 * 3) result == DS_RESULT_SUCCESS implies state == in_transition.
	 * 4) If result is not DS_RESULT_SUCCESS nor DS_RESULT_PENDING, the
	 *    in_transition records the task's lifetime stage that could not been
	 *    reached because of the error in result.
	 *
	 * Note that this simply records the lifetime in a worker. Any task
	 * information and validation should be fulfilled before the task is added
	 * to the worker (i.e., before NEW).
	 */

	ds_task_try_state_t state;
	ds_task_try_state_t in_transition;
	ds_result_t result;

	// these 2 are only used on the manager
	struct ds_worker_rep *worker;
	struct ds_task *task;

	struct ds_task_attempt *next;
};

struct ds_task_attempt * ds_task_attempt_create( struct ds_task *task );

#endif

/* vim: set noexpandtab tabstop=4: */
