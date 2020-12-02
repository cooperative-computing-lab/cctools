#ifndef DATASWARM_TASK_REP_H
#define DATASWARM_TASK_REP_H

#include "ds_rpc.h"  /* needed for ds_result_t */

struct ds_task_rep {
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
	 *
	 */

	ds_task_state_t state;
	ds_task_state_t in_transition;
	ds_result_t result;

	/* this task id */
	char *taskid;

	char *worker;
	struct ds_task *task;

	struct ds_task_rep *next;
};

#endif

/* vim: set noexpandtab tabstop=4: */
