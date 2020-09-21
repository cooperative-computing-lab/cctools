#ifndef DATASWARM_BLOB_REP_H
#define DATASWARM_BLOB_REP_H

#include "dataswarm_rpc.h"  /* needed for dataswarm_result_t */

typedef enum {
    DS_TASK_ACTION_NEW = 0,
    DS_TASK_ACTION_CREATE,
    DS_TASK_ACTION_SUBMIT,
    DS_TASK_ACTION_RETRIEVE,
    DS_BLOB_ACTION_DELETE,
} dataswarm_task_action_t;

struct dataswarm_task_rep {
    /* Records the lifetime of a blob in a worker.
     *
     * As with blobs, action, in_transition, and result represent the state of
     * the task in the worker according to the manager according to the
     * following invariants:
     *
     * 1) action always records the latest rpc succesfully completed.
     * 2) result always records the result of the latest rpc, whether it has
     *    completed. If it has not completed, then result == DS_RESULT_PENDING.
     * 3) result == DS_RESULT_SUCCESS implies action == in_transition.
     * 4) If result is not DS_RESULT_SUCCESS nor DS_RESULT_PENDING, the
     *    in_transition records the task's lifetime stage that could not been
     *    reached because of the error in result.
     * 5) action and in_transition are strictly monotonically increasing
     *    according to DS_TASK_ACTION_: NEW, CREATE, SUBMIT, RETRIEVE, DELETE
     *    may occur at any time.
     *
     *    Any task validation should be done when action == NEW and in_transition == CREATE.
     */

    dataswarm_task_action_t action;
    dataswarm_task_action_t in_transition;
    dataswarm_result_t result;

    /* this task id */
    char *taskid;

    struct jx *description;
};

#endif
