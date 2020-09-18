#ifndef DATASWARM_BLOB_REP_H
#define DATASWARM_BLOB_REP_H

#include "dataswarm_rpc.h"  /* needed for dataswarm_result_t */

typedef enum {
    DS_BLOB_ACTION_NEW = 0,
    DS_BLOB_ACTION_CREATE,
    DS_BLOB_ACTION_PUT, DS_BLOB_ACTION_COPY,
    DS_BLOB_ACTION_REQ_GET,
    DS_BLOB_ACTION_GET,
    DS_BLOB_ACTION_COMMIT,
    DS_BLOB_ACTION_DELETE,
} dataswarm_blob_action_t;

struct dataswarm_blob_rep {
    /* Records the lifetime of a blob in a worker.
     *
     * action, in_transition, and result represent the state of the blob in the
     * worker according to the manager according to the following invariants:
     *
     * 1) action always records the latest rpc succesfully completed.
     * 2) result always records the result of the latest rpc, whether it has
     *    completed. If it has not completed, then result == DS_RESULT_PENDING.
     * 3) result == DS_RESULT_SUCCESS implies action == in_transition.
     * 4) If result is not DS_RESULT_SUCCESS nor DS_RESULT_PENDING, the
     *    in_transition records the blob's lifetime stage that could not been
     *    reached because of the error in result.
     * 5) action and in_transition are strictly monotonically increasing
     *    according to DS_BLOB_ACTION_: NEW, CREATE, ((PUT or COPY), COMMIT) or
     *    (REQ_GET, GET). delete may occur at any time after create.
     *
     * To get a blob there are two stages: REQ_GET which prompts the worker to
     * start sending the blob. The manager is free to do other things while
     * an in_transition REQ_GET has a result of DS_RESULT_PENDING. When it
     * becomes DS_RESULT_SUCCESS, then the in_transition becomes GET.
     *
     * Note that GET does not really represent an rpc, but the inflight
     * contents of the buffer. This is necessary, as the REQ_GET may succeed,
     * but the overall transfer may fail.
     *
     */

    dataswarm_blob_action_t action;
    dataswarm_blob_action_t in_transition;
    dataswarm_result_t result;

    /* this blob id */
    char *blobid;

    /* defined for rpc blob-put or blob-get only. */
    char *put_get_path;
};

#endif
