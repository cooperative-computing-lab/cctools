#ifndef DATASWARM_BLOB_REP_H
#define DATASWARM_BLOB_REP_H

#include "dataswarm_rpc.h"  /* needed for dataswarm_result_t */

typedef enum {
    DS_BLOB_ACTION_NEW = 0,
    DS_BLOB_ACTION_CREATE,
    DS_BLOB_ACTION_PUT, DS_BLOB_ACTION_COPY,
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
     *    according to dataswarm_blob_action_t: new, create, put or copy. delete
     *    may occur at any time after create.
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
