#ifndef DATASWARM_MESSAGE_H
#define DATASWARM_MESSAGE_H

#include "mq.h"
#include "jx.h"
#include "buffer.h"

#include "ds_task.h"
#include "ds_blob.h"

#include <time.h>

typedef enum {
    DS_RESULT_SUCCESS = 0,
    DS_RESULT_BAD_METHOD,     /* method does not specify a known msg, or in the wrong context */
    DS_RESULT_BAD_ID,         /* method that needs a reply is missing the id field */
    DS_RESULT_BAD_PARAMS,     /* params keys missing or of incorrect type */
    DS_RESULT_NO_SUCH_TASKID, /* requested taskid does not exist */
    DS_RESULT_NO_SUCH_BLOBID, /* requested blobid does not exist */
    DS_RESULT_TOO_FULL,       /* insufficient resources to complete request */
    DS_RESULT_BAD_PERMISSION, /* insufficient privileges to complete request */
    DS_RESULT_UNABLE,         /* could not complete request for internal reason */
    DS_RESULT_PENDING,        /* rpc not completed yet. */
    DS_RESULT_BAD_STATE,      /* cannot take that action in this state. */
    DS_RESULT_TASKID_EXISTS,  /* attempt to create a task which already exists. */
    DS_RESULT_BLOBID_EXISTS,  /* attempt to create a task which already exists. */
} ds_result_t;

int ds_json_send( struct mq *mq, struct jx *j );
int ds_message_send( struct mq *mq, const char *str, int length );
int ds_fd_send(struct mq *mq, int fd, size_t length);

struct jx *ds_parse_message(buffer_t *buf);

struct jx * ds_message_standard_response( int64_t id, ds_result_t code, struct jx *params );
struct jx * ds_message_task_update( const char *taskid, const char *state );
struct jx * ds_message_blob_update( const char *blobid, ds_blob_state_t state );

#endif
