#ifndef DATASWARM_MESSAGE_H
#define DATASWARM_MESSAGE_H

#include "mq.h"
#include "jx.h"
#include "buffer.h"

#include "ds_task.h"
#include "ds_blob.h"

#include <time.h>
#include <stdbool.h>

typedef enum {
    DS_RESULT_SUCCESS = 0,
    DS_RESULT_BAD_MESSAGE,    /* invalid/malformed RPC message */
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
int ds_bytes_send( struct mq *mq, const char *str, int length );
int ds_fd_send(struct mq *mq, int fd, size_t length);

struct jx *ds_parse_message(buffer_t *buf);

/* Helper functions for unpacking RPC messages.
 *
 * Return success if the message is valid and matches the indicated type.
 * All of the entry pointers are required, though some may be set to NULL
 * (only params and error data). If these functions return success, all
 entries are present and correctly typed (except as indicated).
 *
 * Note that these functions give back pointers into msg, so don't try to
 * delete the individual components.
 */
ds_result_t ds_unpack_notification(struct jx *msg, const char **method, struct jx **params);
ds_result_t ds_unpack_request(struct jx *msg, const char **method, jx_int_t *id, struct jx **params);
ds_result_t ds_unpack_result(struct jx *msg, jx_int_t *id, struct jx **result);
ds_result_t ds_unpack_error(struct jx *msg, jx_int_t *id, jx_int_t *code, const char **message, struct jx **data);

struct jx * ds_message_request(const char *method, struct jx *params);
struct jx * ds_message_notification(const char *method, struct jx *params);
struct jx * ds_message_response( jx_int_t id, ds_result_t code, struct jx *data);
struct jx * ds_message_task_update( const struct ds_task *t );
struct jx * ds_message_blob_update( const char *blobid, ds_blob_state_t state );

const char *ds_message_result_string(ds_result_t code);

#endif
