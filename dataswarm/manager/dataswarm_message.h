#ifndef DATASWARM_MESSAGE_H
#define DATASWARM_MESSAGE_H

#include "link.h"
#include "jx.h"

#include <time.h>

typedef enum {
    DS_RESULT_SUCCESS = 0,
    DS_RESULT_BAD_METHOD,     /* method does not specify a known msg, or in the wrong context */
    DS_RESULT_BAD_ID,         /* method that needs a reply is missing the id field */
    DS_RESULT_BAD_PARAMS,     /* params keys missing or of incorrect type */
    DS_RESULT_NO_SUCH_TASKID, /* requested taskid does not exist */
    DS_RESULT_NO_SUCH_BLOBID, /* requested blobid does not exist */
    DS_RESULT_TOO_FULL,       /* insufficient resources to complete request */
    DS_RESULT_PERMISSION,     /* insufficient permission to complete request */
    DS_RESULT_UNABLE,         /* could not complete request for internal reason */
    DS_RESULT_CANCELED,       /* request was automatically canceled because of previous rpc in a sequence failed. */
    DS_RESULT_PENDING ,       /* rpc not completed yet. */
} dataswarm_result_t;

int         dataswarm_json_send( struct link *l, struct jx *j, time_t stoptime );
struct jx *dataswarm_json_recv( struct link *l, time_t stoptime );

int    dataswarm_message_send( struct link *l, const char *str, int length, time_t stoptime );
char *dataswarm_message_recv( struct link *l, time_t stoptime );

struct jx * dataswarm_message_standard_response( int64_t id, dataswarm_result_t code, struct jx *params );
struct jx * dataswarm_message_task_update( const char *taskid, const char *state );
struct jx * dataswarm_message_blob_update( const char *blobid, const char *state );

#endif
