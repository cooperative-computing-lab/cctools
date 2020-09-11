#ifndef DATASWARM_MESSAGE_H
#define DATASWARM_MESSAGE_H

#include <time.h>

#include "link.h"
#include "jx.h"

typedef enum {
    DS_RESULT_SUCCESS = 0,
    DS_RESULT_UNEXPECTED_METHOD,      /* method does not specify a known msg, or in the wrong context */
    DS_RESULT_MALFORMED_ID,           /* method that needs a reply is missing the id field */
    DS_RESULT_MALFORMED_MESSAGE,      /* message without the method and params fields */
    DS_RESULT_MALFORMED_PARAMETERS,   /* params keys missing or of incorrect type */
    DS_RESULT_NO_SUCH_TASKID,	   /* requested taskid does not exist */
    DS_RESULT_NO_SUCH_BLOBID,	   /* requested blobid does not exist */
    DS_RESULT_UNABLE,                 /* could not complete request */
} dataswarm_result_t;

int         dataswarm_json_send( struct link *l, struct jx *j, time_t stoptime );
struct jx *dataswarm_json_recv( struct link *l, time_t stoptime );

int    dataswarm_message_send( struct link *l, const char *str, int length, time_t stoptime );
char *dataswarm_message_recv( struct link *l, time_t stoptime );

struct jx *dataswarm_message_standard_response( int64_t id, dataswarm_result_t code, struct jx *params );

#endif
