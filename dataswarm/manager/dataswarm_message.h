#ifndef DATASWARM_MESSAGE_H
#define DATASWARM_MESSAGE_H

#include <time.h>

#include "link.h"
#include "jx.h"

enum dataswarm_message_error {
    DS_MSG_NO_ERROR = 0,
    DS_MSG_UNEXPECTED_METHOD,      /* method does not specify a known msg, or in the wrong context */
    DS_MSG_MALFORMED_ID,           /* method that needs a reply is missing the id field */
    DS_MSG_MALFORMED_MESSAGE,      /* message without the method and params fields */
    DS_MSG_MALFORMED_PARAMETERS    /* params keys missing or of incorrect type */
};

int         dataswarm_json_send( struct link *l, struct jx *j, time_t stoptime );
struct jx *dataswarm_json_recv( struct link *l, time_t stoptime );

int    dataswarm_message_send( struct link *l, const char *str, int length, time_t stoptime );
char *dataswarm_message_recv( struct link *l, time_t stoptime );

/* where evidence is params of original message if DS_MSG_MALFORMED_PARAMETERS, or the whole message otherwise. */
struct jx *dataswarm_message_error_response( enum dataswarm_message_error code, struct jx *evidence );
struct jx *dataswarm_message_state_response( const char *state, const char *reason );

#endif
