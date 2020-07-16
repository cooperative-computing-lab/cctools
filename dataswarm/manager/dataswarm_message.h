#ifndef DATASWARM_MESSAGE_H
#define DATASWARM_MESSAGE_H

#include <time.h>

#include "link.h"
#include "jx.h"

int         dataswarm_json_send( struct link *l, struct jx *j, time_t stoptime );
struct jx * dataswarm_json_recv( struct link *l, time_t stoptime );

int    dataswarm_message_send( struct link *l, const char *str, int length, time_t stoptime );
char * dataswarm_message_recv( struct link *l, time_t stoptime );

#endif
