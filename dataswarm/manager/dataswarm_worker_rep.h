#ifndef DATASWARM_WORKER_REP_H
#define DATASWARM_WORKER_REP_H

/*
dataswarm_client_rep is the data structure representation
of the actual client process that runs somewhere else.
*/

#include "link.h"
#include "jx.h"

struct dataswarm_worker_rep {
	struct link *link;
	char addr[LINK_ADDRESS_MAX];
	int port;
	/* list of files and states */

};

struct dataswarm_worker_rep * dataswarm_worker_rep_create( struct link *l );

void dataswarm_worker_rep_async_update( struct dataswarm_worker_rep *w, struct jx *msg );

#endif
