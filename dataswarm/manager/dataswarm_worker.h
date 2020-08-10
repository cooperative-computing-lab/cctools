#ifndef DATASWARM_WORKER_H
#define DATASWARM_WORKER_H

#include "link.h"

struct dataswarm_worker {
	struct link *link;
	/* list of files and states */

};

struct dataswarm_worker * dataswarm_worker_create( struct link *l );

#endif
