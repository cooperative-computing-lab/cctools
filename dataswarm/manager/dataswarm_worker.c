#include <stdlib.h>

#include "dataswarm_worker.h"

struct dataswarm_worker * dataswarm_worker_create( struct link *l )
{
	struct dataswarm_worker *w = malloc(sizeof(*w));
	w->link = l;
	return w;
}

