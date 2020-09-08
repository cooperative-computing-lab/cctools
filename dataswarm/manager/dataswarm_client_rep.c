#include <stdlib.h>

#include "dataswarm_client_rep.h"

struct dataswarm_client_rep * dataswarm_client_rep_create( struct link *l )
{
	struct dataswarm_client_rep *c = malloc(sizeof(*c));
	c->link = l;
	return c;
}

