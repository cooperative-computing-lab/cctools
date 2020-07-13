#include <stdlib.h>

#include "dataswarm_client.h"

struct dataswarm_client * dataswarm_client_create( struct link *l )
{
	struct dataswarm_client *c = malloc(sizeof(*c));
	c->link = l;
	return c;
}

