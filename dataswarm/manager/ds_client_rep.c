#include <stdlib.h>

#include "ds_client_rep.h"

struct ds_client_rep * ds_client_rep_create( struct link *l )
{
	struct ds_client_rep *c = malloc(sizeof(*c));
	c->link = l;
	return c;
}

