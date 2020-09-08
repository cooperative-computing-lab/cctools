#include "dataswarm_resources.h"
#include "jx.h"

#include <stdlib.h>

struct dataswarm_resources * dataswarm_resources_create( struct jx *jresources )
{
	struct dataswarm_resources *r = calloc(sizeof(*r));
	r->cores = jx_lookup_integer(jresources,"cores");
	r->memory = jx_lookup_integer(jresources,"memory");
	r->disk = jx_lookup_integer(jresources,"disk");
	return r;
}

void dataswarm_resources_delete( struct dataswarm_resources *r )
{
	free(r);
}

