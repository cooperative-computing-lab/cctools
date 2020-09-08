#include "dataswarm_resources.h"
#include "jx.h"

#include <stdlib.h>
#include <string.h>

struct dataswarm_resources * dataswarm_resources_create( struct jx *jresources )
{
	struct dataswarm_resources *r = malloc(sizeof(*r));
	memset(r,0,sizeof(*r));
	r->cores = jx_lookup_integer(jresources,"cores");
	r->memory = jx_lookup_integer(jresources,"memory");
	r->disk = jx_lookup_integer(jresources,"disk");
	return r;
}

struct jx * dataswarm_resources_to_jx( struct dataswarm_resources *r )
{
	struct jx *j = jx_object(0);
	if(r->cores) jx_insert_integer(j,"cores",r->cores);
	if(r->memory) jx_insert_integer(j,"memory",r->memory);
	if(r->disk) jx_insert_integer(j,"disk",r->disk);
	return j;
}

void dataswarm_resources_delete( struct dataswarm_resources *r )
{
	free(r);
}

