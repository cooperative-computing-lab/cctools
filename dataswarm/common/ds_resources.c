#include "ds_resources.h"
#include "jx.h"

#include <stdlib.h>
#include <string.h>

struct ds_resources * ds_resources_create( struct jx *jresources )
{
	struct ds_resources *r = malloc(sizeof(*r));
	memset(r,0,sizeof(*r));
	r->cores = jx_lookup_integer(jresources,"cores");
	r->memory = jx_lookup_integer(jresources,"memory");
	r->disk = jx_lookup_integer(jresources,"disk");
	return r;
}

struct jx * ds_resources_to_jx( struct ds_resources *r )
{
	struct jx *j = jx_object(0);
	if(r->cores) jx_insert_integer(j,"cores",r->cores);
	if(r->memory) jx_insert_integer(j,"memory",r->memory);
	if(r->disk) jx_insert_integer(j,"disk",r->disk);
	return j;
}

void ds_resources_delete( struct ds_resources *r )
{
	free(r);
}

