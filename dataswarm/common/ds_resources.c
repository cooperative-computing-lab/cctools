#include "ds_resources.h"
#include "jx.h"

#include <stdlib.h>
#include <string.h>

struct ds_resources * ds_resources_create( int64_t cores, int64_t memory, int64_t disk )
{
	struct ds_resources *r = malloc(sizeof(*r));
	memset(r,0,sizeof(*r));
	r->cores = cores;
	r->memory = memory;
	r->disk = disk;
	return r;
}

struct ds_resources * ds_resources_create_from_jx( struct jx *jresources )
{
	return ds_resources_create(
		jx_lookup_integer(jresources,"cores"),
		jx_lookup_integer(jresources,"memory"),
		jx_lookup_integer(jresources,"disk")
	);
}

struct jx * ds_resources_to_jx( struct ds_resources *r )
{
	struct jx *j = jx_object(0);
	if(r->cores) jx_insert_integer(j,"cores",r->cores);
	if(r->memory) jx_insert_integer(j,"memory",r->memory);
	if(r->disk) jx_insert_integer(j,"disk",r->disk);
	return j;
}

int ds_resources_compare( struct ds_resources *a, struct ds_resources *b )
{
	return a->cores <= b->cores && a->memory <= b->memory && a->disk <= b->disk;
}

void ds_resources_sub( struct ds_resources *a, struct ds_resources *b )
{
	a->cores  -= b->cores;
	a->memory -= b->memory;
	a->disk   -= b->disk;
}

void ds_resources_add( struct ds_resources *a, struct ds_resources *b )
{
	a->cores  += b->cores;
	a->memory += b->memory;
	a->disk   += b->disk;
}

void ds_resources_delete( struct ds_resources *r )
{
	free(r);
}

