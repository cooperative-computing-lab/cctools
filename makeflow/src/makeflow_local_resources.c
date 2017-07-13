
#include "makeflow_local_resources.h"

#include "host_disk_info.h"
#include "host_memory_info.h"
#include "load_average.h"
#include "macros.h"
#include "debug.h"

struct rmsummary * makeflow_local_resources_create()
{
	struct rmsummary *r = malloc(sizeof(*r));
	memset(r,0,sizeof(*r));
	return r;
}

void makeflow_local_resources_delete( struct rmsummary *r )
{
	free(r);
}

void makeflow_local_resources_print( struct rmsummary *r )
{
	printf("local resources: %" PRId64 " cores, %" PRId64 " MB memory, %" PRId64 " MB disk\n",r->cores,r->memory,r->disk);
}

void makeflow_local_resources_debug( struct rmsummary *r )
{
	debug(D_MAKEFLOW,"local resources: %" PRId64 " cores, %" PRId64 " MB memory, %" PRId64 " MB disk\n",r->cores,r->memory,r->disk);
}

void makeflow_local_resources_measure( struct rmsummary *r )
{
	UINT64_T avail, total;

	r->cores = load_average_get_cpus();

	host_memory_info_get(&avail,&total);
	r->memory = total / MEGA;

	host_disk_info_get(".",&avail,&total);
	r->disk = avail / MEGA;
}

int  makeflow_local_resources_available( struct rmsummary *r, struct dag_node *n )
{
	struct rmsummary *s = n->resources_requested;
	return s->cores<=r->cores && s->memory<=r->memory && s->disk<=r->disk;
}

void makeflow_local_resources_subtract( struct rmsummary *r, struct dag_node *n )
{
	struct rmsummary *s = n->resources_requested;
	if(s->cores>=0)  r->cores -= s->cores;
	if(s->memory>=0) r->memory -= s->memory;		
	if(s->disk>=0)   r->disk -= s->disk;
	makeflow_local_resources_debug(r);
}

void makeflow_local_resources_add( struct rmsummary *r, struct dag_node *n )
{
	struct rmsummary *s = n->resources_requested;
	if(s->cores>=0)  r->cores += s->cores;
	if(s->memory>=0) r->memory += s->memory;		
	if(s->disk>=0)   r->disk += s->disk;
	makeflow_local_resources_debug(r);
}


