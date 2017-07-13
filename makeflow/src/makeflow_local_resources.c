
#include "makeflow_local_resources.h"

#include "host_disk_info.h"
#include "host_memory_info.h"
#include "load_average.h"
#include "macros.h"
#include "debug.h"

struct makeflow_local_resources * makeflow_local_resources_create()
{
	struct makeflow_local_resources *r = malloc(sizeof(*r));
	memset(r,0,sizeof(*r));
	return r;
}

void makeflow_local_resources_delete( struct makeflow_local_resources *r )
{
	free(r);
}

void makeflow_local_resources_print( struct makeflow_local_resources *r )
{
	printf("local resources: %d jobs, %d cores, %d MB memory, %d MB disk\n",r->jobs,r->cores,r->memory,r->disk);
}

void makeflow_local_resources_debug( struct makeflow_local_resources *r )
{
	debug(D_MAKEFLOW,"local resources: %d jobs, %d cores, %d MB memory, %d MB disk\n",r->jobs,r->cores,r->memory,r->disk);
}

void makeflow_local_resources_measure( struct makeflow_local_resources *r )
{
	UINT64_T avail, total;

	r->jobs = r->cores = load_average_get_cpus();

	host_memory_info_get(&avail,&total);
	r->memory = total / MEGA;

	host_disk_info_get(".",&avail,&total);
	r->disk = avail / MEGA;
}

int  makeflow_local_resources_available( struct makeflow_local_resources *r, struct dag_node *n )
{
	struct rmsummary *s = n->resources_requested;
	return r->jobs>0 && s->cores<=r->cores && s->memory<=r->memory && s->disk<=r->disk;
}

void makeflow_local_resources_subtract( struct makeflow_local_resources *r, struct dag_node *n )
{
	struct rmsummary *s = n->resources_requested;
	r->jobs--;
	if(s->cores>=0)  r->cores -= s->cores;
	if(s->memory>=0) r->memory -= s->memory;		
	if(s->disk>=0)   r->disk -= s->disk;
	makeflow_local_resources_debug(r);
}

void makeflow_local_resources_add( struct makeflow_local_resources *r, struct dag_node *n )
{
	struct rmsummary *s = n->resources_requested;
	r->jobs++;
	if(s->cores>=0)  r->cores += s->cores;
	if(s->memory>=0) r->memory += s->memory;		
	if(s->disk>=0)   r->disk += s->disk;
	makeflow_local_resources_debug(r);
}


