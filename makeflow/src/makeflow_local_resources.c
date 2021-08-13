
#include "makeflow_local_resources.h"

#include "host_disk_info.h"
#include "host_memory_info.h"
#include "load_average.h"
#include "macros.h"
#include "debug.h"

void makeflow_local_resources_print( struct rmsummary *r )
{
	printf("local resources: %s, ", rmsummary_resource_to_str("cores", r->cores, 1));
	printf("%s memory, ", rmsummary_resource_to_str("memory", r->memory, 1));
	printf("%s disk\n", rmsummary_resource_to_str("disk", r->disk, 1));
}

void makeflow_local_resources_debug( struct rmsummary *r )
{
	debug(D_MAKEFLOW, "local resources: %s, ", rmsummary_resource_to_str("cores", r->cores, 1));
	debug(D_MAKEFLOW, "%s memory, ", rmsummary_resource_to_str("memory", r->memory, 1));
	debug(D_MAKEFLOW, "%s disk\n", rmsummary_resource_to_str("disk", r->disk, 1));
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

int  makeflow_local_resources_available(struct rmsummary *local, const struct rmsummary *resources_asked)
{
	const struct rmsummary *s = resources_asked;
	return s->cores<=local->cores && s->memory<=local->memory && s->disk<=local->disk;
}

void makeflow_local_resources_subtract( struct rmsummary *local, struct dag_node *n )
{
	const struct rmsummary *s = n->resources_allocated;
	if(s->cores>=0)  local->cores -= s->cores;
	if(s->memory>=0) local->memory -= s->memory;		
	if(s->disk>=0)   local->disk -= s->disk;
	makeflow_local_resources_debug(local);
}

void makeflow_local_resources_add( struct rmsummary *local, struct dag_node *n )
{
	const struct rmsummary *s = n->resources_allocated;
	if(s->cores>=0)  local->cores += s->cores;
	if(s->memory>=0) local->memory += s->memory;		
	if(s->disk>=0)   local->disk += s->disk;
	makeflow_local_resources_debug(local);
}


