/*
Copyright (C) 2022- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "ds_resources.h"

#include "link.h"
#include "load_average.h"
#include "host_disk_info.h"
#include "host_memory_info.h"
#include "gpu_info.h"
#include "macros.h"
#include "debug.h"
#include "nvpair.h"

#include <stdlib.h>
#include <string.h>

struct ds_resources * ds_resources_create()
{
	struct ds_resources *r = malloc(sizeof(*r));
	memset(r, 0, sizeof(struct ds_resources));

	r->tag = -1;
	return r;
}

void ds_resources_delete( struct ds_resources *r )
{
	free(r);
}

void ds_resources_measure_locally( struct ds_resources *r, const char *disk_path )
{
	static int gpu_check = 0;

	UINT64_T avail,total;

	r->cores.total = load_average_get_cpus();
	r->cores.largest = r->cores.smallest = r->cores.total;

	/* For disk and memory, we compute the total thinking that the worker is
	 * not executing by itself, but that it has to share its resources with
	 * other processes/workers. */

	host_disk_info_get(disk_path,&avail,&total);
	r->disk.total = (avail / (UINT64_T) MEGA) + r->disk.inuse; // Free + whatever we are using.
	r->disk.largest = r->disk.smallest = r->disk.total;

	host_memory_info_get(&avail,&total);
	r->memory.total = (total / (UINT64_T) MEGA);
	r->memory.largest = r->memory.smallest = r->memory.total;

	if(!gpu_check)
	{
		r->gpus.total = gpu_count_get();
		r->gpus.largest = r->gpus.smallest = r->gpus.total;
		gpu_check = 1;
	}

	r->workers.total = 1;
	r->workers.largest = r->workers.smallest = r->workers.total;
}

static void ds_resource_debug( struct ds_resource *r, const char *name )
{
	debug(D_DS,"%8s %6"PRId64" inuse %6"PRId64" total %6"PRId64" smallest %6"PRId64" largest",name, r->inuse, r->total, r->smallest, r->largest);
}


static void ds_resource_send( struct link *manager, struct ds_resource *r, const char *name, time_t stoptime )
{
	ds_resource_debug(r, name);
	link_printf(manager, stoptime, "resource %s %"PRId64" %"PRId64" %"PRId64"\n", name, r->total, r->smallest, r->largest );
}

void ds_resources_send( struct link *manager, struct ds_resources *r, time_t stoptime )
{
	debug(D_DS, "Sending resource description to manager:");
	ds_resource_send(manager, &r->workers, "workers",stoptime);
	ds_resource_send(manager, &r->disk,    "disk",   stoptime);
	ds_resource_send(manager, &r->memory,  "memory", stoptime);
	ds_resource_send(manager, &r->gpus,    "gpus",   stoptime);
	ds_resource_send(manager, &r->cores,   "cores",  stoptime);

	/* send the tag last, the manager knows when the resource update is complete */
	link_printf(manager, stoptime, "resource tag %"PRId64"\n", r->tag);
}

void ds_resources_debug( struct ds_resources *r )
{
	ds_resource_debug(&r->workers, "workers");
	ds_resource_debug(&r->disk,    "disk");
	ds_resource_debug(&r->memory,  "memory");
	ds_resource_debug(&r->gpus,    "gpus");
	ds_resource_debug(&r->cores,   "cores");
}

void ds_resources_clear( struct ds_resources *r )
{
	memset(r,0,sizeof(*r));
}

static void ds_resource_add( struct ds_resource *total, struct ds_resource *r )
{
	total->inuse += r->inuse;
	total->total += r->total;
	total->smallest = MIN(total->smallest,r->smallest);
	total->largest = MAX(total->largest,r->largest);
}

void ds_resources_add( struct ds_resources *total, struct ds_resources *r )
{
	ds_resource_add(&total->workers, &r->workers);
	ds_resource_add(&total->memory,  &r->memory);
	ds_resource_add(&total->disk,    &r->disk);
	ds_resource_add(&total->gpus,    &r->gpus);
	ds_resource_add(&total->cores,   &r->cores);
}

void ds_resources_add_to_jx( struct ds_resources *r, struct jx *nv )
{
	jx_insert_integer(nv, "workers_inuse",   r->workers.inuse);
	jx_insert_integer(nv, "workers_total",   r->workers.total);
	jx_insert_integer(nv, "workers_smallest",r->workers.smallest);
	jx_insert_integer(nv, "workers_largest", r->workers.largest);
	jx_insert_integer(nv, "cores_inuse",     r->cores.inuse);
	jx_insert_integer(nv, "cores_total",     r->cores.total);
	jx_insert_integer(nv, "cores_smallest",  r->cores.smallest);
	jx_insert_integer(nv, "cores_largest",   r->cores.largest);
	jx_insert_integer(nv, "memory_inuse",    r->memory.inuse);
	jx_insert_integer(nv, "memory_total",    r->memory.total);
	jx_insert_integer(nv, "memory_smallest", r->memory.smallest);
	jx_insert_integer(nv, "memory_largest",  r->memory.largest);
	jx_insert_integer(nv, "disk_inuse",      r->disk.inuse);
	jx_insert_integer(nv, "disk_total",      r->disk.total);
	jx_insert_integer(nv, "disk_smallest",   r->disk.smallest);
	jx_insert_integer(nv, "disk_largest",    r->disk.largest);
	jx_insert_integer(nv, "gpus_inuse",      r->gpus.inuse);
	jx_insert_integer(nv, "gpus_total",      r->gpus.total);
	jx_insert_integer(nv, "gpus_smallest",   r->gpus.smallest);
	jx_insert_integer(nv, "gpus_largest",    r->gpus.largest);

}


/* vim: set noexpandtab tabstop=4: */
