/*
Copyright (C) 2013- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "work_queue_resources.h"

#include "link.h"
#include "load_average.h"
#include "memory_info.h"
#include "disk_info.h"
#include "gpu_info.h"
#include "macros.h"
#include "debug.h"
#include "nvpair.h"

#include <stdlib.h>
#include <string.h>

struct work_queue_resources * work_queue_resources_create()
{
	struct work_queue_resources *r = malloc(sizeof(*r));
	memset(r,0,sizeof(*r));
	r->tag = -1;
	return r;
}

void work_queue_resources_delete( struct work_queue_resources *r )
{
	free(r);
}

void work_queue_resources_measure_locally( struct work_queue_resources *r, const char *disk_path )
{
	static int gpu_check = 0;

	UINT64_T avail,total;

	r->cores.total = load_average_get_cpus();
	r->cores.largest = r->cores.smallest = r->cores.total;

	/* For disk and memory, we compute the total thinking that the worker is
	 * not executing by itself, but that it has to share its resources with
	 * other processes/workers. */

	disk_info_get(disk_path,&avail,&total);
	r->disk.total = (avail / (UINT64_T) MEGA) + r->disk.inuse; // Free + whatever we are using.
	r->disk.largest = r->disk.smallest = r->disk.total;

	memory_info_get(&avail,&total);
	r->memory.total = (avail / (UINT64_T) MEGA) + r->memory.inuse; // Free + whatever we are using.
	r->memory.largest = r->memory.smallest = r->memory.total;

	if(!gpu_check)
	{
		r->gpus.total = gpu_info_get();
		r->gpus.largest = r->gpus.smallest = r->gpus.total;
		gpu_check = 1;
	}
	
	r->workers.total = 1;
	r->workers.largest = r->workers.smallest = r->workers.total;
}

static void work_queue_resource_debug( struct work_queue_resource *r, const char *name )
{
	debug(D_WQ,"%8s %6"PRId64" inuse %6"PRId64" committed %6"PRId64" total %6"PRId64" smallest %6"PRId64" largest",name, r->inuse, r->committed, r->total, r->smallest, r->largest);
}


static void work_queue_resource_send( struct link *master, struct work_queue_resource *r, const char *name, time_t stoptime )
{
	work_queue_resource_debug(r, name);
	link_putfstring(master, "resource %s %"PRId64" %"PRId64" %"PRId64" %"PRId64" %"PRId64"\n", stoptime, name, r->inuse, r->committed, r->total, r->smallest, r->largest );
}

void work_queue_resources_send( struct link *master, struct work_queue_resources *r, time_t stoptime )
{
	debug(D_WQ, "Sending resource description to master:");
	work_queue_resource_send(master, &r->workers, "workers",stoptime);
	work_queue_resource_send(master, &r->disk,    "disk",   stoptime);
	work_queue_resource_send(master, &r->memory,  "memory", stoptime);
	work_queue_resource_send(master, &r->gpus,    "gpus",   stoptime);
	work_queue_resource_send(master, &r->unlabeled, "unlabeled", stoptime);
	work_queue_resource_send(master, &r->cores,   "cores",  stoptime);

	/* send the tag last, the master knows when the resource update is complete */
	link_putfstring(master, "resource tag %"PRId64"\n", stoptime, r->tag);
}

void work_queue_resources_debug( struct work_queue_resources *r )
{
	work_queue_resource_debug(&r->workers, "workers");
	work_queue_resource_debug(&r->disk,    "disk");
	work_queue_resource_debug(&r->memory,  "memory");
	work_queue_resource_debug(&r->gpus,    "gpus");
	work_queue_resource_debug(&r->unlabeled, "unlabeled");
	work_queue_resource_debug(&r->cores,   "cores");
}

void work_queue_resources_clear( struct work_queue_resources *r )
{
	memset(r,0,sizeof(*r));
}

static void work_queue_resource_add( struct work_queue_resource *total, struct work_queue_resource *r )
{
	total->inuse += r->inuse;
	total->committed += r->inuse;
	total->total += r->total;
	total->smallest = MIN(total->smallest,r->smallest);
	total->largest = MAX(total->largest,r->largest);
}

void work_queue_resources_add( struct work_queue_resources *total, struct work_queue_resources *r )
{
	work_queue_resource_add(&total->workers, &r->workers);
	work_queue_resource_add(&total->memory,  &r->memory);
	work_queue_resource_add(&total->disk,    &r->disk);
	work_queue_resource_add(&total->gpus,    &r->gpus);
	work_queue_resource_add(&total->unlabeled, &r->unlabeled);
	work_queue_resource_add(&total->cores,   &r->cores);
}

void work_queue_resources_add_to_nvpair( struct work_queue_resources *r, struct nvpair *nv )
{
	nvpair_insert_integer(nv, "workers_inuse",   r->workers.inuse);
	nvpair_insert_integer(nv, "workers_total",   r->workers.total);
	nvpair_insert_integer(nv, "workers_smallest",r->workers.smallest);
	nvpair_insert_integer(nv, "workers_largest", r->workers.largest);
	nvpair_insert_integer(nv, "workers_committed", r->workers.committed);
	nvpair_insert_integer(nv, "cores_inuse",     r->cores.inuse);
	nvpair_insert_integer(nv, "cores_total",     r->cores.total);
	nvpair_insert_integer(nv, "cores_smallest",  r->cores.smallest);
	nvpair_insert_integer(nv, "cores_largest",   r->cores.largest);
	nvpair_insert_integer(nv, "cores_committed", r->cores.committed);
	nvpair_insert_integer(nv, "memory_inuse",    r->memory.inuse);
	nvpair_insert_integer(nv, "memory_total",    r->memory.total);
	nvpair_insert_integer(nv, "memory_smallest", r->memory.smallest);
	nvpair_insert_integer(nv, "memory_largest",  r->memory.largest);
	nvpair_insert_integer(nv, "memory_committed", r->memory.committed);
	nvpair_insert_integer(nv, "disk_inuse",      r->disk.inuse);
	nvpair_insert_integer(nv, "disk_total",      r->disk.total);
	nvpair_insert_integer(nv, "disk_smallest",   r->disk.smallest);
	nvpair_insert_integer(nv, "disk_largest",    r->disk.largest);
	nvpair_insert_integer(nv, "disk_committed",  r->disk.committed);
	nvpair_insert_integer(nv, "gpus_inuse",      r->gpus.inuse);
	nvpair_insert_integer(nv, "gpus_total",      r->gpus.total);
	nvpair_insert_integer(nv, "gpus_smallest",   r->gpus.smallest);
	nvpair_insert_integer(nv, "gpus_largest",    r->gpus.largest);
	nvpair_insert_integer(nv, "gpus_committed",  r->gpus.committed);

	/* total, etc. are not meaningful for unlabeled */
	nvpair_insert_integer(nv, "unlabeled_inuse",     r->unlabeled.inuse);
	nvpair_insert_integer(nv, "unlabeled_committed", r->unlabeled.committed);
}


/* vim: set noexpandtab tabstop=4: */
