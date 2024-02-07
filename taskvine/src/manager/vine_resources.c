/*
Copyright (C) 2022- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "vine_resources.h"

#include "debug.h"
#include "gpu_info.h"
#include "host_disk_info.h"
#include "host_memory_info.h"
#include "link.h"
#include "load_average.h"
#include "macros.h"
#include "nvpair.h"

#include <stdlib.h>
#include <string.h>

struct vine_resources *vine_resources_create()
{
	struct vine_resources *r = malloc(sizeof(*r));
	memset(r, 0, sizeof(struct vine_resources));

	r->tag = -1;
	return r;
}

void vine_resources_delete(struct vine_resources *r)
{
	free(r);
}

struct vine_resources *vine_resources_copy(struct vine_resources *r)
{
	struct vine_resources *t = vine_resources_create();
	t->tag = r->tag;
	t->workers = r->workers;
	t->disk = r->disk;
	t->cores = r->cores;
	t->memory = r->memory;
	t->gpus = r->gpus;
	return t;
}

void vine_resources_measure_locally(struct vine_resources *r, const char *disk_path)
{
	static int gpu_check = 0;

	UINT64_T avail, total;

	r->cores.total = load_average_get_cpus();

	/* For disk and memory, we compute the total thinking that the worker is
	 * not executing by itself, but that it has to share its resources with
	 * other processes/workers. */

	host_disk_info_get(disk_path, &avail, &total);
	r->disk.total = (avail / (UINT64_T)MEGA) + r->disk.inuse; // Free + whatever we are using.

	host_memory_info_get(&avail, &total);
	r->memory.total = (total / (UINT64_T)MEGA);

	if (!gpu_check) {
		r->gpus.total = gpu_count_get();
		gpu_check = 1;
	}

	r->workers.total = 1;
}

static void vine_resource_debug(struct vine_resource *r, const char *name)
{
	debug(D_VINE, "%8s %6" PRId64 " inuse %6" PRId64 " total", name, r->inuse, r->total);
}

static void vine_resource_send(struct link *manager, struct vine_resource *r, const char *name, time_t stoptime)
{
	vine_resource_debug(r, name);
	link_printf(manager, stoptime, "resource %s %" PRId64 "\n", name, r->total);
}

void vine_resources_send(struct link *manager, struct vine_resources *r, time_t stoptime)
{
	debug(D_VINE, "Sending resource description to manager:");
	vine_resource_send(manager, &r->workers, "workers", stoptime);
	vine_resource_send(manager, &r->disk, "disk", stoptime);
	vine_resource_send(manager, &r->memory, "memory", stoptime);
	vine_resource_send(manager, &r->gpus, "gpus", stoptime);
	vine_resource_send(manager, &r->cores, "cores", stoptime);

	/* send the tag last, the manager knows when the resource update is complete */
	link_printf(manager, stoptime, "resource tag %" PRId64 "\n", r->tag);
}

void vine_resources_debug(struct vine_resources *r)
{
	vine_resource_debug(&r->workers, "workers");
	vine_resource_debug(&r->disk, "disk");
	vine_resource_debug(&r->memory, "memory");
	vine_resource_debug(&r->gpus, "gpus");
	vine_resource_debug(&r->cores, "cores");
}

void vine_resources_clear(struct vine_resources *r)
{
	memset(r, 0, sizeof(*r));
}

static void vine_resource_add(struct vine_resource *total, struct vine_resource *r)
{
	total->inuse += r->inuse;
	total->total += r->total;
}

static void vine_resource_min(struct vine_resource *total, struct vine_resource *r)
{
	total->inuse = MIN(total->inuse, r->inuse);
	total->total = MIN(total->total, r->total);
}

static void vine_resource_max(struct vine_resource *total, struct vine_resource *r)
{
	total->inuse = MAX(total->inuse, r->inuse);
	total->total = MAX(total->total, r->total);
}

void vine_resources_add(struct vine_resources *total, struct vine_resources *r)
{
	vine_resource_add(&total->workers, &r->workers);
	vine_resource_add(&total->memory, &r->memory);
	vine_resource_add(&total->disk, &r->disk);
	vine_resource_add(&total->gpus, &r->gpus);
	vine_resource_add(&total->cores, &r->cores);
}

void vine_resources_min(struct vine_resources *total, struct vine_resources *r)
{
	vine_resource_min(&total->workers, &r->workers);
	vine_resource_min(&total->memory, &r->memory);
	vine_resource_min(&total->disk, &r->disk);
	vine_resource_min(&total->gpus, &r->gpus);
	vine_resource_min(&total->cores, &r->cores);
}

void vine_resources_max(struct vine_resources *total, struct vine_resources *r)
{
	vine_resource_max(&total->workers, &r->workers);
	vine_resource_max(&total->memory, &r->memory);
	vine_resource_max(&total->disk, &r->disk);
	vine_resource_max(&total->gpus, &r->gpus);
	vine_resource_max(&total->cores, &r->cores);
}

void vine_resources_add_to_jx(struct vine_resources *r, struct jx *nv)
{
	jx_insert_integer(nv, "workers_inuse", r->workers.inuse);
	jx_insert_integer(nv, "workers_total", r->workers.total);
	jx_insert_integer(nv, "cores_inuse", r->cores.inuse);
	jx_insert_integer(nv, "cores_total", r->cores.total);
	jx_insert_integer(nv, "memory_inuse", r->memory.inuse);
	jx_insert_integer(nv, "memory_total", r->memory.total);
	jx_insert_integer(nv, "disk_inuse", r->disk.inuse);
	jx_insert_integer(nv, "disk_total", r->disk.total);
	jx_insert_integer(nv, "gpus_inuse", r->gpus.inuse);
	jx_insert_integer(nv, "gpus_total", r->gpus.total);
}

/* vim: set noexpandtab tabstop=8: */
