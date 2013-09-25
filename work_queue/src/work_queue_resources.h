/*
Copyright (C) 2013- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef WORK_QUEUE_RESOURCES_H
#define WORK_QUEUE_RESOURCES_H

#include "link.h"
#include "nvpair.h"

struct work_queue_resource {
	int inuse;
	int total;
	int smallest;
	int largest;
};

struct work_queue_resources {
	struct work_queue_resource workers;
	struct work_queue_resource disk;
	struct work_queue_resource cores;
	struct work_queue_resource memory;
	struct work_queue_resource gpus;
};

struct work_queue_resources * work_queue_resources_create();
void work_queue_resources_delete( struct work_queue_resources *r );
void work_queue_resources_debug( struct work_queue_resources *r );
void work_queue_resources_measure_locally( struct work_queue_resources *r, const char *workspace );
void work_queue_resources_send( struct link *master, struct work_queue_resources *r, time_t stoptime );
void work_queue_resources_clear( struct work_queue_resources *r );
void work_queue_resources_add( struct work_queue_resources *total, struct work_queue_resources *r );
void work_queue_resources_add_to_nvpair( struct work_queue_resources *r, struct nvpair *nv );

#endif
