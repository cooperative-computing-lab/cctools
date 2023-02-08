/*
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef WORK_QUEUE_RESOURCES_H
#define WORK_QUEUE_RESOURCES_H

#include "link.h"
#include "jx.h"

struct work_queue_resource {
	int64_t inuse;
	int64_t total;
	int64_t smallest;
	int64_t largest;
};

struct work_queue_resources {
	int64_t tag;                       // Identifies the resource snapshot.
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
void work_queue_resources_send( struct link *manager, struct work_queue_resources *r, time_t stoptime );
void work_queue_coprocess_resources_send( struct link *manager, struct work_queue_resources *r, time_t stoptime );
void work_queue_resources_clear( struct work_queue_resources *r );
void work_queue_resources_add( struct work_queue_resources *total, struct work_queue_resources *r );
void work_queue_resources_add_to_jx( struct work_queue_resources *r, struct jx *j );

#endif
