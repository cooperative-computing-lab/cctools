/*
Copyright (C) 2022- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef VINE_RESOURCES_H
#define VINE_RESOURCES_H

#include "link.h"
#include "jx.h"

struct vine_resource {
	int64_t inuse;
	int64_t total;
};

struct vine_resources {
	int64_t tag;                       // Identifies the resource snapshot.
	struct vine_resource workers;
	struct vine_resource disk;
	struct vine_resource cores;
	struct vine_resource memory;
	struct vine_resource gpus;
};

struct vine_resources * vine_resources_create();
void vine_resources_delete( struct vine_resources *r );
struct vine_resources* vine_resources_copy( struct vine_resources* r);
void vine_resources_debug( struct vine_resources *r );
void vine_resources_measure_locally( struct vine_resources *r, const char *workspace );
void vine_resources_send( struct link *manager, struct vine_resources *r, time_t stoptime );
void vine_resources_clear( struct vine_resources *r );
void vine_resources_add( struct vine_resources *total, struct vine_resources *r );
void vine_resources_min( struct vine_resources *total, struct vine_resources *r );
void vine_resources_max( struct vine_resources *total, struct vine_resources *r );
void vine_resources_add_to_jx( struct vine_resources *r, struct jx *j );

#endif
