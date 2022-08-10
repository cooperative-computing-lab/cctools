/*
Copyright (C) 2022- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef DS_RESOURCES_H
#define DS_RESOURCES_H

#include "link.h"
#include "jx.h"

struct ds_resource {
	int64_t inuse;
	int64_t total;
	int64_t smallest;
	int64_t largest;
};

struct ds_resources {
	int64_t tag;                       // Identifies the resource snapshot.
	struct ds_resource workers;
	struct ds_resource disk;
	struct ds_resource cores;
	struct ds_resource memory;
	struct ds_resource gpus;
};

struct ds_resources * ds_resources_create();
void ds_resources_delete( struct ds_resources *r );
void ds_resources_debug( struct ds_resources *r );
void ds_resources_measure_locally( struct ds_resources *r, const char *workspace );
void ds_resources_send( struct link *manager, struct ds_resources *r, time_t stoptime );
void ds_resources_clear( struct ds_resources *r );
void ds_resources_add( struct ds_resources *total, struct ds_resources *r );
void ds_resources_add_to_jx( struct ds_resources *r, struct jx *j );

#endif
