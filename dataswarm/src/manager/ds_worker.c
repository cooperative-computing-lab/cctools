/*
Copyright (C) 2022- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "ds_worker.h"

struct ds_worker * ds_worker_create( struct link * lnk )
{
	struct ds_worker *w =  malloc(sizeof(*w));
	if(!w) return 0;

	memset(w, 0, sizeof(*w));
	w->hostname = strdup("unknown");
	w->os = strdup("unknown");
	w->arch = strdup("unknown");
	w->version = strdup("unknown");
	w->type = DS_WORKER_TYPE_UNKNOWN;
	w->draining = 0;
	w->link = lnk;
	w->current_files = hash_table_create(0, 0);
	w->current_tasks = itable_create(0);
	w->current_tasks_boxes = itable_create(0);
	w->finished_tasks = 0;
	w->start_time = timestamp_get();
	w->end_time = -1;

	w->last_update_msg_time = w->start_time;

	w->resources = ds_resources_create();

	w->workerid = NULL;

	w->stats     = calloc(1, sizeof(struct ds_stats));

	return w;
}

void ds_worker_delete( struct ds_worker *w )
{
	if(w->link) link_close(w->link);

	itable_delete(w->current_tasks);
	itable_delete(w->current_tasks_boxes);
	hash_table_delete(w->current_files);

	ds_resources_delete(w->resources);
	free(w->workerid);

	if(w->features)
		hash_table_delete(w->features);

	free(w->stats);
	free(w->hostname);
	free(w->os);
	free(w->arch);
	free(w->version);
	free(w->factory_name);
	free(w);
}

