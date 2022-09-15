/*
Copyright (C) 2022- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "ds_worker_info.h"
#include "ds_remote_file_info.h"

struct ds_worker_info * ds_worker_create( struct link * lnk )
{
	struct ds_worker_info *w =  malloc(sizeof(*w));
	if(!w) return 0;

	memset(w, 0, sizeof(*w));

	w->type = DS_WORKER_TYPE_UNKNOWN;
	w->link = lnk;

	w->hostname = strdup("unknown");
	w->os = strdup("unknown");
	w->arch = strdup("unknown");
	w->version = strdup("unknown");
	w->factory_name = 0;
	w->workerid = 0;

	w->resources = ds_resources_create();
	w->features  = hash_table_create(0,0);
	w->stats     = calloc(1, sizeof(struct ds_stats));

	w->current_files = hash_table_create(0, 0);
	w->current_tasks = itable_create(0);
	w->current_tasks_boxes = itable_create(0);

	w->start_time = timestamp_get();
	w->end_time = -1;

	w->last_update_msg_time = w->start_time;

	return w;
}

void ds_worker_delete( struct ds_worker_info *w )
{
	if(w->link) link_close(w->link);

	free(w->hostname);
	free(w->os);
	free(w->arch);
	free(w->version);
	free(w->factory_name);
	free(w->workerid);
	free(w->addrport);
	free(w->hashkey);

	ds_resources_delete(w->resources);
	hash_table_clear(w->features,(void*)free);
	hash_table_delete(w->features);
	free(w->stats);

	hash_table_clear(w->current_files,(void*)ds_remote_file_info_delete);
	hash_table_delete(w->current_files);
	itable_delete(w->current_tasks);
	itable_delete(w->current_tasks_boxes);

	free(w);
}

