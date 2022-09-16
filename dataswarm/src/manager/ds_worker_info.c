/*
Copyright (C) 2022- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "ds_worker_info.h"
#include "ds_remote_file_info.h"
#include "ds_protocol.h"
#include "ds_task.h"
#include "ds_resources.h"

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

static void current_tasks_to_jx( struct jx *j, struct ds_worker_info *w )
{
	struct ds_task *t;
	uint64_t taskid;
	int n = 0;

	itable_firstkey(w->current_tasks);
	while(itable_nextkey(w->current_tasks, &taskid, (void**)&t)) {
		char task_string[DS_LINE_MAX];

		sprintf(task_string, "current_task_%03d_id", n);
		jx_insert_integer(j,task_string,t->taskid);

		sprintf(task_string, "current_task_%03d_command", n);
		jx_insert_string(j,task_string,t->command_line);
		n++;
	}
}

struct jx * ds_worker_to_jx( struct ds_worker_info *w )
{
	struct jx *j = jx_object(0);
	if(!j) return 0;

	if(strcmp(w->hostname, "QUEUE_STATUS") == 0){
		return 0;
	}

	jx_insert_string(j,"hostname",w->hostname);
	jx_insert_string(j,"os",w->os);
	jx_insert_string(j,"arch",w->arch);
	jx_insert_string(j,"addrport",w->addrport);
	jx_insert_string(j,"version",w->version);
	if(w->factory_name) jx_insert_string(j,"factory_name",w->factory_name);
	if(w->factory_name) jx_insert_string(j,"workerid",w->workerid);

	ds_resources_add_to_jx(w->resources,j);

	jx_insert_integer(j,"ncpus",w->resources->cores.total);
	jx_insert_integer(j,"total_tasks_complete",w->total_tasks_complete);
	jx_insert_integer(j,"total_tasks_running",itable_size(w->current_tasks));
	jx_insert_integer(j,"total_bytes_transferred",w->total_bytes_transferred);
	jx_insert_integer(j,"total_transfer_time",w->total_transfer_time);

	jx_insert_integer(j,"start_time",w->start_time);
	jx_insert_integer(j,"current_time",timestamp_get());

	current_tasks_to_jx(j, w);

	return j;
}


