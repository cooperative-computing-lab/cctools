/*
Copyright (C) 2022- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "vine_worker_info.h"
#include "vine_counters.h"
#include "vine_file_replica.h"
#include "vine_protocol.h"
#include "vine_resources.h"
#include "vine_task.h"
#include "priority_queue.h"
#include "vine_file.h"

struct vine_worker_info *vine_worker_create(struct link *lnk)
{
	struct vine_worker_info *w = malloc(sizeof(*w));
	if (!w)
		return 0;

	memset(w, 0, sizeof(*w));

	w->type = VINE_WORKER_TYPE_UNKNOWN;
	w->link = lnk;

	w->hostname = strdup("unknown");
	w->os = strdup("unknown");
	w->arch = strdup("unknown");
	w->version = strdup("unknown");
	w->factory_name = 0;
	w->workerid = 0;

	w->resources = vine_resources_create();
	w->features = hash_table_create(4, 0);

	w->current_files = hash_table_create(0, 0);
	w->current_tasks = itable_create(0);
	w->current_libraries = itable_create(0);

	w->start_time = timestamp_get();
	w->end_time = -1;

	w->last_update_msg_time = w->start_time;
	w->last_transfer_failure = 0;
	w->last_failure_time = 0;

	w->tasks_waiting_retrieval = 0;

	vine_counters.worker.created++;

	w->is_checkpoint_worker = 0;
	w->checkpointed_files = priority_queue_create(0);

	w->incoming_xfer_counter = 0;
	w->outgoing_xfer_counter = 0;

	return w;
}

void vine_worker_delete(struct vine_worker_info *w)
{
	if (w->link)
		link_close(w->link);

	free(w->hostname);
	free(w->os);
	free(w->arch);
	free(w->version);
	free(w->factory_name);
	free(w->workerid);
	free(w->addrport);
	free(w->hashkey);
	free(w->transfer_url);

	vine_resources_delete(w->resources);
	hash_table_clear(w->features, 0);
	hash_table_delete(w->features);

	hash_table_clear(w->current_files, (void *)vine_file_replica_delete);
	hash_table_delete(w->current_files);
	itable_delete(w->current_tasks);
	itable_delete(w->current_libraries);

	free(w);

	vine_counters.worker.deleted++;
}

static void current_tasks_to_jx(struct jx *j, struct vine_worker_info *w)
{
	struct vine_task *t;
	uint64_t task_id;
	int n = 0;

	ITABLE_ITERATE(w->current_tasks, task_id, t)
	{

		char task_string[VINE_LINE_MAX];

		sprintf(task_string, "current_task_%03d_id", n);
		jx_insert_integer(j, task_string, t->task_id);

		sprintf(task_string, "current_task_%03d_command", n);
		jx_insert_string(j, task_string, t->command_line);
		n++;
	}
}

struct jx *vine_worker_to_jx(struct vine_worker_info *w)
{
	struct jx *j = jx_object(0);
	if (!j)
		return 0;

	if (strcmp(w->hostname, "QUEUE_STATUS") == 0) {
		return 0;
	}

	jx_insert_string(j, "hostname", w->hostname);
	jx_insert_string(j, "os", w->os);
	jx_insert_string(j, "arch", w->arch);
	jx_insert_string(j, "addrport", w->addrport);
	jx_insert_string(j, "version", w->version);
	if (w->factory_name)
		jx_insert_string(j, "factory_name", w->factory_name);
	if (w->factory_name)
		jx_insert_string(j, "workerid", w->workerid);

	vine_resources_add_to_jx(w->resources, j);

	jx_insert_integer(j, "ncpus", w->resources->cores.total);
	jx_insert_integer(j, "total_tasks_complete", w->total_tasks_complete);
	jx_insert_integer(j, "total_tasks_running", itable_size(w->current_tasks));
	jx_insert_integer(j, "total_bytes_transferred", w->total_bytes_transferred);
	jx_insert_integer(j, "total_transfer_time", w->total_transfer_time);

	jx_insert_integer(j, "start_time", w->start_time);
	jx_insert_integer(j, "current_time", timestamp_get());

	current_tasks_to_jx(j, w);

	return j;
}
