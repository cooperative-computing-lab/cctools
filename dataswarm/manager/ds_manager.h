#ifndef DATASWARM_MANAGER_H
#define DATASWARM_MANAGER_H

#include "hash_table.h"
#include "mq.h"
#include "set.h"

#include "ds_worker_rep.h"

struct ds_manager {
	struct set *worker_table;
	struct set *client_table;
    struct hash_table *task_table;
    struct hash_table *file_table;

	struct mq *manager_socket;
	struct mq_poll *polling_group;

	int connect_timeout;
	int stall_timeout;
	int server_port;

	int task_id;
	int blob_id;

	int force_update;
	time_t catalog_last_update_time;
	int update_interval;
	char * catalog_hosts;
	time_t start_time;
	const char *project_name;
};

struct ds_manager *ds_manager_create();


/* declares a blob in a worker so that it can be manipulated via blob rpcs. */
struct ds_blob_rep *ds_manager_add_blob_to_worker( struct ds_manager *m, struct ds_worker_rep *r, const char *blobid);

/* declares a task in a worker so that it can be manipulated via blob rpcs. */
struct ds_task_attempt *ds_manager_add_task_to_worker( struct ds_manager *m, struct ds_worker_rep *r, const char *taskid);

/* notify all clients subscribed to the given task of an update */
void ds_manager_task_notify( struct ds_manager *m, struct ds_task *t, struct jx *msg);

#endif

/* vim: set noexpandtab tabstop=4: */
