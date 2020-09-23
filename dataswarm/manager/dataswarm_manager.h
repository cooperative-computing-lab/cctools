#ifndef DS_MANAGER_H
#define DS_MANAGER_H

#include "hash_table.h"
#include "link.h"

#include "dataswarm_worker_rep.h"

struct dataswarm_manager {
	struct hash_table *worker_table;
	struct hash_table *client_table;
    struct hash_table *task_table;
    struct hash_table *file_table;

	struct link *manager_link;

	int connect_timeout;
	int stall_timeout;
	int server_port;
	int message_id;

	int task_id;

	int force_update;
	time_t catalog_last_update_time;
	int update_interval;
	char * catalog_hosts;
	time_t start_time;
	const char *project_name;
};

struct dataswarm_manager *dataswarm_manager_create();


/* declares a blob in a worker so that it can be manipulated via blob rpcs. */
struct dataswarm_blob_rep *dataswarm_manager_add_blob_to_worker( struct dataswarm_manager *m, struct dataswarm_worker_rep *r, const char *blobid);

/* declares a task in a worker so that it can be manipulated via blob rpcs. */
struct dataswarm_task_rep *dataswarm_manager_add_task_to_worker( struct dataswarm_manager *m, struct dataswarm_worker_rep *r, const char *taskid);

char *dataswarm_manager_submit_task( struct dataswarm_manager *m, struct jx *taskinfo );

#endif

/* vim: set noexpandtab tabstop=4: */
