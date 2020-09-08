#ifndef DATASWARM_WORKER_H
#define DATASWARM_WORKER_H

struct dataswarm_worker {
	struct link *manager_link;

	// taskid -> dataswarm_task *
	struct hash_table *task_table;

	char *workspace;

	// Give up and reconnect if no message received after this time.
	int idle_timeout;

	//
	int long_timeout;

	// Minimum time between connection attempts.
	int min_connect_retry;

	// Maximum time between connection attempts.
	int max_connect_retry;

	// Maximum time to wait for a catalog query
	int catalog_timeout;

	// id msg counter
	int message_id;
};

struct dataswarm_worker *dataswarm_worker_create();
void dataswarm_worker_delete(struct dataswarm_worker *w);


#endif
