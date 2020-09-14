#ifndef DATASWARM_MANAGER_H
#define DATASWARM_MANAGER_H

#include "hash_table.h"
#include "link.h"

struct dataswarm_manager {
	struct hash_table *worker_table;
	struct hash_table *client_table;
	struct link *manager_link;

	int connect_timeout;
	int stall_timeout;
	int server_port;
	int message_id;

	int force_update;
	time_t catalog_last_update_time;
	int update_interval;
	char * catalog_hosts;
	time_t start_time;
	const char *project_name;
};

struct dataswarm_manager * dataswarm_manager_create();

#endif
