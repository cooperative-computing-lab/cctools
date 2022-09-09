#ifndef DS_WORKER_H
#define DS_WORKER_H

#include "dataswarm.h"
#include "ds_resources.h"

#include "hash_table.h"
#include "link.h"
#include "itable.h"

typedef enum {
	DS_WORKER_TYPE_UNKNOWN = 1,
	DS_WORKER_TYPE_WORKER  = 2,
	DS_WORKER_TYPE_STATUS  = 4,
} ds_worker_type_t;

#define DS_WORKER_ADDRPORT_MAX 64
#define DS_WORKER_HASHKEY_MAX 32

struct ds_worker {
	char *hostname;
	char *os;
	char *arch;
	char *version;
	char *factory_name;
	char addrport[DS_WORKER_ADDRPORT_MAX];
	char hashkey[DS_WORKER_HASHKEY_MAX];

	char transfer_addr[LINK_ADDRESS_MAX];
	int transfer_port;
	int transfer_port_active;
  
	ds_worker_type_t type;                         // unknown, regular worker, status worker

	int  draining;                            // if 1, worker does not accept anymore tasks. It is shutdown if no task running.

	int  fast_abort_alarm;                    // if 1, no task has finished since a task triggered fast abort.
	                                          // 0 otherwise. A 2nd task triggering fast abort will cause the worker to disconnect

	struct ds_stats     *stats;
	struct ds_resources *resources;
	struct hash_table           *features;

	char *workerid;

	struct hash_table *current_files;
	struct link *link;
	struct itable *current_tasks;
	struct itable *current_tasks_boxes;
	int finished_tasks;
	int64_t total_tasks_complete;
	int64_t total_bytes_transferred;
	timestamp_t total_task_time;
	timestamp_t total_transfer_time;
	timestamp_t start_time;
	timestamp_t last_msg_recv_time;
	timestamp_t last_update_msg_time;
	int64_t end_time;                   // epoch time (in seconds) at which the worker terminates
										// If -1, means the worker has not reported in. If 0, means no limit.
};

struct ds_worker * ds_worker_create( struct link * lnk );
void ds_worker_delete( struct ds_worker *w );

#endif
