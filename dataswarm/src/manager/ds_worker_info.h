/*
Copyright (C) 2022- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef DS_WORKER_INFO_H
#define DS_WORKER_INFO_H

#include "dataswarm.h"
#include "ds_resources.h"

#include "hash_table.h"
#include "link.h"
#include "itable.h"

typedef enum {
	DS_WORKER_TYPE_UNKNOWN = 1,    // connection has not yet identified itself
	DS_WORKER_TYPE_WORKER  = 2,    // connection is known to be a worker
	DS_WORKER_TYPE_STATUS  = 4,    // connection is known to be a status client
} ds_worker_type_t;

#define DS_WORKER_ADDRPORT_MAX 64
#define DS_WORKER_HASHKEY_MAX 32

struct ds_worker {
	/* Type of connection: unknown, worker, status client. */
	ds_worker_type_t type;

	/* Connection to the worker or other client. */
	struct link *link;

	/* Static properties reported by worker when it connects. */
	char *hostname;
	char *os;
	char *arch;
	char *version;
	char *factory_name;
	char *workerid;

	/* Remote address of worker. */
	char addrport[DS_WORKER_ADDRPORT_MAX];

	/* Hash key used to locally identify this worker. */
	char hashkey[DS_WORKER_HASHKEY_MAX];

	/* Address and port where this worker will accept transfers from peers. */
	char transfer_addr[LINK_ADDRESS_MAX];
	int  transfer_port;
	int  transfer_port_active;
  
	/* Worker condition that may affect task start or cancellation. */
	int  draining;                            // if 1, worker does not accept anymore tasks. It is shutdown if no task running.
	int  fast_abort_alarm;                    // if 1, no task has finished since a task triggered fast abort.
	                                          // 0 otherwise. A 2nd task triggering fast abort will cause the worker to disconnect
	int64_t     end_time;                   // epoch time (in seconds) at which the worker terminates
	                                        // If -1, means the worker has not reported in. If 0, means no limit.

	/* Resources and features that describe this worker. */
	struct ds_resources *resources;
	struct hash_table   *features;
	struct ds_stats     *stats;

	/* Current files and tasks that have been transfered to this worker */
	struct hash_table   *current_files;
	struct itable       *current_tasks;
	struct itable       *current_tasks_boxes;

	/* Accumulated stats about tasks about this worker. */
	int         finished_tasks;
	int64_t     total_tasks_complete;
	int64_t     total_bytes_transferred;
	timestamp_t total_task_time;
	timestamp_t total_transfer_time;
	timestamp_t start_time;
	timestamp_t last_msg_recv_time;
	timestamp_t last_update_msg_time;
};

struct ds_worker * ds_worker_create( struct link * lnk );
void ds_worker_delete( struct ds_worker *w );

#endif
