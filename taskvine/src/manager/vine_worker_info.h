/*
Copyright (C) 2022- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef VINE_WORKER_INFO_H
#define VINE_WORKER_INFO_H

#include "taskvine.h"
#include "vine_resources.h"

#include "hash_table.h"
#include "link.h"
#include "itable.h"

typedef enum {
	VINE_WORKER_TYPE_UNKNOWN = 1,    // connection has not yet identified itself
	VINE_WORKER_TYPE_WORKER  = 2,    // connection is known to be a worker
	VINE_WORKER_TYPE_STATUS  = 4,    // connection is known to be a status client
} vine_worker_type_t;

struct vine_worker_info {
	/* Type of connection: unknown, worker, status client. */
	vine_worker_type_t type;

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
	char *addrport;

	/* Hash key used to locally identify this worker. */
	char *hashkey;

	/* Address and port where this worker will accept transfers from peers. */
	char transfer_addr[LINK_ADDRESS_MAX];
	int  transfer_port;
	int  transfer_port_active;

	/* Worker condition that may affect task start or cancellation. */
	int  draining;                          // if 1, worker does not accept anymore tasks. It is shutdown if no task running.
	int  alarm_slow_worker;                 // if 1, no task has finished since a slow running task triggered a disconnection.
	                                        // 0 otherwise. A 2nd task triggering disconnection will cause the worker to disconnect
	int64_t     end_time;                   // epoch time (in seconds) at which the worker terminates
	                                        // If -1, means the worker has not reported in. If 0, means no limit.

	/* Resources and features that describe this worker. */
	struct vine_resources *resources;
	struct hash_table     *features;
	struct vine_stats     *stats;

	/* Current files and tasks that have been transfered to this worker */
	struct hash_table   *current_files;
	struct itable       *current_tasks;

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

struct vine_worker_info * vine_worker_create( struct link * lnk );
void vine_worker_delete( struct vine_worker_info *w );

struct jx * vine_worker_to_jx( struct vine_worker_info *w );
#endif
