#ifndef DATASWARM_WORKER_REP_H
#define DATASWARM_WORKER_REP_H

/*
ds_client_rep is the data structure representation
of the actual client process that runs somewhere else.
*/

#include "mq.h"
#include "jx.h"
#include "hash_table.h"
#include "buffer.h"
#include "link.h"

#include "ds_message.h"

struct ds_worker_rep {
	struct mq *connection;
	char addr[LINK_ADDRESS_MAX];
	int port;
	/* list of files and states */

	/* map from blobid's to struct ds_blob_rep */
	struct hash_table *blobs;

	/* map from tasksid's to struct ds_task_attempt */
	struct hash_table *tasks;

	/* map from currently active rpc ids to (struct ds_rpc *)
	 */
	struct itable *rpcs;

	buffer_t recv_buffer;
};

struct ds_worker_rep * ds_worker_rep_create( struct mq *conn );
void ds_worker_rep_disconnect(struct ds_worker_rep *w);

ds_result_t ds_worker_rep_update_task( struct ds_worker_rep *r, struct jx *params );
ds_result_t ds_worker_rep_update_blob( struct ds_worker_rep *r, struct jx *params );

#endif
