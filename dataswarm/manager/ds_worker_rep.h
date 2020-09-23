#ifndef DS_WORKER_REP_H
#define DS_WORKER_REP_H

/*
ds_client_rep is the data structure representation
of the actual client process that runs somewhere else.
*/

#include "link.h"
#include "jx.h"
#include "hash_table.h"

#include "comm/ds_message.h"

struct ds_worker_rep {
	struct link *link;
	char addr[LINK_ADDRESS_MAX];
	int port;
	/* list of files and states */

    /* map from blobid's to struct ds_blob_rep */
    struct hash_table *blobs;

    /* map from tasksid's to struct ds_task_rep */
    struct hash_table *tasks;

    /* map from currently active rpc ids to the struct ds_blob that is waiting for them, if any. */
    struct itable *blob_of_rpc;

    /* map from currently active rpc ids to the struct ds_task that is waiting for them, if any. */
    struct itable *task_of_rpc;
};

struct ds_worker_rep * ds_worker_rep_create( struct link *l );

ds_result_t ds_worker_rep_async_update( struct ds_worker_rep *w, struct jx *msg );

#endif
