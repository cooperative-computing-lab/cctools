#ifndef DATASWARM_WORKER_REP_H
#define DATASWARM_WORKER_REP_H

/*
dataswarm_client_rep is the data structure representation
of the actual client process that runs somewhere else.
*/

#include "link.h"
#include "jx.h"
#include "hash_table.h"

#include "dataswarm_message.h"

struct dataswarm_worker_rep {
	struct link *link;
	char addr[LINK_ADDRESS_MAX];
	int port;
	/* list of files and states */

    /* map from blobid's to struct dataswarm_blob_rep */
    struct hash_table *blobs;

    /* map from tasksid's to struct dataswarm_task_rep */
    struct hash_table *tasks;

    /* map from currently active rpc ids to the struct dataswarm_blob that is waiting for them, if any. */
    struct itable *blob_of_rpc;

    /* map from currently active rpc ids to the struct dataswarm_task that is waiting for them, if any. */
    struct itable *task_of_rpc;
};

struct dataswarm_worker_rep * dataswarm_worker_rep_create( struct link *l );

dataswarm_result_t dataswarm_worker_rep_async_update( struct dataswarm_worker_rep *w, struct jx *msg );

#endif
