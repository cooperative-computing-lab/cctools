#ifndef DATASWARM_RPC_H
#define DATASWARM_RPC_H

#include "ds_message.h"
#include "ds_manager.h"
#include "ds_worker_rep.h"

typedef enum {
	DS_RPC_OP_TASK_SUBMIT = 1,
	DS_RPC_OP_TASK_GET,
	DS_RPC_OP_TASK_REMOVE,
	DS_RPC_OP_TASK_LIST,
	DS_RPC_OP_BLOB_CREATE,
	DS_RPC_OP_BLOB_PUT,
	DS_RPC_OP_BLOB_GET,
	DS_RPC_OP_BLOB_DELETE,
	DS_RPC_OP_BLOB_COMMIT,
	DS_RPC_OP_BLOB_COPY,
	DS_RPC_OP_BLOB_LIST,
} ds_rpc_op_t;

struct ds_rpc {
	ds_rpc_op_t operation;
	struct ds_blob_rep *blob;
	struct ds_task_attempt *task;
};

ds_result_t ds_rpc_handle_message( struct ds_manager *m, struct ds_worker_rep *w);

/* rpcs return their msg ids */
jx_int_t ds_rpc_blob_create( struct ds_manager *m, struct ds_worker_rep *r, const char *blobid, int64_t size, struct jx *metadata );
jx_int_t ds_rpc_blob_commit( struct ds_manager *m, struct ds_worker_rep *r, const char *blobid );
jx_int_t ds_rpc_blob_delete( struct ds_manager *m, struct ds_worker_rep *r, const char *blobid );
jx_int_t ds_rpc_blob_copy( struct ds_manager *m, struct ds_worker_rep *r, const char *blobid_source, const char *blobid_target );
jx_int_t ds_rpc_blob_list( struct ds_manager *m, struct ds_worker_rep *r );

jx_int_t ds_rpc_blob_put( struct ds_manager *m, struct ds_worker_rep *r, const char *blobid, const char *filename );
jx_int_t ds_rpc_blob_get( struct ds_manager *m, struct ds_worker_rep *r, const char *blobid, const char *filename );

jx_int_t ds_rpc_task_submit( struct ds_manager *m, struct ds_worker_rep *r, const char *taskid );
jx_int_t ds_rpc_task_remove( struct ds_manager *m, struct ds_worker_rep *r, const char *taskid );
jx_int_t ds_rpc_task_list( struct ds_manager *m, struct ds_worker_rep *r );

#endif
