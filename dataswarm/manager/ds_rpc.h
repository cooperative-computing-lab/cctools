#ifndef DATASWARM_RPC_H
#define DATASWARM_RPC_H

#include "common/ds_message.h"
#include "ds_manager.h"
#include "ds_worker_rep.h"

ds_result_t ds_rpc_blob_result( struct ds_manager *m, struct ds_worker_rep *r, jx_int_t msg_id );

void ds_rpc_blob_dispatch( struct ds_manager *m, struct ds_worker_rep *r);
ds_result_t ds_rpc_get_response( struct ds_manager *m, struct ds_worker_rep *w);

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
