#ifndef DATASWARM_RPC_H
#define DATASWARM_RPC_H

#include "dataswarm_message.h"
#include "dataswarm_manager.h"
#include "dataswarm_worker_rep.h"

dataswarm_result_t dataswarm_rpc_blob_create( struct dataswarm_manager *m, struct dataswarm_worker_rep *r, const char *blobid, int64_t size );
dataswarm_result_t dataswarm_rpc_blob_commit( struct dataswarm_manager *m, struct dataswarm_worker_rep *r, const char *blobid );
dataswarm_result_t dataswarm_rpc_blob_delete( struct dataswarm_manager *m, struct dataswarm_worker_rep *r, const char *blobid );
dataswarm_result_t dataswarm_rpc_blob_copy( struct dataswarm_manager *m, struct dataswarm_worker_rep *r, const char *blobid_source, const char *blobid_target );

dataswarm_result_t dataswarm_rpc_blob_put( struct dataswarm_manager *m, struct dataswarm_worker_rep *r, const char *blobid, const char *filename );
dataswarm_result_t dataswarm_rpc_blob_get( struct dataswarm_manager *m, struct dataswarm_worker_rep *r, const char *blobid, const char *filename );

dataswarm_result_t dataswarm_rpc_task_submit( struct dataswarm_manager *m, struct dataswarm_worker_rep *r, const char *taskid, const char *bloba, const char *blobb );
dataswarm_result_t dataswarm_rpc_task_remove( struct dataswarm_manager *m, struct dataswarm_worker_rep *r, const char *taskid );

#endif
