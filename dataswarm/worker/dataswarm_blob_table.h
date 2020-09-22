#ifndef DATASWARM_BLOB_TABLE_H
#define DATASWARM_BLOB_TABLE_H

#include "dataswarm_worker.h"
#include "dataswarm_message.h"

#include "jx.h"
#include "link.h"

dataswarm_result_t dataswarm_blob_table_create( struct dataswarm_worker *w, const char *blobid, jx_int_t size, struct jx *meta );
dataswarm_result_t dataswarm_blob_table_put( struct dataswarm_worker *w, const char *blobid, struct link *l);
dataswarm_result_t dataswarm_blob_table_get( struct dataswarm_worker *w, const char *blobid, struct link *l);
dataswarm_result_t dataswarm_blob_table_delete( struct dataswarm_worker *w, const char *blobid);
dataswarm_result_t dataswarm_blob_table_commit( struct dataswarm_worker *w, const char *blobid);
dataswarm_result_t dataswarm_blob_table_copy( struct dataswarm_worker *w, const char *blobid, const char *blobid_src);

void dataswarm_blob_table_purge( struct dataswarm_worker *w );


#endif
