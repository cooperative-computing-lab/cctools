#ifndef DS_BLOB_TABLE_H
#define DS_BLOB_TABLE_H

#include "dataswarm_worker.h"
#include "comm/ds_message.h"

#include "jx.h"
#include "link.h"

ds_result_t dataswarm_blob_table_create( struct dataswarm_worker *w, const char *blobid, jx_int_t size, struct jx *meta );
ds_result_t dataswarm_blob_table_put( struct dataswarm_worker *w, const char *blobid, struct link *l);
ds_result_t dataswarm_blob_table_get(struct dataswarm_worker *w, const char *blobid, struct link *l, jx_int_t msgid, int *should_respond);
ds_result_t dataswarm_blob_table_delete( struct dataswarm_worker *w, const char *blobid);
ds_result_t dataswarm_blob_table_commit( struct dataswarm_worker *w, const char *blobid);
ds_result_t dataswarm_blob_table_copy( struct dataswarm_worker *w, const char *blobid, const char *blobid_src);

void dataswarm_blob_table_purge( struct dataswarm_worker *w );


#endif
