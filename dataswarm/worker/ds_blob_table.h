#ifndef DATASWARM_BLOB_TABLE_H
#define DATASWARM_BLOB_TABLE_H

#include "ds_message.h"
#include "ds_worker.h"

#include "jx.h"

void ds_blob_table_advance( struct ds_worker *w );
ds_result_t ds_blob_table_create( struct ds_worker *w, const char *blobid, jx_int_t size, struct jx *meta );
ds_result_t ds_blob_table_put( struct ds_worker *w, const char *blobid);
ds_result_t ds_blob_table_get(struct ds_worker *w, const char *blobid, jx_int_t msgid, int *should_respond);
ds_result_t ds_blob_table_deleting( struct ds_worker *w, const char *blobid);
ds_result_t ds_blob_table_delete( struct ds_worker *w, const char *blobid);
ds_result_t ds_blob_table_commit( struct ds_worker *w, const char *blobid);
ds_result_t ds_blob_table_copy( struct ds_worker *w, const char *blobid, const char *blobid_src);
ds_result_t ds_blob_table_list( struct ds_worker *w, struct jx **result );

void ds_blob_table_recover( struct ds_worker *w );


#endif
