#ifndef DS_BLOB_TABLE_H
#define DS_BLOB_TABLE_H

#include "comm/ds_message.h"

#include "jx.h"
#include "link.h"

ds_result_t ds_blob_table_create( const char *workspace, const char *blobid, jx_int_t size, struct jx *meta );
ds_result_t ds_blob_table_put( const char *workspace, const char *blobid, struct link *l);
ds_result_t ds_blob_table_get(const char *workspace, const char *blobid, struct link *l, int64_t timeout, jx_int_t msgid, int *should_respond);
ds_result_t ds_blob_table_delete( const char *workspace, const char *blobid);
ds_result_t ds_blob_table_commit( const char *workspace, const char *blobid);
ds_result_t ds_blob_table_copy( const char *workspace, const char *blobid, const char *blobid_src);

void ds_blob_table_purge( const char *workspace );


#endif
