#ifndef DATASWARM_BLOB_H
#define DATASWARM_BLOB_H

#include "jx.h"
#include "link.h"
struct jx *dataswarm_blob_create(const char *blobid, jx_int_t size, struct jx *meta, struct jx *user);
struct jx *dataswarm_blob_put(const char *blobid, struct link *l);
struct jx *dataswarm_blob_get(const char *blobid, struct link *l);
struct jx *dataswarm_blob_delete(const char *blobid);
struct jx *dataswarm_blob_commit(const char *blobid);
struct jx *dataswarm_blob_copy(const char *blobid, const char *blobid_src);

#endif
