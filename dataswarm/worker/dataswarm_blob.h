#ifndef DATASWARM_BLOB_H
#define DATASWARM_BLOB_H

#include "dataswarm_worker.h"

#include "jx.h"
#include "link.h"

struct jx *dataswarm_blob_create( struct dataswarm_worker *w, const char *blobid, jx_int_t size, struct jx *meta );
struct jx *dataswarm_blob_put( struct dataswarm_worker *w, const char *blobid, struct link *l);
struct jx *dataswarm_blob_get( struct dataswarm_worker *w, const char *blobid, struct link *l);
struct jx *dataswarm_blob_delete( struct dataswarm_worker *w, const char *blobid);
struct jx *dataswarm_blob_commit( struct dataswarm_worker *w, const char *blobid);
struct jx *dataswarm_blob_copy( struct dataswarm_worker *w, const char *blobid, const char *blobid_src);

#endif
