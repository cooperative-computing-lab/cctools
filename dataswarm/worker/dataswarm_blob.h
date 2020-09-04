#ifndef DATASWARM_BLOB_H
#define DATASWARM_BLOB_H

#include "jx.h"
#include "link.h"

struct jx *dataswarm_blob_create( struct jx *params );
struct jx *dataswarm_blob_put( struct link *l, struct jx *params );
struct jx *dataswarm_blob_get( struct link *l, struct jx *params );
struct jx *dataswarm_blob_delete( struct jx *params );
struct jx *dataswarm_blob_commit( struct jx *params );
struct jx *dataswarm_blob_copy( struct jx *params );

#endif
