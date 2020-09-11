#ifndef DATASWARM_BLOB_H
#define DATASWARM_BLOB_H

#include "dataswarm_worker.h"
#include "dataswarm_message.h"

#include "jx.h"
#include "link.h"

dataswarm_message_error_t dataswarm_blob_create( struct dataswarm_worker *w, const char *blobid, jx_int_t size, struct jx *meta );
dataswarm_message_error_t dataswarm_blob_put( struct dataswarm_worker *w, const char *blobid, struct link *l);
dataswarm_message_error_t dataswarm_blob_get( struct dataswarm_worker *w, const char *blobid, struct link *l);
dataswarm_message_error_t dataswarm_blob_delete( struct dataswarm_worker *w, const char *blobid);
dataswarm_message_error_t dataswarm_blob_commit( struct dataswarm_worker *w, const char *blobid);
dataswarm_message_error_t dataswarm_blob_copy( struct dataswarm_worker *w, const char *blobid, const char *blobid_src);

void dataswarm_blob_purge( struct dataswarm_worker *w );


#endif
