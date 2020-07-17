#ifndef DATASWARM_BLOB_H
#define DATASWARM_BLOB_H

#include "jx.h"
#include "link.h"

int dataswarm_blob_create( const char *blobid, struct jx *metadata );
int dataswarm_blob_upload( const char *blobid, struct link *l );
int dataswarm_blob_download( const char *blobid, struct link *l );
int dataswarm_blob_commit( const char *blobid );
int dataswarm_blob_delete( const char *blobid );

#endif
