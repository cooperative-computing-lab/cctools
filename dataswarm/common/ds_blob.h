#ifndef DATASWARM_BLOB_H
#define DATASWARM_BLOB_H

#include "jx.h"

typedef enum {
	DS_BLOB_RW,
	DS_BLOB_RO,
	DS_BLOB_DELETING,
	DS_BLOB_DELETED
} ds_blob_state_t;

struct ds_blob {
	char *blobid;
	ds_blob_state_t state;
	int64_t size;
	struct jx *meta;
};

struct ds_blob * ds_blob_create( const char *blobid, jx_int_t size, struct jx *meta);
struct ds_blob * ds_blob_create_from_jx( struct jx *jblob );
struct ds_blob * ds_blob_create_from_file( const char *filename );

struct jx * ds_blob_to_jx( struct ds_blob *b );
int ds_blob_to_file( struct ds_blob *b, const char *filename );

void ds_blob_delete( struct ds_blob *b );

#endif
