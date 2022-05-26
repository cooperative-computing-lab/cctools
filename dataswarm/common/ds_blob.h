#ifndef DATASWARM_BLOB_H
#define DATASWARM_BLOB_H

#include "jx.h"

/* blob_state_t is used to track the state of blobs in workers, and the
 * knowledge of blob transitions as seen by the manager. The worker only needs
 * DS_BLOB_RW for new blobs declared from the manager, DS_BLOB_RO for committed blobs, and
 * DS_BLOB_DELETING for blobs in the process of being deleted, and DS_BLOB_DELETED as a terminal state.
 *
 * The manager declares new blobs as BLOB_NEW, and only transitions to
 * DS_BLOB_RW once the blob is declared in the worker, and so on. See
 * manager/ds_blob_rep.h for the blob state transitions according to the
 * manager. */

typedef enum {
	DS_BLOB_NEW = 0,
	DS_BLOB_RW,                   /* blobs are created as read-write */
	DS_BLOB_PUT, DS_BLOB_COPIED,
	DS_BLOB_RO,                   /* committed blobs are read-only */
	DS_BLOB_GET,
	DS_BLOB_DELETING,
	DS_BLOB_DELETED,
	DS_BLOB_ERROR
} ds_blob_state_t;

struct ds_blob {
	char *blobid;
	ds_blob_state_t state;
	int64_t size;
	char *md5hash;
	struct jx *meta;
};

struct ds_blob * ds_blob_create( const char *blobid, jx_int_t size, struct jx *meta);
struct ds_blob * ds_blob_create_from_jx( struct jx *jblob );
struct ds_blob * ds_blob_create_from_file( const char *filename );

struct jx * ds_blob_to_jx( struct ds_blob *b );
int ds_blob_to_file( struct ds_blob *b, const char *filename );

void ds_blob_delete( struct ds_blob *b );

char *ds_blob_state_string( ds_blob_state_t state );

#endif
