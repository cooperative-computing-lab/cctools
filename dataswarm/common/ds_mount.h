#ifndef DATASWARM_MOUNT_H
#define DATASWARM_MOUNT_H

#include "jx.h"

typedef enum {
	DS_FLAGS_READ=1,
	DS_FLAGS_WRITE=2,
	DS_FLAGS_APPEND=4,
	DS_FLAGS_TRUNCATE=8
} dataswarm_flags_t;

typedef enum {
	DS_MOUNT_PATH,
	DS_MOUNT_FD
} ds_mount_t;

struct ds_mount {
	const char *uuid;
	ds_mount_t type;

	// would be better to make this a variant type
	int fd;	
	char *path;
	dataswarm_flags_t flags;
	struct ds_mount *next;
};

// Parse a whole object full of mounts 
struct ds_mount * ds_mounts_create( struct jx *jmounts );
struct jx * ds_mounts_to_jx( struct ds_mount *m );

// Parse a single mount object.
struct ds_mount * ds_mount_create( const char *uuid, struct jx *jmount );
struct jx * ds_mount_to_jx( struct ds_mount *m );

void ds_mount_delete( struct ds_mount *m );

#endif
