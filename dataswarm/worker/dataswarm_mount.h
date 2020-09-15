#ifndef DATASWARM_MOUNT_H
#define DATASWARM_MOUNT_H

#include "jx.h"

typedef enum {
	DATASWARM_FLAGS_READ=1,
	DATASWARM_FLAGS_WRITE=2,
	DATASWARM_FLAGS_APPEND=4,
	DATASWARM_FLAGS_TRUNCATE=8
} dataswarm_flags_t;

typedef enum {
	DATASWARM_MOUNT_PATH,
	DATASWARM_MOUNT_FD
} dataswarm_mount_t;

struct dataswarm_mount {
	const char *uuid;
	dataswarm_mount_t type;

	// would be better to make this a variant type
	int fd;	
	char *path;
	dataswarm_flags_t flags;
	struct dataswarm_mount *next;
};

// Parse a whole object full of mounts 
struct dataswarm_mount * dataswarm_mounts_create( struct jx *jmounts );
struct jx * dataswarm_mounts_to_jx( struct dataswarm_mount *m );

// Parse a single mount object.
struct dataswarm_mount * dataswarm_mount_create( const char *uuid, struct jx *jmount );
struct jx * dataswarm_mount_to_jx( struct dataswarm_mount *m );

void dataswarm_mount_delete( struct dataswarm_mount *m );

#endif
