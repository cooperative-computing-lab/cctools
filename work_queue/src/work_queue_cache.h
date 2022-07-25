#ifndef WORK_QUEUE_CACHE_H
#define WORK_QUEUE_CACHE_H

/*
The cache module keeps track of the intention and state of objects
in the worker cache.  This includes plain files which have been
sent directly by the manager, as well as requests to create files
by transferring urls or executing Unix commands.  Requests for
transfers or commands are queued and not executed immediately.
When a task is about to be executed, each input file is checked
via work_queue_cache_ensure and downloaded if needed.  This allow
for file transfers to occur asynchronously of the manager.
*/


#include <stdint.h>

struct link;

typedef enum {
	WORK_QUEUE_CACHE_FILE,
	WORK_QUEUE_CACHE_TRANSFER,
	WORK_QUEUE_CACHE_COMMAND,
} work_queue_cache_type_t;

struct work_queue_cache * work_queue_cache_create( const char *cachedir );
void work_queue_cache_delete( struct work_queue_cache *c );

char *work_queue_cache_full_path( struct work_queue_cache *c, const char *cachename );

int work_queue_cache_addfile( struct work_queue_cache *c, int64_t size, const char *cachename );
int work_queue_cache_queue( struct work_queue_cache *c, work_queue_cache_type_t, const char *source, const char *cachename, int64_t size, int mode );
int work_queue_cache_ensure( struct work_queue_cache *c, const char *cachename, struct link *manager );
int work_queue_cache_remove( struct work_queue_cache *c, const char *cachename );

#endif
