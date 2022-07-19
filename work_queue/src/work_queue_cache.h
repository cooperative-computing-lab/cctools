#ifndef WORK_QUEUE_CACHE_H
#define WORK_QUEUE_CACHE_H

#include <stdint.h>

typedef enum {
	WORK_QUEUE_CACHE_FILE,
	WORK_QUEUE_CACHE_TRANSFER,
	WORK_QUEUE_CACHE_COMMAND,
} work_queue_cache_type_t;

struct work_queue_cache * work_queue_cache_create( const char *cachedir );

void work_queue_cache_delete( struct work_queue_cache *c );
int work_queue_cache_addfile( struct work_queue_cache *c, int64_t size, const char *cachename );
int work_queue_cache_queue( struct work_queue_cache *c, work_queue_cache_type_t, const char *source, const char *cachename );
int work_queue_cache_ensure( struct work_queue_cache *c, const char *cachename );
int work_queue_cache_remove( struct work_queue_cache *c, const char *cachename );

#endif
