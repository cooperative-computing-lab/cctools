#ifndef VINE_CACHE_META_H
#define VINE_CACHE_META_H

#include "vine_file.h"

#include "link.h"
#include "taskvine.h"
#include "timestamp.h"

#include <sys/time.h>

typedef enum {
	VINE_CACHE_LEVEL_TASK=0,
	VINE_CACHE_LEVEL_WORKFLOW=1,
	VINE_CACHE_LEVEL_WORKER=2,
	VINE_CACHE_LEVEL_FOREVER=3
} vine_cache_level_t;

struct vine_cache_meta {
	vine_file_type_t type;     // type of the object: file, url, temp, etc..
	vine_cache_level_t cache_level; // how long to cache the object.
	uint64_t size;             // summed size of the file or dir tree in bytes
	time_t mtime;              // source mtime as reported by the manager
	timestamp_t transfer_time; // time to transfer (or create) the object
};

struct vine_cache_meta * vine_cache_meta_create( vine_file_type_t type, vine_cache_level_t cache_level, uint64_t size, time_t mtime, timestamp_t transfer_time );
void vine_cache_meta_delete( struct vine_cache_meta *m );

struct vine_cache_meta * vine_cache_meta_load( const char *filename );
int vine_cache_meta_save( struct vine_cache_meta *m, const char *filename );

#endif
