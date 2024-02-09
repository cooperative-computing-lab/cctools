/*
Copyright (C) 2022- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef VINE_CACHE_H
#define VINE_CACHE_H

/*
The cache module keeps track of the intention and state of objects
in the worker cache.  This includes plain files which have been
sent directly by the manager, as well as requests to create files
by transferring urls or executing Unix commands.  Requests for
transfers or commands are queued and not executed immediately.
When a task is about to be executed, each input file is checked
via vine_cache_ensure and downloaded if needed.  This allow
for file transfers to occur asynchronously of the manager.
*/

#include <stdint.h>
#include "vine_file.h"
#include "vine_cache_meta.h"

struct link;
struct vine_cache_file;

typedef enum {
	VINE_CACHE_FILE,
	VINE_CACHE_TRANSFER,
	VINE_CACHE_MINI_TASK,
} vine_cache_type_t;

typedef enum {
	VINE_CACHE_FLAGS_ON_TASK = 1,
	VINE_CACHE_FLAGS_NOW = 2,
} vine_cache_flags_t;

typedef enum {
	VINE_CACHE_STATUS_NOT_PRESENT,
	VINE_CACHE_STATUS_PROCESSING,
	VINE_CACHE_STATUS_READY,
	VINE_CACHE_STATUS_FAILED,       
} vine_cache_status_t;

struct vine_cache * vine_cache_create( const char *cachedir );
void vine_cache_delete( struct vine_cache *c );
void vine_cache_load( struct vine_cache *c );
void vine_cache_scan( struct vine_cache *c, struct link *manager );

char *vine_cache_data_path( struct vine_cache *c, const char *cachename );
char *vine_cache_meta_path( struct vine_cache *c, const char *cachename );

int vine_cache_addfile( struct vine_cache *c, struct vine_cache_meta *meta, const char *cachename );
int vine_cache_queue_transfer( struct vine_cache *c, const char *source, const char *cachename, struct vine_cache_meta *meta, int flags );
int vine_cache_queue_mini_task( struct vine_cache *c, struct vine_task *minitask, const char *source, const char *cachename, struct vine_cache_meta *meta );

vine_cache_status_t vine_cache_ensure( struct vine_cache *c, const char *cachename);
int vine_cache_remove( struct vine_cache *c, const char *cachename, struct link *manager );
int vine_cache_contains( struct vine_cache *c, const char *cachename );
int vine_cache_wait( struct vine_cache *c, struct link *manager );

#endif
