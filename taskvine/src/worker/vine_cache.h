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

#include "link.h"

typedef enum {
	VINE_CACHE_FILE,               /**< A normal file provided by the manager. */
	VINE_CACHE_TRANSFER,           /**< Obtain the file by performing a transfer. */
	VINE_CACHE_MINI_TASK,          /**< Obtain the file by executing a mini-task. */
} vine_cache_type_t;

typedef enum {
	VINE_CACHE_FLAGS_ON_TASK = 1,   /**< Do this transfer as needed for task. */
	VINE_CACHE_FLAGS_NOW = 2,       /**< Start this transfer now for replication. */
} vine_cache_flags_t;

typedef enum {
	VINE_CACHE_STATUS_PENDING,      /**< File is known but does not exist yet. */
	VINE_CACHE_STATUS_PROCESSING,   /**< Transfer process is running now. */
	VINE_CACHE_STATUS_TRANSFERRED,  /**< Transfer process complete, not ingested yet. */
	VINE_CACHE_STATUS_READY,        /**< File is present and ready to use. */
	VINE_CACHE_STATUS_FAILED,       /**< Transfer process failed. */
	VINE_CACHE_STATUS_UNKNOWN,      /**< File is not known at all to the cache manager. */
} vine_cache_status_t;

struct vine_cache * vine_cache_create( const char *cachedir, int max_procs );
void vine_cache_delete( struct vine_cache *c );
void vine_cache_load( struct vine_cache *c );
void vine_cache_scan( struct vine_cache *c, struct link *manager );
void vine_cache_prune( struct vine_cache *c, vine_cache_level_t level );

char *vine_cache_data_path( struct vine_cache *c, const char *cachename );
char *vine_cache_meta_path( struct vine_cache *c, const char *cachename );
char *vine_cache_transfer_path( struct vine_cache *c, const char *cachename );
char *vine_cache_error_path( struct vine_cache *c, const char *cachename );

int vine_cache_add_file( struct vine_cache *c, const char *cachename, const char *transfer_path, vine_cache_level_t level, int mode, uint64_t size, time_t mtime, timestamp_t start_time, timestamp_t transfer_time, struct link *manager );
int vine_cache_add_transfer( struct vine_cache *c, const char *cachename, const char *source, vine_cache_level_t level, int mode, uint64_t size, vine_cache_flags_t flags );
int vine_cache_add_mini_task( struct vine_cache *c, const char *cachename, const char *source, struct vine_task *mini_task, vine_cache_level_t level, int mode, uint64_t size );

vine_cache_status_t vine_cache_ensure( struct vine_cache *c, const char *cachename);
int vine_cache_remove( struct vine_cache *c, const char *cachename, struct link *manager );
int vine_cache_contains( struct vine_cache *c, const char *cachename );

int vine_cache_check_files( struct vine_cache *c, struct link *manager );
int vine_cache_start_transfers(struct vine_cache *c);

#endif
