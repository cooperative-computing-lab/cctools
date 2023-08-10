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

struct link;
struct cache_file;

typedef enum {
	VINE_CACHE_FILE,
	VINE_CACHE_TRANSFER,
	VINE_CACHE_MINI_TASK,
} vine_cache_type_t;

typedef enum {
	VINE_FILE_STATUS_UNKNOWN,
	VINE_FILE_STATUS_READY,
	VINE_FILE_STATUS_FAILED,
	VINE_FILE_STATUS_PROCESSING,
} vine_file_status_type_t;



struct vine_cache * vine_cache_create( const char *cachedir );
void vine_cache_delete( struct vine_cache *c );
void vine_cache_load( struct vine_cache *c );
void vine_cache_scan( struct vine_cache *c, struct link *manager );
void vine_cache_get_file(struct cache_file *f, struct vine_cache *c, const char *cachename);

char *vine_cache_full_path( struct vine_cache *c, const char *cachename );

int vine_cache_addfile( struct vine_cache *c, int64_t size, int mode, const char *cachename );
int vine_cache_queue_transfer( struct vine_cache *c, const char *source, const char *cachename, int64_t size, int mode );
int vine_cache_queue_command( struct vine_cache *c, struct vine_task *minitask, const char *cachename, int64_t size, int mode );
vine_file_status_type_t vine_cache_ensure( struct vine_cache *c, const char *cachename);
int vine_cache_remove( struct vine_cache *c, const char *cachename );
int vine_cache_contains( struct vine_cache *c, const char *cachename );
int vine_cache_wait( struct vine_cache *c, struct link *manager );
void vine_cache_process_entry(struct cache_file *f, char *cachename, struct vine_cache *c, struct link *manager);
void vine_cache_check_outputs(struct cache_file *f, char *cachename, struct vine_cache *c, struct link *manager);
void vine_cache_handle_exit_status(int status, char *cachename, struct cache_file *f, struct link *manager);

#endif
