/*
Copyright (C) 2022- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef DS_CACHE_H
#define DS_CACHE_H

/*
The cache module keeps track of the intention and state of objects
in the worker cache.  This includes plain files which have been
sent directly by the manager, as well as requests to create files
by transferring urls or executing Unix commands.  Requests for
transfers or commands are queued and not executed immediately.
When a task is about to be executed, each input file is checked
via ds_cache_ensure and downloaded if needed.  This allow
for file transfers to occur asynchronously of the manager.
*/

#include <stdint.h>
#include "ds_file.h"

struct link;

typedef enum {
	DS_CACHE_FILE,
	DS_CACHE_TRANSFER,
	DS_CACHE_COMMAND,
} ds_cache_type_t;

struct ds_cache * ds_cache_create( const char *cachedir );
void ds_cache_delete( struct ds_cache *c );

char *ds_cache_full_path( struct ds_cache *c, const char *cachename );

int ds_cache_addfile( struct ds_cache *c, int64_t size, const char *cachename );
int ds_cache_queue( struct ds_cache *c, ds_cache_type_t type, const char *source, const char *cachename, int64_t size, int mode, ds_file_flags_t flags );
int ds_cache_ensure( struct ds_cache *c, const char *cachename, struct link *manager );
int ds_cache_remove( struct ds_cache *c, const char *cachename );

#endif
