/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2005- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef FILE_CACHE_H
#define FILE_CACHE_H

#include <sys/types.h>

#include "int_sizes.h"

struct file_cache * file_cache_init( const char *root );
void file_cache_fini( struct file_cache *c );
void file_cache_cleanup( struct file_cache *c );

int file_cache_open( struct file_cache *c, const char *path, char *lpath, INT64_T size, time_t mtime );
int file_cache_delete( struct file_cache *f, const char *path );
int file_cache_contains( struct file_cache *f, const char *path, char *lpath);

int file_cache_begin( struct file_cache *c, const char *path, char *txn );
int file_cache_commit( struct file_cache *c, const char *path, const char *txn );
int file_cache_abort( struct file_cache *c, const char *path, const char *txn );

#endif
