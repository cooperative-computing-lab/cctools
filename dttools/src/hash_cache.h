/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2005- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef HASH_CACHE_H
#define HASH_CACHE_H

#include "hash_table.h"

/*
A hash_cache is like a hash_table, with one key difference:
Each item is inserted with a lifetime given in seconds.
Once the lifetime has expired, the item is automatically
deleted (using the given cleanup function) and the user
will not see it again.
*/


typedef void (*hash_cache_cleanup_t) ( void *value );

struct hash_cache * hash_cache_create( int size, hash_func_t func, hash_cache_cleanup_t cleanup );
void                hash_cache_delete( struct hash_cache *cache );

int    hash_cache_insert( struct hash_cache *cache, const char *key, void *value, int lifetime );
void * hash_cache_remove( struct hash_cache *cache, const char *key );
void * hash_cache_lookup( struct hash_cache *cache, const char *key );

void   hash_cache_firstkey( struct hash_cache *cache );
int    hash_cache_nextkey( struct hash_cache *cache, char **key, void **item );

#endif

