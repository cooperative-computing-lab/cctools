/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2005- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef PFS_CHANNEL_CACHE_H
#define PFS_CHANNEL_CACHE_H

#ifdef __cplusplus
extern "C" {
#endif

#include "pfs_types.h"

int   pfs_channel_cache_alloc( const char *name, int fd, pfs_size_t *length, pfs_size_t *start );
int   pfs_channel_cache_freename( const char *name );
void  pfs_channel_cache_freeaddr( pfs_size_t start, pfs_size_t length );

int   pfs_channel_cache_make_dirty( const char *name );
int   pfs_channel_cache_is_dirty( const char *name );
int   pfs_channel_cache_refs( const char *name );
int   pfs_channel_cache_start( const char *name, pfs_size_t *start );

#ifdef __cplusplus
}
#endif

#endif
