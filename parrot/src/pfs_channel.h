/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2005- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef PFS_CHANNEL_H
#define PFS_CHANNEL_H

#include "pfs_types.h"

#ifdef __cplusplus
extern "C" {
#endif

int    pfs_channel_init( pfs_size_t size );

char * pfs_channel_base();
int    pfs_channel_fd();

int    pfs_channel_alloc( pfs_size_t length, pfs_size_t *start );
void   pfs_channel_free( pfs_size_t start );

#ifdef __cplusplus
}
#endif

#endif
