/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2005- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef PFS_FILE_CACHE_H
#define PFS_FILE_CACHE_H

#include "pfs_file.h"

pfs_file * pfs_cache_open( pfs_name *name, int flags, mode_t mode );
int        pfs_cache_invalidate( pfs_name *name );

#endif
