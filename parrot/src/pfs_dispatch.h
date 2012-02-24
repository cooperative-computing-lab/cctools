/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2005- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef PFS_DISPATCH_H
#define PFS_DISPATCH_H

#include "pfs_process.h"

void pfs_dispatch( struct pfs_process *p, INT64_T signum );
void pfs_dispatch32( struct pfs_process *p, INT64_T signum );
void pfs_dispatch64( struct pfs_process *p, INT64_T signum );

#endif
