/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2005- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef PFS_RESOLVE
#define PFS_RESOLVE

#include <time.h>
#include "pfs_types.h"

typedef enum {
	PFS_RESOLVE_UNCHANGED,
	PFS_RESOLVE_CHANGED,
	PFS_RESOLVE_DENIED,
	PFS_RESOLVE_ENOENT,
	PFS_RESOLVE_FAILED
} pfs_resolve_t;

void pfs_resolve_file_config( const char *mountfile );
void pfs_resolve_manual_config( const char *string );

void pfs_resolve_add_entry( const char *path, const char *device, mode_t mode );
int pfs_resolve_remove_entry( const char *path );

pfs_resolve_t pfs_resolve( const char *logical_name, char *physical_name, mode_t mode, time_t stoptime );

#endif
