/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2005- The University of Notre Dame
This software is distributed under a BSD-style license.
See the file COPYING for details.
*/

#ifndef PFS_RESOLVE
#define PFS_RESOLVE

#include <time.h>

typedef enum {
	PFS_RESOLVE_UNCHANGED,
	PFS_RESOLVE_CHANGED,
	PFS_RESOLVE_DENIED,
	PFS_RESOLVE_ENOENT,
	PFS_RESOLVE_FAILED
} pfs_resolve_t;

void pfs_resolve_file_config( const char *mountfile );
void pfs_resolve_manual_config( const char *string );

pfs_resolve_t pfs_resolve( const char *logical_name, char *physical_name, time_t stoptime );

#endif

