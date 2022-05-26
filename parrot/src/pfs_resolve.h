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
	PFS_RESOLVE_FAILED,
	PFS_RESOLVE_LOCAL,
} pfs_resolve_t;

struct pfs_mount_entry {
	unsigned refcount;
	char prefix[PFS_PATH_MAX];
	char redirect[PFS_PATH_MAX];
	mode_t mode;
	struct pfs_mount_entry *next;
	struct pfs_mount_entry *parent;
};

void pfs_resolve_init(void);

void pfs_resolve_add_entry( const char *path, const char *device, mode_t mode );
int pfs_resolve_remove_entry( const char *path );

int pfs_resolve_mount ( const char *path, const char *destination, const char *mode );

pfs_resolve_t pfs_resolve( const char *logical_name, char *physical_name, mode_t mode, time_t stoptime );

struct pfs_mount_entry *pfs_resolve_fork_ns(struct pfs_mount_entry *ns);
struct pfs_mount_entry *pfs_resolve_share_ns(struct pfs_mount_entry *ns);
void pfs_resolve_drop_ns(struct pfs_mount_entry *ns);
void pfs_resolve_seal_ns(void);

#endif
