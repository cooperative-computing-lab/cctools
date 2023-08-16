/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "pfs_service.h"
#include "pfs_process.h"
#include "pfs_dir.h"

extern "C" {
#include "debug.h"
#include "hash_table.h"
#include "path.h"
#include "stringtools.h"
}

#include <unistd.h>
#include <errno.h>
#include <stdlib.h>
#include <dirent.h>
#include <string.h>
#include <fcntl.h>
#include <limits.h>
#include <stdio.h>

pfs_dir::pfs_dir( pfs_name *n ) : pfs_file(n)
{
	iterations = 0;
	if(strcmp(n->path, "/") == 0) {
		extern struct hash_table *available_services;
		char *key;
		void *value;
		hash_table_firstkey(available_services);
		while(hash_table_nextkey(available_services, &key, &value)) {
			append(key);
		}
	}
}

int pfs_dir::fstat( struct pfs_stat *buf )
{
	return name.service->stat(&name,buf);
}

int pfs_dir::fstatfs( struct pfs_statfs *buf )
{
	return name.service->statfs(&name,buf);
}

int pfs_dir::fchmod( mode_t mode )
{
	return name.service->chmod(&name,mode);
}

int pfs_dir::fchown( uid_t uid, gid_t gid )
{
	return name.service->chown(&name,uid,gid);
}

struct dirent * pfs_dir::fdreaddir( pfs_off_t offset, pfs_off_t *next_offset )
{
	errno = 0;

	if (offset < 0)
		return 0;

	if ((size_t)offset >= entries.size()) {
		iterations += 1;
		return 0;
	}

	/* Hack: Newer versions of rm keep re-reading a directory until all entries
	 * are gone. Since Parrot generates a snapshot of a directory at open-time,
	 * this results in an infinite loop.
	 */
	if(iterations>0 && (!strcmp(pfs_process_name(),"/bin/rm") || !strcmp(pfs_process_name(),"/usr/bin/rm")) ) {
		debug(D_LIBCALL,"end of directory reached, shortcutting further iterations by rm");
		return 0;
	}

	*next_offset = offset+1;
	return &entries[offset];
}

// A directory object is always seekable, since it constructs
// sequentially in memory, and is then accessed randomly.

int pfs_dir::is_seekable()
{
	return 1;
}

int pfs_dir::append( const struct dirent *d )
{
	debug(D_DEBUG, "append dirent `%s':%d", d->d_name, (int)d->d_type);
	struct dirent dcopy = *d;
	dcopy.d_reclen = sizeof(dcopy);
	dcopy.d_off = entries.size();
	entries.push_back(dcopy);
	return 1;
}

int pfs_dir::append (const char *name)
{
	debug(D_DEBUG, "append `%s'", name);
	const char *s;
	struct dirent d;
	memset(&d, 0, sizeof(d));
	strncpy(d.d_name, name, sizeof(d.d_name)-1);

	/* Clean up the insane names that systems give us */
	string_chomp(d.d_name);

	/* Some place the name of the listed directory in the listing itself, followed by a colon.  Sheesh. */
	s = &d.d_name[strlen(d.d_name)-1];
	if((*s==':') || (*s==' ' && *(s-1)==':'))
		return 1;

	/* Some hose up directory names by adding slashes */
	path_remove_trailing_slashes(d.d_name);

	/* Strip off any leading directory parts. */
	s = strrchr(name,'/');
	if(s)
		memmove(d.d_name, s+1, strlen(s+1)+1);

	/* If that leaves nothing, then skip it */
	if (strlen(d.d_name) == 0)
		return 1;

	/* Insane little hack: tcsh will not consider a directory entry executable
	 * if its inode field happens to be zero.
	 */
	d.d_ino = hash_string(d.d_name);

	return pfs_dir::append(&d);
}

/* vim: set noexpandtab tabstop=8: */
