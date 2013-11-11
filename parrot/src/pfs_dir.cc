/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2005- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "pfs_service.h"
#include "pfs_process.h"
#include "pfs_dir.h"

extern "C" {
#include "stringtools.h"
#include "hash_table.h"
#include "debug.h"
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
	data = 0;
	length = 0;
	maxlength = 0;
	total_iterations = 0;
}

pfs_dir::~pfs_dir()
{
	if(data) free(data);
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
	static union {
		struct dirent entry;
		char padding[sizeof(struct dirent)+_POSIX_PATH_MAX];
	} d;

	/*
	Insane little hack: tcsh will not consider a directory
	entry executable if its inode field happens to be zero.
	*/

	errno = 0;

	if(!data) return 0;
	if(offset<0) return 0;

	if(offset>=length) {
		total_iterations++;
		return 0;
	}

	/*
	Hack: Newer versions of rm keep re-reading a directory until
	all entries are gone.  Since Parrot generates a snapshot of
	a directory at open-time, this results in an infinite loop.
	*/

	if(total_iterations>0 && (!strcmp(pfs_process_name(),"/bin/rm") || !strcmp(pfs_process_name(),"/usr/bin/rm")) ) {
		debug(D_LIBCALL,"end of directory reached, shortcutting further iterations by rm");
		return 0;
	}


	char *result = &data[offset];

	memset(&d,0,sizeof(d));
	strcpy(d.entry.d_name,result);
	d.entry.d_ino = hash_string(d.entry.d_name);
	d.entry.d_off = offset;
	d.entry.d_reclen = sizeof(d.entry) + strlen(d.entry.d_name);
	d.entry.d_type = 0;

	*next_offset = offset + strlen(result)+1;

	return &d.entry;
}

// A directory object is always seekable, since it constructs
// sequentially in memory, and is then accessed randomly.

int pfs_dir::is_seekable()
{
	return 1;
}

int pfs_dir::append( const char *srcname )
{
	char name[PFS_PATH_MAX];
	char *s;

	/* Clean up the insane names that systems give us */
	strcpy(name,srcname);
	string_chomp(name);

	/* Some place the name of the listed directory in the listing itself, followed by a colon.  Sheesh. */
	s = &name[strlen(name)-1];
	if( (*s==':') || (*s==' ' && *(s-1)==':') ) {
		return 1;
	}

	/* Some hose up directory names by adding slashes */
	s = name + strlen(name)-1;
	while( s>=name && *s=='/' ) {
		*s = 0;
		s--;
	}

	/* If that leaves nothing, then skip it */
	if( s<name ) {
		return 1;
	}

	/* Strip off any leading directory parts. */
	s = strrchr(name,'/');
	if(s) {
		int length;
		s++;
		length = strlen(s);
		memmove(name,s,length);
		name[length] = 0;
	}

	/* Finally, use the cleaned-up name. */

	if(!data) {
		maxlength = 4096;
		data = (char*) malloc(maxlength);
		if(!data) return 0;
	}

	if( ((int)(strlen(name)+length+1)) > (int)maxlength ) {
		char *newdata = (char*) realloc(data,maxlength*2);
		if(!newdata) return 0;
		data = newdata;
		maxlength*=2;
	}

	strcpy(&data[length],name);
	length += strlen(name)+1;

	return 1;
}

/* vim: set noexpandtab tabstop=4: */
