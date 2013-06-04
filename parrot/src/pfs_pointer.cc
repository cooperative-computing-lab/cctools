/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2005- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "pfs_pointer.h"
#include "pfs_file.h"

#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <string.h>
#include <stdio.h>

pfs_pointer::pfs_pointer(pfs_file * f, int fl, int m)
{
	file = f;
	flags = fl;
	mode = m;
	offset = 0;

	/* Remove any flags that have a one-time effect */
	flags = flags & ~(O_TRUNC);
	flags = flags & ~(O_CREAT);
}

pfs_pointer::~pfs_pointer()
{
}

pfs_off_t pfs_pointer::seek(pfs_off_t value, int whence)
{
	if(whence == SEEK_SET) {
		offset = value;
	} else if(whence == SEEK_CUR) {
		offset += value;
	} else {
		offset = file->get_size() + value;
	}

	return offset;
}

void pfs_pointer::bump(pfs_off_t value)
{
	offset += value;
}

pfs_off_t pfs_pointer::tell()
{
	return offset;
}
