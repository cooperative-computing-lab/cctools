/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/
#include "preadwrite.h"

#include <unistd.h>
#include <errno.h>
#include <sys/types.h>

#ifndef HAS_PREAD
ssize_t pread(int fd, void *data, size_t length, off_t offset)
{
	ssize_t result;
	off_t save_offset;
	int save_errno;
	save_offset = lseek(fd, offset, SEEK_SET);
	if(save_offset == -1)
		return -1;
	result = read(fd, data, length);
	save_errno = errno;
	lseek(fd, save_offset, SEEK_SET);
	errno = save_errno;
	return result;
}


#endif /*  */

#ifndef HAS_PWRITE
ssize_t pwrite(int fd, const void *data, size_t length, off_t offset)
{
	ssize_t result;
	off_t save_offset;
	int save_errno;
	save_offset = lseek(fd, offset, SEEK_SET);
	if(save_offset == -1)
		return -1;
	result = write(fd, data, length);
	save_errno = errno;
	lseek(fd, save_offset, SEEK_SET);
	errno = save_errno;
	return result;
}


#endif /*  */

/* vim: set noexpandtab tabstop=8: */
