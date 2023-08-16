/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#define _XOPEN_SOURCE 500
#define _LARGEFILE64_SOURCE 1

#include <fcntl.h>
#include <unistd.h>

#include <sys/types.h>
#include <sys/uio.h>

#include <errno.h>
#include <stdint.h>
#include <stdio.h>

#include "full_io.h"

#define _FULL_IO(expr,offset,err,errno) \
	do {\
		ssize_t total = 0;\
		while(count > 0) {\
			ssize_t chunk = (expr);\
			if(err) {\
				if(errno == EINTR) {\
					continue;\
				} else if (total == 0) {\
					return -1;\
				} else {\
					break;\
				}\
			} else if(chunk == 0) {\
				break;\
			} else {\
				total += chunk;\
				count -= chunk;\
				offset;\
				buf = ((uint8_t *) buf) + chunk;\
			}\
		}\
		return total;\
	} while (0)

#define FULL_IO(expr) _FULL_IO(expr,(void)0,chunk<0,errno)
#define FULL_PIO(expr) _FULL_IO(expr,offset+=chunk,chunk<0,errno)
#define FULL_FIO(expr) _FULL_IO(expr,(void)0,ferror(file),ferror(file))

ssize_t full_read(int fd, void *buf, size_t count)
{
	FULL_IO(read(fd, buf, count));
}

ssize_t full_write(int fd, const void *buf, size_t count)
{
	FULL_IO(write(fd, buf, count));
}

ssize_t full_pread64(int fd, void *buf, size_t count, int64_t offset)
{
#ifdef CCTOOLS_OPSYS_DARWIN
	FULL_PIO(pread(fd, buf, count, offset));
#else
	FULL_PIO(pread64(fd, buf, count, offset));
#endif
}

ssize_t full_pwrite64(int fd, const void *buf, size_t count, int64_t offset)
{
#ifdef CCTOOLS_OPSYS_DARWIN
	FULL_PIO(pwrite(fd, buf, count, offset));
#else
	FULL_PIO(pwrite64(fd, buf, count, offset));
#endif
}

ssize_t full_pread(int fd, void *buf, size_t count, off_t offset)
{
	FULL_PIO(pread(fd, buf, count, offset));
}

ssize_t full_pwrite(int fd, const void *buf, size_t count, off_t offset)
{
	FULL_PIO(pwrite(fd, buf, count, offset));
}

ssize_t full_fread(FILE * file, void *buf, size_t count)
{
	FULL_FIO(fread(buf, 1, count, file));
}

ssize_t full_fwrite(FILE * file, const void *buf, size_t count)
{
	FULL_FIO(fwrite(buf, 1, count, file));
}

/* vim: set noexpandtab tabstop=8: */
