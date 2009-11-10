/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2005- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef PREADWRITE_H
#define PREADWRITE_H

#include <unistd.h>

ssize_t pread(int fd, void *data, size_t length, off_t offset );
ssize_t pwrite(int fd, const void *data, size_t length, off_t offset );

#endif
