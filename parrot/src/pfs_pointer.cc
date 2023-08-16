/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

extern "C" {
#include "debug.h"
}

#include "pfs_pointer.h"
#include "pfs_file.h"

#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <string.h>
#include <stdio.h>

std::map<std::pair<dev_t, ino_t>, pfs_pointer *> pfs_pointer::pointers;

pfs_pointer::pfs_pointer( pfs_file *f, int fl, int m )
{
	file = f;
	flags = fl;
	mode = m;
	offset = 0;

	dev = 0;
	ino = 0;

	/* Remove any flags that have a one-time effect */
	flags = flags & ~(O_TRUNC);
	flags = flags & ~(O_CREAT);
}

void pfs_pointer::bind( dev_t dev, ino_t ino )
{
	debug(D_DEBUG, "binding to <dev=%d, ino=%d>", (int)dev, (int)ino);
	this->dev = dev;
	this->ino = ino;
	pointers[std::pair<dev_t, ino_t>(dev, ino)] = this;
}

pfs_pointer *pfs_pointer::lookup( dev_t dev, ino_t ino )
{
	debug(D_DEBUG, "looking up <dev=%d, ino=%d>", (int)dev, (int)ino);
	return pointers[std::pair<dev_t, ino_t>(dev, ino)];
}

pfs_pointer::~pfs_pointer()
{
	if (this->dev)
		pointers.erase(std::pair<dev_t, ino_t>(this->dev, this->ino));
}

pfs_off_t pfs_pointer::seek( pfs_off_t value, int whence )
{
	if(whence==SEEK_SET) {
		if(value<0) return (errno = EINVAL, -1);
		offset = value;
	} else if(whence==SEEK_CUR) {
		if(offset+value<0) return (errno = EINVAL, -1);
		offset += value;
	} else {
		if(file->get_size()+value<0) return (errno = EINVAL, -1);
		offset = file->get_size()+value;
	}

	return offset;
}

void pfs_pointer::bump( pfs_off_t value )
{
	offset += value;
}

pfs_off_t pfs_pointer::tell()
{
	return offset;
}

/* vim: set noexpandtab tabstop=8: */
