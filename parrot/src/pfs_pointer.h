/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef PFS_FILE_POINTER_H
#define PFS_FILE_POINTER_H

#include "pfs_types.h"
#include "pfs_file.h"
#include "pfs_refcount.h"

#include <map>

#include <sys/stat.h>

class pfs_pointer : public pfs_refcount {
private:
	static std::map<std::pair<dev_t, ino_t>, pfs_pointer *> pointers;
	dev_t dev;
	ino_t ino;

public:
	pfs_pointer( pfs_file *f, int flags, int mode );
	~pfs_pointer();

	pfs_off_t seek( pfs_off_t offset, int whence );
	pfs_off_t tell();
	void bump( pfs_off_t offset );

	void bind(dev_t dev, ino_t ino);
	static pfs_pointer *lookup(dev_t dev, ino_t ino);

	pfs_file *file;

	int flags;
	int mode;
	pfs_off_t offset;
};

#endif

/* vim: set noexpandtab tabstop=4: */
