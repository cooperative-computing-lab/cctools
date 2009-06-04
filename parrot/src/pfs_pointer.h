/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2005- The University of Notre Dame
This software is distributed under a BSD-style license.
See the file COPYING for details.
*/

#ifndef PFS_FILE_POINTER_H
#define PFS_FILE_POINTER_H

#include "pfs_types.h"
#include "pfs_file.h"
#include "pfs_refcount.h"

class pfs_pointer : public pfs_refcount {
public:
	pfs_pointer( pfs_file *f, int flags, int mode );
	~pfs_pointer();

	pfs_off_t seek( pfs_off_t offset, int whence );
	pfs_off_t tell();
	void bump( pfs_off_t offset );

	pfs_file *file;

	int flags;
	int mode;
	pfs_off_t offset;
};

#endif
