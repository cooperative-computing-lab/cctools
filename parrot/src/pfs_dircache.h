/*
Copyright (C) 2009- The University of Notre Dame
This software is distributed under a BSD-style license.
See the file COPYING for details.
*/

#ifndef PFS_DIRCACHE_H
#define PFS_DIRCACHE_H

#include "pfs_types.h"

extern "C" {
#include "hash_table.h"
#include "stringtools.h"
#include "xmalloc.h"
}

class pfs_dir;

class pfs_dircache {
public:
	pfs_dircache();
	virtual ~pfs_dircache();

	virtual void invalidate();
	virtual void begin( const char *path );
	virtual void insert( const char *name, struct pfs_stat *buf, pfs_dir *dir);
	virtual int lookup( const char *path, struct pfs_stat *buf );

protected:
	struct hash_table *dircache_table;
	char *dircache_path;	
};

#endif
