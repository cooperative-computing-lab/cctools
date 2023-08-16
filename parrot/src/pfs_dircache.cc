/*
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "pfs_dircache.h"
#include "pfs_dir.h"
#include "pfs_types.h"

extern "C" {
#include "hash_table.h"
#include "path.h"
#include "xxmalloc.h"
}

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/mman.h>
#include <string.h>

pfs_dircache::pfs_dircache()
{
	dircache_table = 0;
	dircache_path  = 0;
}

pfs_dircache::~pfs_dircache()
{
	invalidate();

	if (dircache_table)
		hash_table_delete(dircache_table);

	if (dircache_path)
		free(dircache_path);
}

void pfs_dircache::invalidate()
{
	char *key;
	void *value;

	if (dircache_table) {
		hash_table_firstkey(dircache_table);
		while (hash_table_nextkey(dircache_table, &key, &value)) {
			hash_table_remove(dircache_table, key);
			free(value);
		}
	}

	if (dircache_path) {
		free(dircache_path);
		dircache_path = 0;
	}
}

void pfs_dircache::begin( const char *path )
{
	invalidate();
	dircache_path = xxstrdup(path);
}

void pfs_dircache::insert( const char *name, struct pfs_stat *buf, pfs_dir *dir )
{
	char path[PFS_PATH_MAX];
	struct pfs_stat *copy;

	if (!dircache_table) dircache_table = hash_table_create(0, 0);

	dir->append(name);

	copy = (struct pfs_stat *)xxmalloc(sizeof(struct pfs_stat));
	*copy = *buf;

	sprintf(path, "%s/%s", dircache_path, path_basename(name));
	hash_table_insert(dircache_table, path, copy);
}

int pfs_dircache::lookup( const char *path, struct pfs_stat *buf )
{
	struct pfs_stat *value;
	int result = 0;

	if (!dircache_table) dircache_table = hash_table_create(0, 0);

	value = (struct pfs_stat *)hash_table_lookup(dircache_table, path);
	if (value) {
		*buf = *value;
		hash_table_remove(dircache_table, path);
		free(value);
		result = 1;
	}

	return (result);
}

/* vim: set noexpandtab tabstop=8: */
