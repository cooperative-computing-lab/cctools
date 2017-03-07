/*
Copyright (C) 2017- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef GROW_H
#define GROW_H

#define GROW_LINE_MAX 4096
#define GROW_EPOCH 1199163600

#include <stdint.h>
#include <stdio.h>
#include <time.h>

#include "sha1.h"
#include "pfs_types.h"

/*
A grow_dirent is a node in a tree representing the
entire directory structure of a grow_filesystem.
Each node describes its name, metadata, checksum,
and children (if a directory)
*/

struct grow_dirent {
	char *name;
	char *linkname;
	unsigned mode;
	UINT64_T size;
	UINT64_T inode;
	time_t   mtime;
	char checksum[SHA1_DIGEST_ASCII_LENGTH];
	struct grow_dirent *children;
	struct grow_dirent *parent;
	struct grow_dirent *next;
};

void grow_dirent_delete( struct grow_dirent *d );
int compare_path_element( const char *a, const char *b );
const char * compare_path_prefix( const char *a, const char *b );
struct grow_dirent * grow_dirent_create_from_file( FILE *file, struct grow_dirent *parent );
void grow_dirent_delete( struct grow_dirent *d );
void grow_dirent_to_pfs_stat( struct grow_dirent *d, struct pfs_stat *s );
void grow_dirent_to_stat( struct grow_dirent *d, struct stat *s );
struct grow_dirent * grow_dirent_lookup_recursive( const char *path, struct grow_dirent *root, int link_count );

#endif
