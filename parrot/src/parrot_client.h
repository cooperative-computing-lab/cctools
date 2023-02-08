/*
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef PARROTCLIENT_H
#define PARROTCLIENT_H

#include "pfs_search.h"

#include <stdint.h>
#include <stdlib.h>

int parrot_whoami( const char *path, char *buf, int size );
int parrot_locate( const char *path, char *buf, int size );
int parrot_getacl( const char *path, char *buf, int size );
int parrot_setacl( const char *path, const char *subject, const char *rights );
int parrot_md5( const char *filename, unsigned char *digest );
int parrot_cp( const char *source, const char *dest );
int parrot_mkalloc( const char *path, int64_t size, mode_t mode );
int parrot_lsalloc( const char *path, char *alloc_path, int64_t *total, int64_t *inuse );
int parrot_timeout( const char *time );
SEARCH *parrot_opensearch(const char *path, const char *pattern, int flags);
struct searchent *parrot_readsearch(SEARCH *search);
int parrot_closesearch(SEARCH *search);
int parrot_debug( const char *flags, const char *path, off_t size );
int parrot_mount( const char *path, const char *destination, const char *mode );
int parrot_unmount( const char *path );
ssize_t parrot_version ( char *buf, size_t len );
int parrot_fork_namespace ( void );

#endif
