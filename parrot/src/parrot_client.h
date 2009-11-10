/*
Copyright (C) 2008- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef PARROTCLIENT_H_
#define PARROTCLIENT_H_

#include "int_sizes.h"
#include <stdlib.h>

int parrot_whoami( const char *path, char *buf, int size );
int parrot_locate( const char *path, char *buf, int size );
int parrot_getacl( const char *path, char *buf, int size );
int parrot_setacl( const char *path, const char *subject, const char *rights );
int parrot_md5( const char *filename, unsigned char *digest );
int parrot_cp( const char *source, const char *dest );
int parrot_mkalloc( const char *path, INT64_T size, mode_t mode );
int parrot_lsalloc( const char *path, char *alloc_path, INT64_T *total, INT64_T *inuse );

#endif

