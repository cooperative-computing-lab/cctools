/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2005- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef IRODS_RELI_H
#define IRODS_RELI_H

#include "pfs_types.h"

struct irods_file;

struct irods_file * irods_reli_open ( const char *server, const char *path, int flags, int mode );
int irods_reli_pread    ( struct irods_file *file, char *data, int length, INT64_T offset );
int irods_reli_pwrite   ( struct irods_file *file, const char *data, int length, INT64_T offset );
int irods_reli_fsync    ( struct irods_file *file );
int irods_reli_close    ( struct irods_file *file );

int irods_reli_getdir   ( const char *server, const char *path, void (*callback) ( const char *name, void *arg ), void *arg );
int irods_reli_stat     ( const char *server, const char *path, struct pfs_stat *info );
int irods_reli_statfs   ( const char *server, const char *path, struct pfs_statfs *info );
int irods_reli_unlink   ( const char *server, const char *path );
int irods_reli_mkdir    ( const char *server, const char *path );
int irods_reli_rmdir    ( const char *server, const char *path );
int irods_reli_rename   ( const char *server, const char *path, const char *newpath );
int irods_reli_truncate ( const char *server, const char *path, INT64_T size );
int irods_reli_md5      ( const char *server, const char *path, char *digest );

#endif
