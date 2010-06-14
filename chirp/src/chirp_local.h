/*
Copyright (C) 2008- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef CHIRP_LOCAL_H
#define CHIRP_LOCAL_H

#include "chirp_client.h"
#include "link.h"

#include <sys/types.h>
#include <stdio.h>

INT64_T chirp_local_open( const char *path, INT64_T flags, INT64_T mode );
INT64_T chirp_local_close( int fd );
INT64_T chirp_local_pread( int fd, void *buffer, INT64_T length, INT64_T offset );
INT64_T chirp_local_pwrite( int fd, const void *buffer, INT64_T length, INT64_T offset );
INT64_T chirp_local_sread( int fd, void *buffer, INT64_T length, INT64_T stride_length, INT64_T stride_skip, INT64_T offset );
INT64_T chirp_local_swrite( int fd, const void *buffer, INT64_T length, INT64_T stride_length, INT64_T stride_skip, INT64_T offset );
INT64_T chirp_local_fstat( int fd, struct chirp_stat *buf );
INT64_T chirp_local_fstatfs( int fd, struct chirp_statfs *buf );
INT64_T chirp_local_fchown( int fd, INT64_T uid, INT64_T gid );
INT64_T chirp_local_fchmod( int fd, INT64_T mode );
INT64_T chirp_local_ftruncate( int fd, INT64_T length );
INT64_T chirp_local_fsync( int fd );

void *  chirp_local_opendir( const char *path );
char *  chirp_local_readdir( void *dir );
void    chirp_local_closedir( void *dir );

INT64_T chirp_local_getfile( const char *path, struct link *link, time_t stoptime );
INT64_T chirp_local_putfile( const char *path, struct link *link, INT64_T length, INT64_T mode, time_t stoptime );

INT64_T chirp_local_mkfifo( const char *path );
INT64_T chirp_local_unlink( const char *path );
INT64_T chirp_local_rename( const char *path, const char *newpath );
INT64_T chirp_local_link( const char *path, const char *newpath );
INT64_T chirp_local_symlink( const char *path, const char *newpath );
INT64_T chirp_local_readlink( const char *path, char *buf, INT64_T length );
INT64_T chirp_local_mkdir( const char *path, INT64_T mode );
INT64_T chirp_local_rmdir( const char *path );
INT64_T chirp_local_stat( const char *path, struct chirp_stat *buf );
INT64_T chirp_local_lstat( const char *path, struct chirp_stat *buf );
INT64_T chirp_local_statfs( const char *path, struct chirp_statfs *buf );
INT64_T chirp_local_access( const char *path, INT64_T mode );
INT64_T chirp_local_chmod( const char *path, INT64_T mode );
INT64_T chirp_local_chown( const char *path, INT64_T uid, INT64_T gid );
INT64_T chirp_local_lchown( const char *path, INT64_T uid, INT64_T gid );
INT64_T chirp_local_truncate( const char *path, INT64_T length );
INT64_T chirp_local_utime( const char *path, time_t actime, time_t modtime );
INT64_T chirp_local_md5( const char *path, unsigned char digest[16] );

INT64_T chirp_local_file_size( const char *path );
INT64_T chirp_local_fd_size( int fd );

void chirp_local_bandwidth_limit_set( int bytes_per_second );

extern struct chirp_filesystem chirp_local_fs;

#endif
