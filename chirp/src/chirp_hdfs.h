/*
Copyright (C) 2008- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#if !defined(CHIRP_HDFS_H) && HAS_HDFS
#define CHIRP_HDFS_H

#include "chirp_client.h"
#include "link.h"

#include <sys/types.h>
#include <stdio.h>

INT64_T chirp_hdfs_open( const char *path, INT64_T flags, INT64_T mode );
INT64_T chirp_hdfs_close( int fd );
INT64_T chirp_hdfs_pread( int fd, void *buffer, INT64_T length, INT64_T offset );
INT64_T chirp_hdfs_pwrite( int fd, const void *buffer, INT64_T length, INT64_T offset );
INT64_T chirp_hdfs_sread( int fd, void *buffer, INT64_T length, INT64_T stride_length, INT64_T stride_skip, INT64_T offset );
INT64_T chirp_hdfs_swrite( int fd, const void *buffer, INT64_T length, INT64_T stride_length, INT64_T stride_skip, INT64_T offset );
INT64_T chirp_hdfs_fstat( int fd, struct chirp_stat *buf );
INT64_T chirp_hdfs_fstatfs( int fd, struct chirp_statfs *buf );
INT64_T chirp_hdfs_fchown( int fd, INT64_T uid, INT64_T gid );
INT64_T chirp_hdfs_fchmod( int fd, INT64_T mode );
INT64_T chirp_hdfs_ftruncate( int fd, INT64_T length );
INT64_T chirp_hdfs_fsync( int fd );

void *  chirp_hdfs_opendir( const char *path );
char *  chirp_hdfs_readdir( void *dir );
void    chirp_hdfs_closedir( void *dir );

INT64_T chirp_hdfs_getfile( const char *path, struct link *link, time_t stoptime );
INT64_T chirp_hdfs_putfile( const char *path, struct link *link, INT64_T length, INT64_T mode, time_t stoptime );

INT64_T chirp_hdfs_mkfifo( const char *path );
INT64_T chirp_hdfs_unlink( const char *path );
INT64_T chirp_hdfs_rename( const char *path, const char *newpath );
INT64_T chirp_hdfs_link( const char *path, const char *newpath );
INT64_T chirp_hdfs_symlink( const char *path, const char *newpath );
INT64_T chirp_hdfs_readlink( const char *path, char *buf, INT64_T length );
INT64_T chirp_hdfs_mkdir( const char *path, INT64_T mode );
INT64_T chirp_hdfs_rmdir( const char *path );
INT64_T chirp_hdfs_stat( const char *path, struct chirp_stat *buf );
INT64_T chirp_hdfs_lstat( const char *path, struct chirp_stat *buf );
INT64_T chirp_hdfs_statfs( const char *path, struct chirp_statfs *buf );
INT64_T chirp_hdfs_access( const char *path, INT64_T mode );
INT64_T chirp_hdfs_chmod( const char *path, INT64_T mode );
INT64_T chirp_hdfs_chown( const char *path, INT64_T uid, INT64_T gid );
INT64_T chirp_hdfs_lchown( const char *path, INT64_T uid, INT64_T gid );
INT64_T chirp_hdfs_truncate( const char *path, INT64_T length );
INT64_T chirp_hdfs_utime( const char *path, time_t actime, time_t modtime );
INT64_T chirp_hdfs_md5( const char *path, unsigned char digest[16] );

INT64_T chirp_hdfs_lsalloc( const char *path, char *alloc_path, INT64_T *total, INT64_T *inuse );
INT64_T chirp_hdfs_mkalloc( const char *path, INT64_T size, INT64_T mode );

INT64_T chirp_hdfs_file_size( const char *path );
INT64_T chirp_hdfs_fd_size( int fd );

INT64_T chirp_hdfs_chdir (const char *path);

void chirp_hdfs_bandwidth_limit_set( int bytes_per_second );

extern struct chirp_filesystem chirp_hdfs_fs;

extern char *chirp_hdfs_hostname;
extern UINT16_T chirp_hdfs_port;

#endif
