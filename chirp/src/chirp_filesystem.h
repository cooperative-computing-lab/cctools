/*
Copyright (C) 2008- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef CHIRP_FILESYSTEM_H
#define CHIRP_FILESYSTEM_H

#include "chirp_job.h"
#include "chirp_sqlite.h"
#include "chirp_types.h"

#include "buffer.h"
#include "link.h"

#include <sys/types.h>

enum {
	CHIRP_FILESYSTEM_MAXFD = 1024,
};

typedef struct CHIRP_FILE CHIRP_FILE;

struct chirp_filesystem {
	int (*init) ( const char url[CHIRP_PATH_MAX] );

	int (*fname) ( int fd, char path[CHIRP_PATH_MAX] );

	INT64_T (*open)      ( const char *path, INT64_T flags, INT64_T mode );
	INT64_T (*close)     ( int fd );
	INT64_T (*pread)     ( int fd, void *data, INT64_T length, INT64_T offset );
	INT64_T (*pwrite)    ( int fd, const void *data, INT64_T length, INT64_T offset );
	INT64_T (*sread)     ( int fd, void *data, INT64_T, INT64_T, INT64_T, INT64_T );
	INT64_T (*swrite)    ( int fd, const void *data, INT64_T, INT64_T, INT64_T, INT64_T );
	INT64_T (*lockf)     ( int fd, int cmd, INT64_T len);
	INT64_T (*fstat)     ( int fd, struct chirp_stat *buf );
	INT64_T (*fstatfs)   ( int fd, struct chirp_statfs *buf );
	INT64_T (*fchown)    ( int fd, INT64_T uid, INT64_T gid );
	INT64_T (*fchmod)    ( int fd, INT64_T mode );
	INT64_T (*ftruncate) ( int fd, INT64_T length );
	INT64_T (*fsync)     ( int fd );

	INT64_T (*search) ( const char *subject, const char *dir, const char *patt, int flags, struct link *l, time_t stoptime );

	struct chirp_dir    * (*opendir)   ( const char *path );
	struct chirp_dirent * (*readdir)   ( struct chirp_dir *dir );
	void                  (*closedir)  ( struct chirp_dir *dir );

	INT64_T (*getfile)   ( const char *path, struct link *l, time_t stoptime );
	INT64_T (*putfile)   ( const char *path, struct link *l, INT64_T mode, INT64_T length, time_t stoptime );

	INT64_T (*unlink)    ( const char *path );
	INT64_T (*rmall)     ( const char *path );
	INT64_T (*rename)    ( const char *path, const char *newpath );
	INT64_T (*link)      ( const char *path, const char *newpath );
	INT64_T (*symlink)   ( const char *path, const char *newpath );
	INT64_T (*readlink)  ( const char *path, char *target, INT64_T length );
	INT64_T (*chdir)     ( const char *path );
	INT64_T (*mkdir)     ( const char *path, INT64_T mode );
	INT64_T (*rmdir)     ( const char *path );
	INT64_T (*stat)      ( const char *path, struct chirp_stat *buf );
	INT64_T (*lstat)     ( const char *path, struct chirp_stat *buf );
	INT64_T (*statfs)    ( const char *path, struct chirp_statfs *buf );
	INT64_T (*access)    ( const char *path, INT64_T mode );
	INT64_T (*chmod)     ( const char *path, INT64_T mode );
	INT64_T (*chown)     ( const char *path, INT64_T uid, INT64_T gid );
	INT64_T (*lchown)    ( const char *path, INT64_T uid, INT64_T gid );
	INT64_T (*truncate)  ( const char *path, INT64_T length );
	INT64_T (*utime)     ( const char *path, time_t atime, time_t mtime  );
	INT64_T (*md5)       ( const char *path, unsigned char digest[16] );
	INT64_T (*setrep)    ( const char *path, int nreps );

    INT64_T (*getxattr)  ( const char *path, const char *name, void *data, size_t size );
    INT64_T (*fgetxattr)  ( int fd, const char *name, void *data, size_t size );
    INT64_T (*lgetxattr)  ( const char *path, const char *name, void *data, size_t size );
    INT64_T (*listxattr)  ( const char *path, char *data, size_t size );
    INT64_T (*flistxattr)  ( int fd, char *data, size_t size );
    INT64_T (*llistxattr)  ( const char *path, char *data, size_t size );
    INT64_T (*setxattr)  ( const char *path, const char *name, const void *data, size_t size, int flags );
    INT64_T (*fsetxattr)  ( int fd, const char *name, const void *data, size_t size, int flags );
    INT64_T (*lsetxattr)  ( const char *path, const char *name, const void *data, size_t size, int flags );
    INT64_T (*removexattr)  ( const char *path, const char *name );
    INT64_T (*fremovexattr)  ( int fd, const char *name );
    INT64_T (*lremovexattr)  ( const char *path, const char *name );

	int (*do_acl_check) ();

	int (*job_dbinit) (sqlite3 *db);
	int (*job_schedule) (sqlite3 *db);
};

/* Lookup of a backend FS associated with a URL */
struct chirp_filesystem * cfs_lookup( const char *url );
/* Normalize a URL */
void cfs_normalize ( char url[CHIRP_PATH_MAX] );

/* CFS implementation for many stdio.h things */
int         cfs_create_dir(const char *path, int mode);
int         cfs_delete_dir(const char *path);
int         cfs_exists( const char *path );
int         cfs_fclose(CHIRP_FILE * file);
INT64_T     cfs_fd_size( int fd );
int         cfs_ferror(CHIRP_FILE * file);
int         cfs_fflush(CHIRP_FILE * file);
char *      cfs_fgets(char *s, int n, CHIRP_FILE * file);
INT64_T     cfs_file_size( const char *path );
CHIRP_FILE *cfs_fopen(const char *path, const char *mode);
CHIRP_FILE *cfs_fopen_local(const char *path, const char *mode);
void        cfs_fprintf(CHIRP_FILE * file, const char *format, ...);
size_t      cfs_fread(void *ptr, size_t size, size_t nitems, CHIRP_FILE * f);
int         cfs_freadall(CHIRP_FILE * file, buffer_t *B);
size_t      cfs_fwrite(const void *ptr, size_t size, size_t nitems, CHIRP_FILE * f);
int         cfs_isdir(const char *filename);
int         cfs_isnotdir(const char *filename);

/* "basic" implementation made of primitives for operations the backend FS does not implement */
INT64_T cfs_basic_sread(int fd, void *vbuffer, INT64_T length, INT64_T stride_length, INT64_T stride_skip, INT64_T offset);
INT64_T cfs_basic_swrite(int fd, const void *vbuffer, INT64_T length, INT64_T stride_length, INT64_T stride_skip, INT64_T offset);
INT64_T cfs_basic_putfile(const char *path, struct link * link, INT64_T length, INT64_T mode, time_t stoptime);
INT64_T cfs_basic_getfile(const char *path, struct link * link, time_t stoptime );
INT64_T cfs_basic_md5(const char *path, unsigned char digest[16]);

/* stubs for operations not implemented in the backend FS */
INT64_T cfs_stub_lockf (int fd, int cmd, INT64_T len);
INT64_T cfs_stub_getxattr (const char *path, const char *name, void *data, size_t size);
INT64_T cfs_stub_fgetxattr (int fd, const char *name, void *data, size_t size);
INT64_T cfs_stub_lgetxattr (const char *path, const char *name, void *data, size_t size);
INT64_T cfs_stub_listxattr (const char *path, char *list, size_t size);
INT64_T cfs_stub_flistxattr (int fd, char *list, size_t size);
INT64_T cfs_stub_llistxattr (const char *path, char *list, size_t size);
INT64_T cfs_stub_setxattr (const char *path, const char *name, const void *data, size_t size, int flags);
INT64_T cfs_stub_fsetxattr (int fd, const char *name, const void *data, size_t size, int flags);
INT64_T cfs_stub_lsetxattr (const char *path, const char *name, const void *data, size_t size, int flags);
INT64_T cfs_stub_removexattr (const char *path, const char *name);
INT64_T cfs_stub_fremovexattr (int fd, const char *name);
INT64_T cfs_stub_lremovexattr (const char *path, const char *name);

int     cfs_stub_job_dbinit (sqlite3 *db);
int     cfs_stub_job_schedule (sqlite3 *db);


extern struct chirp_filesystem *cfs;
extern char   chirp_url[CHIRP_PATH_MAX];

#endif /* CHIRP_FILESYSTEM_H */

/* vim: set noexpandtab tabstop=4: */
