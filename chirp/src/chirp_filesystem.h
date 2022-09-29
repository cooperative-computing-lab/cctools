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
#include "uuid.h"

#include <sys/types.h>

enum {
	CHIRP_FILESYSTEM_MAXFD = 1024,
};

typedef struct CHIRP_FILE CHIRP_FILE;

struct chirp_filesystem {
	int (*init) ( const char url[CHIRP_PATH_MAX], cctools_uuid_t *uuid );
	void (*destroy) ( void );

	int (*fname) ( int fd, char path[CHIRP_PATH_MAX] );

	int64_t (*open)      ( const char *path, int64_t flags, int64_t mode );
	int64_t (*close)     ( int fd );
	int64_t (*pread)     ( int fd, void *data, int64_t length, int64_t offset );
	int64_t (*pwrite)    ( int fd, const void *data, int64_t length, int64_t offset );
	int64_t (*sread)     ( int fd, void *data, int64_t, int64_t, int64_t, int64_t );
	int64_t (*swrite)    ( int fd, const void *data, int64_t, int64_t, int64_t, int64_t );
	int64_t (*lockf)     ( int fd, int cmd, int64_t len);
	int64_t (*fstat)     ( int fd, struct chirp_stat *buf );
	int64_t (*fstatfs)   ( int fd, struct chirp_statfs *buf );
	int64_t (*fchown)    ( int fd, int64_t uid, int64_t gid );
	int64_t (*fchmod)    ( int fd, int64_t mode );
	int64_t (*ftruncate) ( int fd, int64_t length );
	int64_t (*fsync)     ( int fd );

	int64_t (*search) ( const char *subject, const char *dir, const char *patt, int flags, struct link *l, time_t stoptime );

	struct chirp_dir    * (*opendir)   ( const char *path );
	struct chirp_dirent * (*readdir)   ( struct chirp_dir *dir );
	void                  (*closedir)  ( struct chirp_dir *dir );

	int64_t (*unlink)    ( const char *path );
	int64_t (*rmall)     ( const char *path );
	int64_t (*rename)    ( const char *path, const char *newpath );
	int64_t (*link)      ( const char *path, const char *newpath );
	int64_t (*symlink)   ( const char *path, const char *newpath );
	int64_t (*readlink)  ( const char *path, char *target, int64_t length );
	int64_t (*mkdir)     ( const char *path, int64_t mode );
	int64_t (*rmdir)     ( const char *path );
	int64_t (*stat)      ( const char *path, struct chirp_stat *buf );
	int64_t (*lstat)     ( const char *path, struct chirp_stat *buf );
	int64_t (*statfs)    ( const char *path, struct chirp_statfs *buf );
	int64_t (*access)    ( const char *path, int64_t mode );
	int64_t (*chmod)     ( const char *path, int64_t mode );
	int64_t (*chown)     ( const char *path, int64_t uid, int64_t gid );
	int64_t (*lchown)    ( const char *path, int64_t uid, int64_t gid );
	int64_t (*truncate)  ( const char *path, int64_t length );
	int64_t (*utime)     ( const char *path, time_t atime, time_t mtime  );
	int64_t (*hash)      ( const char *path, const char *algorithm, unsigned char digest[CHIRP_DIGEST_MAX] );
	int64_t (*setrep)    ( const char *path, int nreps );

	int64_t (*getxattr)  ( const char *path, const char *name, void *data, size_t size );
	int64_t (*fgetxattr)  ( int fd, const char *name, void *data, size_t size );
	int64_t (*lgetxattr)  ( const char *path, const char *name, void *data, size_t size );
	int64_t (*listxattr)  ( const char *path, char *data, size_t size );
	int64_t (*flistxattr)  ( int fd, char *data, size_t size );
	int64_t (*llistxattr)  ( const char *path, char *data, size_t size );
	int64_t (*setxattr)  ( const char *path, const char *name, const void *data, size_t size, int flags );
	int64_t (*fsetxattr)  ( int fd, const char *name, const void *data, size_t size, int flags );
	int64_t (*lsetxattr)  ( const char *path, const char *name, const void *data, size_t size, int flags );
	int64_t (*removexattr)  ( const char *path, const char *name );
	int64_t (*fremovexattr)  ( int fd, const char *name );
	int64_t (*lremovexattr)  ( const char *path, const char *name );

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
int         cfs_exists( const char *path );
int         cfs_fclose(CHIRP_FILE * file);
int64_t     cfs_fd_size( int fd );
int         cfs_ferror(CHIRP_FILE * file);
int         cfs_fflush(CHIRP_FILE * file);
char *      cfs_fgets(char *s, int n, CHIRP_FILE * file);
int64_t     cfs_file_size( const char *path );
CHIRP_FILE *cfs_fopen(const char *path, const char *mode);
CHIRP_FILE *cfs_fopen_local(const char *path, const char *mode);
void        cfs_fprintf(CHIRP_FILE * file, const char *format, ...);
size_t      cfs_fread(void *ptr, size_t size, size_t nitems, CHIRP_FILE * f);
int         cfs_freadall(CHIRP_FILE * file, buffer_t *B);
size_t      cfs_fwrite(const void *ptr, size_t size, size_t nitems, CHIRP_FILE * f);
int         cfs_isdir(const char *filename);
int         cfs_isnotdir(const char *filename);

/* "basic" implementation made of primitives for operations the backend FS does not implement */
int64_t cfs_basic_chown(const char *path, int64_t uid, int64_t gid);
int64_t cfs_basic_fchown(int fd, int64_t uid, int64_t gid);
int64_t cfs_basic_hash (const char *path, const char *algorithm, unsigned char digest[CHIRP_DIGEST_MAX]);
int64_t cfs_basic_lchown(const char *path, int64_t uid, int64_t gid);
int64_t cfs_basic_rmall(const char *path);
int64_t cfs_basic_search(const char *subject, const char *dir, const char *patt, int flags, struct link *l, time_t stoptime);
int64_t cfs_basic_sread(int fd, void *vbuffer, int64_t length, int64_t stride_length, int64_t stride_skip, int64_t offset);
int64_t cfs_basic_swrite(int fd, const void *vbuffer, int64_t length, int64_t stride_length, int64_t stride_skip, int64_t offset);

/* stubs for operations not implemented in the backend FS */
void cfs_stub_destroy(void);
int64_t cfs_stub_lockf (int fd, int cmd, int64_t len);
int64_t cfs_stub_getxattr (const char *path, const char *name, void *data, size_t size);
int64_t cfs_stub_fgetxattr (int fd, const char *name, void *data, size_t size);
int64_t cfs_stub_lgetxattr (const char *path, const char *name, void *data, size_t size);
int64_t cfs_stub_listxattr (const char *path, char *list, size_t size);
int64_t cfs_stub_flistxattr (int fd, char *list, size_t size);
int64_t cfs_stub_llistxattr (const char *path, char *list, size_t size);
int64_t cfs_stub_setxattr (const char *path, const char *name, const void *data, size_t size, int flags);
int64_t cfs_stub_fsetxattr (int fd, const char *name, const void *data, size_t size, int flags);
int64_t cfs_stub_lsetxattr (const char *path, const char *name, const void *data, size_t size, int flags);
int64_t cfs_stub_removexattr (const char *path, const char *name);
int64_t cfs_stub_fremovexattr (int fd, const char *name);
int64_t cfs_stub_lremovexattr (const char *path, const char *name);

int     cfs_stub_job_dbinit (sqlite3 *db);
int     cfs_stub_job_schedule (sqlite3 *db);


extern struct chirp_filesystem *cfs;
extern char   chirp_url[CHIRP_PATH_MAX];

#define STAT_TO_CSTAT(cbuf, buf)\
	do {\
		memset(&(cbuf),0,sizeof(cbuf));\
		(cbuf).cst_dev = (buf).st_dev;\
		(cbuf).cst_ino = (buf).st_ino;\
		(cbuf).cst_mode = (buf).st_mode;\
		(cbuf).cst_nlink = (buf).st_nlink;\
		(cbuf).cst_uid = (buf).st_uid;\
		(cbuf).cst_gid = (buf).st_gid;\
		(cbuf).cst_rdev = (buf).st_rdev;\
		(cbuf).cst_size = (buf).st_size;\
		(cbuf).cst_blksize = (buf).st_blksize;\
		(cbuf).cst_blocks = (buf).st_blocks;\
		(cbuf).cst_atime = (buf).st_atime;\
		(cbuf).cst_mtime = (buf).st_mtime;\
		(cbuf).cst_ctime = (buf).st_ctime;\
	} while (0)

#endif /* CHIRP_FILESYSTEM_H */

/* vim: set noexpandtab tabstop=4: */
