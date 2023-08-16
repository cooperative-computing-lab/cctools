/*
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "chirp_filesystem.h"
#include "chirp_fs_chirp.h"
#include "chirp_protocol.h"
#include "chirp_reli.h"

#include "debug.h"
#include "path.h"
#include "uuid.h"

#include <assert.h>
#include <stdlib.h>
#include <stdio.h>
#include <errno.h>
#include <string.h>

static char chirp_hostport[CHIRP_PATH_MAX];
static char chirp_root[CHIRP_PATH_MAX];
static int  chirp_timeout = 60;

static struct {
	char path[CHIRP_PATH_MAX];
	struct chirp_file *file;
} open_files[CHIRP_FILESYSTEM_MAXFD];

#define fdvalid(fd) (0 <= fd && fd < CHIRP_FILESYSTEM_MAXFD && open_files[fd].file)
#define SETUP_FILE \
if(!fdvalid(fd)) return (errno = EBADF, -1);\
struct chirp_file *file = open_files[fd].file;\
(void)file; /* silence unused warnings */

#define RESOLVE(path) \
char resolved_##path[CHIRP_PATH_MAX];\
if (chirp_fs_chirp_resolve(path, resolved_##path) == -1) return -1;\
path = resolved_##path;

#define RESOLVENULL(path) \
char resolved_##path[CHIRP_PATH_MAX];\
if (chirp_fs_chirp_resolve(path, resolved_##path) == -1) return NULL;\
path = resolved_##path;

#define STOPTIME (time(0)+chirp_timeout)

#define strprfx(s,p) (strncmp(s,p "",sizeof(p)-1) == 0)
static int chirp_fs_chirp_init(const char url[CHIRP_PATH_MAX], cctools_uuid_t *uuid)
{
	int i;
	char *path;

	debug(D_CHIRP, "url: %s", url);

	assert(strprfx(url, "chirp://"));
	strcpy(chirp_hostport, url+strlen("chirp://"));
	path = strchr(chirp_hostport, '/');
	if (path) {
		path_collapse(path, chirp_root, 1);
		*path = '\0'; /* remove path from chirp_hostport */
	} else {
		strcpy(chirp_root, "/");
	}

	for (i = 0; i < CHIRP_FILESYSTEM_MAXFD; i++)
		open_files[i].file = NULL;

	cctools_uuid_create(uuid);

	return cfs_create_dir("/", 0711);
}

static int chirp_fs_chirp_fname (int fd, char path[CHIRP_PATH_MAX])
{
	SETUP_FILE
	strcpy(path, open_files[fd].path);
	return 0;
}

static int chirp_fs_chirp_resolve (const char *path, char resolved[CHIRP_PATH_MAX])
{
	int n;
	char collapse[CHIRP_PATH_MAX];
	char absolute[CHIRP_PATH_MAX];
	path_collapse(path, collapse, 1);
	n = snprintf(absolute, sizeof(absolute), "%s/%s", chirp_root, collapse);
	assert(n >= 0); /* this should never happen */
	if ((size_t)n >= CHIRP_PATH_MAX) {
		errno = ENAMETOOLONG;
		return -1;
	}
	path_collapse(absolute, resolved, 1);
	return 0;
}

static INT64_T getfd(void)
{
	INT64_T fd;
	/* find an unused file descriptor */
	for(fd = 0; fd < CHIRP_FILESYSTEM_MAXFD; fd++)
		if(!open_files[fd].file)
			return fd;
	debug(D_CHIRP, "too many files open");
	errno = EMFILE;
	return -1;
}

static INT64_T chirp_fs_chirp_open(const char *path, INT64_T flags, INT64_T mode)
{
	const char *unresolved = path;
	RESOLVE(path)
	int fd = getfd();
	if (fd == -1) return -1;

	struct chirp_file *file = chirp_reli_open(chirp_hostport, path, flags, mode, STOPTIME);
	if (file) {
		strcpy(open_files[fd].path, unresolved);
		open_files[fd].file = file;
		return fd;
	} else {
		return -1;
	}
}

static INT64_T chirp_fs_chirp_close(int fd)
{
	SETUP_FILE
	open_files[fd].path[0] = '\0';
	open_files[fd].file = NULL;
	return chirp_reli_close(file, STOPTIME);
}

static INT64_T chirp_fs_chirp_pread(int fd, void *buffer, INT64_T length, INT64_T offset)
{
	SETUP_FILE
	return chirp_reli_pread(file, buffer, length, offset, STOPTIME);
}

static INT64_T chirp_fs_chirp_sread(int fd, void *vbuffer, INT64_T length, INT64_T stride_length, INT64_T stride_skip, INT64_T offset)
{
	SETUP_FILE
	return chirp_reli_sread(file, vbuffer, length, stride_length, stride_skip, offset, STOPTIME);
}

static INT64_T chirp_fs_chirp_pwrite(int fd, const void *buffer, INT64_T length, INT64_T offset)
{
	SETUP_FILE
	return chirp_reli_pwrite(file, buffer, length, offset, STOPTIME);
}

static INT64_T chirp_fs_chirp_swrite(int fd, const void *vbuffer, INT64_T length, INT64_T stride_length, INT64_T stride_skip, INT64_T offset)
{
	SETUP_FILE
	return chirp_reli_swrite(file, vbuffer, length, stride_length, stride_skip, offset, STOPTIME);
}

static INT64_T chirp_fs_chirp_fstat(int fd, struct chirp_stat *buf)
{
	SETUP_FILE
	return chirp_reli_fstat(file, buf, STOPTIME);
}

static INT64_T chirp_fs_chirp_fstatfs(int fd, struct chirp_statfs *buf)
{
	SETUP_FILE
	return chirp_reli_fstatfs(file, buf, STOPTIME);
}

static INT64_T chirp_fs_chirp_fchown(int fd, INT64_T uid, INT64_T gid)
{
	SETUP_FILE
	return chirp_reli_fchown(file, uid, gid, STOPTIME);
}

static INT64_T chirp_fs_chirp_fchmod(int fd, INT64_T mode)
{
	SETUP_FILE
	return chirp_reli_fchmod(file, mode, STOPTIME);
}

static INT64_T chirp_fs_chirp_ftruncate(int fd, INT64_T length)
{
	SETUP_FILE
	return chirp_reli_ftruncate(file, length, STOPTIME);
}

static INT64_T chirp_fs_chirp_fsync(int fd)
{
	SETUP_FILE
	return chirp_reli_fsync(file, STOPTIME);
}

static struct chirp_dir *chirp_fs_chirp_opendir(const char *path)
{
	RESOLVENULL(path)
	return chirp_reli_opendir(chirp_hostport, path, STOPTIME);
}

static struct chirp_dirent *chirp_fs_chirp_readdir(struct chirp_dir *dir)
{
	return chirp_reli_readdir(dir);
}

static void chirp_fs_chirp_closedir(struct chirp_dir *dir)
{
	chirp_reli_closedir(dir);
}

static INT64_T chirp_fs_chirp_unlink(const char *path)
{
	RESOLVE(path)
	return chirp_reli_unlink(chirp_hostport, path, STOPTIME);
}

static INT64_T chirp_fs_chirp_rmall(const char *path)
{
	RESOLVE(path)
	return chirp_reli_rmall(chirp_hostport, path, STOPTIME);
}

static INT64_T chirp_fs_chirp_rename(const char *path, const char *newpath)
{
	RESOLVE(path);
	RESOLVE(newpath);
	return chirp_reli_rename(chirp_hostport, path, newpath, STOPTIME);
}

static INT64_T chirp_fs_chirp_link(const char *path, const char *newpath)
{
	RESOLVE(path)
	RESOLVE(newpath)
	return chirp_reli_link(chirp_hostport, path, newpath, STOPTIME);
}

static INT64_T chirp_fs_chirp_symlink(const char *path, const char *newpath)
{
	RESOLVE(path)
	RESOLVE(newpath)
	return chirp_reli_symlink(chirp_hostport, path, newpath, STOPTIME);
}

static INT64_T chirp_fs_chirp_readlink(const char *path, char *buf, INT64_T length)
{
	RESOLVE(path)
	return chirp_reli_readlink(chirp_hostport, path, buf, length, STOPTIME);
}

static INT64_T chirp_fs_chirp_mkdir(const char *path, INT64_T mode)
{
	RESOLVE(path)
	return chirp_reli_mkdir(chirp_hostport, path, mode, STOPTIME);
}

static INT64_T chirp_fs_chirp_rmdir(const char *path)
{
	RESOLVE(path)
	return chirp_reli_rmdir(chirp_hostport, path, STOPTIME);
}

static INT64_T chirp_fs_chirp_stat(const char *path, struct chirp_stat *buf)
{
	RESOLVE(path)
	return chirp_reli_stat(chirp_hostport, path, buf, STOPTIME);
}

static INT64_T chirp_fs_chirp_lstat(const char *path, struct chirp_stat *buf)
{
	RESOLVE(path)
	return chirp_reli_lstat(chirp_hostport, path, buf, STOPTIME);
}

static INT64_T chirp_fs_chirp_statfs(const char *path, struct chirp_statfs *buf)
{
	RESOLVE(path)
	return chirp_reli_statfs(chirp_hostport, path, buf, STOPTIME);
}

static INT64_T chirp_fs_chirp_access(const char *path, INT64_T mode)
{
	RESOLVE(path)
	return chirp_reli_access(chirp_hostport, path, mode, STOPTIME);
}

static INT64_T chirp_fs_chirp_chmod(const char *path, INT64_T mode)
{
	RESOLVE(path)
	return chirp_reli_chmod(chirp_hostport, path, mode, STOPTIME);
}

static INT64_T chirp_fs_chirp_chown(const char *path, INT64_T uid, INT64_T gid)
{
	RESOLVE(path)
	return chirp_reli_chown(chirp_hostport, path, uid, gid, STOPTIME);
}

static INT64_T chirp_fs_chirp_lchown(const char *path, INT64_T uid, INT64_T gid)
{
	RESOLVE(path)
	return chirp_reli_lchown(chirp_hostport, path, uid, gid, STOPTIME);
}

static INT64_T chirp_fs_chirp_truncate(const char *path, INT64_T length)
{
	RESOLVE(path)
	return chirp_reli_truncate(chirp_hostport, path, length, STOPTIME);
}

static INT64_T chirp_fs_chirp_utime(const char *path, time_t actime, time_t modtime)
{
	RESOLVE(path)
	return chirp_reli_utime(chirp_hostport, path, actime, modtime, STOPTIME);
}

static INT64_T chirp_fs_chirp_hash(const char *path, const char *algorithm, unsigned char digest[CHIRP_DIGEST_MAX])
{
	RESOLVE(path)
	return chirp_reli_hash(chirp_hostport, path, algorithm, digest, STOPTIME);
}

static INT64_T chirp_fs_chirp_setrep(const char *path, int nreps)
{
	RESOLVE(path)
	return chirp_reli_setrep(chirp_hostport, path, nreps, STOPTIME);
}

static INT64_T chirp_fs_chirp_getxattr(const char *path, const char *name, void *data, size_t size)
{
	RESOLVE(path)
	return chirp_reli_getxattr(chirp_hostport, path, name, data, size, STOPTIME);
}

static INT64_T chirp_fs_chirp_fgetxattr(int fd, const char *name, void *data, size_t size)
{
	SETUP_FILE
	return chirp_reli_fgetxattr(file, name, data, size, STOPTIME);
}

static INT64_T chirp_fs_chirp_lgetxattr(const char *path, const char *name, void *data, size_t size)
{
	RESOLVE(path)
	return chirp_reli_lgetxattr(chirp_hostport, path, name, data, size, STOPTIME);
}

static INT64_T chirp_fs_chirp_listxattr(const char *path, char *list, size_t size)
{
	RESOLVE(path)
	return chirp_reli_listxattr(chirp_hostport, path, list, size, STOPTIME);
}

static INT64_T chirp_fs_chirp_flistxattr(int fd, char *list, size_t size)
{
	SETUP_FILE
	return chirp_reli_flistxattr(file, list, size, STOPTIME);
}

static INT64_T chirp_fs_chirp_llistxattr(const char *path, char *list, size_t size)
{
	RESOLVE(path)
	return chirp_reli_llistxattr(chirp_hostport, path, list, size, STOPTIME);
}

static INT64_T chirp_fs_chirp_setxattr(const char *path, const char *name, const void *data, size_t size, int flags)
{
	RESOLVE(path)
	return chirp_reli_setxattr(chirp_hostport, path, name, data, size, flags, STOPTIME);
}

static INT64_T chirp_fs_chirp_fsetxattr(int fd, const char *name, const void *data, size_t size, int flags)
{
	SETUP_FILE
	return chirp_reli_fsetxattr(file, name, data, size, flags, STOPTIME);
}

static INT64_T chirp_fs_chirp_lsetxattr(const char *path, const char *name, const void *data, size_t size, int flags)
{
	RESOLVE(path)
	return chirp_reli_lsetxattr(chirp_hostport, path, name, data, size, flags, STOPTIME);
}

static INT64_T chirp_fs_chirp_removexattr(const char *path, const char *name)
{
	RESOLVE(path)
	return chirp_reli_removexattr(chirp_hostport, path, name, STOPTIME);
}

static INT64_T chirp_fs_chirp_fremovexattr(int fd, const char *name)
{
	SETUP_FILE
	return chirp_reli_fremovexattr(file, name, STOPTIME);
}

static INT64_T chirp_fs_chirp_lremovexattr(const char *path, const char *name)
{
	RESOLVE(path)
	return chirp_reli_lremovexattr(chirp_hostport, path, name, STOPTIME);
}

static int chirp_fs_chirp_do_acl_check()
{
	return 0;
}

struct chirp_filesystem chirp_fs_chirp = {
	chirp_fs_chirp_init,
	cfs_stub_destroy,

	chirp_fs_chirp_fname,

	chirp_fs_chirp_open,
	chirp_fs_chirp_close,
	chirp_fs_chirp_pread,
	chirp_fs_chirp_pwrite,
	chirp_fs_chirp_sread,
	chirp_fs_chirp_swrite,
	cfs_stub_lockf,
	chirp_fs_chirp_fstat,
	chirp_fs_chirp_fstatfs,
	chirp_fs_chirp_fchown,
	chirp_fs_chirp_fchmod,
	chirp_fs_chirp_ftruncate,
	chirp_fs_chirp_fsync,

	/* TODO ideally we'd pass this on to the proxy, but we'd have to deal with buffers/links. */
	cfs_basic_search,

	chirp_fs_chirp_opendir,
	chirp_fs_chirp_readdir,
	chirp_fs_chirp_closedir,

	chirp_fs_chirp_unlink,
	chirp_fs_chirp_rmall,
	chirp_fs_chirp_rename,
	chirp_fs_chirp_link,
	chirp_fs_chirp_symlink,
	chirp_fs_chirp_readlink,
	chirp_fs_chirp_mkdir,
	chirp_fs_chirp_rmdir,
	chirp_fs_chirp_stat,
	chirp_fs_chirp_lstat,
	chirp_fs_chirp_statfs,
	chirp_fs_chirp_access,
	chirp_fs_chirp_chmod,
	chirp_fs_chirp_chown,
	chirp_fs_chirp_lchown,
	chirp_fs_chirp_truncate,
	chirp_fs_chirp_utime,
	chirp_fs_chirp_hash,
	chirp_fs_chirp_setrep,

	chirp_fs_chirp_getxattr,
	chirp_fs_chirp_fgetxattr,
	chirp_fs_chirp_lgetxattr,
	chirp_fs_chirp_listxattr,
	chirp_fs_chirp_flistxattr,
	chirp_fs_chirp_llistxattr,
	chirp_fs_chirp_setxattr,
	chirp_fs_chirp_fsetxattr,
	chirp_fs_chirp_lsetxattr,
	chirp_fs_chirp_removexattr,
	chirp_fs_chirp_fremovexattr,
	chirp_fs_chirp_lremovexattr,

	chirp_fs_chirp_do_acl_check,

	cfs_stub_job_dbinit,
	cfs_stub_job_schedule,
};

/* vim: set noexpandtab tabstop=8: */
