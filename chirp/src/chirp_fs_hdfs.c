/*
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "chirp_filesystem.h"
#include "chirp_fs_hdfs.h"
#include "chirp_protocol.h"

#include "debug.h"
#include "hash_table.h"
#include "macros.h"
#include "path.h"
#include "stringtools.h"
#include "username.h"
#include "uuid.h"
#include "xxmalloc.h"

#include "hdfs_library.h"

#include <dlfcn.h>
#include <fcntl.h>
#include <grp.h>
#include <pwd.h>
#include <unistd.h>

#include <sys/stat.h>

#include <assert.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

extern char chirp_owner[USERNAME_MAX];

/*
This file must take into account several oddities of HDFS:
- Files are sequential access only, for both read and write.
- Once written and closed, files may not be re-opened.
- A file is not visible in the namespace until closed.
- An attempt to re-open or rename over a file fails.
- HDFS does not track an execute bit for files.

We cannot solve all of these limitations, but we do the
following things to accomodate most existing programs:
- The current size of an open file is kept internally,
and used to ensure that all operations are performed sequentially.
- A unix operation that would truncate or write-over a file
is performed by unlinking the file first, then proceeding.
- All files are marked as executable.

Where something is not possible, we use the following errnos:
- EACCES is used when HDFS doesn't permit the sequence of operations,
such as random seeking during a write.
- ENOTSUP is used when HDFS doesn't implement the call in any form,
such as creating symbolic links.
file has already been created.
*/

static char hdfs_host[CHIRP_PATH_MAX];
static int  hdfs_nreps = 0;
static int  hdfs_port = 0;
static char hdfs_root[CHIRP_PATH_MAX];

static struct hdfs_library *hdfs_services = 0;
static hdfsFS fs = NULL;

/* Array of open HDFS Files */
static struct {
	char path[CHIRP_PATH_MAX];
	hdfsFile file;
} open_files[CHIRP_FILESYSTEM_MAXFD];

#define fdvalid(fd) (0 <= fd && fd < CHIRP_FILESYSTEM_MAXFD && open_files[fd].file)
#define SETUP_FILE \
if(!fdvalid(fd)) return (errno = EBADF, -1);\
hdfsFile *file = open_files[fd].file;\
(void)file; /* silence unused warnings */

#define RESOLVE(path) \
char resolved_##path[CHIRP_PATH_MAX];\
if (chirp_fs_hdfs_resolve(path, resolved_##path) == -1) return -1;\
path = resolved_##path;

#define RESOLVENULL(path) \
char resolved_##path[CHIRP_PATH_MAX];\
if (chirp_fs_hdfs_resolve(path, resolved_##path) == -1) return NULL;\
path = resolved_##path;

#define strprfx(s,p) (strncmp(s,p "",sizeof(p)-1) == 0)
static int chirp_fs_hdfs_init(const char url[CHIRP_PATH_MAX], cctools_uuid_t *uuid)
{
	static const char *groups[] = { "supergroup" };
	int i;
	char *path;

	if(!hdfs_services) {
		if (hdfs_library_envinit() == -1)
			return -1;
		hdfs_services = hdfs_library_open();
		if(!hdfs_services)
			return -1;
	}

	debug(D_CHIRP, "url: %s", url);

	assert(strprfx(url, "hdfs://"));
	strcpy(hdfs_host, url+strlen("hdfs://"));
	path = strchr(hdfs_host, '/');
	if (path) {
		path_collapse(path, hdfs_root, 1);
		*path = '\0'; /* remove path from hdfs_host */
		/* now hdfs_host holds 'host[:port]' */
	} else {
		strcpy(hdfs_root, "/");
	}

	if (strlen(hdfs_host) == 0) {
		/* use default */
		strcpy(hdfs_host, "default");
		hdfs_port = 0;
	} else if (strchr(hdfs_host, ':')) {
		hdfs_port = atoi(strchr(hdfs_host, ':')+1);
		*strchr(hdfs_host, ':') = '\0';
	} else {
		hdfs_port = 50070; /* default namenode port */
	}

	debug(D_HDFS, "connecting to hdfs://%s:%u%s as '%s'\n", hdfs_host, hdfs_port, hdfs_root, chirp_owner);
	assert(fs == NULL);
	fs = hdfs_services->connect_as_user(hdfs_host, hdfs_port, chirp_owner, groups, 1);
	if (fs == NULL) {
		errno = EIO;
		return -1;
	}

	for (i = 0; i < CHIRP_FILESYSTEM_MAXFD; i++)
		open_files[i].file = NULL;

	cctools_uuid_create(uuid);

	return cfs_create_dir("/", 0711);
}

static void chirp_fs_hdfs_destroy (void)
{
	hdfs_services->disconnect(fs);
	hdfs_library_close(hdfs_services);
	hdfs_services = NULL;
}

static int chirp_fs_hdfs_fname (int fd, char path[CHIRP_PATH_MAX])
{
	SETUP_FILE
	strcpy(path, open_files[fd].path);
	return 0;
}

static int chirp_fs_hdfs_resolve (const char *path, char resolved[CHIRP_PATH_MAX])
{
	int n;
	char collapse[CHIRP_PATH_MAX];
	char absolute[CHIRP_PATH_MAX];
	path_collapse(path, collapse, 1);
	n = snprintf(absolute, sizeof(absolute), "%s/%s", hdfs_root, collapse);
	assert(n >= 0); /* this should never happen */
	if ((size_t)n >= CHIRP_PATH_MAX) {
		errno = ENAMETOOLONG;
		return -1;
	}
	path_collapse(absolute, resolved, 1);
	return 0;
}

static void copystat(struct chirp_stat *cs, hdfsFileInfo * hs, const char *path)
{
	memset(cs, 0, sizeof(*cs));
	cs->cst_dev = -1;
	cs->cst_rdev = -2;
	cs->cst_ino = hash_string(path);
	cs->cst_mode = hs->mKind == kObjectKindDirectory ? S_IFDIR : S_IFREG;

	/* HDFS does not have execute bit, lie and set it for all files */
	cs->cst_mode |= hs->mPermissions | S_IXUSR | S_IXGRP;
	cs->cst_nlink = hs->mReplication;
	cs->cst_uid = 0;
	cs->cst_gid = 0;
	cs->cst_size = hs->mSize;
	cs->cst_blksize = hs->mBlockSize;

	/* If the blocksize is not set, assume 64MB chunksize */
	if(cs->cst_blksize < 1)
		cs->cst_blksize = 64 * 1024 * 1024;
	cs->cst_blocks = MAX(1, cs->cst_size / cs->cst_blksize);

	/* Note that hs->mLastAccess is typically zero. */
	cs->cst_atime = cs->cst_mtime = cs->cst_ctime = hs->mLastMod;
}

static INT64_T do_stat(const char *path, struct chirp_stat *buf)
{
	debug(D_HDFS, "stat %s", path);
	hdfsFileInfo *file_info = hdfs_services->stat(fs, path);
	if(file_info == NULL)
		return (errno = ENOENT, -1);
	copystat(buf, file_info, path);
	hdfs_services->free_stat(file_info, 1);
	return 0;
}

static INT64_T chirp_fs_hdfs_stat(const char *path, struct chirp_stat *buf)
{
	RESOLVE(path)
	return do_stat(path, buf);
}

static INT64_T chirp_fs_hdfs_fstat(int fd, struct chirp_stat *buf)
{
	SETUP_FILE
	return do_stat(open_files[fd].path, buf);
}

struct chirp_dir {
	int i;
	int n;
	hdfsFileInfo *info;
	char path[CHIRP_PATH_MAX];
	struct chirp_dirent entry;
};

static struct chirp_dir *chirp_fs_hdfs_opendir(const char *path)
{
	struct chirp_dir *dir;
	RESOLVENULL(path)

	debug(D_HDFS, "listdir %s", path);

	dir = xxmalloc(sizeof(*dir));

	dir->info = hdfs_services->listdir(fs, path, &dir->n);
	if(!dir->info) {
		free(dir);
		errno = ENOENT;
		return 0;
	}

	dir->i = 0;
	strcpy(dir->path, path);

	return dir;
}

static struct chirp_dirent *chirp_fs_hdfs_readdir(struct chirp_dir *dir)
{
	if(dir->i < dir->n) {
		/* mName is of the form hdfs:/hostname:port/path/to/file */
		char *name = dir->info[dir->i].mName;
		name += strlen(name);	/* now points to nul byte */
		while(name[-1] != '/')
			name--;
		dir->entry.name = name;
		copystat(&dir->entry.info, &dir->info[dir->i], dir->info[dir->i].mName);
		dir->i++;
		return &dir->entry;
	} else {
		return NULL;
	}
}

static void chirp_fs_hdfs_closedir(struct chirp_dir *dir)
{
	debug(D_HDFS, "closedir %s", dir->path);
	hdfs_services->free_stat(dir->info, dir->n);
	free(dir);
}

/*
HDFS is known to return bogus errnos from unlink,
so check for directories beforehand, and set the errno
properly afterwards if needed.
*/
static INT64_T do_unlink(const char *path, int recursive )
{
	struct chirp_stat info;

	if(do_stat(path, &info) < 0)
		return -1;

	if(!recursive && S_ISDIR(info.cst_mode)) {
		errno = EISDIR;
		return -1;
	}

	debug(D_HDFS, "unlink %s", path);
	if(hdfs_services->unlink(fs, path, recursive) == -1) {
		errno = EACCES;
		return -1;
	}

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

static INT64_T chirp_fs_hdfs_open(const char *path, INT64_T flags, INT64_T mode)
{
	struct chirp_stat info;
	RESOLVE(path)
	INT64_T fd = getfd();
	if(fd == -1) return -1;

	int result = do_stat(path, &info);

	int file_exists = (result == 0);

	/* HDFS doesn't handle the errnos for this properly */
	if(file_exists && S_ISDIR(info.cst_mode)) {
		errno = EISDIR;
		return -1;
	}

	if(file_exists && (flags & O_EXCL))
		return errno = EEXIST, -1;

	mode &= S_IXUSR|S_IRWXG|S_IRWXO;
	mode |= S_IRUSR|S_IWUSR;

	switch (flags & O_ACCMODE) {
		case O_RDONLY:
			debug(D_HDFS, "opening file %s (flags: %"PRIo64") for reading; mode: %"PRIo64"", path, flags, mode);
			if(!file_exists) {
				errno = ENOENT;
				return -1;
			}
			break;
		case O_WRONLY:
			// You may truncate the file by deleting it.
			debug(D_HDFS, "opening file %s (flags: %"PRIo64") for writing; mode: %"PRIo64"", path, flags, mode);
			if(flags & O_TRUNC) {
				if(file_exists) {
					do_unlink(path,0);
					file_exists = 0;
					flags ^= O_TRUNC;
				}
			} else if(file_exists && info.cst_size == 0) {
				/* file is empty, just delete it as if O_TRUNC (useful for FUSE with some UNIX utils, like mv) */
				do_unlink(path,0);
				file_exists = 0;
			} else {
				if(file_exists) {
					errno = EACCES;
					return -1;
				}
			}

			// You cannot append to an existing file.
			if(flags & O_APPEND) {
				if(file_exists) {
					errno = EACCES;
					return -1;
				}
			}
			break;
		default:
			debug(D_HDFS, "file %s must be opened O_RDONLY or O_WRONLY but not O_RDWR", path);
			errno = EACCES;
			return -1;
	}

	hdfsFile file = hdfs_services->open(fs, path, flags, 0, hdfs_nreps, 0);
	if (file) {
		open_files[fd].file = file;
		strcpy(open_files[fd].path, path);
		return fd;
	} else {
		debug(D_HDFS, "open %s failed: %s", path, strerror(errno));
		return -1;
	}
}

static INT64_T chirp_fs_hdfs_close(int fd)
{
	SETUP_FILE
	debug(D_HDFS, "close %s", open_files[fd].path);
	open_files[fd].path[0] = 0;
	open_files[fd].file = NULL;
	return hdfs_services->close(fs, file);
}

static INT64_T chirp_fs_hdfs_pread(int fd, void *buffer, INT64_T length, INT64_T offset)
{
	SETUP_FILE
	debug(D_HDFS, "pread %d %"PRId64" %"PRId64"", fd, length, offset);
	return hdfs_services->pread(fs, file, offset, buffer, length);
}

static void chirp_fs_hdfs_write_zeroes(hdfsFile file, INT64_T length)
{
	/* ANSI C standard requires this be initialized with 0.
	 *
	 * Also note, despite its large size, the executable size does not increase
	 * as this is put in the "uninitialized" .bss section. [Putting it in
	 * .rodata would increase the executable size.]
	 */
	static const char zero[1<<20];

	while(length > 0) {
		int chunksize = MIN(sizeof(zero), (size_t)length);
		hdfs_services->write(fs, file, zero, chunksize);
		length -= chunksize;
	}
}

static INT64_T chirp_fs_hdfs_pwrite(int fd, const void *buffer, INT64_T length, INT64_T offset)
{
	SETUP_FILE
	INT64_T current = hdfs_services->tell(fs, file);

	/* We cannot seek backwards while writing */
	if(offset < current) {
		debug(D_HDFS, "pwrite: seeking backwards on a write is not supported by HDFS.");
		errno = EACCES;
		return -1;
	}


	/*
	   But, if the write is after the end of the file,
	   it can be simulated by writing a sufficent number of zeros.
	   This is commonly done by cp, which re-creates sparse files.
	 */

	if(offset > current) {
		debug(D_HDFS, "zero %d %"PRId64, fd, length);
		chirp_fs_hdfs_write_zeroes(file, offset - current);
	}

	debug(D_HDFS, "write %d %"PRId64"", fd, length);
	return hdfs_services->write(fs, file, buffer, length);
}

static INT64_T chirp_fs_hdfs_swrite(int fd, const void *vbuffer, INT64_T length, INT64_T stride_length, INT64_T stride_skip, INT64_T offset)
{
	SETUP_FILE
	/* Strided write won't work on HDFS because it is a variation on random write. */
	errno = ENOTSUP;
	return -1;
}

static INT64_T chirp_fs_hdfs_fchmod(int fd, INT64_T mode)
{
	struct chirp_stat info;
	SETUP_FILE
	mode &= S_IXUSR|S_IRWXG|S_IRWXO; /* users can only set owner execute and group/other bits */
	if (do_stat(open_files[fd].path, &info) == -1)
		return -1;
	if(S_ISDIR(info.cst_mode)) {
		mode |= S_IRWXU; /* all owner bits must be set */
	} else {
		mode |= S_IRUSR|S_IWUSR; /* owner read/write must be set */
	}
	debug(D_HDFS, "fchmod %s %lo", open_files[fd].path, (long) mode);
	return hdfs_services->chmod(fs, open_files[fd].path, mode);
}

static INT64_T chirp_fs_hdfs_ftruncate(int fd, INT64_T length)
{
	SETUP_FILE
	debug(D_HDFS, "ftruncate %d %"PRId64, fd, length);

	tOffset current = hdfs_services->tell(fs, file);

	if(length < current) {
		errno = EACCES;
		return -1;
	} else if(length == current) {
		return 0;
	} else {
		debug(D_HDFS, "zero %d %"PRId64, fd, length);
		chirp_fs_hdfs_write_zeroes(file, length - current);
		return 0;
	}
}

static INT64_T chirp_fs_hdfs_fsync(int fd)
{
	SETUP_FILE
	debug(D_HDFS, "fsync %s", open_files[fd].path);
	return hdfs_services->flush(fs, file);
}

static INT64_T chirp_fs_hdfs_unlink(const char *path)
{
	RESOLVE(path)
	debug(D_HDFS,"unlink %s",path);
	return do_unlink(path,0);
}

static INT64_T chirp_fs_hdfs_rmall(const char *path)
{
	RESOLVE(path)
	debug(D_HDFS,"rmall %s",path);
	return do_unlink(path,1);
}

static INT64_T chirp_fs_hdfs_rename(const char *path, const char *newpath)
{
	RESOLVE(path)
	RESOLVE(newpath)
	do_unlink(newpath,0);
	debug(D_HDFS, "rename %s %s", path, newpath);
	return hdfs_services->rename(fs, path, newpath);
}

static INT64_T chirp_fs_hdfs_link(const char *path, const char *newpath)
{
	RESOLVE(path)
	RESOLVE(newpath)
	debug(D_HDFS, "link %s %s", path, newpath);
	return (errno = ENOTSUP, -1);
}

static INT64_T chirp_fs_hdfs_symlink(const char *path, const char *newpath)
{
	RESOLVE(path)
	RESOLVE(newpath)
	debug(D_HDFS, "symlink %s %s", path, newpath);
	return (errno = ENOTSUP, -1);
}

static INT64_T chirp_fs_hdfs_readlink(const char *path, char *buf, INT64_T length)
{
	RESOLVE(path)
	debug(D_HDFS, "readlink %s %"PRId64"", path, length);
	return (errno = EINVAL, -1);
}

static INT64_T chirp_fs_hdfs_mkdir(const char *path, INT64_T mode)
{
	struct chirp_stat info;
	RESOLVE(path)

	/* hdfs mkdir incorrectly returns EPERM if it already exists. */
	if(do_stat(path, &info) == 0 && S_ISDIR(info.cst_mode)) {
		errno = EEXIST;
		return -1;
	}

	debug(D_HDFS, "mkdir %s %d", path, (int) mode);
	return hdfs_services->mkdir(fs, path);
}

static INT64_T chirp_fs_hdfs_rmdir(const char *path)
{
	RESOLVE(path)
	debug(D_HDFS, "rmdir %s", path);

	struct chirp_stat info;

	if(do_stat(path, &info) < 0)
		return -1;

	if(!S_ISDIR(info.cst_mode)) {
		errno = ENOTDIR;
		return -1;
	}

	if(hdfs_services->unlink(fs, path, 0)<0) {
		errno = EACCES;
		return -1;
	}

	return 0;
}

static INT64_T chirp_fs_hdfs_lstat(const char *path, struct chirp_stat *buf)
{
	RESOLVE(path)
	debug(D_HDFS, "lstat %s", path);
	return do_stat(path, buf);
}

static INT64_T do_statfs(const char *path, struct chirp_statfs *buf)
{
	debug(D_HDFS, "statfs %s", path);

	INT64_T capacity = hdfs_services->get_capacity(fs);
	INT64_T used = hdfs_services->get_used(fs);
	INT64_T blocksize = hdfs_services->get_default_block_size(fs);

	if(capacity < 0 || used < 0 || blocksize < 0)
		return (errno = EIO, -1);

	buf->f_type = 0;
	buf->f_bsize = blocksize;
	buf->f_blocks = capacity / blocksize;
	buf->f_bavail = buf->f_bfree = (capacity - used) / blocksize;
	buf->f_files = buf->f_ffree = 0;

	return 0;
}

static INT64_T chirp_fs_hdfs_statfs(const char *path, struct chirp_statfs *buf)
{
	RESOLVE(path)
	return do_statfs(path, buf);
}

static INT64_T chirp_fs_hdfs_fstatfs(int fd, struct chirp_statfs *buf)
{
	SETUP_FILE
	return do_statfs(open_files[fd].path, buf);
}

static INT64_T chirp_fs_hdfs_access(const char *path, INT64_T mode)
{
	/* W_OK is ok to delete, not to write, but we can't distinguish intent */
	/* Chirp ACL will check that we can access the file the way we want, so
	   we just do a redundant "exists" check */
	RESOLVE(path)
	debug(D_HDFS, "access %s %ld", path, (long) mode);
	return hdfs_services->exists(fs, path);
}

static INT64_T chirp_fs_hdfs_chmod(const char *path, INT64_T mode)
{
	struct chirp_stat info;
	RESOLVE(path)
	mode &= S_IXUSR|S_IRWXG|S_IRWXO; /* users can only set owner execute and group/other bits */
	if (do_stat(path, &info) == -1)
		return -1;
	if(S_ISDIR(info.cst_mode)) {
		mode |= S_IRWXU; /* all owner bits must be set */
	} else {
		mode |= S_IRUSR|S_IWUSR; /* owner read/write must be set */
	}
	debug(D_HDFS, "chmod %s %ld", path, (long) mode);
	return hdfs_services->chmod(fs, path, mode);
}

static INT64_T chirp_fs_hdfs_truncate(const char *path, INT64_T length)
{
	struct chirp_stat info;
	RESOLVE(path)
	debug(D_HDFS, "truncate %s %"PRId64, path, length);
	if(do_stat(path, &info) == -1)
		return -1;	/* probably doesn't exist, return ENOENT... */
	else if(length == 0) {
		/* FUSE is particularly obnoxious about changing open with O_TRUNC to
		 * truncate(path);
		 * open(path, ...);
		 */
		hdfs_services->unlink(fs, path, 0);
		hdfsFile file = hdfs_services->open(fs, path, O_WRONLY | O_CREAT, 0, 0, 0);
		hdfs_services->close(fs, file);
		return 0;
	} else {
		errno = EACCES;
		return -1;
	}
}

static INT64_T chirp_fs_hdfs_utime(const char *path, time_t actime, time_t modtime)
{
	RESOLVE(path)
	debug(D_HDFS, "utime %s %ld %ld", path, (long) actime, (long) modtime);
	return hdfs_services->utime(fs, path, modtime, actime);
}

static INT64_T chirp_fs_hdfs_setrep(const char *path, int nreps)
{
	RESOLVE(path)
	debug(D_HDFS, "setrep %s %d", path, nreps);

	/* If the path is @@@, then it sets the replication factor for all newly created files in this session. Zero is valid and indicates the default value selected by HDFS. */

	if(!strcmp(string_back(path, 3), "@@@")) {
		if(nreps >= 0) {
			hdfs_nreps = nreps;
			return 0;
		} else {
			errno = EINVAL;
			return -1;
		}
	} else {
		return hdfs_services->setrep(fs, path, nreps);
	}
}

static int chirp_fs_hdfs_do_acl_check()
{
	return 1;
}

struct chirp_filesystem chirp_fs_hdfs = {
	chirp_fs_hdfs_init,
	chirp_fs_hdfs_destroy,

	chirp_fs_hdfs_fname,

	chirp_fs_hdfs_open,
	chirp_fs_hdfs_close,
	chirp_fs_hdfs_pread,
	chirp_fs_hdfs_pwrite,
	cfs_basic_sread,
	chirp_fs_hdfs_swrite,
	cfs_stub_lockf,
	chirp_fs_hdfs_fstat,
	chirp_fs_hdfs_fstatfs,
	cfs_basic_fchown,
	chirp_fs_hdfs_fchmod,
	chirp_fs_hdfs_ftruncate,
	chirp_fs_hdfs_fsync,

	cfs_basic_search,

	chirp_fs_hdfs_opendir,
	chirp_fs_hdfs_readdir,
	chirp_fs_hdfs_closedir,

	chirp_fs_hdfs_unlink,
	chirp_fs_hdfs_rmall,
	chirp_fs_hdfs_rename,
	chirp_fs_hdfs_link,
	chirp_fs_hdfs_symlink,
	chirp_fs_hdfs_readlink,
	chirp_fs_hdfs_mkdir,
	chirp_fs_hdfs_rmdir,
	chirp_fs_hdfs_stat,
	chirp_fs_hdfs_lstat,
	chirp_fs_hdfs_statfs,
	chirp_fs_hdfs_access,
	chirp_fs_hdfs_chmod,
	cfs_basic_chown,
	cfs_basic_lchown,
	chirp_fs_hdfs_truncate,
	chirp_fs_hdfs_utime,
	cfs_basic_hash,
	chirp_fs_hdfs_setrep,

	cfs_stub_getxattr,
	cfs_stub_fgetxattr,
	cfs_stub_lgetxattr,
	cfs_stub_listxattr,
	cfs_stub_flistxattr,
	cfs_stub_llistxattr,
	cfs_stub_setxattr,
	cfs_stub_fsetxattr,
	cfs_stub_lsetxattr,
	cfs_stub_removexattr,
	cfs_stub_fremovexattr,
	cfs_stub_lremovexattr,

	chirp_fs_hdfs_do_acl_check,

	cfs_stub_job_dbinit,
	cfs_stub_job_schedule,
};

/* vim: set noexpandtab tabstop=8: */
