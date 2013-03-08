/*
Copyright (C) 2008- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "chirp_filesystem.h"
#include "chirp_fs_hdfs.h"
#include "chirp_protocol.h"

#include "macros.h"
#include "xxmalloc.h"
#include "hash_table.h"
#include "debug.h"
#include "username.h"
#include "stringtools.h"

#include "hdfs_library.h"

#include <assert.h>
#include <string.h>
#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <fcntl.h>
#include <errno.h>
#include <utime.h>
#include <dlfcn.h>
#include <pwd.h>
#include <grp.h>
#include <sys/stat.h>

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

/*
HDFS gets upset if a path begins with two slashes.
This macro simply skips over the first slash if needed.
*/

#define FIXPATH(p) ( (p[0]=='/' && p[1]=='/') ? &p[1] : p )

static char *chirp_fs_hdfs_hostname = NULL;
static int chirp_fs_hdfs_port = 0;
static int chirp_fs_hdfs_nreps = 0;

static struct hdfs_library *hdfs_services = 0;
static hdfsFS fs = NULL;

extern char chirp_owner[USERNAME_MAX];

/* Array of open HDFS Files */
#define BASE_SIZE 1024
static struct chirp_fs_hdfs_file {
	char *name;
	hdfsFile file;
} open_files[BASE_SIZE];	// = NULL;

static const char* chirp_fs_hdfs_init( const char *url )
{
	static const char *groups[] = { "supergroup" };
	const char *path;

	char *h, *p;

	/* find the first slash after hdfs: */
	h = strchr(url,'/');

	/* skip over one or more slashes to get the hostname */
	while(*h=='/') h++;
	if(!h) return 0;

	/* find the port following a colon */
	p = strchr(h,':');
	if(!p) fatal("couldn't find a port number in %s",url);

	chirp_fs_hdfs_port = atoi(p+1);
	
	/* now find the slash following the hostname */
	p = strchr(h,'/');
	if(p) {
		path = strdup(p);
	} else {
		path = "/";
	}

	chirp_fs_hdfs_hostname = strdup(h);

	p = strchr(chirp_fs_hdfs_hostname,':');
	if(p) *p = 0;

	int i;

	assert(fs == NULL);

	for(i = 0; i < BASE_SIZE; i++)
		open_files[i].name = NULL;

	if(!hdfs_services) {
		hdfs_services = hdfs_library_open();
		if(!hdfs_services)
			return 0;
	}

	debug(D_HDFS, "connecting to hdfs://%s:%u%s as '%s'\n", chirp_fs_hdfs_hostname, chirp_fs_hdfs_port, path, chirp_owner);
	fs = hdfs_services->connect_as_user(chirp_fs_hdfs_hostname, chirp_fs_hdfs_port, chirp_owner, groups, 1);

	if(fs == NULL) 
		return 0;
	else
		return strdup(path);
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

static INT64_T chirp_fs_hdfs_stat(const char *path, struct chirp_stat * buf)
{
	hdfsFileInfo *file_info;

	path = FIXPATH(path);

	debug(D_HDFS, "stat %s", path);

	file_info = hdfs_services->stat(fs, path);
	if(file_info == NULL)
		return (errno = ENOENT, -1);
	copystat(buf, file_info, path);
	hdfs_services->free_stat(file_info, 1);

	return 0;
}

static INT64_T chirp_fs_hdfs_fstat(int fd, struct chirp_stat *buf)
{
	return chirp_fs_hdfs_stat(open_files[fd].name, buf);
}

static INT64_T chirp_fs_hdfs_search( const char *subject, const char *dir, const char *patt, int flags, struct link *l, time_t stoptime )
{
	return 1; //FIXME
}

struct chirp_dir {
	int i;
	int n;
	hdfsFileInfo *info;
	char *path;
};

static struct chirp_dir * chirp_fs_hdfs_opendir( const char *path )
{
	struct chirp_dir *dir;

	path = FIXPATH(path);

	debug(D_HDFS, "listdir %s", path);

	dir = xxmalloc(sizeof(*dir));

	dir->info = hdfs_services->listdir(fs, path, &dir->n);
	if(!dir->info) {
		free(dir);
		errno = ENOENT;
		return 0;
	}

	dir->i = 0;
	dir->path = xxstrdup(path);

	return dir;
}

static struct chirp_dirent * chirp_fs_hdfs_readdir( struct chirp_dir *dir )
{
	static struct chirp_dirent d;

	if(dir->i < dir->n) {
		/* mName is of the form hdfs:/hostname:port/path/to/file */
		char *name = dir->info[dir->i].mName;
		name += strlen(name);	/* now points to nul byte */
		while(name[-1] != '/')
			name--;

		d.name = name;
		copystat(&d.info,&dir->info[dir->i],dir->info[dir->i].mName);
		dir->i++;
		return &d;
	} else {
		return NULL;
	}
}

static void chirp_fs_hdfs_closedir( struct chirp_dir *dir )
{
	debug(D_HDFS, "closedir %s", dir->path);
	hdfs_services->free_stat(dir->info, dir->n);
	free(dir->path);
	free(dir);
}

static INT64_T get_fd(void)
{
	INT64_T fd;
	/* find an unused file descriptor */
	for(fd = 0; fd < BASE_SIZE; fd++)
		if(open_files[fd].name == NULL)
			return fd;
	debug(D_HDFS, "too many files open");
	errno = EMFILE;
	return -1;
}

static INT64_T chirp_fs_hdfs_unlink( const char *path );

static INT64_T chirp_fs_hdfs_open(const char *path, INT64_T flags, INT64_T mode)
{
	INT64_T fd;
	struct chirp_stat info;

	path = FIXPATH(path);

	int result = chirp_fs_hdfs_stat(path, &info);

	int file_exists = (result == 0);

    /* HDFS doesn't handle the errnos for this properly */
	if (file_exists && S_ISDIR(info.cst_mode)) {
		errno = EISDIR;
		return -1;
	}

	fd = get_fd();
	if(fd == -1)
		return -1;

	mode = 0600 | (mode & 0100);

	switch (flags & O_ACCMODE) {
	case O_RDONLY:
		debug(D_HDFS, "opening file %s (flags: %o) for reading; mode: %o", path, flags, mode);
		if(!file_exists) {
			errno = ENOENT;
			return -1;
		}
		break;
	case O_WRONLY:
		// You may truncate the file by deleting it.
		debug(D_HDFS, "opening file %s (flags: %o) for writing; mode: %o", path, flags, mode);
		if(flags & O_TRUNC) {
			if(file_exists) {
				chirp_fs_hdfs_unlink(path);
				file_exists = 0;
				flags ^= O_TRUNC;
			}
		} else if(file_exists && info.cst_size == 0) {
			/* file is empty, just delete it as if O_TRUNC (useful for FUSE with some UNIX utils, like mv) */
			chirp_fs_hdfs_unlink(path);
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

	open_files[fd].file = hdfs_services->open(fs, path, flags, 0, chirp_fs_hdfs_nreps, 0);
	if(open_files[fd].file == NULL) {
		debug(D_HDFS, "open %s failed: %s", path, strerror(errno));
		return -1;
	} else {
		open_files[fd].name = xxstrdup(path);
		return fd;
	}
}

static INT64_T chirp_fs_hdfs_close(int fd)
{
	debug(D_HDFS, "close %s", open_files[fd].name);
	free(open_files[fd].name);
	open_files[fd].name = NULL;
	return hdfs_services->close(fs, open_files[fd].file);
}

static INT64_T chirp_fs_hdfs_pread(int fd, void *buffer, INT64_T length, INT64_T offset)
{
	debug(D_HDFS, "pread %d %lld %lld", fd, length, offset);
	return hdfs_services->pread(fs, open_files[fd].file, offset, buffer, length);
}

static INT64_T chirp_fs_hdfs_sread(int fd, void *vbuffer, INT64_T length, INT64_T stride_length, INT64_T stride_skip, INT64_T offset)
{
	INT64_T total = 0;
	INT64_T actual = 0;
	char *buffer = vbuffer;

	if(stride_length < 0 || stride_skip < 0 || offset < 0) {
		errno = EINVAL;
		return -1;
	}

	while(length >= stride_length) {
		actual = chirp_fs_hdfs_pread(fd, &buffer[total], stride_length, offset);
		if(actual > 0) {
			length -= actual;
			total += actual;
			offset += stride_skip;
			if(actual == stride_length) {
				continue;
			} else {
				break;
			}
		} else {
			break;
		}
	}

	if(total > 0) {
		return total;
	} else {
		if(actual < 0) {
			return -1;
		} else {
			return 0;
		}
	}
}

static void chirp_fs_hdfs_write_zeroes(int fd, INT64_T length)
{
	int zero_size = 65536;
	char *zero = malloc(zero_size);
	memset(zero, 0, zero_size);

	debug(D_HDFS, "zero %d %d", fd, length);

	while(length > 0) {
		int chunksize = MIN(zero_size, length);
		hdfs_services->write(fs, open_files[fd].file, zero, chunksize);
		length -= chunksize;
	}

	free(zero);
}

static INT64_T chirp_fs_hdfs_pwrite(int fd, const void *buffer, INT64_T length, INT64_T offset)
{
	INT64_T current = hdfs_services->tell(fs, open_files[fd].file);

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
		chirp_fs_hdfs_write_zeroes(fd, offset - current);
	}

	debug(D_HDFS, "write %d %lld", fd, length);
	return hdfs_services->write(fs, open_files[fd].file, buffer, length);
}

static INT64_T chirp_fs_hdfs_swrite(int fd, const void *vbuffer, INT64_T length, INT64_T stride_length, INT64_T stride_skip, INT64_T offset)
{
	/* Strided write won't work on HDFS because it is a variation on random write. */
	errno = ENOTSUP;
	return -1;
}

static INT64_T chirp_fs_hdfs_fchown(int fd, INT64_T uid, INT64_T gid)
{
	// Changing file ownership is silently ignored,
	// because permissions are handled through the ACL model.
	debug(D_HDFS, "fchown %s %ld %ld", open_files[fd].name, (long) uid, (long) gid);
	return 0;
}

static INT64_T chirp_fs_hdfs_fchmod(int fd, INT64_T mode)
{
	// The owner may only add or remove the execute bit,
	// because permissions are handled through the ACL model.
	debug(D_HDFS, "fchmod %s %lo", open_files[fd].name, (long) mode);
	mode = 0600 | (mode & 0100);
	return hdfs_services->chmod(fs, open_files[fd].name, mode);
}

static INT64_T chirp_fs_hdfs_ftruncate(int fd, INT64_T length)
{
	debug(D_HDFS, "ftruncate %d %lld", fd, length);

	tOffset current = hdfs_services->tell(fs, open_files[fd].file);

	if(length < current) {
		errno = EACCES;
		return -1;
	} else if(length == current) {
		return 0;
	} else {
		chirp_fs_hdfs_write_zeroes(fd, length - current);
		return 0;
	}
}

static INT64_T chirp_fs_hdfs_fsync(int fd)
{
	debug(D_HDFS, "fsync %s", open_files[fd].name);
	return hdfs_services->flush(fs, open_files[fd].file);
}

static INT64_T chirp_fs_hdfs_getfile(const char *path, struct link * link, time_t stoptime)
{
	return cfs_basic_getfile(FIXPATH(path),link,stoptime);
}

static INT64_T chirp_fs_hdfs_putfile(const char *path, struct link * link, INT64_T length, INT64_T mode, time_t stoptime)
{
	return cfs_basic_putfile(FIXPATH(path),link,length,mode,stoptime);
}

/*
HDFS is known to return bogus errnos from unlink,
so check for directories beforehand, and set the errno
properly afterwards if needed.
*/

static INT64_T chirp_fs_hdfs_unlink(const char *path)
{
	path = FIXPATH(path);

	struct chirp_stat info;

	if(chirp_fs_hdfs_stat(path,&info)<0) return -1;

	if(S_ISDIR(info.cst_mode)) {
		errno = EISDIR;
		return -1;
	}

	debug(D_HDFS, "unlink %s", path);

	int result = hdfs_services->unlink(fs,path,0);
	if(result<0) {
		errno = EACCES;
		return -1;
	}

	return 0;
}

static INT64_T chirp_fs_hdfs_rmall(const char *path)
{
	path = FIXPATH(path);
	debug(D_HDFS, "rmall %s", path);
	return hdfs_services->unlink(fs, path,1);
}

static INT64_T chirp_fs_hdfs_rename(const char *path, const char *newpath)
{
	path = FIXPATH(path);
	newpath = FIXPATH(newpath);
	chirp_fs_hdfs_unlink(newpath);
	debug(D_HDFS, "rename %s %s", path, newpath);
	return hdfs_services->rename(fs, path, newpath);
}

static INT64_T chirp_fs_hdfs_link(const char *path, const char *newpath)
{
	path = FIXPATH(path);
	newpath = FIXPATH(newpath);
	debug(D_HDFS, "link %s %s", path, newpath);
	return (errno = ENOTSUP, -1);
}

static INT64_T chirp_fs_hdfs_symlink(const char *path, const char *newpath)
{
	path = FIXPATH(path);
	newpath = FIXPATH(newpath);
	debug(D_HDFS, "symlink %s %s", path, newpath);
	return (errno = ENOTSUP, -1);
}

static INT64_T chirp_fs_hdfs_readlink(const char *path, char *buf, INT64_T length)
{
	path = FIXPATH(path);
	debug(D_HDFS, "readlink %s %lld", path, length);
	return (errno = EINVAL, -1);
}

static INT64_T chirp_fs_hdfs_mkdir(const char *path, INT64_T mode)
{
	path = FIXPATH(path);

	/* hdfs mkdir incorrectly returns EPERM if it already exists. */
	struct chirp_stat info;
	int result = chirp_fs_hdfs_stat(path, &info);
	if(result == 0 && S_ISDIR(info.cst_mode)) {
		errno = EEXIST;
		return -1;
	}

	debug(D_HDFS, "mkdir %s %lld", path, mode);
	return hdfs_services->mkdir(fs, path);
}

static INT64_T chirp_fs_hdfs_rmdir(const char *path)
{
	path = FIXPATH(path);
	debug(D_HDFS,"rmdir %s", path);
	return hdfs_services->unlink(fs,path,1);
}

static INT64_T chirp_fs_hdfs_lstat(const char *path, struct chirp_stat * buf)
{
	path = FIXPATH(path);
	debug(D_HDFS, "lstat %s", path);
	return chirp_fs_hdfs_stat(path, buf);
}

static INT64_T chirp_fs_hdfs_statfs(const char *path, struct chirp_statfs * buf)
{
	path = FIXPATH(path);
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

static INT64_T chirp_fs_hdfs_fstatfs(int fd, struct chirp_statfs * buf)
{
	return chirp_fs_hdfs_statfs(open_files[fd].name, buf);
}

static INT64_T chirp_fs_hdfs_access(const char *path, INT64_T mode)
{
	/* W_OK is ok to delete, not to write, but we can't distinguish intent */
	/* Chirp ACL will check that we can access the file the way we want, so
	   we just do a redundant "exists" check */
	path = FIXPATH(path);
	debug(D_HDFS, "access %s %ld", path, (long) mode);
	return hdfs_services->exists(fs, path);
}

static INT64_T chirp_fs_hdfs_chmod(const char *path, INT64_T mode)
{
	// The owner may only add or remove the execute bit,
	// because permissions are handled through the ACL model.
	path = FIXPATH(path);
	debug(D_HDFS, "chmod %s %ld", path, (long) mode);
	mode = 0600 | (mode & 0100);
	return hdfs_services->chmod(fs, path, mode);
}

static INT64_T chirp_fs_hdfs_chown(const char *path, INT64_T uid, INT64_T gid)
{
	// Changing file ownership is silently ignored,
	// because permissions are handled through the ACL model.
	path = FIXPATH(path);
	debug(D_HDFS, "chown (ignored) %s %ld %ld", path, (long) uid, (long) gid);
	return 0;
}

static INT64_T chirp_fs_hdfs_lchown(const char *path, INT64_T uid, INT64_T gid)
{
	// Changing file ownership is silently ignored,
	// because permissions are handled through the ACL model.
	path = FIXPATH(path);
	debug(D_HDFS, "lchown (ignored) %s %ld %ld", path, (long) uid, (long) gid);
	return 0;
}

static INT64_T chirp_fs_hdfs_truncate(const char *path, INT64_T length)
{
	struct chirp_stat info;
	path = FIXPATH(path);
	debug(D_HDFS, "truncate %s %lld", path, length);
	int result = chirp_fs_hdfs_stat(path, &info);
	if(result < 0)
		return -1; /* probably doesn't exist, return ENOENT... */
	else if (length == 0) {
		/* FUSE is particularly obnoxious about changing open with O_TRUNC to
		* truncate(path);
		* open(path, ...);
		*/
		hdfs_services->unlink(fs, path,0);
		hdfsFile file = hdfs_services->open(fs, path, O_WRONLY|O_CREAT, 0, 0, 0);
		hdfs_services->close(fs, file);
		return 0;
	} else {
		errno = EACCES;
		return -1;
	}
}

static INT64_T chirp_fs_hdfs_utime(const char *path, time_t actime, time_t modtime)
{
	path = FIXPATH(path);
	debug(D_HDFS, "utime %s %ld %ld", path, (long) actime, (long) modtime);
	return hdfs_services->utime(fs, path, modtime, actime);
}

static INT64_T chirp_fs_hdfs_md5(const char *path, unsigned char digest[16])
{
	return cfs_basic_md5(FIXPATH(path),digest);
}

static INT64_T chirp_fs_hdfs_chdir(const char *path)
{
	debug(D_HDFS, "chdir %s", path);
	return hdfs_services->chdir(fs, path);
}

static INT64_T chirp_fs_hdfs_setrep( const char *path, int nreps )
{
	debug(D_HDFS,"setrep %s %d",path,nreps);

	/* If the path is @@@, then it sets the replication factor for all newly created files in this session. Zero is valid and indicates the default value selected by HDFS. */

	if(!strcmp(string_back(path,3),"@@@")) {
		if(nreps>=0) {
			chirp_fs_hdfs_nreps = nreps;
			return 0;
		} else {
			errno = EINVAL;
			return -1;
		}
	} else {
		return hdfs_services->setrep(fs,path,nreps);
	}
}

static int chirp_fs_hdfs_do_acl_check()
{
	return 1;
}

struct chirp_filesystem chirp_fs_hdfs = {
	chirp_fs_hdfs_init,

	chirp_fs_hdfs_open,
	chirp_fs_hdfs_close,
	chirp_fs_hdfs_pread,
	chirp_fs_hdfs_pwrite,
	chirp_fs_hdfs_sread,
	chirp_fs_hdfs_swrite,
	chirp_fs_hdfs_fstat,
	chirp_fs_hdfs_fstatfs,
	chirp_fs_hdfs_fchown,
	chirp_fs_hdfs_fchmod,
	chirp_fs_hdfs_ftruncate,
	chirp_fs_hdfs_fsync,

	chirp_fs_hdfs_search,

	chirp_fs_hdfs_opendir,
	chirp_fs_hdfs_readdir,
	chirp_fs_hdfs_closedir,

	chirp_fs_hdfs_getfile,
	chirp_fs_hdfs_putfile,

	chirp_fs_hdfs_unlink,
	chirp_fs_hdfs_rmall,
	chirp_fs_hdfs_rename,
	chirp_fs_hdfs_link,
	chirp_fs_hdfs_symlink,
	chirp_fs_hdfs_readlink,
	chirp_fs_hdfs_chdir,
	chirp_fs_hdfs_mkdir,
	chirp_fs_hdfs_rmdir,
	chirp_fs_hdfs_stat,
	chirp_fs_hdfs_lstat,
	chirp_fs_hdfs_statfs,
	chirp_fs_hdfs_access,
	chirp_fs_hdfs_chmod,
	chirp_fs_hdfs_chown,
	chirp_fs_hdfs_lchown,
	chirp_fs_hdfs_truncate,
	chirp_fs_hdfs_utime,
	chirp_fs_hdfs_md5,
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
};
