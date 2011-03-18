/*
Copyright (C) 2008- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "chirp_filesystem.h"
#include "chirp_hdfs.h"
#include "chirp_protocol.h"

#include "macros.h"
#include "xmalloc.h"
#include "hash_table.h"
#include "debug.h"
#include "md5.h"
#include "username.h"

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
#include <sys/statfs.h>

// HDFS gets upset if a path begins with two slashes.
// This macro simply skips over the first slash if needed.
#define FIXPATH(p) ( (p[0]=='/' && p[1]=='/') ? &p[1] : p )

char *chirp_hdfs_hostname = NULL;
UINT16_T chirp_hdfs_port = 0;

extern char chirp_owner[USERNAME_MAX];

static struct hdfs_library *hdfs_services = 0;
static hdfsFS fs = NULL;

/* Array of open HDFS Files */
#define BASE_SIZE 1024
static struct chirp_hdfs_file {
	char *name;
	hdfsFile file;
} open_files[BASE_SIZE];	// = NULL;

INT64_T chirp_hdfs_init(const char *path)
{
	static const char *groups[] = { "supergroup" };

	(void) path;

	int i;

	if(chirp_hdfs_hostname == NULL)
		fatal("hostname and port must be specified, use -x option");

	debug(D_HDFS, "initializing", chirp_hdfs_hostname, chirp_hdfs_port);

	assert(fs == NULL);

	for(i = 0; i < BASE_SIZE; i++)
		open_files[i].name = NULL;

	if(!hdfs_services) {
		hdfs_services = hdfs_library_open();
		if(!hdfs_services)
			return -1;
	}

	debug(D_HDFS, "connecting to %s:%u as '%s'\n", chirp_hdfs_hostname, chirp_hdfs_port, chirp_owner);
	fs = hdfs_services->connect_as_user(chirp_hdfs_hostname, chirp_hdfs_port, chirp_owner, groups, 1);

	if(fs == NULL)
		return (errno = ENOSYS, -1);
	else
		return 0;
}

INT64_T chirp_hdfs_destroy(void)
{
	int ret;
	if(fs == NULL)
		return 0;
	debug(D_HDFS, "destroying hdfs connection", chirp_hdfs_hostname, chirp_hdfs_port);
	ret = hdfs_services->disconnect(fs);
	if(ret == -1)
		return ret;
	fs = NULL;
	hdfs_library_close(hdfs_services);
	return 0;
}

static void copystat(struct chirp_stat *cs, hdfsFileInfo * hs, const char *path )
{
	memset(cs,0,sizeof(*cs));
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
	if(cs->cst_blksize<1) cs->cst_blksize = 64*1024*1024;
	cs->cst_blocks = MAX(1,cs->cst_size/cs->cst_blksize);

	/* Note that hs->mLastAccess is typically zero. */
	cs->cst_atime = cs->cst_mtime = cs->cst_ctime = hs->mLastMod;
}

INT64_T chirp_hdfs_fstat(int fd, struct chirp_stat *buf)
{
	return chirp_hdfs_stat(open_files[fd].name, buf);
}

INT64_T chirp_hdfs_stat(const char *path, struct chirp_stat * buf)
{
	hdfsFileInfo *file_info;

	path = FIXPATH(path);

	debug(D_HDFS, "stat %s", path);

	file_info = hdfs_services->stat(fs,path);
	if(file_info == NULL)
		return (errno = ENOENT, -1);
	copystat(buf, file_info, path);
	hdfs_services->free_stat(file_info, 1);

	return 0;
}

struct chirp_hdfs_dir {
	int i;
	int n;
	hdfsFileInfo *info;
	char *path;
};

void *chirp_hdfs_opendir(const char *path)
{
	struct chirp_hdfs_dir *d;

	path = FIXPATH(path);

	debug(D_HDFS, "opendir %s", path);

	d = xxmalloc(sizeof(struct chirp_hdfs_dir));
	d->info = hdfs_services->listdir(fs,path, &d->n);
	d->i = 0;
	d->path = xstrdup(path);

	if(d->info == NULL)
		return (free(d), errno = ENOENT, NULL);

	return d;
}

char *chirp_hdfs_readdir(void *dir)
{
	struct chirp_hdfs_dir *d = (struct chirp_hdfs_dir *) dir;
	debug(D_HDFS, "readdir %s", d->path);
	if(d->i < d->n) {
		/* mName is of the form hdfs:/hostname:port/path/to/file */
		char *entry = d->info[d->i++].mName;
		entry += strlen(entry);	/* now points to nul byte */
		while(entry[-1] != '/')
			entry--;
		return entry;
	} else
		return NULL;
}

void chirp_hdfs_closedir(void *dir)
{
	struct chirp_hdfs_dir *d = (struct chirp_hdfs_dir *) dir;
	debug(D_HDFS, "closedir", d->path);
	hdfs_services->free_stat(d->info, d->n);
	free(d->path);
	free(d);
}

INT64_T chirp_hdfs_file_size(const char *path)
{
	struct chirp_stat info;
	path = FIXPATH(path);
	if(chirp_hdfs_stat(path, &info) == 0) {
		return info.cst_size;
	} else {
		return -1;
	}
}

INT64_T chirp_hdfs_fd_size(int fd)
{
	struct chirp_stat info;
	debug(D_HDFS, "fstat on file descriptor %d, path = %s", fd, open_files[fd].name);
	if(chirp_hdfs_fstat(fd, &info) == 0) {
		return info.cst_size;
	} else {
		return -1;
	}
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

static char *read_buffer(const char *path, int entire_file, INT64_T * size)
{
	hdfsFile file;
	char *buffer;
	INT64_T current = 0;

	if(entire_file) {	/* read entire file? */
		struct chirp_stat info;
		if(chirp_hdfs_stat(path, &info) == -1)
			return NULL;
		*size = info.cst_size;
	}

	file = hdfs_services->open(fs,path, O_RDONLY, 0, 0, 0);
	if(file == NULL)
		return NULL;

	buffer = xxmalloc(sizeof(char) * (*size));
	memset(buffer, 0, sizeof(char) * (*size));

	while(current < *size) {
		INT64_T ractual = hdfs_services->read(fs, file, buffer + current, *size - current);
		if(ractual <= 0)
			break;
		current += ractual;
	}
	hdfs_services->close(fs, file);
	return buffer;
}

static INT64_T write_buffer(const char *path, const char *buffer, size_t size)
{
	hdfsFile file;
	INT64_T current = 0;
	INT64_T fd;

	fd = get_fd();
	if(fd == -1)
		return -1;

	file = hdfs_services->open(fs,path, O_WRONLY, 0, 0, 0);
	if(file == NULL)
		return -1;	/* errno is set */

	while(current < size) {
		INT64_T wactual = hdfs_services->write(fs, file, buffer, size - current);
		if(wactual == -1)
			return -1;
		current += wactual;
	}
	open_files[fd].file = file;
	open_files[fd].name = xstrdup(path);
	return fd;

}

INT64_T chirp_hdfs_open(const char *path, INT64_T flags, INT64_T mode)
{
	INT64_T fd, stat_result;
	struct chirp_stat info;

	path = FIXPATH(path);

	stat_result = chirp_hdfs_stat(path, &info);

	fd = get_fd();
	if(fd == -1)
		return -1;

	mode = 0600 | (mode & 0100);
	switch (flags & O_ACCMODE) {
	case O_RDONLY:
		debug(D_HDFS, "opening file %s (flags: %o) for reading; mode: %o", path, flags, mode);
		if(stat_result == -1)
			return (errno = ENOENT, -1);	/* HDFS screws this up */
		break;
	case O_WRONLY:
		debug(D_HDFS, "opening file %s (flags: %o) for writing; mode: %o", path, flags, mode);
		/* Check if file exists already */
		if(stat_result < 0) {
			flags = O_WRONLY;
			break;	/* probably doesn't exist, continue.... */
		} else if(S_ISDIR(info.cst_mode))
			return (errno = EISDIR, -1);
		else if(O_TRUNC & flags) {
			/* delete file, then open again */
			INT64_T result = hdfs_services->unlink(fs,path);
			if(result == -1)
				return (errno = EIO, -1);
			flags ^= O_TRUNC;
			break;
		} else if(!(O_APPEND & flags)) {
			debug(D_HDFS, "file does not have append flag set, setting it anyway");
			/* return (errno = ENOTSUP, -1); */
			flags |= O_APPEND;
		}
		INT64_T size;
		char *buffer = read_buffer(path, 1, &size);
		if(buffer == NULL)
			return -1;
		INT64_T fd = write_buffer(path, buffer, size);
		free(buffer);
		return fd;
	default:
		debug(D_HDFS, "invalid file open flag %o", flags & O_ACCMODE);
		return (errno = EINVAL, -1);
	}

	open_files[fd].file = hdfs_services->open(fs, path, flags, 0, 0, 0);
	if(open_files[fd].file == NULL) {
		debug(D_HDFS, "could not open file %s", path);
		return -1;
	} else {
		open_files[fd].name = xstrdup(path);
		return fd;
	}
}

INT64_T chirp_hdfs_close(int fd)
{
	debug(D_HDFS, "closing file %s", open_files[fd].name);
	free(open_files[fd].name);
	open_files[fd].name = NULL;
	return hdfs_services->close(fs, open_files[fd].file);
}

INT64_T chirp_hdfs_pread(int fd, void *buffer, INT64_T length, INT64_T offset)
{
	debug(D_HDFS, "pread %s", open_files[fd].name);
	return hdfs_services->pread(fs, open_files[fd].file, offset, buffer, length);
}

INT64_T chirp_hdfs_sread(int fd, void *vbuffer, INT64_T length, INT64_T stride_length, INT64_T stride_skip, INT64_T offset)
{
	INT64_T total = 0;
	INT64_T actual = 0;
	char *buffer = vbuffer;

	if(stride_length < 0 || stride_skip < 0 || offset < 0) {
		errno = EINVAL;
		return -1;
	}

	while(length >= stride_length) {
		actual = chirp_hdfs_pread(fd, &buffer[total], stride_length, offset);
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

INT64_T chirp_hdfs_pwrite(int fd, const void *buffer, INT64_T length, INT64_T offset)
{
	/* FIXME deal with non-appends gracefully using an error if not costly */
	debug(D_HDFS, "pwrite %s", open_files[fd].name);
	return hdfs_services->write(fs, open_files[fd].file, buffer, length);
}

INT64_T chirp_hdfs_swrite(int fd, const void *vbuffer, INT64_T length, INT64_T stride_length, INT64_T stride_skip, INT64_T offset)
{
	INT64_T total = 0;
	INT64_T actual = 0;
	const char *buffer = vbuffer;

	if(stride_length < 0 || stride_skip < 0 || offset < 0) {
		errno = EINVAL;
		return -1;
	}

	while(length >= stride_length) {
		actual = chirp_hdfs_pwrite(fd, &buffer[total], stride_length, offset);
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

INT64_T chirp_hdfs_fchown(int fd, INT64_T uid, INT64_T gid)
{
	// Changing file ownership is silently ignored,
	// because permissions are handled through the ACL model.
	debug(D_HDFS, "fchown %s %ld %ld", open_files[fd].name, (long) uid, (long) gid);
	return 0;
}

INT64_T chirp_hdfs_fchmod(int fd, INT64_T mode)
{
	// The owner may only add or remove the execute bit,
	// because permissions are handled through the ACL model.
	debug(D_HDFS, "fchmod %s %lo", open_files[fd].name, (long) mode);
	mode = 0600 | (mode & 0100);
	return hdfs_services->chmod(fs, open_files[fd].name, mode);
}

INT64_T chirp_hdfs_ftruncate(int fd, INT64_T length)
{
	debug(D_HDFS, "ftruncate %s %ld", open_files[fd].name, (long) length);
	INT64_T size = length;
	char *buffer = read_buffer(open_files[fd].name, 0, &size);
	if(buffer == NULL)
		return -1;
	/* simulate truncate */
	if(hdfs_services->close(fs, open_files[fd].file) == -1)
		return (free(buffer), -1);
	INT64_T fd2 = write_buffer(open_files[fd].name, buffer, size);
	open_files[fd].file = open_files[fd2].file;	/* copy over new file */
	free(open_files[fd2].name);	/* close new fd */
	open_files[fd2].name = NULL;
	return 0;
}

INT64_T chirp_hdfs_fsync(int fd)
{
	debug(D_HDFS, "fsync %s", open_files[fd].name);
	return hdfs_services->flush(fs, open_files[fd].file);
}

INT64_T chirp_hdfs_getfile(const char *path, struct link * link, time_t stoptime)
{
	int fd;
	INT64_T result;
	struct chirp_stat info;

	path = FIXPATH(path);
	debug(D_HDFS, "getfile %s", path);

	result = chirp_hdfs_stat(path, &info);
	if(result < 0)
		return result;

	if(S_ISDIR(info.cst_mode)) {
		errno = EISDIR;
		return -1;
	}

	fd = chirp_hdfs_open(path, O_RDONLY, 0);
	if(fd >= 0) {
		char buffer[65536];
		INT64_T total = 0;
		INT64_T ractual, wactual;
		INT64_T length = info.cst_size;

		link_putfstring(link, "%lld\n", stoptime, length);

		// Copy Pasta from link.c

		while(length > 0) {
			INT64_T chunk = MIN(sizeof(buffer), length);

			ractual = hdfs_services->read(fs, open_files[fd].file, buffer, chunk);
			if(ractual <= 0)
				break;

			wactual = link_putlstring(link, buffer, ractual, stoptime);
			if(wactual != ractual) {
				total = -1;
				break;
			}

			total += ractual;
			length -= ractual;
		}
		result = total;
		chirp_hdfs_close(fd);
	} else {
		result = -1;
	}

	return result;
}

INT64_T chirp_hdfs_putfile(const char *path, struct link * link, INT64_T length, INT64_T mode, time_t stoptime)
{
	int fd;
	INT64_T result;

	path = FIXPATH(path);

	debug(D_HDFS, "putfile %s", path);

	mode = 0600 | (mode & 0100);

	fd = chirp_hdfs_open(path, O_WRONLY | O_CREAT | O_TRUNC, (int) mode);
	if(fd >= 0) {
		char buffer[65536];
		INT64_T total = 0;

		link_putliteral(link, "0\n", stoptime);

		// Copy Pasta from link.c

		while(length > 0) {
			INT64_T ractual, wactual;
			INT64_T chunk = MIN(sizeof(buffer), length);

			ractual = link_read(link, buffer, chunk, stoptime);
			if(ractual <= 0)
				break;

			wactual = hdfs_services->write(fs, open_files[fd].file, buffer, ractual);
			if(wactual != ractual) {
				total = -1;
				break;
			}

			total += ractual;
			length -= ractual;
		}

		result = total;

		if(length != 0) {
			if(result >= 0)
				link_soak(link, length - result, stoptime);
			result = -1;
		}
		chirp_hdfs_close(fd);
	} else {
		result = -1;
	}
	return result;
}

INT64_T chirp_hdfs_mkfifo(const char *path)
{
	path = FIXPATH(path);
	debug(D_HDFS, "mkfifo %s", path);
	return (errno = ENOTSUP, -1);
}

INT64_T chirp_hdfs_unlink(const char *path)
{
	path = FIXPATH(path);
	debug(D_HDFS, "unlink %s", path);
	/* FIXME unlink does not set errno properly on failure! */
	int ret = hdfs_services->unlink(fs, path);
	if(ret == -1)
		errno = EEXIST;	/* FIXME bad fix to above problem */
	return 0;
}

INT64_T chirp_hdfs_rename(const char *path, const char *newpath)
{
	path = FIXPATH(path);
	newpath = FIXPATH(path);
	debug(D_HDFS, "rename %s -> %s", path, newpath);
	hdfs_services->unlink(fs, newpath);
	return hdfs_services->rename(fs, path, newpath);
}

INT64_T chirp_hdfs_link(const char *path, const char *newpath)
{
	path = FIXPATH(path);
	newpath = FIXPATH(path);
	debug(D_HDFS, "link %s -> %s", path, newpath);
	return (errno = ENOTSUP, -1);
}

INT64_T chirp_hdfs_symlink(const char *path, const char *newpath)
{
	path = FIXPATH(path);
	newpath = FIXPATH(path);
	debug(D_HDFS, "symlink %s -> %s", path, newpath);
	return (errno = ENOTSUP, -1);
}

INT64_T chirp_hdfs_readlink(const char *path, char *buf, INT64_T length)
{
	path = FIXPATH(path);
	debug(D_HDFS, "readlink %s", path);
	return (errno = EINVAL, -1);
}

INT64_T chirp_hdfs_mkdir(const char *path, INT64_T mode)
{
	path = FIXPATH(path);
	debug(D_HDFS, "mkdir %s", path);
	return hdfs_services->mkdir(fs, path);
}

/*
rmdir is a little unusual.
An 'empty' directory may contain some administrative
files such as an ACL and an allocation state.
Only delete the directory if it contains only those files.
*/

INT64_T chirp_hdfs_rmdir(const char *path)
{
	void *dir;
	char *d;
	int empty = 1;

	path = FIXPATH(path);
	debug(D_HDFS, "rmdir %s", path);

	dir = chirp_hdfs_opendir(path);
	if(dir) {
		while((d = chirp_hdfs_readdir(dir))) {
			if(!strcmp(d, "."))
				continue;
			if(!strcmp(d, ".."))
				continue;
			if(!strncmp(d, ".__", 3))
				continue;
			empty = 0;
			break;
		}
		chirp_hdfs_closedir(dir);

		if(empty) {
			return hdfs_services->unlink(fs, path);
		} else {
			errno = ENOTEMPTY;
			return -1;
		}
	} else {
		return -1;
	}
}

INT64_T chirp_hdfs_lstat(const char *path, struct chirp_stat * buf)
{
	path = FIXPATH(path);
	debug(D_HDFS, "lstat %s", path);
	return chirp_hdfs_stat(path, buf);
}

INT64_T chirp_hdfs_statfs(const char *path, struct chirp_statfs * buf)
{
	path = FIXPATH(path);
	debug(D_HDFS, "statfs %s", path);

	INT64_T capacity = hdfs_services->get_capacity(fs);
	INT64_T used = hdfs_services->get_used(fs);
	INT64_T blocksize = hdfs_services->get_default_block_size(fs);

	if(capacity == -1 || used == -1 || blocksize == -1)
		return (errno = EIO, -1);

	buf->f_type = 0;	/* FIXME */
	buf->f_bsize = blocksize;
	buf->f_blocks = capacity / blocksize;
	buf->f_bavail = buf->f_bfree = used / blocksize;
	buf->f_files = buf->f_ffree = 0;

	return 0;
}

INT64_T chirp_hdfs_fstatfs(int fd, struct chirp_statfs * buf)
{
	debug(D_HDFS, "fstatfs %d", fd);

	return chirp_hdfs_statfs("/", buf);
}

INT64_T chirp_hdfs_access(const char *path, INT64_T mode)
{
	/* W_OK is ok to delete, not to write, but we can't distinguish intent */
	/* Chirp ACL will check that we can access the file the way we want, so
	   we just do a redundant "exists" check */
	path = FIXPATH(path);
	debug(D_HDFS, "access %s %ld", path, (long) mode);
	return hdfs_services->exists(fs, path);
}

INT64_T chirp_hdfs_chmod(const char *path, INT64_T mode)
{
	// The owner may only add or remove the execute bit,
	// because permissions are handled through the ACL model.
	path = FIXPATH(path);
	debug(D_HDFS, "chmod %s %ld", path, (long) mode);
	mode = 0600 | (mode & 0100);
	return hdfs_services->chmod(fs, path, mode);
}

INT64_T chirp_hdfs_chown(const char *path, INT64_T uid, INT64_T gid)
{
	// Changing file ownership is silently ignored,
	// because permissions are handled through the ACL model.
	path = FIXPATH(path);
	debug(D_HDFS, "chown (ignored) %s %ld %ld", path, (long) uid, (long) gid);
	return 0;
}

INT64_T chirp_hdfs_lchown(const char *path, INT64_T uid, INT64_T gid)
{
	// Changing file ownership is silently ignored,
	// because permissions are handled through the ACL model.
	path = FIXPATH(path);
	debug(D_HDFS, "lchown (ignored) %s %ld %ld", path, (long) uid, (long) gid);
	return 0;
}

INT64_T chirp_hdfs_truncate(const char *path, INT64_T length)
{
	path = FIXPATH(path);
	debug(D_HDFS, "truncate %s %ld", path, (long) length);
	/* simulate truncate */
	INT64_T size = length;
	char *buffer = read_buffer(path, 0, &size);
	if(buffer == NULL)
		return -1;
	INT64_T fd = write_buffer(path, buffer, size);
	free(open_files[fd].name);
	free(buffer);
	open_files[fd].name = NULL;
	return 0;
}

INT64_T chirp_hdfs_utime(const char *path, time_t actime, time_t modtime)
{
	path = FIXPATH(path);
	debug(D_HDFS, "utime %s %ld %ld", path, (long) actime, (long) modtime);
	return hdfs_services->utime(fs, path, modtime, actime);
}

INT64_T chirp_hdfs_md5(const char *path, unsigned char digest[16])
{
	int fd;
	INT64_T result;
	struct chirp_stat info;

	path = FIXPATH(path);

	debug(D_HDFS, "md5sum %s", path);

	result = chirp_hdfs_stat(path, &info);
	if(result < 0)
		return result;

	if(S_ISDIR(info.cst_mode)) {
		errno = EISDIR;
		return -1;
	}

	fd = chirp_hdfs_open(path, O_RDONLY, 0);
	if(fd >= 0) {
		char buffer[65536];
		//INT64_T total=0;
		INT64_T ractual;
		INT64_T length = info.cst_size;
		md5_context_t ctx;

		md5_init(&ctx);

		while(length > 0) {
			INT64_T chunk = MIN(sizeof(buffer), length);

			ractual = hdfs_services->read(fs, open_files[fd].file, buffer, chunk);
			if(ractual <= 0)
				break;

			md5_update(&ctx, (unsigned char *) buffer, ractual);

			//total += ractual;
			length -= ractual;
		}
		result = 0;
		chirp_hdfs_close(fd);
		md5_final(digest, &ctx);
	} else {
		result = -1;
	}

	return result;
}

INT64_T chirp_hdfs_chdir(const char *path)
{
	debug(D_HDFS, "chdir %s", path);
	return hdfs_services->chdir(fs, path);
}

struct chirp_filesystem chirp_hdfs_fs = {
	chirp_hdfs_init,
	chirp_hdfs_destroy,

	chirp_hdfs_open,
	chirp_hdfs_close,
	chirp_hdfs_pread,
	chirp_hdfs_pwrite,
	chirp_hdfs_sread,
	chirp_hdfs_swrite,
	chirp_hdfs_fstat,
	chirp_hdfs_fstatfs,
	chirp_hdfs_fchown,
	chirp_hdfs_fchmod,
	chirp_hdfs_ftruncate,
	chirp_hdfs_fsync,

	chirp_hdfs_opendir,
	chirp_hdfs_readdir,
	chirp_hdfs_closedir,

	chirp_hdfs_getfile,
	chirp_hdfs_putfile,

	chirp_hdfs_mkfifo,
	chirp_hdfs_unlink,
	chirp_hdfs_rename,
	chirp_hdfs_link,
	chirp_hdfs_symlink,
	chirp_hdfs_readlink,
	chirp_hdfs_chdir,
	chirp_hdfs_mkdir,
	chirp_hdfs_rmdir,
	chirp_hdfs_stat,
	chirp_hdfs_lstat,
	chirp_hdfs_statfs,
	chirp_hdfs_access,
	chirp_hdfs_chmod,
	chirp_hdfs_chown,
	chirp_hdfs_lchown,
	chirp_hdfs_truncate,
	chirp_hdfs_utime,
	chirp_hdfs_md5,

	chirp_hdfs_file_size,
	chirp_hdfs_fd_size,
};
