/*
Copyright (C) 2018- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifdef HAS_EXT2FS

#include <assert.h>
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <string.h>
#include <stdio.h>
#include <errno.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <dirent.h>

#if defined(HAS_ATTR_XATTR_H)
#include <attr/xattr.h>
#elif defined(HAS_SYS_XATTR_H)
#include <sys/xattr.h>
#endif
#ifndef ENOATTR
#define ENOATTR  EINVAL
#endif

#include <ext2fs/ext2fs.h>
#include <com_err.h>

#include "pfs_service.h"

extern "C" {
	#include "debug.h"
	#include "xxmalloc.h"
}

class pfs_file_ext : public pfs_file {
public:
	pfs_file_ext(pfs_name *name) : pfs_file(name) {
		last_offset = 0;
	}

	virtual int canbenative(char *path, size_t len) {
		return 0;
	}

	virtual int close() {
		return -1;
	}

	virtual pfs_ssize_t read(void *data, pfs_size_t length, pfs_off_t offset) {
		return -1;
	}

	virtual pfs_ssize_t write(const void *data, pfs_size_t length, pfs_off_t offset) {
		return -1;
	}

	virtual int fstat(struct pfs_stat *buf) {
		return -1;
	}

	virtual int fstatfs(struct pfs_statfs *buf) {
		return -1;
	}

	virtual int ftruncate(pfs_size_t length) {
		return -1;
	}

	virtual int fsync() {
		return -1;
	}

	virtual int fcntl(int cmd, void *arg) {
		return -1;
	}

	virtual int fchmod(mode_t mode) {
		return -1;
	}

	virtual int fchown(uid_t uid, gid_t gid) {
		return -1;
	}

#if defined(HAS_SYS_XATTR_H) || defined(HAS_ATTR_XATTR_H)
	virtual ssize_t fgetxattr(const char *name, void *data, size_t size) {
		return -1;
	}

	virtual ssize_t flistxattr(char *list, size_t size) {
		return -1;
	}

	virtual int fsetxattr(const char *name, const void *data, size_t size, int flags) {
		return -1;
	}

	virtual int fremovexattr(const char *name) {
		return -1;
	}

#endif

	virtual int flock(int op) {
		return -1;
	}

	virtual void *mmap() {
		return NULL;
	}

	virtual pfs_ssize_t get_size() {
		return -1;
	}

	virtual int get_real_fd() {
		return -1;
	}

	virtual int get_local_name(char *n) {
		strcpy(n, name.rest);
		return 0;
	}

	virtual int is_seekable() {
		return 1;
	}
};

class pfs_service_ext: public pfs_service {
private:
	ext2_filsys fs;
	char *path;

public:
	pfs_service_ext(ext2_filsys handle, const char *img) {
		fs = handle;
		path = xxstrdup(img);
	}

	~pfs_service_ext() {
		debug(D_EXT, "closing ext fs %s", path);
		errcode_t rc = ext2fs_close(fs);
		if (rc != 0) {
			debug(D_NOTICE, "failed to close ext filesystem at %s: %s", path, error_message(rc));
		}
		free(path);
	}

	virtual pfs_file *open(pfs_name *name, int flags, mode_t mode) {
		assert(name);
		debug(D_EXT, "open %s %d %d", name->rest, flags, mode);
		return NULL;
	}

	virtual pfs_dir *getdir(pfs_name *name) {
		assert(name);
		debug(D_EXT, "getdir %s", name->rest);
		return NULL;
	}

	virtual int stat(pfs_name *name, struct pfs_stat *buf) {
		assert(name);
		assert(buf);
		debug(D_EXT, "stat %s", name->rest);
		return -1;
	}

	virtual int statfs(pfs_name *name, struct pfs_statfs *buf) {
		assert(name);
		assert(buf);
		debug(D_EXT, "statfs %s", name->rest);
		return -1;
	}

	virtual int lstat(pfs_name *name, struct pfs_stat *buf) {
		assert(name);
		assert(buf);
		debug(D_EXT, "lstat %s", name->rest);
		return -1;
	}

	virtual int access(pfs_name *name, mode_t mode) {
		assert(name);
		debug(D_EXT, "access %s %d", name->rest, mode);
		return -1;
	}

	virtual int readlink(pfs_name *name, char *buf, pfs_size_t bufsiz) {
		assert(name);
		assert(buf);
		debug(D_EXT, "readlink %s", name->rest);
		return -1;
	}

	virtual ssize_t getxattr(pfs_name *name, const char *attrname, void *data, size_t size) {
		assert(name);
		assert(attrname);
		assert(data);
		debug(D_EXT, "getxattr %s %s", name->rest, attrname);
		return -1;
	}

	virtual ssize_t lgetxattr(pfs_name *name, const char *attrname, void *data, size_t size) {
		assert(name);
		assert(attrname);
		assert(data);
		debug(D_EXT, "lgetxattr %s %s", name->rest, attrname);
		return -1;
	}

	virtual ssize_t listxattr(pfs_name *name, char *list, size_t size) {
		assert(name);
		assert(list);
		debug(D_EXT, "listxattr %s", name->rest);
		return -1;
	}

	virtual ssize_t llistxattr(pfs_name *name, char *list, size_t size) {
		assert(name);
		assert(list);
		debug(D_EXT, "llistxattr %s", name->rest);
		return -1;
	}

	virtual int is_seekable() {
		return 1;
	}

	virtual int is_local() {
		return 1;
	}
};

pfs_service *pfs_service_ext_init(const char *image) {
	assert(image);

	initialize_ext2_error_table();
	debug(D_EXT, "loading ext image %s", image);

	ext2_filsys fs;
	errcode_t rc = ext2fs_open(image, 0, 0, 0, unix_io_manager, &fs);
	if (rc != 0) {
		fatal("failed to load ext image %s: %s", image, error_message(rc));
	}

	return new pfs_service_ext(fs, image);
}

#else

pfs_service *pfs_service_ext_init(const char *image) {
	fatal("parrot was not configured with ext2fs support");
}

#endif

/* vim: set noexpandtab tabstop=4: */
