/*
Copyright (C) 2018- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifdef HAS_EXT2FS

#include "pfs_service.h"

extern "C" {
#include "debug.h"
}

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
public:
	pfs_service_ext(const char *image) {
	}

	virtual pfs_file *open(pfs_name *name, int flags, mode_t mode) {
		return NULL;
	}

	virtual pfs_dir *getdir(pfs_name *name) {
		return NULL;
	}

	virtual int stat(pfs_name *name, struct pfs_stat *buf) {
		return -1;
	}

	virtual int statfs(pfs_name *name, struct pfs_statfs *buf) {
		return -1;
	}

	virtual int lstat(pfs_name *name, struct pfs_stat *buf) {
		return -1;
	}

	virtual int access(pfs_name *name, mode_t mode) {
		return -1;
	}

	virtual int chmod(pfs_name *name, mode_t mode) {
		return -1;
	}

	virtual int chown(pfs_name *name, uid_t uid, gid_t gid) {
		return -1;
	}

	virtual int lchown(pfs_name *name, uid_t uid, gid_t gid) {
		return -1;
	}

	virtual int truncate(pfs_name *name, pfs_off_t length) {
		return -1;
	}

	virtual int utime(pfs_name *name, struct utimbuf *buf) {
		return -1;
	}

	virtual int utimens(pfs_name *name, const struct timespec times[2]) {
		return -1;
	}

	virtual int lutimens(pfs_name *name, const struct timespec times[2]) {
		return -1;
	}

	virtual int unlink(pfs_name *name) {
		return -1;
	}

	virtual int rename(pfs_name *oldname, pfs_name *newname) {
		return -1;
	}

	virtual ssize_t getxattr(pfs_name *name, const char *attrname, void *data, size_t size) {
		return -1;
	}

	virtual ssize_t lgetxattr(pfs_name *name, const char *attrname, void *data, size_t size) {
		return -1;
	}

	virtual ssize_t listxattr(pfs_name *name, char *list, size_t size) {
		return -1;
	}

	virtual ssize_t llistxattr(pfs_name *name, char *list, size_t size) {
		return -1;
	}

	virtual int setxattr(pfs_name *name, const char *attrname, const void *data, size_t size, int flags) {
		return -1;
	}

	virtual int lsetxattr(pfs_name *name, const char *attrname, const void *data, size_t size, int flags) {
		return -1;
	}

	virtual int removexattr(pfs_name *name, const char *attrname) {
		return -1;
	}

	virtual int lremovexattr(pfs_name *name, const char *attrname) {
		return -1;
	}

	virtual int chdir(pfs_name *name, char *newpath) {
		return -1;
	}

	virtual int link(pfs_name *oldname, pfs_name *newname) {
		return -1;
	}

	virtual int symlink(const char *linkname, pfs_name *newname) {
		return -1;
	}

	virtual int readlink(pfs_name *name, char *buf, pfs_size_t size) {
		return -1;
	}

	virtual int mknod(pfs_name *name, mode_t mode, dev_t dev) {
		return -1;
	}

	virtual int mkdir(pfs_name *name, mode_t mode) {
		return -1;
	}

	virtual int rmdir(pfs_name *name) {
		return -1;
	}

	virtual int whoami(pfs_name *name, char *buf, int size) {
		return -1;
	}

	virtual pfs_location* locate(pfs_name *name) {
		return NULL;
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
	return new pfs_service_ext(image);
}

#endif

/* vim: set noexpandtab tabstop=4: */
