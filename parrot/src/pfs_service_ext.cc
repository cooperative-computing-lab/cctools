/*
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include <assert.h>
#include <stdlib.h>
#include <stdbool.h>
#include <unistd.h>
#include <fcntl.h>
#include <string.h>
#include <stdio.h>
#include <errno.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <dirent.h>

#include "pfs_service.h"

extern "C" {
	#include "debug.h"
	#include "macros.h"
	#include "xxmalloc.h"
}

#ifdef HAS_EXT2FS

#include <ext2fs/ext2fs.h>
#include <et/com_err.h>

#define LOOKUP_INODE(name, inode, follow, fail) { \
	errcode_t rc = lookup_inode(fs, mountpoint, EXT2_ROOT_INO, name, inode, follow ? 0 : -1); \
	if (rc == 0) { \
		debug(D_EXT, "lookup %s -> inode %d", name, *inode); \
	} else { \
		debug(D_EXT, "lookup %s failed: %s", name, error_message(rc)); \
		errno = fix_errno(rc); \
		return fail; \
	} \
} while (false)

#define READ_INODE(inode, buf, fail) { \
	errcode_t rc = ext2fs_read_inode(fs, inode, buf); \
	if (rc == 0) { \
		debug(D_EXT, "read inode %d", inode); \
	} else { \
		debug(D_EXT, "read inode %d failed: %s", inode, error_message(rc)); \
		return fail; \
	} \
} while (false)

#define OPEN_FILE(inode, file, fail) { \
	errcode_t rc = ext2fs_file_open(fs, inode, 0, file); \
	if (rc == 0) { \
		debug(D_EXT, "open inode %d -> file %p", inode, *file); \
	} else { \
		debug(D_EXT, "open inode %d failed: %s", inode, error_message(rc)); \
		return fail; \
	} \
} while (false)

#define CLOSE_FILE(file, fail) { \
	errcode_t rc = ext2fs_file_close(file); \
	if (rc == 0) { \
		debug(D_EXT, "close file %p", file); \
	} else { \
		debug(D_EXT, "close file %p failed: %s", file, error_message(rc)); \
		return fail; \
	} \
} while (false)

static errcode_t lookup_inode(ext2_filsys fs, const char *mountpoint, ext2_ino_t cwd, const char *path, ext2_ino_t *inode, int depth);
static errcode_t follow_link(ext2_filsys fs, const char *mountpoint, ext2_ino_t cwd, ext2_ino_t inode, ext2_ino_t *out, int depth);

static int fix_errno(errcode_t rc) {
	if (((rc>>8) & ((1<<24) - 1)) == 0) {
		// literal errno value
		return rc;
	}
	// very rough translation
	switch (rc) {
		case 0: return 0;
		case EXT2_ET_RO_FILSYS: return EROFS;
		case EXT2_ET_SYMLINK_LOOP: return ELOOP;
		case EXT2_ET_NO_MEMORY: return ENOMEM;
		case EXT2_ET_UNSUPP_FEATURE: return ENOSYS;
		case EXT2_ET_RO_UNSUPP_FEATURE: return ENOSYS;
		case EXT2_ET_INVALID_ARGUMENT: return EINVAL;
		case EXT2_ET_NO_DIRECTORY: return ENOENT;
		case EXT2_ET_TOO_MANY_REFS: return EMLINK;
		case EXT2_ET_FILE_NOT_FOUND: return ENOENT;
		case EXT2_ET_FILE_RO: return EROFS;
		case EXT2_ET_DIR_EXISTS: return EEXIST;
		case EXT2_ET_UNIMPLEMENTED: return ENOSYS;
		case EXT2_ET_FILE_TOO_BIG: return EFBIG;
		default: return EINVAL;
	}
}

static void inode2stat(ext2_ino_t i, struct ext2_inode *b, struct pfs_stat *p) {
	assert(i);
	assert(p);

	memset(p, 0, sizeof(*p));
	p->st_dev = (dev_t) -1;
	p->st_ino = i;
	p->st_mode = b->i_mode;
	p->st_uid = b->i_uid;
	p->st_gid = b->i_gid;
	p->st_size = b->i_size;
	p->st_nlink = b->i_links_count;
	p->st_blksize = 65336;
	p->st_blocks = b->i_blocks;
	p->st_atime = b->i_atime;
	p->st_ctime = b->i_ctime;
	p->st_mtime = b->i_mtime;
}

static int append_dirents(struct ext2_dir_entry *dirent, int offset, int blocksize, char *buf, void *p) {
	char name[PATH_MAX] = {0};
	pfs_dir *d = (pfs_dir *) p;

	assert(dirent);
	assert(d);

	size_t name_len = dirent->name_len & ((1<<8) - 1);
	strncpy(name, dirent->name, MIN(name_len, sizeof(name) - 1));
	d->append(name);

	return 0;
}

static errcode_t follow_link(ext2_filsys fs, const char *mountpoint, ext2_ino_t cwd, ext2_ino_t inode, ext2_ino_t *out, int depth) {
	struct ext2_inode buf;
	ext2_file_t file;
	unsigned size;
	char target[PATH_MAX] = {0};

	assert(out);
	// debug(D_EXT, "follow %d in %d", inode, cwd);
	if (depth > 64) return EXT2_ET_SYMLINK_LOOP;

	errcode_t rc = ext2fs_read_inode(fs, inode, &buf);
	if (rc) return rc;
	if (!S_ISLNK(buf.i_mode)) {
		// debug(D_EXT, "not a symlink");
		*out = inode;
		return 0;
	}

	rc = ext2fs_file_open(fs, inode, 0, &file);
	if (rc) return rc;
	rc = ext2fs_file_read(file, target, sizeof(target) - 1, &size);
	ext2fs_file_close(file);
	if (rc == EXT2_ET_SHORT_READ) {
		// debug(D_EXT, "short read, inline link");
		size = MIN(buf.i_size, sizeof(target) - 1);
		memcpy(target, buf.i_block, size);
	} else if (rc) {
		return rc;
	}

	if (target[0] == '/') {
		cwd = EXT2_ROOT_INO;
		const char *m = mountpoint;
		char *current = &target[0];
		while (*m != '\0') {
			if (*m != *current) goto BADLINK;
			while (*(++m) == '/');
			while (*(++current) == '/');
			if (*m == '\0' && *current != '\0' && *(current - 1) != '/') goto BADLINK;
		}
		memmove(target, current, strlen(current) + 1);
		// debug(D_EXT, "stripped to %s", target);
	}

	return lookup_inode(fs, mountpoint, cwd, target, out, depth + 1);
BADLINK:
	debug(D_EXT, "symlinks cannot point out of the image");
	return EXT2_ET_FILE_NOT_FOUND;
}

static errcode_t lookup_inode(ext2_filsys fs, const char *mountpoint, ext2_ino_t cwd, const char *path, ext2_ino_t *inode, int depth) {
	errcode_t rc;

	assert(path);
	assert(inode);
	debug(D_EXT, "lookup %s in %d", path, cwd);

	while (*path == '/') {
		cwd = EXT2_ROOT_INO;
		++path;
	}

	char tmp[PATH_MAX] = {0};
	strncpy(tmp, path, sizeof(tmp) - 1);
	char *current = &tmp[0];
	char *last = strrchr(tmp, '/');

	while (last && current < last) {
		char *split = strchr(current, '/');
		*split = '\0';
		// debug(D_EXT, "component %s", current);

		ext2_ino_t dir;
		rc = ext2fs_lookup(fs, cwd, current, strlen(current), NULL, &dir);
		if (rc) return rc;
		// debug(D_EXT, "\t-> inode %d", dir);

		rc = follow_link(fs, mountpoint, cwd, dir, &dir, depth);
		if (rc) return rc;
		// debug(D_EXT, "\t-> inode %d", dir);

		cwd = dir;
		current = split + 1;
	}

	if (*current == '\0') {
		*inode = cwd;
		return 0;
	}

	// debug(D_EXT, "leaf %s", current);
	ext2_ino_t out;
	rc = ext2fs_lookup(fs, cwd, current, strlen(current), NULL, &out);
	if (rc) return rc;
	// debug(D_EXT, "\t-> inode %d", out);
	if (depth >= 0) rc = follow_link(fs, mountpoint, cwd, out, &out, depth);
	if (rc) return rc;
	// debug(D_EXT, "\t-> inode %d", out);
	*inode = out;

	return 0;
}

class pfs_file_ext : public pfs_file {
private:
	ext2_ino_t inode;
	ext2_filsys fs;

public:
	pfs_file_ext(pfs_name *name, ext2_ino_t inode, ext2_filsys fs)
			: pfs_file(name)
			, inode(inode)
			, fs(fs) {
		assert(name);
		debug(D_EXT, "open %s (inode %d) -> %p", name->rest, inode, this);
	}

	virtual int canbenative(char *path, size_t len) {
		return 0;
	}

	virtual int close() {
		debug(D_EXT, "close %p", this);
		// not holding any resources open, so noop here
		return 0;
	}

	virtual pfs_ssize_t read(void *data, pfs_size_t length, pfs_off_t offset) {
		ext2_file_t file;
		unsigned size;

		assert(data);

		debug(D_EXT, "read %" PRIi64 "B from %p at %" PRIi64, length, this, offset);
		OPEN_FILE(inode, &file, -1);
		errcode_t rc = ext2fs_file_llseek(file, offset, EXT2_SEEK_SET, NULL);
		if (rc != 0) {
			debug(D_EXT, "failed to seek to %" PRIi64 " in %p: %s", offset, this, error_message(rc));
			errno = fix_errno(rc);
			return -1;
		}

		rc = ext2fs_file_read(file, data, length, &size);
		if (rc == 0) {
			debug(D_EXT, "read %u/%" PRIi64 " bytes from file %p", size, length, file);
		} else {
			debug(D_EXT, "read file %p failed: %s", file, error_message(rc));
			CLOSE_FILE(file, -1);
			return -1;
		}

		CLOSE_FILE(file, -1);

		return size;

	}

	virtual int fstat(struct pfs_stat *buf) {
		struct ext2_inode inode_buf;

		assert(buf);

		debug(D_EXT, "fstat %p", this);
		READ_INODE(inode, &inode_buf, -1);
		inode2stat(inode, &inode_buf, buf);

		return 0;
	}

	virtual int fstatfs(struct pfs_statfs *buf) {
		assert(buf);

		debug(D_EXT, "fstatfs %p", this);
		pfs_service_emulate_statfs(buf);

		return 0;
	}

	virtual int flock(int op) {
		// noop
		return 0;
	}

	virtual	int fsync() {
		// noop
		return 0;
	}

	virtual pfs_ssize_t get_size() {
		struct ext2_inode inode_buf;

		debug(D_EXT, "fstat %p", this);
		READ_INODE(inode, &inode_buf, -1);

		return inode_buf.i_size;
	}
};

class pfs_service_ext: public pfs_service {
private:
	char *image;
	char *mountpoint;
	ext2_filsys fs;

public:
	pfs_service_ext(ext2_filsys fs, const char *img, const char *m)
			: fs(fs) {
		assert(image);
		assert(m);

		image = strdup(img);
		mountpoint = strdup(m);
	}

	~pfs_service_ext() {
		debug(D_EXT, "closing ext fs %s", image);
		free(image);
		free(mountpoint);
		errcode_t rc = ext2fs_close(fs);
		if (rc != 0) {
			debug(D_NOTICE, "failed to close ext filesystem at %s: %s", image, error_message(rc));
		}
	}

	virtual pfs_file *open(pfs_name *name, int flags, mode_t mode) {
		ext2_ino_t inode;
		struct ext2_inode inode_buf;

		assert(name);
		if (flags&(O_WRONLY|O_RDWR)) {
			errno = EROFS;
			return NULL;
		}

		debug(D_EXT, "open %s %d %d", name->rest, flags, mode);
		LOOKUP_INODE(name->rest, &inode, !(flags&O_NOFOLLOW), NULL);
		READ_INODE(inode, &inode_buf, NULL);
		if (S_ISLNK(inode_buf.i_mode)) {
			errno = ELOOP;
			return NULL;
		}

		pfs_file_ext *result = new pfs_file_ext(name, inode, fs);
		debug(D_EXT, "open %s in image %s -> %p", name->rest, image, result);
		return result;
	}

	virtual pfs_dir *getdir(pfs_name *name) {
		ext2_ino_t inode;
		struct ext2_inode inode_buf;

		assert(name);

		debug(D_EXT, "getdir %s", name->rest);
		LOOKUP_INODE(name->rest, &inode, true, NULL);
		READ_INODE(inode, &inode_buf, NULL);
		if (!S_ISDIR(inode_buf.i_mode)) {
			errno = ENOTDIR;
			return NULL;
		}
		pfs_dir *result = new pfs_dir(name);
		ext2fs_dir_iterate(fs, inode, 0, NULL, append_dirents, result);

		return result;
	}

	virtual int statfs(pfs_name *name, struct pfs_statfs *buf) {
		ext2_ino_t inode;

		assert(name);
		assert(buf);

		debug(D_EXT, "statfs %s", name->rest);
		LOOKUP_INODE(name->rest, &inode, true, -1);
		pfs_service_emulate_statfs(buf);

		return 0;
	}

	virtual int stat(pfs_name *name, struct pfs_stat *buf) {
		ext2_ino_t inode;
		struct ext2_inode inode_buf;

		assert(name);
		assert(buf);

		debug(D_EXT, "stat %s", name->rest);
		LOOKUP_INODE(name->rest, &inode, true, -1);
		READ_INODE(inode, &inode_buf, -1);
		inode2stat(inode, &inode_buf, buf);

		return 0;
	}

	virtual int lstat(pfs_name *name, struct pfs_stat *buf) {
		ext2_ino_t inode;
		struct ext2_inode inode_buf;

		assert(name);
		assert(buf);

		debug(D_EXT, "lstat %s", name->rest);
		LOOKUP_INODE(name->rest, &inode, false, -1);
		READ_INODE(inode, &inode_buf, -1);
		inode2stat(inode, &inode_buf, buf);

		return 0;
	}

	virtual int access(pfs_name *name, mode_t mode) {
		ext2_ino_t inode;

		assert(name);

		debug(D_EXT, "access %s %d", name->rest, mode);
		LOOKUP_INODE(name->rest, &inode, true, -1);

		// we don't do permission checks, so just go ahead
		return 0;
	}

	virtual int readlink(pfs_name *name, char *buf, pfs_size_t bufsiz) {
		ext2_ino_t inode;
		ext2_file_t file;
		struct ext2_inode inode_buf;
		unsigned size;

		assert(name);
		assert(buf);

		debug(D_EXT, "readlink %s", name->rest);
		LOOKUP_INODE(name->rest, &inode, false, -1);
		READ_INODE(inode, &inode_buf, -1);

		if (!S_ISLNK(inode_buf.i_mode)) {
			errno = EINVAL;
			return -1;
		}
		OPEN_FILE(inode, &file, -1);

		errcode_t rc = ext2fs_file_read(file, buf, bufsiz, &size);
		if (rc == 0) {
			debug(D_EXT, "read %u/%" PRIu64 " bytes from file %p", size, bufsiz, file);
		} else if (rc == EXT2_ET_SHORT_READ) {
			debug(D_EXT, "short read on %p, inline link", file);
			size = inode_buf.i_size;
			memcpy(buf, inode_buf.i_block, size);
		} else {
			debug(D_EXT, "read file %p failed: %s", file, error_message(rc));
			CLOSE_FILE(file, -1);
			return -1;
		}

		CLOSE_FILE(file, -1);

		return size;
	}

	virtual int is_seekable() {
		return 1;
	}
};

#endif

pfs_service *pfs_service_ext_init(const char *image, const char *mountpoint) {

#ifdef HAS_EXT2FS

	assert(image);

	initialize_ext2_error_table();
	debug(D_EXT, "loading ext image %s", image);

	ext2_filsys fs;
	errcode_t rc = ext2fs_open(image, 0, 0, 0, unix_io_manager, &fs);
	if (rc != 0) {
		if (rc == EXT2_ET_SHORT_READ) {
			fprintf(stderr, "got short read on %s, could indicate trying to open directory as ext image\n", image);
		}
		fatal("failed to load ext image %s: %s", image, error_message(rc));
	}

	return new pfs_service_ext(fs, image, mountpoint);

#else

	fatal("parrot was not configured with ext2fs support");
	// unreachable
	return NULL;

#endif

}

/* vim: set noexpandtab tabstop=4: */
