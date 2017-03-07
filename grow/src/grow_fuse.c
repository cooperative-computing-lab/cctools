/*
 * Copyright (C) 2017- The University of Notre Dame
 * This software is distributed under the GNU General Public License.
 * See the file COPYING for details.
 */

#define FUSE_USE_VERSION 26

#include <fuse.h>
#include <stdio.h>
#include <stddef.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <limits.h>
#include <sys/stat.h>
#include <dirent.h>
#include <errno.h>
#include <sys/time.h>

#include "grow.h"
#include "debug.h"
#include "stats.h"
#include "macros.h"

#ifndef O_PATH
#define O_PATH O_RDONLY
#endif

#define OPTION(t, p) \
	{ t, offsetof(struct options, p), 1 }

#define GETCONTEXT \
	struct fuse_context *ctx = fuse_get_context(); \
	assert(ctx); \
	assert(ctx->private_data);

#define GETROOT \
	GETCONTEXT \
	struct fuse_root *root = ctx->private_data;

static struct options {
	int show_help;
	const char *basedir;
	const char *debug;
} options;

static const struct fuse_opt option_spec[] = {
	OPTION("--basedir %s", basedir),
	OPTION("--cctools-debug %s", debug),
	OPTION("-h", show_help),
	OPTION("--help", show_help),
	FUSE_OPT_END
};

struct fuse_root {
	struct grow_dirent *metadata;
	int fd;
};

static int deny_write(const char *path) {
	stats_inc("grow.fuse.deny_write", 1);
	GETROOT
	struct grow_dirent *e = grow_dirent_lookup_recursive(path, root->metadata, 1);
	if (!e) return -errno;
	return -EROFS;
}

static int deny_create(const char *path) {
	stats_inc("grow.fuse.deny_create", 1);
	GETROOT
	struct grow_dirent *e = grow_dirent_lookup_recursive(basename(path), root->metadata, 1);
	if (!e) return -errno;
	if (!S_ISDIR(e->mode)) return -ENOTDIR;
	return -EROFS;
}

static void *grow_fuse_init(struct fuse_conn_info *conn) {
	GETCONTEXT
	return ctx->private_data;
}

static int grow_fuse_getattr(const char *path, struct stat *stbuf) {
	stats_inc("grow.fuse.getattr", 1);
	GETROOT
	struct grow_dirent *e = grow_dirent_lookup_recursive(path, root->metadata, 0);
	if (!e) return -errno;
	grow_dirent_to_stat(e, stbuf);
	return 0;
}

static int grow_fuse_access(const char *path, int mask) {
	stats_inc("grow.fuse.access", 1);
	GETROOT
	struct grow_dirent *e = grow_dirent_lookup_recursive(path, root->metadata, 1);
	if (!e) return -errno;
	if (mask&W_OK) return -EROFS;
	return 0;
}

static int grow_fuse_readlink(const char *path, char *buf, size_t size) {
	stats_inc("grow.fuse.readlink", 1);
	GETROOT
	struct grow_dirent *e = grow_dirent_lookup_recursive(path, root->metadata, 1);
	if (!e) return -errno;
	if (!S_ISLNK(e->mode)) return -EINVAL;
	strncpy(buf, e->linkname, size);
	return MIN(size, strlen(e->linkname));
}

static int grow_fuse_opendir(const char *path, struct fuse_file_info *fi) {
	stats_inc("grow.fuse.opendir", 1);
	GETROOT
	struct grow_dirent *e = grow_dirent_lookup_recursive(path, root->metadata, 1);
	if (!e) return -errno;
	if (!S_ISDIR(e->mode)) return -ENOTDIR;
	return 0;
}

static int grow_fuse_readdir(const char *path, void *buf, fuse_fill_dir_t filler, off_t offset, struct fuse_file_info *fi) {
	stats_inc("grow.fuse.readdir", 1);
	GETROOT
	struct grow_dirent *e = grow_dirent_lookup_recursive(path, root->metadata, 1);
	if (!e) return -errno;
	if (!S_ISDIR(e->mode)) return -ENOTDIR;
	for (struct grow_dirent *c = e->children; c; c = c->next) {
		if (filler(buf, c->name, NULL, 0)) return -ENOMEM;
	}
	return 0;
}

static int grow_fuse_mknod(const char *path, mode_t mode, dev_t rdev) {
	stats_inc("grow.fuse.mknod", 1);
	return deny_create(path);
}

static int grow_fuse_mkdir(const char *path, mode_t mode) {
	stats_inc("grow.fuse.mkdir", 1);
	return deny_create(path);
}

static int grow_fuse_unlink(const char *path) {
	stats_inc("grow.fuse.unlink", 1);
	return deny_write(path);
}

static int grow_fuse_rmdir(const char *path) {
	stats_inc("grow.fuse.rmdir", 1);
	return deny_write(path);
}

static int grow_fuse_symlink(const char *from, const char *to) {
	stats_inc("grow.fuse.symlink", 1);
	return deny_create(to);
}

static int grow_fuse_rename(const char *from, const char *to) {
	stats_inc("grow.fuse.rename", 1);
	GETROOT
	// this isn't exactly correct, but rename has an annoying number of cases
	struct grow_dirent *from_ent = grow_dirent_lookup_recursive(from, root->metadata, 1);
	if (!from_ent) return -errno;
	struct grow_dirent *to_ent = grow_dirent_lookup_recursive(to, root->metadata, 0);
	if (to_ent) {
		// should check if to is empty for -ENOTEMPTY
		if (S_ISDIR(from_ent->mode) && S_ISDIR(to_ent->mode)) return -EROFS;
		if (S_ISDIR(to_ent->mode)) return -EISDIR;
		if (S_ISDIR(from_ent->mode)) return -ENOTDIR;
		return -EROFS;

	}
	struct grow_dirent *parent_ent = grow_dirent_lookup_recursive(basename(to), root->metadata, 1);
	if (parent_ent && S_ISDIR(parent_ent->mode)) {
		return -EROFS;
	} else if (parent_ent) {
		return -ENOTDIR;
	} else {
		return -errno;
	}
}

static int grow_fuse_link(const char *from, const char *to) {
	stats_inc("grow.fuse.link", 1);
	int from_err = deny_write(from);
	int to_err = deny_create(to);
	return from_err == EROFS ? to_err : from_err;
}

static int grow_fuse_chmod(const char *path, mode_t mode) {
	stats_inc("grow.fuse.chmod", 1);
	return deny_write(path);
}

static int grow_fuse_chown(const char *path, uid_t uid, gid_t gid) {
	stats_inc("grow.fuse.chown", 1);
	return deny_write(path);
}

static int grow_fuse_truncate(const char *path, off_t size) {
	stats_inc("grow.fuse.truncate", 1);
	return deny_write(path);
}

#ifndef HAS_UTIMENSAT
static int grow_fuse_utimens(const char *path, const struct timespec ts[2]) {
	stats_inc("grow.fuse.utimens", 1);
	// probably shouldn't follow symlinks
	return deny_write(path);
}
#endif

static int grow_fuse_open(const char *path, struct fuse_file_info *fi) {
	stats_inc("grow.fuse.open", 1);
	GETROOT
	struct grow_dirent *e = grow_dirent_lookup_recursive(path, root->metadata, 1);
	if (!e && errno==ENOENT && fi->flags&O_CREAT) return -EROFS;
	if (!e) return -errno;
	if (fi->flags&O_WRONLY || fi->flags&O_RDWR) return -EROFS;
	while (path[0] == '/') ++path;
	int rc = openat(root->fd, path, fi->flags);
	if (rc < 0) return -errno;
	fi->fh = rc;
	return 0;
}

static int grow_fuse_read(const char *path, char *buf, size_t size, off_t offset, struct fuse_file_info *fi) {
	stats_inc("grow.fuse.read", 1);
	size_t bytes_read = 0;
	while (bytes_read < size) {
		int rc = pread(fi->fh, buf, size - bytes_read, offset);
		if (rc < 0 && errno != EINTR) {
			return -errno;
		} else if (rc > 0) {
			bytes_read += rc;
			offset += rc;
		} else if (rc == 0) {
			break;
		}
	}
	return bytes_read;
}

static int grow_fuse_write(const char *path, const char *buf, size_t size, off_t offset, struct fuse_file_info *fi) {
	stats_inc("grow.fuse.write", 1);
	return deny_write(path);
}

static int grow_fuse_release(const char *path, struct fuse_file_info *fi) {
	stats_inc("grow.fuse.release", 1);
	return close(fi->fh);
}

static int grow_fuse_fsync(const char *path, int isdatasync, struct fuse_file_info *fi) {
	stats_inc("grow.fuse.fsync", 1);
	return 0;
}

struct fuse_operations grow_fuse_ops = {
	.init           = grow_fuse_init,
	.getattr	= grow_fuse_getattr,
	.access		= grow_fuse_access,
	.readlink	= grow_fuse_readlink,
	.opendir	= grow_fuse_opendir,
	.readdir	= grow_fuse_readdir,
	.mknod		= grow_fuse_mknod,
	.mkdir		= grow_fuse_mkdir,
	.symlink	= grow_fuse_symlink,
	.unlink		= grow_fuse_unlink,
	.rmdir		= grow_fuse_rmdir,
	.rename		= grow_fuse_rename,
	.link		= grow_fuse_link,
	.chmod		= grow_fuse_chmod,
	.chown		= grow_fuse_chown,
	.truncate	= grow_fuse_truncate,
#ifndef HAS_UTIMENSAT
	.utimens	= grow_fuse_utimens,
#endif
	.open		= grow_fuse_open,
	.read		= grow_fuse_read,
	.write		= grow_fuse_write,
	.release	= grow_fuse_release,
	.fsync		= grow_fuse_fsync,
};

static void show_help(const char *argv) {
	fprintf(stderr, "usage: %s --basedir SRCDIR MOUNTPOINT\n", argv);
	fprintf(stderr, "options:\n");
	fprintf(stderr, "-h, --help\n");
	fprintf(stderr, "    --cctools-debug LEVEL\n");
	fprintf(stderr, "    --basedir SRCDIR\n");
}

int main(int argc, char *argv[]) {
	struct fuse_root root;

	umask(0);

	debug(D_GROW, "initializing FUSE plugin");
	struct fuse_args args = FUSE_ARGS_INIT(argc, argv);

	memset(&options, 0, sizeof(options));
	if (fuse_opt_parse(&args, &options, option_spec, NULL) == -1) {
		exit(EXIT_FAILURE);
	}

	if (options.debug && !debug_flags_set(options.debug)) {
		show_help(argv[0]);
		exit(EXIT_FAILURE);
	}
	if (options.show_help) {
		show_help(argv[0]);
		assert(fuse_opt_add_arg(&args, "--help") == 0);
		args.argv[0] = (char*) "";
	} else if (!options.basedir) {
		show_help(argv[0]);
		exit(EXIT_FAILURE);
	} else {
		root.fd = open(options.basedir, O_PATH);
		if (root.fd == -1) {
			fatal("failed to open base dir: %s", strerror(errno));
		}
		int index = openat(root.fd, ".growfsdir", O_RDONLY);
		if (index == -1) {
			fatal("failed to open GROW-FS index: %s", strerror(errno));
		}
		FILE *metadata = fdopen(index, "r");
		if (!metadata) {
			fatal("failed to get index FILE: %s", strerror(errno));
		}
		root.metadata = grow_dirent_create_from_file(metadata, NULL);
		if (!root.metadata) {
			fatal("failed to load GROW-FS index");
		}
		fclose(metadata);
	}

	return fuse_main(args.argc, args.argv, &grow_fuse_ops, &root);
}
