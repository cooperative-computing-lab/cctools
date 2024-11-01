/*
 * Copyright (C) 2022 The University of Notre Dame
 * This software is distributed under the GNU General Public License.
 * See the file COPYING for details.
 */

#define FUSE_USE_VERSION 31

#include <assert.h>
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <fuse3/fuse.h>
#include <limits.h>
#include <stddef.h>
#include <stdio.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <unistd.h>

#include "grow.h"
#include "debug.h"
#include "stats.h"
#include "macros.h"
#include "copy_stream.h"
#include "jx_pretty_print.h"

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

static FILE *stats_out = NULL;
static int cache_data = 0;

static struct options {
	int show_help;
	const char *basedir;
	const char *stats_file;
	int cache_data;
} options;

static const struct fuse_opt option_spec[] = {
	OPTION("--basedir %s", basedir),
	OPTION("--stats-file %s", stats_file),
	OPTION("--cache", cache_data),
	OPTION("-h", show_help),
	OPTION("--help", show_help),
	FUSE_OPT_END
};

struct fuse_root {
	struct grow_dirent *metadata;
	int fd;
	int cache;
};

static int cache_open(struct fuse_root *root, const char *path, int flags) {
	unsigned retries = 0;
	char cachepath[PATH_MAX];
	struct grow_dirent *e = grow_lookup(path, root->metadata, 1);
	if (!e) return -errno;
	if (flags&O_WRONLY || flags&O_RDWR) return -EROFS;

	if (!cache_data) {
		stats_inc("grow.fuse.direct_open", 1);
		while (path[0] == '/') ++path;
		return openat(root->fd, path, flags);
	}

retry:
	if (retries > 10) return -ELOOP;
	snprintf(cachepath, sizeof(cachepath), "%c%c/%s", e->checksum[0], e->checksum[1], &e->checksum[2]);
	int fd = openat(root->cache, cachepath, flags);
	if (fd < 0 && errno == ENOENT) {
		stats_inc("grow.fuse.cache.miss", 1);
		while (path[0] == '/') ++path;
		fd = openat(root->fd, path, flags);
		if (fd < 0) return -errno;
		char tmppath[] = "/tmp/.growcache.XXXXXX";
		int tmpfd = mkstemp(tmppath);
		if (tmpfd < 0) {
			int saved_errno = errno;
			close(fd);
			return -saved_errno;
		}
		if (copy_fd_to_fd(fd, tmpfd) < 0) {
			int saved_errno = errno;
			close(fd);
			close(tmpfd);
			unlink(tmppath);
			return -saved_errno;
		}
		close(fd);
		close(tmpfd);
		if (renameat(AT_FDCWD, tmppath, root->cache, cachepath) < 0) {
			int saved_errno = errno;
			unlink(tmppath);
			return -saved_errno;
		}
		stats_inc("grow.fuse.cache.commit", 1);
		++retries;
		goto retry;
	} else if (fd < 0) {
		stats_inc("grow.fuse.cache.error", 1);
		return -errno;
	} else {
		if (retries == 0) stats_inc("grow.fuse.cache.hit", 1);
		return fd;
	}
}

static int deny_write(const char *path) {
	stats_inc("grow.fuse.deny_write", 1);
	GETROOT
	struct grow_dirent *e = grow_lookup(path, root->metadata, 1);
	if (!e) return -errno;
	return -EROFS;
}

static int deny_create(const char *path) {
	stats_inc("grow.fuse.deny_create", 1);
	GETROOT
	struct grow_dirent *e = grow_lookup(basename(path), root->metadata, 1);
	if (!e) return -errno;
	if (!S_ISDIR(e->mode)) return -ENOTDIR;
	return -EROFS;
}

static void *grow_fuse_init(struct fuse_conn_info *conn, struct fuse_config *cfg) {
	GETCONTEXT
	return ctx->private_data;
}

static void grow_fuse_destroy(void *x) {
	if (stats_out) {
		jx_pretty_print_stream(stats_get(), stats_out);
		fprintf(stats_out, "\n");
		fclose(stats_out);
	}
}

static int grow_fuse_getattr(const char *path, struct stat *stbuf, struct fuse_file_info *fa) {
	stats_inc("grow.fuse.getattr", 1);
	GETROOT
	struct grow_dirent *e = grow_lookup(path, root->metadata, 0);
	if (!e) return -errno;
	grow_dirent_to_stat(e, stbuf);
	return 0;
}

static int grow_fuse_access(const char *path, int mask) {
	stats_inc("grow.fuse.access", 1);
	GETROOT
	struct grow_dirent *e = grow_lookup(path, root->metadata, 1);
	if (!e) return -errno;
	if (mask&W_OK) return -EROFS;
	return 0;
}

static int grow_fuse_readlink(const char *path, char *buf, size_t size) {
	stats_inc("grow.fuse.readlink", 1);
	GETROOT
	struct grow_dirent *e = grow_lookup(path, root->metadata, 0);
	if (!e) return -errno;
	if (!S_ISLNK(e->mode)) return -EINVAL;
	snprintf(buf, size, "%s", e->linkname);
	return 0;
}

static int grow_fuse_opendir(const char *path, struct fuse_file_info *fi) {
	stats_inc("grow.fuse.opendir", 1);
	GETROOT
	struct grow_dirent *e = grow_lookup(path, root->metadata, 1);
	if (!e) return -errno;
	if (!S_ISDIR(e->mode)) return -ENOTDIR;
	return 0;
}

static int grow_fuse_readdir(const char *path, void *buf, fuse_fill_dir_t filler, off_t offset, struct fuse_file_info *fi, enum fuse_readdir_flags flags) {
	stats_inc("grow.fuse.readdir", 1);
	GETROOT
	struct grow_dirent *e = grow_lookup(path, root->metadata, 1);
	if (!e) return -errno;
	if (!S_ISDIR(e->mode)) return -ENOTDIR;
	for (struct grow_dirent *c = e->children; c; c = c->next) {
		if (filler(buf, c->name, NULL, 0, 0)) return -ENOMEM;
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

static int grow_fuse_rename(const char *from, const char *to, unsigned int flags) {
	stats_inc("grow.fuse.rename", 1);
	GETROOT
	// this isn't exactly correct, but rename has an annoying number of cases
	struct grow_dirent *from_ent = grow_lookup(from, root->metadata, 1);
	if (!from_ent) return -errno;
	struct grow_dirent *to_ent = grow_lookup(to, root->metadata, 0);
	if (to_ent) {
		// should check if to is empty for -ENOTEMPTY
		if (S_ISDIR(from_ent->mode) && S_ISDIR(to_ent->mode)) return -EROFS;
		if (S_ISDIR(to_ent->mode)) return -EISDIR;
		if (S_ISDIR(from_ent->mode)) return -ENOTDIR;
		return -EROFS;

	}
	struct grow_dirent *parent_ent = grow_lookup(basename(to), root->metadata, 1);
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
	return from_err == -EROFS ? to_err : from_err;
}

static int grow_fuse_chmod(const char *path, mode_t mode, struct fuse_file_info *fa) {
	stats_inc("grow.fuse.chmod", 1);
	return deny_write(path);
}

static int grow_fuse_chown(const char *path, uid_t uid, gid_t gid, struct fuse_file_info *fa) {
	stats_inc("grow.fuse.chown", 1);
	return deny_write(path);
}

static int grow_fuse_truncate(const char *path, off_t size, struct fuse_file_info *fa) {
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

static int grow_fuse_create(const char *path, mode_t mode, struct fuse_file_info *fi) {
	stats_inc("grow.fuse.open", 1);
	GETROOT
	int fd = cache_open(root, path, fi->flags);
	if (fd == -ENOENT) return -EROFS;
	if (fd < 0) return -errno;
	fi->fh = fd;
	return 0;
}

static int grow_fuse_open(const char *path, struct fuse_file_info *fi) {
	stats_inc("grow.fuse.open", 1);
	GETROOT
	int fd = cache_open(root, path, fi->flags);
	if (fd < 0) return -errno;
	fi->fh = fd;
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
	.destroy	= grow_fuse_destroy,
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
	.create		= grow_fuse_create,
	.read		= grow_fuse_read,
	.write		= grow_fuse_write,
	.release	= grow_fuse_release,
	.fsync		= grow_fuse_fsync,
};

static void show_help(const char *argv) {
	fprintf(stderr, "usage: %s --basedir SRCDIR MOUNTPOINT\n", argv);
	fprintf(stderr, "options:\n");
	fprintf(stderr, "-h, --help\n");
	fprintf(stderr, "    --basedir SRCDIR\n");
}

int main(int argc, char *argv[]) {
	struct fuse_root root;

	umask(0);

	struct fuse_args args = FUSE_ARGS_INIT(argc, argv);

	memset(&options, 0, sizeof(options));
	if (fuse_opt_parse(&args, &options, option_spec, NULL) == -1) {
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
		assert(fuse_opt_add_arg(&args, "-ononempty") == 0);
		assert(fuse_opt_add_arg(&args, "-okernel_cache") == 0);

		cache_data = options.cache_data;
		if (options.stats_file) {
			stats_enable();
			stats_out = fopen(options.stats_file, "w");
			if (!stats_out) {
				fatal("could not open stats file %s: %s", options.stats_file, strerror(errno));
			}
		}

		if (cache_data) {
			char path[PATH_MAX];
			char *tmpdir = getenv("TMPDIR");
			if (!tmpdir) tmpdir = "/tmp";
			snprintf(path, sizeof(path), "%s/.growcache", tmpdir);
			if (mkdir(path, 0755) < 0 && errno != EEXIST) {
				fatal("failed to make cache dir %s: %s", path, strerror(errno));
			}
			root.cache = open(path, O_PATH|O_DIRECTORY);
			if (root.cache < 0) {
				fatal("failed to open cache dir %s: %s", path, strerror(errno));
			}
			for (unsigned i = 0; i < 256; i++) {
				sprintf(path, "%02x", i);
				if (mkdirat(root.cache, path, 0755) < 0 && errno != EEXIST) {
					fatal("failed to make cache subdir %s: path", path, strerror(errno));
				}
			}
		}

		root.fd = open(options.basedir, O_PATH);
		if (root.fd < 0) {
			fatal("failed to open base dir: %s", strerror(errno));
		}
		int index = openat(root.fd, ".growfsdir", O_RDONLY);
		if (index < 0) {
			fatal("failed to open GROW-FS index: %s", strerror(errno));
		}
		FILE *metadata = fdopen(index, "r");
		if (!metadata) {
			fatal("failed to get index FILE: %s", strerror(errno));
		}
		root.metadata = grow_from_file(metadata);
		if (!root.metadata) {
			fatal("failed to load GROW-FS index");
		}
		fclose(metadata);
	}

	return fuse_main(args.argc, args.argv, &grow_fuse_ops, &root);
}
