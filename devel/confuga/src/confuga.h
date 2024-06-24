/*
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef CONFUGA_H
#define CONFUGA_H

#include <sys/types.h>

#include <inttypes.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>

#ifdef BUILDING_LIBCONFUGA
#  if defined(__GNUC__) && __GNUC__ >= 3 && __GNUC_MINOR__ >= 4
#    define CONFUGA_API __attribute__((__visibility__("default")))
#  elif defined(_MSC_VER)
#    define CONFUGA_API __declspec(dllexport)
#  else
#    define CONFUGA_API
#  endif
#else
#  define CONFUGA_API
#endif

#define CONFUGA_PATH_MAX  4096

typedef struct confuga confuga;

/* Package in a struct so we can do assignments and sizeof(fid.id) works. */
typedef struct {
	unsigned char id[20]; /* binary SHA1 digest (hardcoded so we don't need to include the header) */
} confuga_fid_t;
#define CONFUGA_FID_PRIFMT "%02X%02X%02X%02X%02X%02X%02X%02X%02X%02X%02X%02X%02X%02X%02X%02X%02X%02X%02X%02X"
#define CONFUGA_FID_DEBFMT "fid:" CONFUGA_FID_PRIFMT
#define CONFUGA_FID_PRIARGS(fid) \
	(unsigned int) (fid).id[0], (unsigned int) (fid).id[1], (unsigned int) (fid).id[2], (unsigned int) (fid).id[3], (unsigned int) (fid).id[4], (unsigned int) (fid).id[5], (unsigned int) (fid).id[6], (unsigned int) (fid).id[7], (unsigned int) (fid).id[8], (unsigned int) (fid).id[9], (unsigned int) (fid).id[10], (unsigned int) (fid).id[11], (unsigned int) (fid).id[12], (unsigned int) (fid).id[13], (unsigned int) (fid).id[14], (unsigned int) (fid).id[15], (unsigned int) (fid).id[16], (unsigned int) (fid).id[17], (unsigned int) (fid).id[18], (unsigned int) (fid).id[19]

typedef uint64_t confuga_sid_t;
#define CONFUGA_SID_PRIFMT "%" PRIu64
#define CONFUGA_SID_DEBFMT "sid:%04" PRIu64

/* `sha1sum < /dev/null` */
#define CONFUGA_FID_EMPTY "\xDA\x39\xA3\xEE\x5E\x6B\x4B\x0D\x32\x55\xBF\xEF\x95\x60\x18\x90\xAF\xD8\x07\x09"

typedef uint64_t confuga_off_t; /* maximum file size */
#define PRIxCONFUGA_OFF_T PRIx64
#define PRIuCONFUGA_OFF_T PRIu64
#define PRICONFUGA_OFF_T PRIuCONFUGA_OFF_T

typedef struct confuga_replica confuga_replica;
typedef struct confuga_file confuga_file;

struct confuga_stat {
	/* file content */
	confuga_fid_t fid;
	confuga_off_t size;

	/* directly from stat on NS */
	ino_t ino;
	mode_t mode;
	uid_t uid;
	gid_t gid;
	nlink_t nlink;
	time_t atime;
	time_t mtime;
	time_t ctime;
};

struct confuga_statfs {
	uint64_t bsize;
	uint64_t blocks;
	uint64_t bfree;
	uint64_t bavail;

	uint64_t files;
	uint64_t ffree;

	uint64_t type;
	uint64_t flag;
};
#define CONFUGA_STATFS_DEBFMT \
	"stat:{" \
		"bsize = %" PRIu64 ", " \
		"blocks = %" PRIu64 ", " \
		"bfree = %" PRIu64 ", " \
		"bavail = %" PRIu64 ", " \
		"files = %" PRIu64 ", " \
		"ffree = %" PRIu64 ", " \
		"type = %" PRIx64 ", " \
		"flag = %" PRIx64 \
	"}"
#define CONFUGA_STATFS_PRIARGS(info) \
	(info).bsize, (info).blocks, (info).bfree, (info).bavail, (info).files, (info).ffree, (info).type, (info).flag

typedef struct confuga_dir confuga_dir;
struct confuga_dirent {
	char *name;
	int lstatus;
	struct confuga_stat info;
};

CONFUGA_API int confuga_connect(confuga **Cp, const char *root, const char *catalog);
CONFUGA_API int confuga_disconnect(confuga *C);
CONFUGA_API int confuga_daemon(confuga *C);

CONFUGA_API int confuga_concurrency (confuga *C, uint64_t n);

#define CONFUGA_SN_UUID 1
#define CONFUGA_SN_ADDR 2
CONFUGA_API int confuga_snadd (confuga *C, const char *id, const char *root, const char *password, int flag);
CONFUGA_API int confuga_snrm (confuga *C, const char *id, int flag);
CONFUGA_API int confuga_nodes (confuga *C, const char *nodes); /* deprecated */

#define CONFUGA_SCHEDULER_FIFO 1
CONFUGA_API int confuga_scheduler_strategy (confuga *C, int strategy, uint64_t n);

CONFUGA_API int confuga_pull_threshold (confuga *C, uint64_t n);

#define CONFUGA_REPLICATION_PUSH_SYNCHRONOUS  1
#define CONFUGA_REPLICATION_PUSH_ASYNCHRONOUS 2
CONFUGA_API int confuga_replication_strategy (confuga *C, int strategy, uint64_t n);

CONFUGA_API int confuga_getid (confuga *C, char **id);

#define CONFUGA_O_EXCL (1L<<0)
CONFUGA_API int confuga_lookup(confuga *C, const char *path, confuga_fid_t *fid, confuga_off_t *size);
CONFUGA_API int confuga_update(confuga *C, const char *path, confuga_fid_t fid, confuga_off_t size, int flags);
CONFUGA_API int confuga_metadata_lookup(confuga *C, const char *path, char **data, size_t *size);
CONFUGA_API int confuga_metadata_update(confuga *C, const char *path, const char *data, size_t size);
CONFUGA_API int confuga_opendir(confuga *C, const char *path, confuga_dir **dir);
CONFUGA_API int confuga_readdir(confuga_dir *dir, struct confuga_dirent **dirent);
CONFUGA_API int confuga_closedir(confuga_dir *dir);
CONFUGA_API int confuga_unlink(confuga *C, const char *path);
CONFUGA_API int confuga_rename(confuga *C, const char *old, const char *path);
CONFUGA_API int confuga_link(confuga *C, const char *target, const char *path);
CONFUGA_API int confuga_symlink(confuga *C, const char *target, const char *path);
CONFUGA_API int confuga_readlink(confuga *C, const char *path, char *buf, size_t length);
CONFUGA_API int confuga_mkdir(confuga *C, const char *path, int mode);
CONFUGA_API int confuga_rmdir(confuga *C, const char *path);
CONFUGA_API int confuga_stat(confuga *C, const char *path, struct confuga_stat *info);
CONFUGA_API int confuga_statfs (confuga *C, struct confuga_statfs *info);
CONFUGA_API int confuga_lstat(confuga *C, const char *path, struct confuga_stat *info);
CONFUGA_API int confuga_access(confuga *C, const char *path, int mode);
CONFUGA_API int confuga_chmod(confuga *C, const char *path, int mode);
CONFUGA_API int confuga_truncate(confuga *C, const char *path, confuga_off_t length);
CONFUGA_API int confuga_utime(confuga *C, const char *path, time_t actime, time_t modtime);
CONFUGA_API int confuga_getxattr(confuga *C, const char *path, const char *name, void *data, size_t size);
CONFUGA_API int confuga_lgetxattr(confuga *C, const char *path, const char *name, void *data, size_t size);
CONFUGA_API int confuga_listxattr(confuga *C, const char *path, char *list, size_t size);
CONFUGA_API int confuga_llistxattr(confuga *C, const char *path, char *list, size_t size);
CONFUGA_API int confuga_setxattr(confuga *C, const char *path, const char *name, const void *data, size_t size, int flags);
CONFUGA_API int confuga_lsetxattr(confuga *C, const char *path, const char *name, const void *data, size_t size, int flags);
CONFUGA_API int confuga_removexattr(confuga *C, const char *path, const char *name);
CONFUGA_API int confuga_lremovexattr(confuga *C, const char *path, const char *name);

CONFUGA_API int confuga_setrep(confuga *C, confuga_fid_t fid, int nreps);
CONFUGA_API int confuga_replica_open(confuga *C, confuga_fid_t fid, confuga_replica **replica, time_t stoptime);
CONFUGA_API int confuga_replica_pread(confuga_replica *replica, void *buffer, size_t size, size_t *n, confuga_off_t offset, time_t stoptime);
CONFUGA_API int confuga_replica_close(confuga_replica *replica, time_t stoptime);
CONFUGA_API int confuga_file_create(confuga *C, confuga_file **file, time_t stoptime);
CONFUGA_API int confuga_file_write(confuga_file *file, const void *buffer, size_t length, size_t *n, time_t stoptime);
CONFUGA_API int confuga_file_truncate(confuga_file *file, confuga_off_t length, time_t stoptime);
CONFUGA_API int confuga_file_close(confuga_file *file, confuga_fid_t *fid, confuga_off_t *size, time_t stoptime);

#include "sqlite3.h"
CONFUGA_API int confuga_job_attach (confuga *C, sqlite3 *db);
CONFUGA_API int confuga_job_dbinit (confuga *C, sqlite3 *db);

#endif

/* vim: set noexpandtab tabstop=8: */
