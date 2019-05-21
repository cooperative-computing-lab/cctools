/*
Copyright (C) 2014- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "confuga.h"

#include "catch.h"
#include "chirp_filesystem.h"
#include "chirp_fs_confuga.h"

#include "debug.h"
#include "list.h"
#include "macros.h"
#include "path.h"
#include "pattern.h"
#include "uuid.h"

#include <assert.h>
#include <errno.h>
#include <string.h>

#define STOPTIME (time(0)+15)

#define COPY_STAT_CONFUGA_TO_CHIRP(cbuf,buf) \
	do {\
		memset(&(cbuf),0,sizeof(cbuf));\
		(cbuf).cst_dev = -1;\
		(cbuf).cst_ino = (buf).ino;\
		(cbuf).cst_mode = (buf).mode;\
		(cbuf).cst_nlink = (buf).nlink;\
		(cbuf).cst_uid = (buf).uid;\
		(cbuf).cst_gid = (buf).gid;\
		(cbuf).cst_rdev = -2;\
		(cbuf).cst_size = (buf).size;\
		(cbuf).cst_blksize = (buf).size;\
		(cbuf).cst_blocks = 1;\
		(cbuf).cst_atime = (buf).atime;\
		(cbuf).cst_mtime = (buf).mtime;\
		(cbuf).cst_ctime = (buf).ctime;\
	} while (0)

#define COPY_STATFS_CONFUGA_TO_CHIRP(cbuf,buf) \
	do {\
		memset(&(cbuf),0,sizeof(cbuf));\
		(cbuf).f_type = (buf).type;\
		(cbuf).f_blocks = (buf).blocks;\
		(cbuf).f_bavail = (buf).bavail;\
		(cbuf).f_bsize = 1;\
		(cbuf).f_bfree = (buf).bfree;\
		(cbuf).f_files = (buf).files;\
		(cbuf).f_ffree = (buf).ffree;\
	} while (0)

#define CATCH_CONFUGA(expr) \
	do {\
		do {\
			rc = (expr);\
		} while (rc == EAGAIN && usleep(10000) == 0);\
		CATCH(rc);\
	} while (0)

static confuga *C;

static struct {
	char path[CHIRP_PATH_MAX]; /* Confuga NS */
	enum {
		CHIRP_FS_CONFUGA_CLOSED,
		CHIRP_FS_CONFUGA_REPL_READ,
		CHIRP_FS_CONFUGA_FILE_WRITE,
		CHIRP_FS_CONFUGA_META_READ,
		CHIRP_FS_CONFUGA_META_WRITE,
	} type;
	union {
		struct {
			confuga_file *file;
			confuga_off_t size; /* current offset for pwrite */
			int flags;
		} file;
		confuga_replica *replica;
		buffer_t metadata;
	} f;
} open_files[CHIRP_FILESYSTEM_MAXFD];

#define PROLOGUE \
	rc = 0;\
	goto out;\
out:\
	return RCUNIX(rc);

extern struct list *catalog_host_list;
#define strprfx(s,p) (strncmp(s,p "",sizeof(p)-1) == 0)
static int chirp_fs_confuga_init (const char url[CHIRP_PATH_MAX], cctools_uuid_t *uuid)
{
	int rc;
	int i;
	char *confuga_id = NULL;
	char *confuga_uuid = NULL;

	CATCH_CONFUGA(confuga_connect(&C, url, list_peek_head(catalog_host_list)));
	CATCH(confuga_getid(C, &confuga_id));

	if (pattern_match(confuga_id, "confuga:(%x+)", &confuga_uuid) >= 0) {
		cctools_uuid_loadhex(uuid, confuga_uuid);
	} else {
		fatal("unexpected confuga id: %s", confuga_id);
	}

	for (i = 0; i < CHIRP_FILESYSTEM_MAXFD; i++)
		open_files[i].type = CHIRP_FS_CONFUGA_CLOSED;

	rc = 0;
	goto out;
out:
	free(confuga_id);
	free(confuga_uuid);
	return RCUNIX(rc);
}

static void chirp_fs_confuga_destroy (void)
{
	confuga_disconnect(C);
	C = NULL;
}

static int chirp_fs_confuga_fname (int fd, char path[CHIRP_PATH_MAX])
{
	if (fd < 0 || fd >= CHIRP_FILESYSTEM_MAXFD || open_files[fd].type == CHIRP_FS_CONFUGA_CLOSED)
		return (errno = EBADF, -1);
	strcpy(path, open_files[fd].path);
	return 0;
}

static int getfd (void)
{
	int fd;
	for (fd = 0; fd < CHIRP_FILESYSTEM_MAXFD; fd++) {
		if(open_files[fd].type == CHIRP_FS_CONFUGA_CLOSED)
			return fd;
	}
	return (errno = EMFILE, -1);
}

#define SETUP_FILE \
	char path[CHIRP_PATH_MAX];\
	if (!(0 <= fd && fd < CHIRP_FILESYSTEM_MAXFD) || open_files[fd].type == CHIRP_FS_CONFUGA_CLOSED)\
		return (errno = EBADF, -1);\
	if (chirp_fs_confuga_fname(fd, path) == -1)\
		return -1;

static INT64_T chirp_fs_confuga_open (const char *path, INT64_T flags, INT64_T mode)
{
	int rc;
	char *data = NULL;
	int fd = getfd();

	strncpy(open_files[fd].path, path, sizeof(open_files[fd].path)-1);
	switch (flags & O_ACCMODE) {
		case O_RDONLY:
			if (strncmp(path_basename(path), ".__", 3) == 0) {
				size_t len;
				buffer_init(&open_files[fd].f.metadata);
				CATCH(confuga_metadata_lookup(C, path, &data, &len));
				CATCHUNIX(buffer_putlstring(&open_files[fd].f.metadata, data, len));
				open_files[fd].type = CHIRP_FS_CONFUGA_META_READ;
			} else {
				confuga_fid_t fid;
				CATCH_CONFUGA(confuga_lookup(C, path, &fid, NULL));
				CATCH_CONFUGA(confuga_replica_open(C, fid, &open_files[fd].f.replica, STOPTIME));
				open_files[fd].type = CHIRP_FS_CONFUGA_REPL_READ;
			}
			break;
		case O_WRONLY:
			if (strncmp(path_basename(path), ".__", 3) == 0) {
				buffer_init(&open_files[fd].f.metadata);
				open_files[fd].type = CHIRP_FS_CONFUGA_META_WRITE;
			} else {
				CATCH_CONFUGA(confuga_file_create(C, &open_files[fd].f.file.file, STOPTIME));
				open_files[fd].f.file.size = 0;
				open_files[fd].f.file.flags = 0;
				if (flags & O_EXCL)
					open_files[fd].f.file.flags |= CONFUGA_O_EXCL;
				open_files[fd].type = CHIRP_FS_CONFUGA_FILE_WRITE;
			}
			break;
		case O_RDWR:
			CATCH(EINVAL);
			break;
		default:
			assert(0);
	}

	rc = 0;
	goto out;
out:
	free(data);
	if (rc)
		return (errno = rc, -1);
	else
		return fd; /* N.B. return fd on success */

}

static INT64_T chirp_fs_confuga_close (int fd)
{
	int rc;
	SETUP_FILE

	switch (open_files[fd].type) {
		case CHIRP_FS_CONFUGA_REPL_READ:
			CATCH_CONFUGA(confuga_replica_close(open_files[fd].f.replica, STOPTIME));
			break;
		case CHIRP_FS_CONFUGA_FILE_WRITE: {
			confuga_fid_t fid;
			confuga_off_t size;
			CATCH_CONFUGA(confuga_file_close(open_files[fd].f.file.file, &fid, &size, STOPTIME));
			assert(open_files[fd].f.file.size == size);
			CATCH_CONFUGA(confuga_update(C, open_files[fd].path, fid, size, open_files[fd].f.file.flags));
			break;
		}
		case CHIRP_FS_CONFUGA_META_READ:
			buffer_free(&open_files[fd].f.metadata);
			break;
		case CHIRP_FS_CONFUGA_META_WRITE:
			CATCH_CONFUGA(confuga_metadata_update(C, open_files[fd].path, buffer_tostring(&open_files[fd].f.metadata), buffer_pos(&open_files[fd].f.metadata)));
			buffer_free(&open_files[fd].f.metadata);
			break;
		default:
			assert(0);
	}
	open_files[fd].type = CHIRP_FS_CONFUGA_CLOSED;

	PROLOGUE
}

static INT64_T chirp_fs_confuga_pread(int fd, void *buffer, INT64_T length, INT64_T offset)
{
	int rc;
	size_t n;
	SETUP_FILE

	if (offset < 0)
		CATCH(EINVAL);

	switch (open_files[fd].type) {
		case CHIRP_FS_CONFUGA_REPL_READ:
			CATCH_CONFUGA(confuga_replica_pread(open_files[fd].f.replica, buffer, length, &n, offset, STOPTIME));
			break;
		case CHIRP_FS_CONFUGA_META_READ:
			if ((size_t)offset < buffer_pos(&open_files[fd].f.metadata)) {
				n = MIN((size_t)length, buffer_pos(&open_files[fd].f.metadata)-(size_t)offset);
				memcpy(buffer, buffer_tostring(&open_files[fd].f.metadata)+offset, n);
			} else {
				n = 0;
			}
			break;
		default:
			CATCH(EBADF);
	}

	rc = 0;
	goto out;
out:
	if (rc) {
		return (errno = rc, -1);
	} else {
		return n; /* N.B. return n on success */
	}
}

static INT64_T chirp_fs_confuga_pwrite(int fd, const void *buffer, INT64_T length, INT64_T offset)
{
	int rc;
	size_t n;
	SETUP_FILE

	if (length < 0 || offset < 0)
		CATCH(EINVAL);

	switch (open_files[fd].type) {
		case CHIRP_FS_CONFUGA_FILE_WRITE: {
			if ((confuga_off_t)offset != open_files[fd].f.file.size)
				CATCH(EINVAL); /* do not allow random writes */
			CATCH_CONFUGA(confuga_file_write(open_files[fd].f.file.file, buffer, length, &n, STOPTIME));
			open_files[fd].f.file.size += n;
			break;
		}
		case CHIRP_FS_CONFUGA_META_WRITE:
			if ((size_t)offset != buffer_pos(&open_files[fd].f.metadata))
				CATCH(EINVAL); /* do not allow random writes */
			CATCHUNIX(buffer_putlstring(&open_files[fd].f.metadata, buffer, length));
			n = length;
			break;
		default:
			CATCH(EBADF);
	}

	rc = 0;
	goto out;
out:
	if (rc) {
		return (errno = rc, -1);
	} else {
		return n; /* N.B. return n on success */
	}
}

static INT64_T chirp_fs_confuga_swrite(int fd, const void *vbuffer, INT64_T length, INT64_T stride_length, INT64_T stride_skip, INT64_T offset)
{
	SETUP_FILE
	return (errno = ENOSYS, -1);
}

struct chirp_dir {
	confuga_dir *dir;
	struct chirp_dirent dirent;
};

static struct chirp_dir *chirp_fs_confuga_opendir(const char *path)
{
	int rc;
	struct chirp_dir *dir = NULL;

	dir = malloc(sizeof(struct chirp_dir));
	if (dir == NULL) CATCH(ENOMEM);

	CATCH_CONFUGA(confuga_opendir(C, path, &dir->dir));
out:
	if (rc) {
		free(dir);
		errno = rc;
		return NULL;
	} else {
		return dir;
	}
}

static struct chirp_dirent *chirp_fs_confuga_readdir(struct chirp_dir *dir)
{
	int rc;
	struct confuga_dirent *dirent;
	struct chirp_dirent *result = NULL;

	CATCH_CONFUGA(confuga_readdir(dir->dir, &dirent));
	if (dirent) {
		result = &dir->dirent;
		result->name = dirent->name;
		result->lstatus = dirent->lstatus;
		COPY_STAT_CONFUGA_TO_CHIRP(result->info, dirent->info);
	}
out:
	if (rc) {
		errno = rc;
		return NULL;
	} else {
		return result;
	}
}

static void chirp_fs_confuga_closedir(struct chirp_dir *dir)
{
	confuga_closedir(dir->dir);
	free(dir);
}

#define WRAP_CONFUGA_SIMPLE(call) \
	int rc;\
	CATCH_CONFUGA(call);\
	PROLOGUE

static INT64_T chirp_fs_confuga_unlink(const char *path)
{
	WRAP_CONFUGA_SIMPLE(confuga_unlink(C, path));
}

static INT64_T chirp_fs_confuga_rename(const char *old, const char *new)
{
	WRAP_CONFUGA_SIMPLE(confuga_rename(C, old, new));
}

static INT64_T chirp_fs_confuga_link(const char *target, const char *path)
{
	WRAP_CONFUGA_SIMPLE(confuga_link(C, target, path));
}

static INT64_T chirp_fs_confuga_symlink(const char *target, const char *path)
{
	WRAP_CONFUGA_SIMPLE(confuga_symlink(C, target, path));
}

static INT64_T chirp_fs_confuga_readlink(const char *path, char *buf, INT64_T length)
{
	WRAP_CONFUGA_SIMPLE(confuga_readlink(C, path, buf, length));
}

static INT64_T chirp_fs_confuga_mkdir(const char *path, INT64_T mode)
{
	WRAP_CONFUGA_SIMPLE(confuga_mkdir(C, path, mode));
}

static INT64_T chirp_fs_confuga_rmdir(const char *path)
{
	WRAP_CONFUGA_SIMPLE(confuga_rmdir(C, path));
}

static INT64_T chirp_fs_confuga_stat(const char *path, struct chirp_stat *buf)
{
	int rc;
	struct confuga_stat info;
	CATCH_CONFUGA(confuga_stat(C, path, &info));
	COPY_STAT_CONFUGA_TO_CHIRP(*buf, info);
	PROLOGUE
}

static INT64_T chirp_fs_confuga_lstat(const char *path, struct chirp_stat *buf)
{
	int rc;
	struct confuga_stat info;
	CATCH_CONFUGA(confuga_lstat(C, path, &info));
	COPY_STAT_CONFUGA_TO_CHIRP(*buf, info);
	PROLOGUE
}

static INT64_T chirp_fs_confuga_statfs(const char *path, struct chirp_statfs *buf)
{
	int rc;
	struct confuga_statfs info;
	CATCH_CONFUGA(confuga_statfs(C, &info));
	COPY_STATFS_CONFUGA_TO_CHIRP(*buf, info);
	PROLOGUE
}

static INT64_T chirp_fs_confuga_access(const char *path, INT64_T mode)
{
	WRAP_CONFUGA_SIMPLE(confuga_access(C, path, mode));
}

static INT64_T chirp_fs_confuga_chmod(const char *path, INT64_T mode)
{
	// A remote user can change some of the permissions bits,
	// which only affect local users, but we don't let them
	// take away the owner bits, which would affect the Chirp server.
	mode &= S_IXUSR|S_IRWXG|S_IRWXO;
	mode |= S_IRUSR|S_IWUSR;
	WRAP_CONFUGA_SIMPLE(confuga_chmod(C, path, mode));
}

static INT64_T chirp_fs_confuga_chown(const char *path, INT64_T uid, INT64_T gid)
{
	// Changing file ownership is silently ignored,
	// because permissions are handled through the ACL model.
	return 0;
}

static INT64_T chirp_fs_confuga_lchown(const char *path, INT64_T uid, INT64_T gid)
{
	// Changing file ownership is silently ignored,
	// because permissions are handled through the ACL model.
	return 0;
}

static INT64_T chirp_fs_confuga_truncate(const char *path, INT64_T length)
{
	WRAP_CONFUGA_SIMPLE(confuga_truncate(C, path, length));
}

static INT64_T chirp_fs_confuga_utime(const char *path, time_t actime, time_t modtime)
{
	WRAP_CONFUGA_SIMPLE(confuga_utime(C, path, actime, modtime));
}

static INT64_T chirp_fs_confuga_getxattr(const char *path, const char *name, void *data, size_t size)
{
	WRAP_CONFUGA_SIMPLE(confuga_getxattr(C, path, name, data, size));
}

static INT64_T chirp_fs_confuga_lgetxattr(const char *path, const char *name, void *data, size_t size)
{
	WRAP_CONFUGA_SIMPLE(confuga_lgetxattr(C, path, name, data, size));
}

static INT64_T chirp_fs_confuga_listxattr(const char *path, char *list, size_t size)
{
	WRAP_CONFUGA_SIMPLE(confuga_listxattr(C, path, list, size));
}

static INT64_T chirp_fs_confuga_llistxattr(const char *path, char *list, size_t size)
{
	WRAP_CONFUGA_SIMPLE(confuga_llistxattr(C, path, list, size));
}

static INT64_T chirp_fs_confuga_setxattr(const char *path, const char *name, const void *data, size_t size, int flags)
{
	WRAP_CONFUGA_SIMPLE(confuga_setxattr(C, path, name, data, size, flags));
}

static INT64_T chirp_fs_confuga_lsetxattr(const char *path, const char *name, const void *data, size_t size, int flags)
{
	WRAP_CONFUGA_SIMPLE(confuga_lsetxattr(C, path, name, data, size, flags));
}

static INT64_T chirp_fs_confuga_removexattr(const char *path, const char *name)
{
	WRAP_CONFUGA_SIMPLE(confuga_removexattr(C, path, name));
}

static INT64_T chirp_fs_confuga_lremovexattr(const char *path, const char *name)
{
	WRAP_CONFUGA_SIMPLE(confuga_lremovexattr(C, path, name));
}

static INT64_T chirp_fs_confuga_setrep(const char *path, int nreps)
{
	int rc;
	confuga_fid_t fid;

	CATCH_CONFUGA(confuga_lookup(C, path, &fid, NULL));
	CATCH_CONFUGA(confuga_setrep(C, fid, nreps));
	PROLOGUE
}

#define FD_WRAP_CONFUGA_SIMPLE(call) \
	int rc;\
	SETUP_FILE\
	CATCH_CONFUGA(call);\
	PROLOGUE

static INT64_T chirp_fs_confuga_fstat(int fd, struct chirp_stat *buf)
{
	int rc;
	SETUP_FILE
	switch (open_files[fd].type) {
		case CHIRP_FS_CONFUGA_REPL_READ: {
			struct confuga_stat info;
			CATCH_CONFUGA(confuga_stat(C, path, &info));
			COPY_STAT_CONFUGA_TO_CHIRP(*buf, info);
			break;
		}
		case CHIRP_FS_CONFUGA_FILE_WRITE: {
			/* files are mapped into the namespace on close, so we fill in what we can after doing stat on "/" */
			struct confuga_stat info;
			CATCH_CONFUGA(confuga_stat(C, "/", &info));
			COPY_STAT_CONFUGA_TO_CHIRP(*buf, info);
			buf->cst_ino = random();
			buf->cst_mode = S_IRUSR|S_IWUSR;
			buf->cst_nlink = 1;
			buf->cst_size = buf->cst_blksize = open_files[fd].f.file.size;
			buf->cst_blocks = 1;
			buf->cst_atime = buf->cst_mtime = buf->cst_ctime = time(NULL);
			break;
		}
		default: assert(0);
	}
	PROLOGUE
}

static INT64_T chirp_fs_confuga_fstatfs(int fd, struct chirp_statfs *buf)
{
	return chirp_fs_confuga_statfs("/", buf);
}

static INT64_T chirp_fs_confuga_fchown(int fd, INT64_T uid, INT64_T gid)
{
	FD_WRAP_CONFUGA_SIMPLE(0);
}

static INT64_T chirp_fs_confuga_fchmod(int fd, INT64_T mode)
{
	// A remote user can change some of the permissions bits,
	// which only affect local users, but we don't let them
	// take away the owner bits, which would affect the Chirp server.
	mode &= S_IXUSR|S_IRWXG|S_IRWXO;
	mode |= S_IRUSR|S_IWUSR;
	FD_WRAP_CONFUGA_SIMPLE(confuga_chmod(C, path, mode));
}

static INT64_T chirp_fs_confuga_ftruncate(int fd, INT64_T length)
{
	int rc;
	SETUP_FILE
	if (length < 0)
		CATCH(EINVAL);
	if (open_files[fd].type == CHIRP_FS_CONFUGA_META_WRITE) {
		if (buffer_pos(&open_files[fd].f.metadata) <= (size_t)length)
			buffer_rewind(&open_files[fd].f.metadata, length);
		else
			CATCH(EINVAL);
	} else if (open_files[fd].type == CHIRP_FS_CONFUGA_FILE_WRITE) {
		CATCH_CONFUGA(confuga_file_truncate(open_files[fd].f.file.file, length, STOPTIME));
		open_files[fd].f.file.size = length;
	} else CATCH(EBADF);
	PROLOGUE
}

static INT64_T chirp_fs_confuga_fsync(int fd)
{
	FD_WRAP_CONFUGA_SIMPLE(0);
}

static INT64_T chirp_fs_confuga_fremovexattr(int fd, const char *name)
{
	FD_WRAP_CONFUGA_SIMPLE(confuga_removexattr(C, path, name));
}

static INT64_T chirp_fs_confuga_fsetxattr(int fd, const char *name, const void *data, size_t size, int flags)
{
	FD_WRAP_CONFUGA_SIMPLE(confuga_setxattr(C, path, name, data, size, flags));
}

static INT64_T chirp_fs_confuga_flistxattr(int fd, char *list, size_t size)
{
	FD_WRAP_CONFUGA_SIMPLE(confuga_listxattr(C, path, list, size));
}

static INT64_T chirp_fs_confuga_fgetxattr(int fd, const char *name, void *data, size_t size)
{
	FD_WRAP_CONFUGA_SIMPLE(confuga_getxattr(C, path, name, data, size));
}

static int chirp_fs_confuga_do_acl_check()
{
	return 1;
}

static int chirp_fs_confuga_job_dbinit (sqlite3 *db)
{
	WRAP_CONFUGA_SIMPLE(confuga_job_dbinit(C, db));
}

static int chirp_fs_confuga_job_schedule (sqlite3 *db)
{
	int rc;

	CATCH_CONFUGA(confuga_job_attach(C, db));
	CATCH_CONFUGA(confuga_daemon(C));

	rc = 0;
	goto out;
out:
	return rc;
}

struct chirp_filesystem chirp_fs_confuga = {
	chirp_fs_confuga_init,
	chirp_fs_confuga_destroy,

	chirp_fs_confuga_fname,

	chirp_fs_confuga_open,
	chirp_fs_confuga_close,
	chirp_fs_confuga_pread,
	chirp_fs_confuga_pwrite,
	cfs_basic_sread,
	chirp_fs_confuga_swrite,
	cfs_stub_lockf,
	chirp_fs_confuga_fstat,
	chirp_fs_confuga_fstatfs,
	chirp_fs_confuga_fchown,
	chirp_fs_confuga_fchmod,
	chirp_fs_confuga_ftruncate,
	chirp_fs_confuga_fsync,

	cfs_basic_search,

	chirp_fs_confuga_opendir,
	chirp_fs_confuga_readdir,
	chirp_fs_confuga_closedir,

	chirp_fs_confuga_unlink,
	cfs_basic_rmall,
	chirp_fs_confuga_rename,
	chirp_fs_confuga_link,
	chirp_fs_confuga_symlink,
	chirp_fs_confuga_readlink,
	chirp_fs_confuga_mkdir,
	chirp_fs_confuga_rmdir,
	chirp_fs_confuga_stat,
	chirp_fs_confuga_lstat,
	chirp_fs_confuga_statfs,
	chirp_fs_confuga_access,
	chirp_fs_confuga_chmod,
	chirp_fs_confuga_chown,
	chirp_fs_confuga_lchown,
	chirp_fs_confuga_truncate,
	chirp_fs_confuga_utime,
	cfs_basic_hash, /* TODO */
	chirp_fs_confuga_setrep,

	chirp_fs_confuga_getxattr,
	chirp_fs_confuga_fgetxattr,
	chirp_fs_confuga_lgetxattr,
	chirp_fs_confuga_listxattr,
	chirp_fs_confuga_flistxattr,
	chirp_fs_confuga_llistxattr,
	chirp_fs_confuga_setxattr,
	chirp_fs_confuga_fsetxattr,
	chirp_fs_confuga_lsetxattr,
	chirp_fs_confuga_removexattr,
	chirp_fs_confuga_fremovexattr,
	chirp_fs_confuga_lremovexattr,

	chirp_fs_confuga_do_acl_check,

	chirp_fs_confuga_job_dbinit,
	chirp_fs_confuga_job_schedule,
};

/* vim: set noexpandtab tabstop=4: */
