/*
Copyright (C) 2014- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "confuga_fs.h"

#include "catch.h"
#include "create_dir.h"
#include "debug.h"
#include "full_io.h"
#include "path.h"

#include <dirent.h>
#include <fcntl.h>
#include <unistd.h>
#include <utime.h>

#include <sys/stat.h>
#if defined(HAS_ATTR_XATTR_H)
#include <attr/xattr.h>
#elif defined(HAS_SYS_XATTR_H)
#include <sys/xattr.h>
#endif

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>

/* Cygwin does not have 64-bit I/O, while Darwin has it by default. */
#if defined(CCTOOLS_OPSYS_CYGWIN) || defined(CCTOOLS_OPSYS_DARWIN) || defined(CCTOOLS_OPSYS_FREEBSD)
#	include <sys/mount.h>
#	include <sys/param.h>
#	define fstat64 fstat
#	define lstat64 lstat
#	define open64 open
#	define stat64 stat
#	define statfs64 statfs
#endif

#ifdef CCTOOLS_OPSYS_DARWIN
#	define _getxattr(path,name,data,size) getxattr(path,name,data,size,0,0)
#	define _lgetxattr(path,name,data,size) getxattr(path,name,data,size,0,XATTR_NOFOLLOW)
#	define _listxattr(path,list,size) listxattr(path,list,size,0)
#	define _llistxattr(path,list,size) listxattr(path,list,size,XATTR_NOFOLLOW)
#	define _lremovexattr(path,name) removexattr(path,name,XATTR_NOFOLLOW)
#	define _lsetxattr(path,name,data,size,flags) setxattr(path,name,data,size,0,XATTR_NOFOLLOW|flags)
#	define _removexattr(path,name) removexattr(path,name,0)
#	define _setxattr(path,name,data,size,flags) setxattr(path,name,data,size,0,flags)
#else
#	define _getxattr(path,name,data,size) getxattr(path,name,data,size)
#	define _lgetxattr(path,name,data,size) lgetxattr(path,name,data,size)
#	define _listxattr(path,list,size) listxattr(path,list,size)
#	define _llistxattr(path,list,size) llistxattr(path,list,size)
#	define _lremovexattr(path,name) lremovexattr(path,name)
#	define _lsetxattr(path,name,data,size,flags) lsetxattr(path,name,data,size,flags)
#	define _removexattr(path,name) removexattr(path,name)
#	define _setxattr(path,name,data,size,flags) setxattr(path,name,data,size,flags)
#endif

#define RESOLVE(path) \
const char *unresolved_##path = path;\
char resolved_##path[CONFUGA_PATH_MAX];\
CATCH(resolve(C, path, resolved_##path));\
path = resolved_##path;\
(void)unresolved_##path; /* silence errors */

#define PROLOGUE \
	rc = 0;\
	goto out;\
out:\
	debug(D_CONFUGA, "= %d (%s)", rc, strerror(rc));\
	return rc;

#define SIMPLE_WRAP_UNIX(expr, fmt, ...) \
	RESOLVE(path);\
	debug(D_CONFUGA, (fmt), __VA_ARGS__);\
	CATCHUNIX((expr));\
	PROLOGUE

/* Each file in the namespace is stored as a replicated file or a metadata object:
 *
 * Replicated file: "file:<fid>:<length>\n". [Include the length so we can avoid the SQL lookup for the length.
 * Metadata file:   "meta:0000000000000000000000000000000000000000:<length>\n<content>".
 */
#define HEADER_LENGTH (4 /* 'file' | 'meta' */ + 1 /* ':' */ + sizeof(((confuga_fid_t *)0)->id)*2 /* SHA1 hash in hexadecimal */ + 1 /* ':' */ + sizeof(confuga_off_t)*2 /* in hexadecimal */ + 1 /* '\n' */)
enum CONFUGA_FILE_TYPE {
	CONFUGA_FILE,
	CONFUGA_META,
};

static int resolve (confuga *C, const char *path, char resolved[CONFUGA_PATH_MAX])
{
	int rc;
	char collapse[CONFUGA_PATH_MAX];
	char absolute[CONFUGA_PATH_MAX];
	path_collapse(path, collapse, 1);
	CATCHUNIX(snprintf(absolute, sizeof(absolute), "%s/root/%s", C->root, collapse));
	if ((size_t)rc >= CONFUGA_PATH_MAX)
		CATCH(ENAMETOOLONG);
	path_collapse(absolute, resolved, 1);

	rc = 0;
	goto out;
out:
	return rc;
}

static int lookup (confuga *C, const char *path, confuga_fid_t *fid, confuga_off_t *size, enum CONFUGA_FILE_TYPE *type)
{
	int rc;
	int fd = -1;
	ssize_t n;
	char header[HEADER_LENGTH+1] = "";
	struct stat64 info;

	fd = open(path, O_RDONLY);
	CATCHUNIX(fd);

	CATCHUNIX(fstat64(fd, &info));
	if (S_ISDIR(info.st_mode))
		CATCH(EISDIR);
	else if (!S_ISREG(info.st_mode))
		CATCH(EINVAL);

	n = full_read(fd, header, HEADER_LENGTH);
	if (n < (ssize_t)HEADER_LENGTH)
		CATCH(EINVAL);
	debug(D_DEBUG, "read `%s'", header); /* debug chomps final newline */

	const char *current = header;
	if (strncmp(current, "file:", strlen("file:")) == 0)
		*type = CONFUGA_FILE;
	else if (strncmp(current, "meta:", strlen("meta:")) == 0)
		*type = CONFUGA_FILE;
	else
		CATCH(EINVAL);
	current += 5;

	/* read hash */
	int i;
	for (i = 0; i < (int)sizeof(fid->id); i += 1, current += 2) {
		char byte[3] = {current[0], current[1], '\0'};
		unsigned long value = strtoul(byte, NULL, 16);
		fid->id[i] = value;
	}
	assert(current[0] == ':');
	current += 1;

	/* read size */
	if (size) {
		char *endptr;
		*size = strtoull(current, &endptr, 16);
		assert(*endptr == '\n');
	}

	rc = 0;
	goto out;
out:
	close(fd);
	return rc;
}

static int update (confuga *C, const char *path, confuga_fid_t fid, confuga_off_t size, int flags)
{
	int rc;
	int fd = -1;
	int n;
	char header[HEADER_LENGTH+1] = "";
	int oflags = O_CREAT|O_WRONLY|O_TRUNC|O_SYNC;

	if (flags & CONFUGA_O_EXCL)
		oflags |= O_EXCL;

	fd = open(path, oflags, S_IRUSR|S_IWUSR);
	CATCHUNIX(fd);
	n = snprintf(header, sizeof(header), "file:" CONFUGA_FID_PRIFMT ":%0*" PRIxCONFUGA_OFF_T "\n", CONFUGA_FID_PRIARGS(fid), (int)sizeof(confuga_off_t)*2, (confuga_off_t)size);
	assert((size_t)n == HEADER_LENGTH);
	rc = full_write(fd, header, HEADER_LENGTH);
	CATCHUNIX(rc);
	if ((size_t)rc < HEADER_LENGTH)
		CATCH(EINVAL); /* FIXME */
	debug(D_DEBUG, "write `%s'", header); /* debug chomps final newline */

	rc = 0;
	goto out;
out:
	close(fd); /* -1 is a NOP */
	return rc;
}

CONFUGA_API int confuga_metadata_lookup (confuga *C, const char *path, char **data, size_t *size)
{
	int rc;
	int fd = -1;
	struct stat64 info;
	ssize_t n;
	size_t _size;
	char header[HEADER_LENGTH+1] = "";
	confuga_fid_t fid;
	RESOLVE(path);

	if (size == NULL)
		size = &_size;

	debug(D_CONFUGA, "metadata_lookup(`%s')", unresolved_path);

	fd = open(path, O_RDONLY);
	CATCHUNIX(fd);

	CATCHUNIX(fstat64(fd, &info));
	if (S_ISDIR(info.st_mode))
		CATCH(EISDIR);
	else if (!S_ISREG(info.st_mode))
		CATCH(EINVAL);

	n = read(fd, header, HEADER_LENGTH);
	if (n < (ssize_t)HEADER_LENGTH)
		CATCH(EINVAL);
	debug(D_DEBUG, "read `%s'", header); /* debug chomps final newline */

	const char *current = header;
	if (!(strncmp(current, "meta:", strlen("meta:")) == 0))
		CATCH(EINVAL);
	current += 5;

	/* read hash */
	int i;
	for (i = 0; i < (int)sizeof(fid.id); i += 1, current += 2) {
		char byte[3] = {current[0], current[1], '\0'};
		unsigned long value = strtoul(byte, NULL, 16);
		fid.id[i] = value;
	}
	assert(current[0] == ':');
	current += 1;

	/* read size */
	char *endptr;
	*size = strtoull(current, &endptr, 16);
	assert(*endptr == '\n');

	*data = calloc(*size+1, sizeof(char));
	if (*data == NULL)
		CATCH(ENOMEM);
	n = read(fd, *data, *size);
	if (n < (ssize_t)*size)
		CATCH(EINVAL);
	debug(D_DEBUG, "read `%s'", *data); /* debug chomps final newline */

	rc = 0;
	goto out;
out:
	debug(D_CONFUGA, "= %d (%s)", rc, strerror(rc));
	close(fd); /* -1 is a NOP */
	return rc;
}

CONFUGA_API int confuga_metadata_update (confuga *C, const char *path, const char *data, size_t size)
{
	static const confuga_fid_t fid = {""};

	int rc;
	int fd = -1;
	int n;
	char header[HEADER_LENGTH+1] = "";
	RESOLVE(path);

	debug(D_CONFUGA, "metadata_update(`%s')", unresolved_path);

	fd = open(path, O_CREAT|O_WRONLY|O_TRUNC|O_SYNC, S_IRUSR|S_IWUSR);
	CATCHUNIX(fd);
	n = snprintf(header, sizeof(header), "meta:" CONFUGA_FID_PRIFMT ":%0*" PRIxCONFUGA_OFF_T "\n", CONFUGA_FID_PRIARGS(fid), (int)sizeof(confuga_off_t)*2, (confuga_off_t)size);
	assert((size_t)n == HEADER_LENGTH);
	rc = full_write(fd, header, HEADER_LENGTH);
	CATCHUNIX(rc);
	if ((size_t)rc < HEADER_LENGTH)
		CATCH(EINVAL); /* FIXME */
	debug(D_DEBUG, "write `%s'", header); /* debug chomps final newline */

	rc = full_write(fd, data, size);
	CATCHUNIX(rc);
	if ((size_t)rc < size)
		CATCH(EINVAL); /* FIXME */
	debug(D_DEBUG, "write `%s'", data); /* debug chomps final newline */

	rc = 0;
	goto out;
out:
	debug(D_CONFUGA, "= %d (%s)", rc, strerror(rc));
	close(fd); /* -1 is a NOP */
	return rc;
}

struct confuga_dir {
	confuga *C;
	DIR *dir;
	char path[CONFUGA_PATH_MAX];
	struct confuga_dirent dirent;
};

CONFUGA_API int confuga_opendir(confuga *C, const char *path, confuga_dir **D)
{
	int rc;
	RESOLVE(path);
	debug(D_CONFUGA, "opendir(`%s')", unresolved_path);
	*D = malloc(sizeof(confuga_dir));
	if (*D == NULL) CATCH(ENOMEM);
	DIR *dir = opendir(path);
	if(dir) {
		(*D)->C = C;
		(*D)->dir = dir;
		strcpy((*D)->path, unresolved_path);
		return 0;
	} else {
		CATCH(errno);
	}
out:
	debug(D_CONFUGA, "= %d (%s)", rc, strerror(rc));
	if (rc)
		free(*D);
	return rc;
}

CONFUGA_API int confuga_readdir(confuga_dir *dir, struct confuga_dirent **dirent)
{
	int rc;
	debug(D_CONFUGA, "readdir(`%s')", dir->path);
	/* N.B. only way to detect an error in readdir is to set errno to 0 and check after. */
	errno = 0;
	struct dirent *d = readdir(dir->dir);
	if (d) {
		char path[CONFUGA_PATH_MAX];
		snprintf(path, sizeof(path)-1, "%s/%s", dir->path, d->d_name);
		dir->dirent.name = d->d_name;
		memset(&dir->dirent.info, 0, sizeof(dir->dirent.info));
		dir->dirent.lstatus = confuga_lstat(dir->C, path, &dir->dirent.info);
		*dirent = &dir->dirent;
	} else {
		*dirent = NULL;
		CATCH(errno);
	}
	PROLOGUE
}

CONFUGA_API int confuga_closedir(confuga_dir *dir)
{
	int rc;
	DIR *ldir = dir->dir;
	debug(D_CONFUGA, "closedir(`%s')", dir->path);
	free(dir);
	CATCHUNIX(closedir(ldir));
	PROLOGUE
}

CONFUGA_API int confuga_unlink(confuga *C, const char *path)
{
	int rc;
	SIMPLE_WRAP_UNIX(unlink(path), "unlink(`%s')", unresolved_path);
}

CONFUGA_API int confuga_rename(confuga *C, const char *old, const char *path)
{
	int rc;
	RESOLVE(old);
	SIMPLE_WRAP_UNIX(rename(old, path), "rename(`%s', `%s')", unresolved_old, unresolved_path);
}

CONFUGA_API int confuga_link(confuga *C, const char *target, const char *path)
{
	/* This deserves some explanation:
	 *
	 * Since the NM manages both the Confuga NS and the file metadata, the
	 * inode on the local file system contains all the file metadata and a
	 * pointer to file contents. So, when we create a link, we really want to
	 * have both entries point to the local file system inode.
	 *
	 * The inode also points to file data which includes the Confuga file ID.
	 * This would be an identifier for the content, not the metadata.
	 */
	int rc;
	RESOLVE(target);
	SIMPLE_WRAP_UNIX(link(target, path), "link(`%s', `%s')", unresolved_target, unresolved_path);
}

CONFUGA_API int confuga_symlink(confuga *C, const char *target, const char *path)
{
	/* `target' is effectively userdata, we do not resolve it */
	int rc;
	SIMPLE_WRAP_UNIX(symlink(target, path), "symlink(`%s', `%s')", target, unresolved_path);
}

CONFUGA_API int confuga_readlink(confuga *C, const char *path, char *buf, size_t length)
{
	int rc;
	SIMPLE_WRAP_UNIX(readlink(path, buf, length), "readlink(`%s', %p, %zu)", unresolved_path, buf, length);
}

CONFUGA_API int confuga_mkdir(confuga *C, const char *path, int mode)
{
	int rc;
	SIMPLE_WRAP_UNIX(mkdir(path, mode), "mkdir(`%s', %d)", unresolved_path, mode);
}

CONFUGA_API int confuga_rmdir(confuga *C, const char *path)
{
	int rc;
	SIMPLE_WRAP_UNIX(rmdir(path), "rmdir(`%s')", unresolved_path);
}

static int do_stat (confuga *C, const char *path, struct confuga_stat *info, int (*statf) (const char *, struct stat64 *))
{
	int rc;
	struct stat64 linfo;
	enum CONFUGA_FILE_TYPE type;
	RESOLVE(path)
	CATCHUNIX(statf(path, &linfo));
	if (S_ISREG(linfo.st_mode)) {
		CATCH(lookup(C, path, &info->fid, &info->size, &type));
	} else {
		info->size = linfo.st_size;
	}
	info->ino = linfo.st_ino;
	info->mode = linfo.st_mode;
	info->uid = linfo.st_uid;
	info->gid = linfo.st_gid;
	info->nlink = linfo.st_nlink;
	info->atime = linfo.st_atime;
	info->mtime = linfo.st_mtime;
	info->ctime = linfo.st_ctime;
	PROLOGUE
}

CONFUGA_API int confuga_stat(confuga *C, const char *path, struct confuga_stat *info)
{
	debug(D_CONFUGA, "stat(`%s', %p)", path, info);
	return do_stat(C, path, info, stat64);
}

CONFUGA_API int confuga_lstat(confuga *C, const char *path, struct confuga_stat *info)
{
	debug(D_CONFUGA, "lstat(`%s', %p)", path, info);
	return do_stat(C, path, info, lstat64);
}

CONFUGA_API int confuga_access(confuga *C, const char *path, int mode)
{
	int rc;
	SIMPLE_WRAP_UNIX(access(path, mode), "access(`%s', %d)", unresolved_path, mode);
}

CONFUGA_API int confuga_chmod(confuga *C, const char *path, int mode)
{
	// A remote user can change some of the permissions bits,
	// which only affect local users, but we don't let them
	// take away the owner bits, which would affect the Chirp server.
	int rc;
	mode |= S_IRUSR|S_IWUSR;
	mode &= S_IRWXU|S_IRWXG|S_IRWXO;
	SIMPLE_WRAP_UNIX(chmod(path, mode), "chmod(`%s', %d)", unresolved_path, mode);
}

CONFUGA_API int confuga_truncate(confuga *C, const char *path, confuga_off_t length)
{
	static const confuga_fid_t empty = {CONFUGA_FID_EMPTY};

	int rc;
	RESOLVE(path)
	debug(D_CONFUGA, "truncate(`%s', %" PRIuCONFUGA_OFF_T ")", unresolved_path, length);
	confuga_fid_t fid;
	confuga_off_t size;
	enum CONFUGA_FILE_TYPE type;
	CATCH(lookup(C, path, &fid, &size, &type));
	if (length > 0)
		CATCH(EINVAL);
	CATCH(update(C, path, empty, 0, 0));
	PROLOGUE
}

CONFUGA_API int confuga_utime(confuga *C, const char *path, time_t actime, time_t modtime)
{
	int rc;
	struct utimbuf ut;
	ut.actime = actime;
	ut.modtime = modtime;
	SIMPLE_WRAP_UNIX(utime(path, &ut), "utime(`%s', actime = %lu, modtime = %lu)", unresolved_path, (unsigned long)actime, (unsigned long)modtime);
}

CONFUGA_API int confuga_getxattr(confuga *C, const char *path, const char *name, void *data, size_t size)
{
	int rc;
#if defined(HAS_SYS_XATTR_H) || defined(HAS_ATTR_XATTR_H)
	SIMPLE_WRAP_UNIX(_getxattr(path, name, data, size), "getxattr(`%s', `%s', %p, %zu)", unresolved_path, name, data, size);
#else
	return ENOSYS;
#endif
}

CONFUGA_API int confuga_lgetxattr(confuga *C, const char *path, const char *name, void *data, size_t size)
{
	int rc;
#if defined(HAS_SYS_XATTR_H) || defined(HAS_ATTR_XATTR_H)
	SIMPLE_WRAP_UNIX(_lgetxattr(path, name, data, size), "lgetxattr(`%s', `%s', %p, %zu)", unresolved_path, name, data, size);
#else
	return ENOSYS;
#endif
}

CONFUGA_API int confuga_listxattr(confuga *C, const char *path, char *list, size_t size)
{
	int rc;
#if defined(HAS_SYS_XATTR_H) || defined(HAS_ATTR_XATTR_H)
	SIMPLE_WRAP_UNIX(_listxattr(path, list, size), "listxattr(`%s', %p, %zu)", unresolved_path, list, size);
#else
	return ENOSYS;
#endif
}

CONFUGA_API int confuga_llistxattr(confuga *C, const char *path, char *list, size_t size)
{
	int rc;
#if defined(HAS_SYS_XATTR_H) || defined(HAS_ATTR_XATTR_H)
	SIMPLE_WRAP_UNIX(_llistxattr(path, list, size), "llistxattr(`%s', %p, %zu)", unresolved_path, list, size);
#else
	return ENOSYS;
#endif
}

CONFUGA_API int confuga_setxattr(confuga *C, const char *path, const char *name, const void *data, size_t size, int flags)
{
	int rc;
#if defined(HAS_SYS_XATTR_H) || defined(HAS_ATTR_XATTR_H)
	SIMPLE_WRAP_UNIX(_setxattr(path, name, data, size, flags), "setxattr(`%s', `%s', %p, %zu, %d)", unresolved_path, name, data, size, flags);
#else
	return ENOSYS;
#endif
}

CONFUGA_API int confuga_lsetxattr(confuga *C, const char *path, const char *name, const void *data, size_t size, int flags)
{
	int rc;
#if defined(HAS_SYS_XATTR_H) || defined(HAS_ATTR_XATTR_H)
	SIMPLE_WRAP_UNIX(_lsetxattr(path, name, data, size, flags), "lsetxattr(`%s', `%s', %p, %zu, %d)", unresolved_path, name, data, size, flags);
#else
	return ENOSYS;
#endif
}

CONFUGA_API int confuga_removexattr(confuga *C, const char *path, const char *name)
{
	int rc;
#if defined(HAS_SYS_XATTR_H) || defined(HAS_ATTR_XATTR_H)
	SIMPLE_WRAP_UNIX(_removexattr(path, name), "removexattr(`%s', `%s')", unresolved_path, name);
#else
	return ENOSYS;
#endif
}

CONFUGA_API int confuga_lremovexattr(confuga *C, const char *path, const char *name)
{
	int rc;
#if defined(HAS_SYS_XATTR_H) || defined(HAS_ATTR_XATTR_H)
	SIMPLE_WRAP_UNIX(_lremovexattr(path, name), "lremovexattr(`%s', `%s')", unresolved_path, name);
#else
	return ENOSYS;
#endif
}

CONFUGA_API int confuga_lookup (confuga *C, const char *path, confuga_fid_t *fid, confuga_off_t *size)
{
	int rc;
	enum CONFUGA_FILE_TYPE type;
	RESOLVE(path)
	debug(D_CONFUGA, "lookup(`%s')", unresolved_path);
	CATCH(lookup(C, path, fid, size, &type));
	PROLOGUE
}

CONFUGA_API int confuga_update (confuga *C, const char *path, confuga_fid_t fid, confuga_off_t size, int flags)
{
	int rc;
	RESOLVE(path)
	debug(D_CONFUGA, "update(`%s', fid = " CONFUGA_FID_PRIFMT ", size = %" PRIuCONFUGA_OFF_T ", flags = %d)", unresolved_path, CONFUGA_FID_PRIARGS(fid), size, flags);
	CATCH(update(C, path, fid, size, flags));
	PROLOGUE
}

CONFUGA_IAPI int confugaN_init (confuga *C)
{
	int rc;
	char path[CONFUGA_PATH_MAX];

	CATCHUNIX(snprintf(path, sizeof(path), "%s/root", C->root));
	CATCHUNIXIGNORE(mkdir(path, S_IRWXU), EEXIST);
	CATCHUNIX(snprintf(path, sizeof(path), "%s/root/.confuga", C->root));
	CATCHUNIXIGNORE(mkdir(path, S_IRWXU), EEXIST);
	CATCHUNIX(snprintf(path, sizeof(path), "%s/root/.confuga/jobs", C->root));
	CATCHUNIXIGNORE(mkdir(path, S_IRWXU), EEXIST);
	CATCHUNIX(snprintf(path, sizeof(path), "%s/root/.confuga/push", C->root));
	CATCHUNIXIGNORE(mkdir(path, S_IRWXU), EEXIST);

	rc = 0;
	goto out;
out:
	return rc;
}

CONFUGA_IAPI int confugaN_special_update (confuga *C, const char *path, confuga_fid_t fid, confuga_off_t size)
{
	int rc;
	char special[CONFUGA_PATH_MAX];
	char resolved[CONFUGA_PATH_MAX];
	char dirname[CONFUGA_PATH_MAX];

	CATCHUNIX(snprintf(special, sizeof(special), "/.confuga/%s", path));
	CATCH(resolve(C, special, resolved));
	path_dirname(resolved, dirname);
	CATCHUNIX(create_dir(dirname, S_IRWXU) ? 0 : -1);
	CATCH(update(C, resolved, fid, size, 0));

	rc = 0;
	goto out;
out:
	return rc;
}

/* vim: set noexpandtab tabstop=4: */
