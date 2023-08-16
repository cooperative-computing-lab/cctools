/*
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "confuga_fs.h"

#include "catch.h"
#include "compat-at.h"
#include "debug.h"
#include "full_io.h"
#include "mkdir_recursive.h"
#include "path.h"
#include "random.h"
#include "unlink_recursive.h"

#include <dirent.h>
#include <fcntl.h>
#include <unistd.h>
#include <utime.h>

#include <sys/file.h>
#include <sys/stat.h>
#if defined(HAS_ATTR_XATTR_H)
#	include <attr/xattr.h>
#elif defined(HAS_SYS_XATTR_H)
#	include <sys/xattr.h>
#endif

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>

#ifdef CCTOOLS_OPSYS_DARWIN
	/* Cygwin does not have 64-bit I/O, while FreeBSD/Darwin has it by default. */
#	define stat64 stat
#	define fstat64 fstat
#	define ftruncate64 ftruncate
#	define statfs64 statfs
#	define fstatfs64 fstatfs
#	define fstatat64 fstatat
#endif

#ifdef CCTOOLS_OPSYS_DARWIN
#	define _fgetxattr(fd,name,data,size) fgetxattr(fd,name,data,size,0,0)
#	define _flistxattr(fd,list,size) flistxattr(fd,list,size,0)
#	define _fremovexattr(fd,name) fremovexattr(fd,name,XATTR_NOFOLLOW)
#	define _fsetxattr(fd,name,data,size,flags) fsetxattr(fd,name,data,size,0,XATTR_NOFOLLOW|flags)
#else
#	define _fgetxattr(fd,name,data,size) fgetxattr(fd,name,data,size)
#	define _flistxattr(fd,list,size) flistxattr(fd,list,size)
#	define _fremovexattr(fd,name) fremovexattr(fd,name)
#	define _fsetxattr(fd,name,data,size,flags) fsetxattr(fd,name,data,size,flags)
#endif

#ifndef O_CLOEXEC
#	define O_CLOEXEC 0
#endif
#ifndef O_DIRECTORY
#	define O_DIRECTORY 0
#endif
#ifndef O_NOFOLLOW
#	define O_NOFOLLOW 0
#endif

static const char nulpath[1] = "";

#define RESOLVE(path, follow) \
	int dirfd_##path;\
	char basename_##path[CONFUGA_PATH_MAX];\
	CATCH(resolve(C, path, &dirfd_##path, basename_##path, follow));\
	strcpy(basename, basename_##path);\
	path = nulpath;\
	dirfd = dirfd_##path;

#define PREAMBLE(fmt, ...) \
	int rc;\
	int dirfd = -1;\
	char basename[CONFUGA_PATH_MAX];\
	debug(D_CONFUGA, fmt, __VA_ARGS__);\
	(void)rc;\
	(void)dirfd;\
	(void)basename[1];

#define PROLOGUE \
	goto out;\
out:\
	debug(D_CONFUGA, "= %d (%s)", rc, strerror(rc));\
	CLOSE_FD(dirfd);\
	return rc;

#define SIMPLE_WRAP_UNIX(follow, expr, fmt, ...) \
	PREAMBLE(fmt, __VA_ARGS__)\
	RESOLVE(path, (follow))\
	rc = UNIXRC(expr);\
	PROLOGUE

static int resolve (confuga *C, const char *path, int *dirfd, char basename[CONFUGA_PATH_MAX], int follow)
{
	int i;
	int rc;
	int fd=-1;
	char working[CONFUGA_PATH_MAX] = "";
	struct stat rootinfo;

	if (path[0] == 0)
		CATCH(EINVAL);

	CATCHUNIX(fstat(C->nsrootfd, &rootinfo));

	CATCHUNIX(fd = dup(C->nsrootfd));

	CATCHUNIX(snprintf(working, sizeof(working), "%s", path));
	if (rc >= CONFUGA_PATH_MAX)
		CATCH(ENAMETOOLONG);

	for (i = 0; i < 100; i++) {
		char component[CONFUGA_PATH_MAX];

		debug(D_DEBUG, "path '%s' resolution: working = '%s'", path, working);
		strcpy(basename, ""); /* mark as incomplete */

		char *slash = strchr(working, '/');
		if (slash) {
			if (slash == working) {
				slash += strspn(slash, "/");
				memmove(working, slash, strlen(slash)+1);
				CATCHUNIX(dup2(C->nsrootfd, fd));
				continue;
			} else {
				size_t len = (size_t)(slash-working);
				memcpy(component, working, len);
				component[len] = 0;
				slash += strspn(slash, "/");
				memmove(working, slash, strlen(slash)+1);
			}
			debug(D_DEBUG, "path '%s' resolution: component = '%s'", path, component);
		} else {
			if (working[0]) {
				strcpy(basename, working);
			} else {
				strcpy(basename, "."); /* refer to dirfd itself */
			}
			debug(D_DEBUG, "path '%s' resolution: final component: %s", path, basename);
			if (!follow)
				break; /* we're done! */
			strcpy(component, working);
			strcpy(working, "");
		}

		if (strcmp(component, "..") == 0) {
			struct stat info;
			CATCHUNIX(fstat(fd, &info));
			if (rootinfo.st_dev == info.st_dev && rootinfo.st_ino == info.st_ino) {
				debug(D_DEBUG, "caught .. at root");
				continue;
			}
		} else if (strcmp(component, ".") == 0) {
			continue;
		} else {
			char _sym[CONFUGA_PATH_MAX] = "";
			char *sym = _sym;
			ssize_t n = readlinkat(fd, component, sym, CONFUGA_PATH_MAX);
			if (n >= 0) {
				if (n < CONFUGA_PATH_MAX) {
					char new[CONFUGA_PATH_MAX];
					debug(D_DEBUG, "path '%s' resolution: component link: '%s' -> '%s'", path, component, sym);
					CATCHUNIX(snprintf(new, CONFUGA_PATH_MAX, "%s/%s", sym, working));
					if (rc >= CONFUGA_PATH_MAX)
						CATCH(ENAMETOOLONG);
					strcpy(working, new);
					continue;
				} else {
					CATCH(ENAMETOOLONG);
				}
			}
		}

		if (basename[0]) {
			break; /* we found the final component and it wasn't a link, we're done */
		}

		if (working[0] == 0) {
			/* On Linux and possibly other kernels, some system calls like
			 * rmdir/mkdir permit a trailing slash. Strictly speaking, this
			 * should always fail since Unix [1] specifies that paths ending in
			 * a forward slash are equivalent to "path/." (i.e. with a trailing
			 * . added). rmdir on a path with a trailing dot shall always fail
			 * [2]. Obviously mkdir on . should also fail with EEXIST.
			 *
			 * At this point, we already removed / after this component, so we
			 * only check if working is empty. The if above checks this. We've
			 * already confirmed component is not a link.
			 *
			 * [1] http://pubs.opengroup.org/onlinepubs/9699919799/basedefs/V1_chap04.html#tag_04_12
			 * [2] http://pubs.opengroup.org/onlinepubs/9699919799/functions/rmdir.html
			*/
			strcpy(basename, component);
			break;
		}

		/* XXX Unavoidable race condition here between readlinkat and openat. O_NOFOLLOW catches it if supported by kernel. */
		/* Solution is using O_PATH if available. */

		int nfd = openat(fd, component, O_RDONLY|O_CLOEXEC|O_DIRECTORY|O_NOFOLLOW|O_NOCTTY, 0);
		CATCHUNIX(nfd);
		close(fd);
		fd = nfd;
#if O_DIRECTORY == 0
		struct stat info;
		CATCHUNIX(fstat(fd, &info));
		if (!S_ISDIR(info.st_mode))
			CATCH(ENOTDIR);
#endif
	}
	if (i == 100)
		CATCH(ELOOP);

	*dirfd = fd;

	rc = 0;
	goto out;
out:
	if (rc) {
		CLOSE_FD(fd);
	}

	return rc;
}

/* Each file in the namespace is stored as a replicated file or a metadata object:
 *
 * Replicated file: "file:<fid>:<length>\n". [Include the length so we can avoid the SQL lookup for the length.
 * Metadata file:   "meta:0000000000000000000000000000000000000000:<length>\n<content>".
 */
#define HEADER_LENGTH (4 /* 'file' | 'meta' */ + 1 /* ':' */ + sizeof(((confuga_fid_t *)0)->id)*2 /* SHA1 hash in hexadecimal */ + 1 /* ':' */ + sizeof(confuga_off_t)*2 /* in hexadecimal */ + 1 /* '\n' */)

CONFUGA_IAPI int confugaN_lookup (confuga *C, int dirfd, const char *basename, confuga_fid_t *fid, confuga_off_t *size, enum CONFUGA_FILE_TYPE *type, int *nlink)
{
	int rc;
	int fd = -1;
	ssize_t n;
	char header[HEADER_LENGTH+1] = "";
	struct stat64 info;

	if (basename[0]) {
		CATCHUNIX(fd = openat(dirfd, basename, O_RDONLY, 0));
	} else {
		fd = dirfd;
	}
	CATCHUNIX(fstat64(fd, &info));
	if (S_ISDIR(info.st_mode))
		CATCH(EISDIR);
	else if (!S_ISREG(info.st_mode))
		CATCH(EINVAL);

	n = full_read(fd, header, HEADER_LENGTH);
	if (n < (ssize_t)HEADER_LENGTH)
		CATCH(EINVAL);
	debug(D_DEBUG, "read %s", header); /* debug chomps final newline */

	const char *current = header;
	if (strncmp(current, "file:", strlen("file:")) == 0)
		*type = CONFUGA_FILE;
	else if (strncmp(current, "meta:", strlen("meta:")) == 0)
		*type = CONFUGA_META;
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
	if (nlink)
		*nlink = info.st_nlink;

	rc = 0;
	goto out;
out:
	if (fd != dirfd)
		CLOSE_FD(fd);
	return rc;
}

static int fupdate (confuga *C, int fd, confuga_fid_t fid, confuga_off_t size)
{
	int rc;
	int n;
	char header[HEADER_LENGTH+1] = "";

	n = snprintf(header, sizeof(header), "file:" CONFUGA_FID_PRIFMT ":%0*" PRIxCONFUGA_OFF_T "\n", CONFUGA_FID_PRIARGS(fid), (int)sizeof(confuga_off_t)*2, (confuga_off_t)size);
	assert((size_t)n == HEADER_LENGTH);
	CATCHUNIX(full_write(fd, header, HEADER_LENGTH));
	if ((size_t)rc < HEADER_LENGTH)
		CATCH(EINVAL); /* FIXME */
	debug(D_DEBUG, "write %s", header); /* debug chomps final newline */

	rc = 0;
	goto out;
out:
	return rc;
}

CONFUGA_IAPI int confugaN_update (confuga *C, int dirfd, const char *basename, confuga_fid_t fid, confuga_off_t size, int flags)
{
	int rc;
	int fd = -1;
	char cname[PATH_MAX] = "";
	char name[PATH_MAX] = "";
	struct stat info;

	assert(basename && basename[0]);

	rc = openat(dirfd, basename, O_WRONLY|O_SYNC, 0);
	if (rc >= 0) {
		fd = rc;
		if ((flags & CONFUGA_O_EXCL)) {
			CATCH(EEXIST);
		} else {
			CATCH(fupdate(C, fd, fid, size));
			rc = 0;
			goto out;
		}
	}

	strcpy(cname, "store/new/");
	random_hex(strchr(cname, '\0'), 41);
	CATCHUNIX(fd = openat(C->rootfd, cname, O_CREAT|O_EXCL|O_WRONLY|O_SYNC, S_IRUSR|S_IWUSR));

	CATCHUNIX(fstat(fd, &info));
	CATCHUNIX(snprintf(name, sizeof name, "store/file/%"PRIu64, (uint64_t)info.st_ino));
	CATCHUNIX(linkat(C->rootfd, cname, C->rootfd, name, 0));

	CATCH(fupdate(C, fd, fid, size));

	if ((flags & CONFUGA_O_EXCL)) {
		CATCHUNIX(linkat(C->rootfd, cname, dirfd, basename, 0));
		CATCHUNIX(unlinkat(C->rootfd, cname, 0));
	} else {
		CATCHUNIX(renameat(C->rootfd, cname, dirfd, basename));
	}

	debug(D_DEBUG, "created new file '%s'", name);

	rc = 0;
	goto out;
out:
	if (rc) {
		unlinkat(C->rootfd, cname, 0);
	}
	CLOSE_FD(fd);
	return rc;
}

CONFUGA_API int confuga_metadata_lookup (confuga *C, const char *path, char **data, size_t *size)
{
	int fd = -1;
	struct stat64 info;
	ssize_t n;
	size_t _size;
	char header[HEADER_LENGTH+1] = "";
	confuga_fid_t fid;

	PREAMBLE("metadata_lookup(`%s')", path)
	RESOLVE(path, 1)

	if (size == NULL)
		size = &_size;

	CATCHUNIX(fd = openat(dirfd, basename, O_RDONLY, 0));
	CATCHUNIX(fstat64(fd, &info));
	if (S_ISDIR(info.st_mode))
		CATCH(EISDIR);
	else if (!S_ISREG(info.st_mode))
		CATCH(EINVAL);

	n = read(fd, header, HEADER_LENGTH);
	if (n < (ssize_t)HEADER_LENGTH)
		CATCH(EINVAL);
	debug(D_DEBUG, "read %s", header); /* debug chomps final newline */

	const char *current = header;
	if (!(strncmp(current, "meta:", strlen("meta:")) == 0))
		CATCH(EINVAL);
	current += 5;

	/* read hash */
	CATCH(confugaF_extract(C, &fid, current, &current));
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
	debug(D_DEBUG, "read '%s'", *data); /* debug chomps final newline */

	rc = 0;
	goto out;
out:
	debug(D_CONFUGA, "= %d (%s)", rc, strerror(rc));
	CLOSE_FD(fd);
	CLOSE_FD(dirfd);
	return rc;
}

CONFUGA_API int confuga_metadata_update (confuga *C, const char *path, const char *data, size_t size)
{
	static const confuga_fid_t fid = {""};

	int fd = -1;
	int n;
	char header[HEADER_LENGTH+1] = "";
	PREAMBLE("metadata_update(`%s')", path)
	RESOLVE(path, 1)

	CATCHUNIX(fd = openat(dirfd, basename, O_CREAT|O_WRONLY|O_TRUNC|O_SYNC, S_IRUSR|S_IWUSR));
	n = snprintf(header, sizeof(header), "meta:" CONFUGA_FID_PRIFMT ":%0*" PRIxCONFUGA_OFF_T "\n", CONFUGA_FID_PRIARGS(fid), (int)sizeof(confuga_off_t)*2, (confuga_off_t)size);
	assert((size_t)n == HEADER_LENGTH);
	CATCHUNIX(full_write(fd, header, HEADER_LENGTH));
	if ((size_t)rc < HEADER_LENGTH)
		CATCH(EINVAL); /* FIXME */
	debug(D_DEBUG, "write %s", header); /* debug chomps final newline */

	CATCHUNIX(full_write(fd, data, size));
	if ((size_t)rc < size)
		CATCH(EINVAL); /* FIXME */
	debug(D_DEBUG, "write '%s'", data); /* debug chomps final newline */

	rc = 0;
	goto out;
out:
	debug(D_CONFUGA, "= %d (%s)", rc, strerror(rc));
	CLOSE_FD(fd);
	CLOSE_FD(dirfd);
	return rc;
}

static int dostat (confuga *C, int dirfd, const char *basename, struct confuga_stat *info, int flag)
{
	int rc;
	struct stat64 linfo;
	enum CONFUGA_FILE_TYPE type;
	CATCHUNIX(fstatat64(dirfd, basename, &linfo, flag));
	if (S_ISREG(linfo.st_mode)) {
		CATCH(confugaN_lookup(C, dirfd, basename, &info->fid, &info->size, &type, NULL));
		if (type == CONFUGA_FILE) {
			debug(D_DEBUG, "%s %d", basename, (int)linfo.st_nlink);
			assert(linfo.st_nlink > 1);
			linfo.st_nlink--;
		}
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
	rc = 0;
	goto out;
out:
	return rc;
}

struct confuga_dir {
	confuga *C;
	DIR *dir;
	struct confuga_dirent dirent;
};

CONFUGA_API int confuga_opendir(confuga *C, const char *path, confuga_dir **D)
{
	int fd = -1;
	DIR *dir = NULL;
	PREAMBLE("opendir(`%s')", path)
	RESOLVE(path, 1)
	*D = malloc(sizeof(confuga_dir));
	if (*D == NULL) CATCH(ENOMEM);
	CATCHUNIX(fd = openat(dirfd, basename, O_CLOEXEC|O_DIRECTORY|O_NOCTTY|O_NOFOLLOW|O_RDONLY, 0));
	dir = fdopendir(fd);
	CATCHUNIX(dir ? 0 : -1);
	debug(D_CONFUGA, "opened dirfd %d", fd);
	(*D)->C = C;
	(*D)->dir = dir;
	rc = 0;
	goto out;
out:
	debug(D_CONFUGA, "= %d (%s)", rc, strerror(rc));
	if (rc) {
		free(*D);
		CLOSE_FD(fd);
	}
	CLOSE_FD(dirfd);
	return rc;
}

CONFUGA_API int confuga_readdir(confuga_dir *dir, struct confuga_dirent **dirent)
{
	int rc;
	debug(D_CONFUGA, "readdir(%d)", dirfd(dir->dir));
	/* N.B. only way to detect an error in readdir is to set errno to 0 and check after. */
	errno = 0;
	struct dirent *d = readdir(dir->dir);
	if (d) {
		assert(strchr(d->d_name, '/') == NULL);
		dir->dirent.name = d->d_name;
		memset(&dir->dirent.info, 0, sizeof(dir->dirent.info));
		dir->dirent.lstatus = dostat(dir->C, dirfd(dir->dir), d->d_name, &dir->dirent.info, AT_SYMLINK_NOFOLLOW);
		*dirent = &dir->dirent;
	} else {
		*dirent = NULL;
		CATCH(errno);
	}
	rc = 0;
	goto out;
out:
	debug(D_CONFUGA, "= %d (%s)", rc, strerror(rc));
	return rc;
}

CONFUGA_API int confuga_closedir(confuga_dir *dir)
{
	int rc;
	DIR *ldir = dir->dir;
	debug(D_CONFUGA, "closedir(%d)", dirfd(dir->dir));
	free(dir);
	CATCHUNIX(closedir(ldir));
	rc = 0;
	goto out;
out:
	debug(D_CONFUGA, "= %d (%s)", rc, strerror(rc));
	return rc;
}

CONFUGA_API int confuga_unlink(confuga *C, const char *path)
{
	SIMPLE_WRAP_UNIX(0, unlinkat(dirfd, basename, 0), "unlink(`%s')", path);
}

CONFUGA_API int confuga_rename(confuga *C, const char *old, const char *path)
{
	PREAMBLE("rename(`%s', `%s')", old, path)
	RESOLVE(old, 0)
	RESOLVE(path, 0)
	dirfd = -1; /* so prologue doesn't close it */
	rc = UNIXRC(renameat(dirfd_old, basename_old, dirfd_path, basename_path)); /* no CATCH so we can close dirfd */
	CLOSE_FD(dirfd_old);
	CLOSE_FD(dirfd_path);
	PROLOGUE
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
	PREAMBLE("link(`%s', `%s')", target, path)
	RESOLVE(target, 0)
	RESOLVE(path, 0)
	dirfd = -1; /* so prologue doesn't close it */
	rc = UNIXRC(linkat(dirfd_target, basename_target, dirfd_path, basename_path, 0)); /* no CATCH so we can close dirfd */
	CLOSE_FD(dirfd_target);
	CLOSE_FD(dirfd_path);
	PROLOGUE
}

CONFUGA_API int confuga_symlink(confuga *C, const char *target, const char *path)
{
	/* `target' is effectively userdata, we do not resolve it */
	SIMPLE_WRAP_UNIX(0, symlinkat(target, dirfd, basename), "symlink(`%s', `%s')", target, path);
}

CONFUGA_API int confuga_readlink(confuga *C, const char *path, char *buf, size_t length)
{
	SIMPLE_WRAP_UNIX(0, readlinkat(dirfd, basename, buf, length), "readlink(`%s', %p, %zu)", path, buf, length);
}

CONFUGA_API int confuga_mkdir(confuga *C, const char *path, int mode)
{
	SIMPLE_WRAP_UNIX(0, mkdirat(dirfd, basename, mode), "mkdir(`%s', %d)", path, mode);
}

CONFUGA_API int confuga_rmdir(confuga *C, const char *path)
{
	SIMPLE_WRAP_UNIX(0, unlinkat(dirfd, basename, AT_REMOVEDIR), "rmdir(`%s')", path);
}

CONFUGA_API int confuga_stat(confuga *C, const char *path, struct confuga_stat *info)
{
	PREAMBLE("stat(`%s', %p)", path, info)
	RESOLVE(path, 1)
	rc = dostat(C, dirfd, basename, info, 0);
	PROLOGUE
}

CONFUGA_API int confuga_lstat(confuga *C, const char *path, struct confuga_stat *info)
{
	PREAMBLE("lstat(`%s', %p)", path, info)
	RESOLVE(path, 0)
	rc = dostat(C, dirfd, basename, info, AT_SYMLINK_NOFOLLOW);
	PROLOGUE
}

CONFUGA_API int confuga_access(confuga *C, const char *path, int mode)
{
	SIMPLE_WRAP_UNIX(1, faccessat(dirfd, basename, mode, 0), "access(`%s', %d)", path, mode);
}

CONFUGA_API int confuga_chmod(confuga *C, const char *path, int mode)
{
	// A remote user can change some of the permissions bits,
	// which only affect local users, but we don't let them
	// take away the owner bits, which would affect the Chirp server.
	mode |= S_IRUSR|S_IWUSR;
	mode &= S_IRWXU|S_IRWXG|S_IRWXO;
	SIMPLE_WRAP_UNIX(0, fchmodat(dirfd, basename, mode, 0), "chmod(`%s', %d)", path, mode);
}

CONFUGA_API int confuga_truncate(confuga *C, const char *path, confuga_off_t length)
{
	static const confuga_fid_t empty = {CONFUGA_FID_EMPTY};

	confuga_fid_t fid;
	confuga_off_t size;
	enum CONFUGA_FILE_TYPE type;
	PREAMBLE("truncate(`%s', %" PRIuCONFUGA_OFF_T ")", path, length)
	RESOLVE(path, 1)
	CATCH(confugaN_lookup(C, dirfd, basename, &fid, &size, &type, NULL));
	if (length > 0)
		CATCH(EINVAL);
	CATCH(confugaN_update(C, dirfd, basename, empty, 0, 0));
	PROLOGUE
}

CONFUGA_API int confuga_utime(confuga *C, const char *path, time_t actime, time_t modtime)
{
	struct timespec times[2] = {{.tv_sec = actime}, {.tv_sec = modtime}};
	SIMPLE_WRAP_UNIX(0, utimensat(dirfd, basename, times, AT_SYMLINK_NOFOLLOW), "utime(`%s', actime = %lu, modtime = %lu)", path, (unsigned long)actime, (unsigned long)modtime);
}

CONFUGA_API int confuga_getxattr(confuga *C, const char *path, const char *name, void *data, size_t size)
{
#if defined(HAS_SYS_XATTR_H) || defined(HAS_ATTR_XATTR_H)
	int fd;
	PREAMBLE("getxattr(`%s', `%s', %p, %zu)", path, name, data, size)
	RESOLVE(path, 1)
	CATCHUNIX(fd = openat(dirfd, basename, O_RDONLY|O_CLOEXEC|O_NOFOLLOW|O_NOCTTY, 0));
#ifdef CCTOOLS_OPSYS_DARWIN
	rc = UNIXRC(fgetxattr(fd, name, data, size, 0, 0));
#else
	rc = UNIXRC(fgetxattr(fd, name, data, size));
#endif
	CLOSE_FD(fd);
	PROLOGUE
#else
	return ENOSYS;
#endif
}

CONFUGA_API int confuga_lgetxattr(confuga *C, const char *path, const char *name, void *data, size_t size)
{
#if defined(HAS_SYS_XATTR_H) || defined(HAS_ATTR_XATTR_H)
	int fd;
	PREAMBLE("lgetxattr(`%s', `%s', %p, %zu)", path, name, data, size)
	RESOLVE(path, 0)
	CATCHUNIX(fd = openat(dirfd, basename, O_RDONLY|O_CLOEXEC|O_NOFOLLOW|O_NOCTTY, 0));
#ifdef CCTOOLS_OPSYS_DARWIN
	rc = UNIXRC(fgetxattr(fd, name, data, size, 0, 0));
#else
	rc = UNIXRC(fgetxattr(fd, name, data, size));
#endif
	CLOSE_FD(fd);
	PROLOGUE
#else
	return ENOSYS;
#endif
}

CONFUGA_API int confuga_listxattr(confuga *C, const char *path, char *list, size_t size)
{
#if defined(HAS_SYS_XATTR_H) || defined(HAS_ATTR_XATTR_H)
	int fd;
	PREAMBLE("listxattr(`%s', %p, %zu)", path, list, size)
	RESOLVE(path, 1)
	CATCHUNIX(fd = openat(dirfd, basename, O_RDONLY|O_CLOEXEC|O_NOFOLLOW|O_NOCTTY, 0));
#ifdef CCTOOLS_OPSYS_DARWIN
	rc = UNIXRC(flistxattr(fd, list, size, 0));
#else
	rc = UNIXRC(flistxattr(fd, list, size));
#endif
	CLOSE_FD(fd);
	PROLOGUE
#else
	return ENOSYS;
#endif
}

CONFUGA_API int confuga_llistxattr(confuga *C, const char *path, char *list, size_t size)
{
#if defined(HAS_SYS_XATTR_H) || defined(HAS_ATTR_XATTR_H)
	int fd;
	PREAMBLE("llistxattr(`%s', %p, %zu)", path, list, size)
	RESOLVE(path, 0)
	CATCHUNIX(fd = openat(dirfd, basename, O_RDONLY|O_CLOEXEC|O_NOFOLLOW|O_NOCTTY, 0));
#ifdef CCTOOLS_OPSYS_DARWIN
	rc = UNIXRC(flistxattr(fd, list, size, 0));
#else
	rc = UNIXRC(flistxattr(fd, list, size));
#endif
	CLOSE_FD(fd);
	PROLOGUE
#else
	return ENOSYS;
#endif
}

CONFUGA_API int confuga_setxattr(confuga *C, const char *path, const char *name, const void *data, size_t size, int flags)
{
#if defined(HAS_SYS_XATTR_H) || defined(HAS_ATTR_XATTR_H)
	int fd;
	PREAMBLE("setxattr(`%s', `%s', %p, %zu, %d)", path, name, data, size, flags)
	RESOLVE(path, 1)
	CATCHUNIX(fd = openat(dirfd, basename, O_RDONLY|O_CLOEXEC|O_NOFOLLOW|O_NOCTTY, 0));
#ifdef CCTOOLS_OPSYS_DARWIN
	rc = UNIXRC(fsetxattr(fd, name, data, size, 0, flags));
#else
	rc = UNIXRC(fsetxattr(fd, name, data, size, flags));
#endif
	CLOSE_FD(fd);
	PROLOGUE
#else
	return ENOSYS;
#endif
}

CONFUGA_API int confuga_lsetxattr(confuga *C, const char *path, const char *name, const void *data, size_t size, int flags)
{
#if defined(HAS_SYS_XATTR_H) || defined(HAS_ATTR_XATTR_H)
	int fd;
	PREAMBLE("lsetxattr(`%s', `%s', %p, %zu, %d)", path, name, data, size, flags)
	RESOLVE(path, 0)
	CATCHUNIX(fd = openat(dirfd, basename, O_RDONLY|O_CLOEXEC|O_NOFOLLOW|O_NOCTTY, 0));
#ifdef CCTOOLS_OPSYS_DARWIN
	rc = UNIXRC(fsetxattr(fd, name, data, size, 0, flags));
#else
	rc = UNIXRC(fsetxattr(fd, name, data, size, flags));
#endif
	CLOSE_FD(fd);
	PROLOGUE
#else
	return ENOSYS;
#endif
}

CONFUGA_API int confuga_removexattr(confuga *C, const char *path, const char *name)
{
#if defined(HAS_SYS_XATTR_H) || defined(HAS_ATTR_XATTR_H)
	int fd;
	PREAMBLE("removexattr(`%s', `%s')", path, name)
	RESOLVE(path, 1)
	CATCHUNIX(fd = openat(dirfd, basename, O_RDONLY|O_CLOEXEC|O_NOFOLLOW|O_NOCTTY, 0));
#ifdef CCTOOLS_OPSYS_DARWIN
	rc = UNIXRC(fremovexattr(fd, name, 0));
#else
	rc = UNIXRC(fremovexattr(fd, name));
#endif
	CLOSE_FD(fd);
	PROLOGUE
#else
	return ENOSYS;
#endif
}

CONFUGA_API int confuga_lremovexattr(confuga *C, const char *path, const char *name)
{
#if defined(HAS_SYS_XATTR_H) || defined(HAS_ATTR_XATTR_H)
	int fd;
	PREAMBLE("lremovexattr(`%s', `%s')", path, name)
	RESOLVE(path, 0)
	CATCHUNIX(fd = openat(dirfd, basename, O_RDONLY|O_CLOEXEC|O_NOFOLLOW|O_NOCTTY, 0));
#ifdef CCTOOLS_OPSYS_DARWIN
	rc = UNIXRC(fremovexattr(fd, name, 0));
#else
	rc = UNIXRC(fremovexattr(fd, name));
#endif
	CLOSE_FD(fd);
	PROLOGUE
#else
	return ENOSYS;
#endif
}

CONFUGA_API int confuga_lookup (confuga *C, const char *path, confuga_fid_t *fid, confuga_off_t *size)
{
	enum CONFUGA_FILE_TYPE type;
	PREAMBLE("lookup(`%s')", path)
	RESOLVE(path, 1)
	CATCH(confugaN_lookup(C, dirfd, basename, fid, size, &type, NULL));
	PROLOGUE
}

CONFUGA_API int confuga_update (confuga *C, const char *path, confuga_fid_t fid, confuga_off_t size, int flags)
{
	PREAMBLE("update(`%s', fid = " CONFUGA_FID_PRIFMT ", size = %" PRIuCONFUGA_OFF_T ", flags = %d)", path, CONFUGA_FID_PRIARGS(fid), size, flags)
	RESOLVE(path, 1)
	CATCH(confugaN_update(C, dirfd, basename, fid, size, flags));
	PROLOGUE
}

static int loadtostore (confuga *C, int filefd, int dfd, const char *basename)
{
	int rc;
	int fd = -1;
	DIR *dir = NULL;
	struct stat info;

	CATCHUNIX(fd = openat(dfd, basename, O_CLOEXEC|O_NOCTTY|O_RDONLY, 0));
	CATCHUNIX(fstat(fd, &info));

	if (S_ISDIR(info.st_mode)) {
		struct dirent *dent;
		dir = fdopendir(fd);
		CATCHUNIX(dir ? 0 : -1);
		fd = -1;

		debug(D_DEBUG, "reading directory %s", basename);
		while (1) {
			errno = 0;
			dent = readdir(dir);
			if (dent) {
				assert(strchr(dent->d_name, '/') == NULL);
				if (strcmp(dent->d_name, ".") == 0 || strcmp(dent->d_name, "..") == 0)
					continue;
				CATCH(loadtostore(C, filefd, dirfd(dir), dent->d_name));
			} else {
				CATCH(errno);
				break;
			}
		}
	} else if (S_ISREG(info.st_mode)) {
		confuga_fid_t fid;
		confuga_off_t size;
		enum CONFUGA_FILE_TYPE type;
		CATCH(confugaN_lookup(C, fd, "", &fid, &size, &type, NULL));
		if (type == CONFUGA_FILE) {
			char name[PATH_MAX];
			CATCHUNIX(snprintf(name, sizeof name, "%"PRIu64, (uint64_t)info.st_ino));
			debug(D_DEBUG, "adding %s to file store as %s", basename, name);
			CATCHUNIXIGNORE(linkat(dfd, basename, filefd, name, AT_SYMLINK_NOFOLLOW), EEXIST);
		}
	} else if (!S_ISLNK(info.st_mode)) {
		fatal("found invalid file in namespace: %s", basename);
	}

	rc = 0;
	goto out;
out:
	CLOSE_FD(fd);
	CLOSE_DIR(dir);
	return rc;
}

static int mkfilestore (confuga *C)
{
	int rc;
	int rootfd = -1;
	int filefd = -1;
	int created = 0;

	CATCHUNIX(rootfd = openat(C->rootfd, ".", O_CLOEXEC|O_NOCTTY|O_RDONLY, 0));
	CATCHUNIX(flock(rootfd, LOCK_EX)); /* released by close */

	CATCHUNIXIGNORE(mkdirat(C->rootfd, "store", S_IRWXU), EEXIST);
	if (rc == EEXIST) {
		rc = 0;
		goto out;
	}
	created = 1;

	debug(D_DEBUG, "building file store");
	CATCHUNIX(mkdirat(C->rootfd, "store/new", S_IRWXU));
	CATCHUNIX(symlinkat("file.0", C->rootfd, "store/file"));
	CATCHUNIX(mkdirat(C->rootfd, "store/file.0", S_IRWXU));
	CATCHUNIX(filefd = openat(C->rootfd, "store/file/.", O_CLOEXEC|O_NOCTTY|O_RDONLY, 0));
	CATCH(loadtostore(C, filefd, C->rootfd, "root")); /* initialize (and handle older versions of Confuga) */

	rc = 0;
	goto out;
out:
	if (rc) {
		if (created) {
			unlinkat_recursive(C->rootfd, "store");
		}
		fatal("could not create file store: %s", strerror(rc));
	}
	CLOSE_FD(rootfd);
	CLOSE_FD(filefd);
	return rc;
}

CONFUGA_IAPI int confugaN_init (confuga *C, const char *root)
{
	PREAMBLE("init(`%s')", root)
	CATCHUNIX(mkdir_recursive(root, S_IRWXU));
	CATCHUNIX(C->rootfd = open(root, O_CLOEXEC|O_DIRECTORY|O_NOCTTY|O_RDONLY));
	CATCHUNIX(snprintf(C->root, sizeof C->root, "%s", root));
	CATCHUNIXIGNORE(mkdirat(C->rootfd, "root", S_IRWXU), EEXIST);
	CATCHUNIX(C->nsrootfd = openat(C->rootfd, "root", O_CLOEXEC|O_DIRECTORY|O_NOCTTY|O_RDONLY, 0));
	CATCH(mkfilestore(C));
	rc = 0;
	PROLOGUE
}

/* vim: set noexpandtab tabstop=8: */
