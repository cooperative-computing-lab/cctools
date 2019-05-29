/*
Copyright (C) 2008- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "chirp_acl.h"
#include "chirp_filesystem.h"
#include "chirp_protocol.h"

#include "chirp_fs_chirp.h"
#include "chirp_fs_hdfs.h"
#include "chirp_fs_local.h"
#include "chirp_fs_confuga.h"

#include "debug.h"
#include "macros.h"
#include "buffer.h"
#include "path.h"
#include "pattern.h"
#include "xxmalloc.h"
#include "md5.h"
#include "sha1.h"
#include "stringtools.h"

#include <fnmatch.h>

#include <sys/stat.h>

#include <assert.h>
#include <errno.h>
#include <stdarg.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>

#define CHIRP_FILESYSTEM_BUFFER  65536

struct chirp_filesystem *cfs = NULL;
char chirp_url[CHIRP_PATH_MAX] = "local://./";

struct CHIRP_FILE {
	enum {
		LOCAL,
		CFS,
	} type;
	union {
		struct {
			INT64_T fd;
			INT64_T offset;
			buffer_t B;
			char read[CHIRP_FILESYSTEM_BUFFER];
			INT64_T read_n;
			int error;
		} cfile;
		FILE *lfile;
	} f;
};

#define strprfx(s,p) (strncmp(s,p "",sizeof(p)-1) == 0)
struct chirp_filesystem *cfs_lookup(const char *url)
{
	if(strprfx(url, "chirp://")) {
		return &chirp_fs_chirp;
	} else if(strprfx(url, "hdfs://")) {
		return &chirp_fs_hdfs;
	} else if(strprfx(url, "confuga://")) {
		return &chirp_fs_confuga;
	} else {
		/* always interpret as a local url */
		return &chirp_fs_local;
	}
}

void cfs_normalize(char url[CHIRP_PATH_MAX])
{
	char *root = NULL;
	char *rest = NULL;

	if(strprfx(url, "chirp:")) {
		return;
	} else if(strprfx(url, "hdfs:")) {
		return;
	} else if(pattern_match(url, "^confuga://([^?]*)(.*)", &root, &rest) >= 0) {
		char absolute[PATH_MAX];
		path_absolute(root, absolute, 0);
		debug(D_CHIRP, "normalizing url `%s' as `confuga://%s%s'", url, absolute, rest);
		string_nformat(url, CHIRP_PATH_MAX, "confuga://%s%s", absolute, rest);
	} else {
		char absolute[PATH_MAX];
		if(strprfx(url, "file:") || strprfx(url, "local:"))
			path_absolute(strstr(url, ":") + 1, absolute, 0);
		else
			path_absolute(url, absolute, 0);
		debug(D_CHIRP, "normalizing url `%s' as `local://%s'", url, absolute);
		strcpy(url, "local://");
		strcat(url, absolute);
	}
	free(root);
	free(rest);
}

CHIRP_FILE *cfs_fopen(const char *path, const char *mode)
{
	CHIRP_FILE *file;
	INT64_T fd;
	INT64_T flags = 0;

	if(strchr(mode, '+')) {
		errno = ENOTSUP;
		return 0;
	}

	if(strchr(mode, 'r'))
		flags |= O_RDONLY;
	else if(strchr(mode, 'w'))
		flags |= O_WRONLY | O_CREAT | O_TRUNC;
	else if(strchr(mode, 'a'))
		flags |= O_APPEND | O_CREAT;
	else {
		errno = EINVAL;
		return 0;
	}

	fd = cfs->open(path, flags, 0600);
	if(fd == -1)
		return NULL;

	file = xxmalloc(sizeof(CHIRP_FILE));
	file->type = CFS;
	buffer_init(&file->f.cfile.B);
	buffer_abortonfailure(&file->f.cfile.B, 1);
	file->f.cfile.fd = fd;
	file->type = CFS;
	file->f.cfile.offset = file->f.cfile.read_n = 0;
	file->f.cfile.error = 0;
	memset(file->f.cfile.read, '\0', sizeof(file->f.cfile.read));
	return file;
}

CHIRP_FILE *cfs_fopen_local(const char *path, const char *mode)
{
	CHIRP_FILE *file;
	FILE *lfile;

	lfile = fopen(path, mode);
	if(lfile == NULL)
		return NULL;

	file = xxmalloc(sizeof(CHIRP_FILE));
	file->type = LOCAL;
	file->f.lfile = lfile;
	return file;
}

int cfs_fflush(CHIRP_FILE * file)
{
	size_t size;
	const char *content;

	if(file->type == LOCAL)
		return fflush(file->f.lfile);

	content = buffer_tolstring(&file->f.cfile.B, &size);

	while((INT64_T) size > file->f.cfile.offset) {	/* finish all writes */
		int w = cfs->pwrite(file->f.cfile.fd, content, size, file->f.cfile.offset);
		if(w == -1) {
			file->f.cfile.error = EIO;
			return EOF;
		}
		file->f.cfile.offset += w;
	}
	return 0;
}

int cfs_fclose(CHIRP_FILE * file)
{
	if(file == NULL)
		return 0;

	if(file->type == LOCAL)
		return fclose(file->f.lfile);

	if(cfs_fflush(file) != 0)
		return EOF;

	buffer_free(&file->f.cfile.B);
	cfs->close(file->f.cfile.fd);
	free(file);

	return 0;
}

/* Easy fprintf wrapper for buffers. We actually write on close */
void cfs_fprintf(CHIRP_FILE * file, const char *format, ...)
{
	va_list va;
	va_start(va, format);
	if(file->type == LOCAL)
		vfprintf(file->f.lfile, format, va);
	else
		buffer_vprintf(&file->f.cfile.B, format, va);
	va_end(va);
}

size_t cfs_fwrite(const void *ptr, size_t size, size_t nitems, CHIRP_FILE * file)
{
	size_t bytes = 0, nbytes = size * nitems;
	if(file->type == LOCAL)
		return fwrite(ptr, size, nitems, file->f.lfile);
	for(; bytes < nbytes; bytes++)
		buffer_putfstring(&file->f.cfile.B, "%c", (int) (((const char *) ptr)[bytes]));
	return nbytes;
}

/* WARNING: fread does not use the fgets buffer!! */
size_t cfs_fread(void *ptr, size_t size, size_t nitems, CHIRP_FILE * file)
{
	if(file->type == LOCAL) {
		return fread(ptr, size, nitems, file->f.lfile);
	} else {
		size_t btotal = size * nitems;
		size_t bavail = btotal;

		while(bavail > size) {
			INT64_T t = cfs->pread(file->f.cfile.fd, ptr, bavail, file->f.cfile.offset);
			if(t == -1) {
				if(errno == EINTR) {
					continue;
				} else {
					file->f.cfile.error = errno;
					break;
				}
			} else if(t == 0) {
				break;
			}
			assert(0 < t && t < (INT64_T) bavail);
			file->f.cfile.offset += t;
			bavail -= t;
			ptr = (char *) ptr + size;
		}
		return (btotal - bavail) / size;
	}
}

char *cfs_fgets(char *s, int n, CHIRP_FILE * file)
{
	char *current = s;
	INT64_T i, empty = file->f.cfile.read_n == 0;

	if(file->type == LOCAL)
		return fgets(s, n, file->f.lfile);

	for(i = 0; i < file->f.cfile.read_n; i++)
		if(i + 2 >= n || file->f.cfile.read[i] == '\n') {	/* we got data now */
			memcpy(s, file->f.cfile.read, i + 1);
			s[i + 1] = '\0';
			memmove(file->f.cfile.read, file->f.cfile.read + i + 1, (file->f.cfile.read_n -= i + 1));
			return s;
		}
	memcpy(current, file->f.cfile.read, i);
	current += i;
	n -= i;
	file->f.cfile.read_n = 0;

	i = cfs->pread(file->f.cfile.fd, file->f.cfile.read, CHIRP_FILESYSTEM_BUFFER - 1, file->f.cfile.offset);
	if(i == -1) {
		file->f.cfile.error = errno;
		return 0;
	} else if(i == 0 && empty) {
		return NULL;
	} else if(i == 0) {
		return s;
	}

	file->f.cfile.read_n += i;
	file->f.cfile.offset += i;

	if(cfs_fgets(current, n, file) == NULL)	/* some error */
		return NULL;
	else
		return s;
}

int cfs_ferror(CHIRP_FILE * file)
{
	if(file->type == LOCAL)
		return ferror(file->f.lfile);
	else
		return file->f.cfile.error;
}

/* copy pasta from dttools/src/create_dir.c */
int cfs_create_dir(const char *path, int mode)
{
	char *temp;
	char *delim;
	char oldchar;
	int result;

	temp = malloc(strlen(path) + 1);
	strcpy(temp, path);

	delim = temp;

	while((delim = strchr(delim, '/'))) {

		if(delim == temp) {
			delim++;
			continue;
		}

		oldchar = *delim;
		*delim = 0;

		result = cfs->mkdir(temp, mode);
		if(result != 0) {
			if(errno == EEXIST) {
				/* no problem, keep going */
			} else {
				free(temp);
				return 0;
			}
		} else {
			/* ok, made it successfully */
		}

		*delim = oldchar;
		delim++;
	}

	/* Now, last chance */

	result = cfs->mkdir(temp, mode);
	free(temp);
	if(result != 0) {
		if(errno == EEXIST) {
			return 1;
		} else {
			return 0;
		}
	} else {
		return 1;
	}
}

int cfs_freadall(CHIRP_FILE * f, buffer_t * B)
{
	size_t n;
	char buf[BUFSIZ];

	while((n = cfs_fread(buf, sizeof(char), sizeof(buf), f)) > 0) {
		if(buffer_putlstring(B, buf, n) == -1)
			return 0;
	}

	return cfs_ferror(f) ? 0 : 1;
}

static int do_stat(const char *filename, struct chirp_stat *buf)
{
	int result;
	do {
		result = cfs->stat(filename, buf);
	} while(result == -1 && errno == EINTR);
	return result;
}

/*
Note that cfs_isdir() is not quite the same as !cfs_isnotdir(),
note the slight difference in errno for the client.
*/

int cfs_isdir(const char *filename)
{
	struct chirp_stat info;

	if(do_stat(filename, &info) == 0) {
		if(S_ISDIR(info.cst_mode)) {
			return 1;
		} else {
			errno = ENOTDIR;
			return 0;
		}
	} else {
		return 0;
	}
}

int cfs_isnotdir(const char *filename)
{
	struct chirp_stat info;

	if(do_stat(filename, &info) == 0) {
		if(S_ISDIR(info.cst_mode)) {
			errno = EISDIR;
			return 0;
		} else {
			return 1;
		}
	} else {
		return 1;
	}
}

INT64_T cfs_file_size(const char *path)
{
	struct chirp_stat info;

	if(cfs->stat(path, &info) >= 0) {
		return info.cst_size;
	} else {
		return -1;
	}
}

INT64_T cfs_fd_size(int fd)
{
	struct chirp_stat info;

	if(cfs->fstat(fd, &info) >= 0) {
		return info.cst_size;
	} else {
		return -1;
	}
}

int cfs_exists(const char *path)
{
	struct chirp_stat statbuf;
	if(cfs->lstat(path, &statbuf) == 0) {
		return 1;
	} else {
		return 0;
	}
}

INT64_T cfs_basic_chown(const char *path, INT64_T uid, INT64_T gid)
{
	// Changing file ownership is silently ignored,
	// because permissions are handled through the ACL model.
	return 0;
}

INT64_T cfs_basic_lchown(const char *path, INT64_T uid, INT64_T gid)
{
	// Changing file ownership is silently ignored,
	// because permissions are handled through the ACL model.
	return 0;
}

INT64_T cfs_basic_fchown(int fd, INT64_T uid, INT64_T gid)
{
	// Changing file ownership is silently ignored,
	// because permissions are handled through the ACL model.
	return 0;
}

INT64_T cfs_basic_hash(const char *path, const char *algorithm, unsigned char digest[CHIRP_DIGEST_MAX])
{
	int fd;
	INT64_T result;
	struct chirp_stat info;

	union {
		md5_context_t md5;
		sha1_context_t sha1;
	} context;
	enum { MD5, SHA1 } type;

	if(strcmp(algorithm, "md5") == 0) {
		type = MD5;
		md5_init(&context.md5);
	} else if(strcmp(algorithm, "sha1") == 0) {
		type = SHA1;
		sha1_init(&context.sha1);
	} else {
		return (errno = EINVAL, -1);
	}

	result = cfs->stat(path, &info);
	if(result < 0)
		return result;

	if(S_ISDIR(info.cst_mode)) {
		errno = EISDIR;
		return -1;
	}

	fd = cfs->open(path, O_RDONLY, 0);
	if(fd >= 0) {
		INT64_T total = 0;
		INT64_T length = info.cst_size;

		while(length > 0) {
			char buffer[65536];
			INT64_T chunk = MIN((int) sizeof(buffer), length);

			INT64_T ractual = cfs->pread(fd, buffer, chunk, total);
			if(ractual <= 0)
				break;

			if(type == MD5)
				md5_update(&context.md5, buffer, ractual);
			else if(type == SHA1)
				sha1_update(&context.sha1, buffer, ractual);

			length -= ractual;
			total += ractual;
		}
		cfs->close(fd);

		if(type == MD5) {
			md5_final(digest, &context.md5);
			return MD5_DIGEST_LENGTH;
		} else if(type == SHA1) {
			sha1_final(digest, &context.sha1);
			return SHA1_DIGEST_LENGTH;
		} else
			assert(0);
	}
	return -1;
}

INT64_T cfs_basic_rmall(const char *path)
{
	INT64_T rc = cfs->unlink(path);
	if(rc == -1 && (errno == EISDIR || errno == EPERM)) {
		struct chirp_dir *dir = cfs->opendir(path);
		if(dir) {
			struct chirp_dirent *d;
			rc = 0;
			while(rc == 0 && (d = cfs->readdir(dir))) {
				if(strcmp(d->name, ".") != 0 && strcmp(d->name, "..") != 0) {
					char subpath[PATH_MAX];
					string_nformat(subpath, sizeof(subpath), "%s/%s", path, d->name);
					rc = cfs_basic_rmall(subpath);
					if(rc == -1) {
						cfs->closedir(dir);
						return -1;
					}
				}
			}
			cfs->closedir(dir);
			rc = cfs->rmdir(path);
		}
	}
	return rc;
}

INT64_T cfs_basic_sread(int fd, void *vbuffer, INT64_T length, INT64_T stride_length, INT64_T stride_skip, INT64_T offset)
{
	INT64_T total = 0;
	INT64_T actual = 0;
	char *buffer = vbuffer;

	if(stride_length < 0 || stride_skip < 0 || offset < 0) {
		errno = EINVAL;
		return -1;
	}

	while(length >= stride_length) {
		actual = cfs->pread(fd, &buffer[total], stride_length, offset);
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

INT64_T cfs_basic_swrite(int fd, const void *vbuffer, INT64_T length, INT64_T stride_length, INT64_T stride_skip, INT64_T offset)
{
	INT64_T total = 0;
	INT64_T actual = 0;
	const char *buffer = vbuffer;

	if(stride_length < 0 || stride_skip < 0 || offset < 0) {
		errno = EINVAL;
		return -1;
	}

	while(length >= stride_length) {
		actual = cfs->pwrite(fd, &buffer[total], stride_length, offset);
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

static int search_to_access(int flags)
{
	int access_flags = F_OK;
	if(flags & CHIRP_SEARCH_R_OK)
		access_flags |= R_OK;
	if(flags & CHIRP_SEARCH_W_OK)
		access_flags |= W_OK;
	if(flags & CHIRP_SEARCH_X_OK)
		access_flags |= X_OK;

	return access_flags;
}

static int search_match_file(const char *pattern, const char *name)
{
	debug(D_DEBUG, "search_match_file(`%s', `%s')", pattern, name);
	/* Decompose the pattern in atoms which are each matched against. */
	while(1) {
		char atom[CHIRP_PATH_MAX];
		const char *end = strchr(pattern, '|');	/* disjunction operator */

		memset(atom, 0, sizeof(atom));
		if(end)
			strncpy(atom, pattern, (size_t) (end - pattern));
		else
			strcpy(atom, pattern);

		/* Here we might have a pattern like '*' which matches any file so we
		 * iteratively pull leading components off of `name' until we get a
		 * match.  In the case of '*', we would pull off all leading components
		 * until we reach the file name, which would always match '*'.
		 */
		const char *test = name;
		do {
			int result = fnmatch(atom, test, FNM_PATHNAME);
			debug(D_DEBUG, "fnmatch(`%s', `%s', FNM_PATHNAME) = %d", atom, test, result);
			if(result == 0) {
				return 1;
			}
			test = strchr(test, '/');
			if(test)
				test += 1;
		} while(test);

		if(end)
			pattern = end + 1;
		else
			break;
	}

	return 0;
}

static int search_should_recurse(const char *base, const char *pattern)
{
	debug(D_DEBUG, "search_should_recurse(base = `%s', pattern = `%s')", base, pattern);
	/* Decompose the pattern in atoms which are each matched against. */
	while(1) {
		char atom[CHIRP_PATH_MAX];

		if(*pattern != '/')
			return 1;	/* unanchored pattern is always recursive */

		const char *end = strchr(pattern, '|');	/* disjunction operator */
		memset(atom, 0, sizeof(atom));
		if(end)
			strncpy(atom, pattern, (size_t) (end - pattern));
		else
			strcpy(atom, pattern);

		/* Here we want to determine if `base' matches earlier parts of
		 * `pattern' to see if we should recurse in the directory `base'. To do
		 * this, we strip off final parts of `pattern' until we get a match.
		 */
		while(*atom) {
			int result = fnmatch(atom, base, FNM_PATHNAME);
			debug(D_DEBUG, "fnmatch(`%s', `%s', FNM_PATHNAME) = %d", atom, base, result);
			if(result == 0) {
				return 1;
			}
			char *last = strrchr(atom, '/');
			if(last) {
				*last = '\0';
			} else {
				break;
			}
		}

		if(end)
			pattern = end + 1;
		else
			break;
	}
	return 0;
}

static int search_directory(const char *subject, const char *const base, char fullpath[CHIRP_PATH_MAX], const char *pattern, int flags, struct link *l, time_t stoptime)
{
	if(strlen(pattern) == 0)
		return 0;

	debug(D_DEBUG, "search_directory(subject = `%s', base = `%s', fullpath = `%s', pattern = `%s', flags = %d, ...)", subject, base, fullpath, pattern, flags);

	int access_flags = search_to_access(flags);
	int includeroot = flags & CHIRP_SEARCH_INCLUDEROOT;
	int metadata = flags & CHIRP_SEARCH_METADATA;
	int stopatfirst = flags & CHIRP_SEARCH_STOPATFIRST;

	int result = 0;
	struct chirp_dir *dirp = cfs->opendir(fullpath);
	char *current = fullpath + strlen(fullpath);	/* point to end to current directory */

	if(dirp) {
		errno = 0;
		struct chirp_dirent *entry;
		while((entry = cfs->readdir(dirp))) {
			char *name = entry->name;

			if(strcmp(name, ".") == 0 || strcmp(name, "..") == 0 || strncmp(name, ".__", 3) == 0)
				continue;
			sprintf(current, "/%s", name);

			if(search_match_file(pattern, base)) {
				const char *matched;
				if(includeroot) {
					if(base - fullpath == 1 && fullpath[0] == '/') {
						matched = base;
					} else {
						matched = fullpath;
					}
				} else {
					matched = base;
				}

				result += 1;
				if(access_flags == F_OK || cfs->access(fullpath, access_flags) == 0) {
					if(metadata) {
						/* A match was found, but the matched file couldn't be statted. Generate a result and an error. */
						if(entry->lstatus == -1) {
							link_putfstring(l, "0:%s::\n", stoptime, matched);	// FIXME is this a bug?
							link_putfstring(l, "%d:%d:%s:\n", stoptime, errno, CHIRP_SEARCH_ERR_STAT, matched);
						} else {
							BUFFER_STACK_ABORT(B, 4096)
							chirp_stat_encode(B, &entry->info);
							link_putfstring(l, "0:%s:%s:\n", stoptime, matched, buffer_tostring(B));
							if(stopatfirst)
								return 1;
						}
					} else {
						link_putfstring(l, "0:%s::\n", stoptime, matched);
						if(stopatfirst)
							return 1;
					}
				}	/* FIXME access failure */
			}

			if(cfs_isdir(fullpath) && search_should_recurse(base, pattern)) {
				if(chirp_acl_check_dir(fullpath, subject, CHIRP_ACL_LIST)) {
					int n = search_directory(subject, base, fullpath, pattern, flags, l, stoptime);
					if(n > 0) {
						result += n;
						if(stopatfirst)
							return result;
					}
				} else {
					link_putfstring(l, "%d:%d:%s:\n", stoptime, EPERM, CHIRP_SEARCH_ERR_OPEN, fullpath);
				}
			}
			*current = '\0';	/* clear current entry */
			errno = 0;
		}

		if(errno)
			link_putfstring(l, "%d:%d:%s:\n", stoptime, errno, CHIRP_SEARCH_ERR_READ, fullpath);

		errno = 0;
		cfs->closedir(dirp);
		if(errno)
			link_putfstring(l, "%d:%d:%s:\n", stoptime, errno, CHIRP_SEARCH_ERR_CLOSE, fullpath);
	} else {
		link_putfstring(l, "%d:%d:%s:\n", stoptime, errno, CHIRP_SEARCH_ERR_OPEN, fullpath);
	}

	return result;
}

/* Note we need the subject because we must check the ACL for any nested directories. */
INT64_T cfs_basic_search(const char *subject, const char *dir, const char *pattern, int flags, struct link * l, time_t stoptime)
{
	char fullpath[CHIRP_PATH_MAX];
	strcpy(fullpath, dir);
	path_remove_trailing_slashes(fullpath);	/* this prevents double slashes from appearing in paths we examine. */

	debug(D_DEBUG, "cfs_basic_search(subject = `%s', dir = `%s', pattern = `%s', flags = %d, ...)", subject, dir, pattern, flags);

	/* FIXME we should still check for literal paths to search since we can optimize that */
	return search_directory(subject, fullpath + strlen(fullpath), fullpath, pattern, flags, l, stoptime);
}

void cfs_stub_destroy(void)
{
	return;
}

INT64_T cfs_stub_lockf(int fd, int cmd, INT64_T len)
{
	errno = ENOSYS;
	return -1;
}

INT64_T cfs_stub_getxattr(const char *path, const char *name, void *data, size_t size)
{
	errno = ENOSYS;
	return -1;
}

INT64_T cfs_stub_fgetxattr(int fd, const char *name, void *data, size_t size)
{
	errno = ENOSYS;
	return -1;
}

INT64_T cfs_stub_lgetxattr(const char *path, const char *name, void *data, size_t size)
{
	errno = ENOSYS;
	return -1;
}

INT64_T cfs_stub_listxattr(const char *path, char *list, size_t size)
{
	errno = ENOSYS;
	return -1;
}

INT64_T cfs_stub_flistxattr(int fd, char *list, size_t size)
{
	errno = ENOSYS;
	return -1;
}

INT64_T cfs_stub_llistxattr(const char *path, char *list, size_t size)
{
	errno = ENOSYS;
	return -1;
}

INT64_T cfs_stub_setxattr(const char *path, const char *name, const void *data, size_t size, int flags)
{
	errno = ENOSYS;
	return -1;
}

INT64_T cfs_stub_fsetxattr(int fd, const char *name, const void *data, size_t size, int flags)
{
	errno = ENOSYS;
	return -1;
}

INT64_T cfs_stub_lsetxattr(const char *path, const char *name, const void *data, size_t size, int flags)
{
	errno = ENOSYS;
	return -1;
}

INT64_T cfs_stub_removexattr(const char *path, const char *name)
{
	errno = ENOSYS;
	return -1;
}

INT64_T cfs_stub_fremovexattr(int fd, const char *name)
{
	errno = ENOSYS;
	return -1;
}

INT64_T cfs_stub_lremovexattr(const char *path, const char *name)
{
	errno = ENOSYS;
	return -1;
}

int cfs_stub_job_dbinit(sqlite3 * db)
{
	return ENOSYS;
}

int cfs_stub_job_schedule(sqlite3 * db)
{
	return ENOSYS;
}

/* vim: set noexpandtab tabstop=4: */
