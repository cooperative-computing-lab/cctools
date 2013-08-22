/*
Copyright (C) 2008- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "chirp_filesystem.h"
#include "chirp_protocol.h"

#include "macros.h"
#include "buffer.h"
#include "xxmalloc.h"
#include "md5.h"

#include <sys/stat.h>

#include <assert.h>
#include <errno.h>
#include <stdarg.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>

#define CHIRP_FILESYSTEM_BUFFER  65536

struct CHIRP_FILE {
	INT64_T fd;
	INT64_T offset;
	buffer_t *b;
	char read[CHIRP_FILESYSTEM_BUFFER];
	INT64_T read_n;
	int error;
};

static CHIRP_FILE CHIRP_FILE_NULL;

extern struct chirp_filesystem chirp_fs_local;
extern struct chirp_filesystem chirp_fs_hdfs;
extern struct chirp_filesystem chirp_fs_chirp;

struct chirp_filesystem *cfs_lookup(const char *url)
{
	if(!strchr(url, ':') || !strncmp(url, "local:", 6) || !strncmp(url, "file:", 5)) {
		return &chirp_fs_local;
	} else if(!strncmp(url, "hdfs:", 5)) {
		return &chirp_fs_hdfs;
	} else if(!strncmp(url, "chirp:", 6)) {
		return &chirp_fs_chirp;
	} else {
		return 0;
	}
}


CHIRP_FILE *cfs_fopen(const char *path, const char *mode)
{
	CHIRP_FILE *file;
	INT64_T fd;
	INT64_T flags = 0;

	if(strcmp(path, "/dev/null") == 0)
		return &CHIRP_FILE_NULL;

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
	file->b = buffer_create();
	file->fd = fd;
	file->offset = file->read_n = 0;
	file->error = 0;
	memset(file->read, '\0', sizeof(file->read));
	return file;
}

int cfs_fflush(CHIRP_FILE * file)
{
	size_t size;
	const char *content;

	if(file == &CHIRP_FILE_NULL)
		return 0;

	content = buffer_tostring(file->b, &size);

	while((INT64_T) size > file->offset) {	/* finish all writes */
		int w = cfs->pwrite(file->fd, content, size, file->offset);
		if(w == -1) {
			file->error = EIO;
			return EOF;
		}
		file->offset += w;
	}
	return 0;
}

int cfs_fclose(CHIRP_FILE * file)
{
	if(cfs_fflush(file) != 0)
		return EOF;

	if(file == &CHIRP_FILE_NULL)
		return 0;

	buffer_delete(file->b);
	cfs->close(file->fd);
	free(file);

	return 0;
}

/* Easy fprintf wrapper for buffers. We actually write on close */
void cfs_fprintf(CHIRP_FILE * file, const char *format, ...)
{
	va_list va;
	if(file == &CHIRP_FILE_NULL)
		return;
	va_start(va, format);
	buffer_vprintf(file->b, format, va);
	va_end(va);
}

size_t cfs_fwrite(const void *ptr, size_t size, size_t nitems, CHIRP_FILE * file)
{
	size_t bytes = 0, nbytes = size * nitems;
	for(; bytes < nbytes; bytes++)
		buffer_printf(file->b, "%c", (int) (((const char *) ptr)[bytes]));
	return nbytes;
}

/* WARNING: fread does not use the fgets buffer!! */
size_t cfs_fread(void *ptr, size_t size, size_t nitems, CHIRP_FILE * file)
{
	size_t nitems_read = 0;

	if(size == 0 || nitems == 0)
		return 0;

	while(nitems_read < nitems) {
		INT64_T t = cfs->pread(file->fd, ptr, size, file->offset);
		if(t == -1 || t == 0)
			return nitems_read;
		file->offset += t;
		ptr = (char *) ptr + size;	//Previously void arithmetic!
		nitems_read++;
	}
	return nitems_read;
}

char *cfs_fgets(char *s, int n, CHIRP_FILE * file)
{
	char *current = s;
	INT64_T i, empty = file->read_n == 0;

	if(file == &CHIRP_FILE_NULL)
		return NULL;

	for(i = 0; i < file->read_n; i++)
		if(i + 2 >= n || file->read[i] == '\n') {	/* we got data now */
			memcpy(s, file->read, i + 1);
			s[i + 1] = '\0';
			memmove(file->read, file->read + i + 1, (file->read_n -= i + 1));
			return s;
		}
	memcpy(current, file->read, i);
	current += i;
	n -= i;
	file->read_n = 0;

	i = cfs->pread(file->fd, file->read, CHIRP_FILESYSTEM_BUFFER - 1, file->offset);
	if(i == -1) {
		file->error = errno;
		return 0;
	} else if(i == 0 && empty) {
		return NULL;
	} else if(i == 0) {
		return s;
	}

	file->read_n += i;
	file->offset += i;

	if(cfs_fgets(current, n, file) == NULL)	/* some error */
		return NULL;
	else
		return s;
}

int cfs_ferror(CHIRP_FILE * file)
{
	return file->error;
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

int cfs_delete_dir(const char *path)
{
	int result = 1;
	struct chirp_dir *dir;
	struct chirp_dirent *d;

	if(cfs->unlink(path) == 0)	/* Handle files and symlinks here */
		return 0;

	dir = cfs->opendir(path);
	if(!dir) {
		return errno == ENOENT;
	}
	while((d = cfs->readdir(dir))) {
		char subdir[PATH_MAX];
		if(!strcmp(d->name, "."))
			continue;
		if(!strcmp(d->name, ".."))
			continue;
		sprintf(subdir, "%s/%s", path, d->name);
		if(!cfs_delete_dir(subdir)) {
			result = 0;
		}
	}
	cfs->closedir(dir);
	return cfs->rmdir(path) == 0 ? result : 0;
}

int cfs_freadall(CHIRP_FILE * f, char **s, size_t * l)
{
	char *buffer = xxrealloc(NULL, 4096);
	INT64_T n;
	*l = 0;
	while((n = cfs->pread(f->fd, buffer + (*l), sizeof(char) * 4096, f->offset)) > 0) {
		f->offset += n;
		*l += n;
		buffer = xxrealloc(buffer, (*l) + 4096);
		*(buffer + (*l)) = '\0';	/* NUL terminator... */
	}
	if(n < 0) {
		free(buffer);
		*s = NULL;
		*l = 0;
		return 0;
	} else {
		*s = buffer;
		return 1;
	}
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


INT64_T cfs_basic_putfile(const char *path, struct link * link, INT64_T length, INT64_T mode, time_t stoptime)
{
	int fd;
	INT64_T result;

	mode = 0600 | (mode & 0100);

	fd = cfs->open(path, O_WRONLY | O_CREAT | O_TRUNC, (int) mode);
	if(fd >= 0) {
		char buffer[65536];
		INT64_T total = 0;

		link_putliteral(link, "0\n", stoptime);

		while(length > 0) {
			INT64_T ractual, wactual;
			INT64_T chunk = MIN((int) sizeof(buffer), length);

			ractual = link_read(link, buffer, chunk, stoptime);
			if(ractual <= 0)
				break;

			wactual = cfs->pwrite(fd, buffer, ractual, total);
			if(wactual != ractual) {
				total = -1;
				break;
			}

			total += ractual;
			length -= ractual;
		}

		result = total;

		if(length != 0) {
			if(result >= 0)
				link_soak(link, length - result, stoptime);
			result = -1;
		}
		cfs->close(fd);
	} else {
		result = -1;
	}
	return result;
}


INT64_T cfs_basic_getfile(const char *path, struct link * link, time_t stoptime)
{
	int fd;
	INT64_T result;
	struct chirp_stat info;

	result = cfs->stat(path, &info);
	if(result < 0)
		return result;

	if(S_ISDIR(info.cst_mode)) {
		errno = EISDIR;
		return -1;
	}

	fd = cfs->open(path, O_RDONLY, 0);
	if(fd >= 0) {
		char buffer[65536];
		INT64_T total = 0;
		INT64_T ractual, wactual;
		INT64_T length = info.cst_size;

		link_putfstring(link, "%" PRId64 "\n", stoptime, length);

		while(length > 0) {
			INT64_T chunk = MIN((int) sizeof(buffer), length);

			ractual = cfs->pread(fd, buffer, chunk, total);
			if(ractual <= 0)
				break;

			wactual = link_putlstring(link, buffer, ractual, stoptime);
			if(wactual != ractual) {
				total = -1;
				break;
			}

			total += ractual;
			length -= ractual;
		}
		result = total;
		cfs->close(fd);
	} else {
		result = -1;
	}

	return result;
}

INT64_T cfs_basic_md5(const char *path, unsigned char digest[16])
{
	int fd;
	INT64_T result;
	struct chirp_stat info;

	result = cfs->stat(path, &info);
	if(result < 0)
		return result;

	if(S_ISDIR(info.cst_mode)) {
		errno = EISDIR;
		return -1;
	}

	fd = cfs->open(path, O_RDONLY, 0);
	if(fd >= 0) {
		char buffer[65536];
		INT64_T ractual;
		INT64_T total = 0;
		INT64_T length = info.cst_size;
		md5_context_t ctx;

		md5_init(&ctx);

		while(length > 0) {
			INT64_T chunk = MIN((int) sizeof(buffer), length);

			ractual = cfs->pread(fd, buffer, chunk, total);
			if(ractual <= 0)
				break;

			md5_update(&ctx, (unsigned char *) buffer, ractual);

			length -= ractual;
			total += ractual;
		}
		result = 0;
		cfs->close(fd);
		md5_final(digest, &ctx);
	} else {
		result = -1;
	}

	return result;
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
