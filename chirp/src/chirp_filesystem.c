/*
Copyright (C) 2008- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "chirp_filesystem.h"
#include "chirp_protocol.h"

#include "macros.h"
#include "buffer.h"
#include "xmalloc.h"

#include <assert.h>
#include <errno.h>
#include <stdarg.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>

#ifdef HAS_ALLOCA_H
#include <alloca.h>
#endif

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
		ptr += size;
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

	temp = alloca(strlen(path) + 1);
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

int cfs_freadall(CHIRP_FILE * f, char **s, size_t * l)
{
	char *buffer = xxrealloc(NULL, 4096);
	size_t n;
	*l = 0;
	while((n = cfs_fread(buffer + (*l), sizeof(char), 4096, f)) > 0) {
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

int cfs_isdir(const char *filename)
{
	struct chirp_stat info;

	if(do_stat(filename, &info) == 0) {
		if(S_ISDIR(info.cst_mode)) {
			return 1;
		} else {
			return 0;
		}  
	} else {
		return 0;
	}
}
