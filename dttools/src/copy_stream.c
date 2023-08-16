/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "copy_stream.h"
#include "full_io.h"
#include "create_dir.h"
#include "path.h"

#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>

#include <errno.h>
#include <limits.h>
#include <signal.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define COPY_BUFFER_SIZE (1<<16)

int64_t copy_fd_to_stream(int fd, FILE *output)
{
	int64_t total = 0;

	while(1) {
		char buffer[COPY_BUFFER_SIZE];

		int64_t actual_read = full_read(fd, buffer, sizeof(buffer));
		if(actual_read <= 0) break;

		int64_t actual_write = full_fwrite(output, buffer, actual_read);
		if(actual_write == -1) {
			if(total == 0)
				return -1;
			else
				break;
		}

		total += actual_write;
	}

	return total;
}

int64_t copy_fd_to_fd(int in, int out)
{
	int64_t total = 0;

	while(1) {
		char buffer[COPY_BUFFER_SIZE];

		int64_t actual_read = full_read(in, buffer, COPY_BUFFER_SIZE);
		if(actual_read <= 0) break;

		int64_t actual_write = full_write(out, buffer, actual_read);
		if(actual_write == -1) {
			if (total == 0)
				return -1;
			else
				break;
		}

		total += actual_write;
	}

	return total;
}

int64_t copy_file_to_file(const char *input, const char *output)
{
	int in = open(input, O_RDONLY);
	if (in == -1)
		return -1;

	struct stat info;
	if (fstat(in, &info) == -1) {
		close(in);
		return -1;
	}

	int out = open(output, O_WRONLY|O_CREAT|O_TRUNC, info.st_mode & (S_IRWXU | S_IRWXG | S_IRWXO));
	if (out == -1 && errno == ENOTDIR) {
		char dir[PATH_MAX];
		path_dirname(output, dir);
		if (create_dir(dir, S_IRWXU)) {
			out = open(output, O_WRONLY|O_CREAT|O_TRUNC, info.st_mode);
		}
	}
	if (out == -1) {
		close(in);
		return -1;
	}

	int64_t total = copy_fd_to_fd(in, out);

	close(in);
	close(out);

	return total;
}

int64_t copy_file_to_buffer(const char *filename, char **buffer, size_t *len)
{
	size_t _len;
	if (len == NULL)
		len = &_len;

	int fd = open(filename, O_RDONLY);
	if (fd == -1)
		return -1;

	struct stat info;
	if (fstat(fd, &info) == -1) {
		close(fd);
		return -1;
	}

	*len = info.st_size;
	*buffer = malloc(*len+1);
	if (*buffer == NULL) {
		close(fd);
		return -1;
	}
	memset(*buffer, 0, *len+1);

	int64_t total = full_read(fd, *buffer, *len);
	if (total == -1) {
		free(*buffer);
	}

	close(fd);

	return total;
}

int64_t copy_stream_to_stream(FILE *input, FILE *output)
{
	int64_t total = 0;

	while(1) {
		char buffer[COPY_BUFFER_SIZE];

		int64_t actual_read = full_fread(input, buffer, COPY_BUFFER_SIZE);
		if(actual_read <= 0) break;

		int64_t actual_write = full_fwrite(output, buffer, actual_read);
		if(actual_write == -1) {
			if (total == 0)
				return -1;
			else
				break;
		}

		total += actual_write;
	}

	return total;
}

int64_t copy_stream_to_buffer(FILE *input, char **buffer, size_t *len)
{
	size_t _len;
	if (len == NULL)
		len = &_len;

	int64_t total = 0;
	buffer_t B;
	buffer_init(&B);

	while(1) {
		char buffer[COPY_BUFFER_SIZE];

		int64_t actual_read = full_fread(input, buffer, COPY_BUFFER_SIZE);
		if(actual_read <= 0) break;

		if (buffer_putlstring(&B, buffer, actual_read) == -1) {
			buffer_free(&B);
			return -1;
		}

		total += actual_read;
	}

	buffer_dupl(&B, buffer, len);
	buffer_free(&B);

	return total;
}

int64_t copy_stream_to_fd(FILE *input, int fd)
{
	int64_t total = 0;

	while(1) {
		char buffer[COPY_BUFFER_SIZE];

		int64_t actual_read = full_fread(input, buffer, COPY_BUFFER_SIZE);
		if(actual_read <= 0) {
			if (total == 0)
				return -1;
			else
				break;
		}

		int64_t actual_write = full_write(fd, buffer, actual_read);
		if(actual_write == -1) {
			if (total == 0)
				return -1;
			else
				break;
		}

		total += actual_write;
	}

	return total;
}

/* vim: set noexpandtab tabstop=8: */
