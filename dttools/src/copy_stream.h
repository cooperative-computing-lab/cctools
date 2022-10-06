/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef COPY_STREAM_H
#define COPY_STREAM_H

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>

int64_t copy_fd_to_fd(int in, int out);
int64_t copy_fd_to_stream(int fd, FILE *output);

int64_t copy_file_to_file(const char *input, const char *output);
int64_t copy_file_to_buffer(const char *path, char **buffer, size_t *len);

int64_t copy_stream_to_buffer(FILE *input, char **buffer, size_t *len);
int64_t copy_stream_to_fd(FILE *input, int fd);
int64_t copy_stream_to_stream(FILE *input, FILE *output);

#endif
