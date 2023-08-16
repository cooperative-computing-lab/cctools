/*
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "chirp_stream.h"
#include "chirp_client.h"

#include "debug.h"
#include "macros.h"
#include "full_io.h"

#include <stdlib.h>
#include <stdio.h>
#include <stdarg.h>
#include <errno.h>
#include <string.h>

struct chirp_stream {
	struct chirp_client *client;
	chirp_stream_mode_t mode;
	char *buffer;
	int buffer_length;
	int buffer_valid;
	int buffer_position;
	int error_count;
};

struct chirp_stream *chirp_stream_open(const char *hostport, const char *path, chirp_stream_mode_t mode, time_t stoptime)
{
	struct chirp_stream *stream;
	struct chirp_client *client;
	int result;

	client = chirp_client_connect(hostport, 1, stoptime);
	if(!client)
		return 0;

	if(mode == CHIRP_STREAM_READ) {
		result = chirp_client_getstream(client, path, stoptime);
	} else {
		result = chirp_client_putstream(client, path, stoptime);
	}

	if(result >= 0) {
		stream = malloc(sizeof(*stream));
		stream->client = client;
		stream->mode = mode;
		stream->buffer_length = 4096;
		stream->buffer_valid = 0;
		stream->buffer_position = 0;
		stream->buffer = malloc(stream->buffer_length);
		return stream;
	} else {
		chirp_client_disconnect(client);
		return 0;
	}
}

int chirp_stream_printf(struct chirp_stream *s, time_t stoptime, const char *fmt, ...)
{
	va_list args;
	int result;

	if(s->mode != CHIRP_STREAM_WRITE) {
		errno = EINVAL;
		return -1;
	}

	while(1) {
		int avail = s->buffer_length - s->buffer_valid;

		va_start(args, fmt);
		result = vsnprintf(&s->buffer[s->buffer_valid], avail, fmt, args);
		va_end(args);

		if(result < 0) {
			return result;
		} else if(result <= avail) {
			s->buffer_valid += result;
			break;
		} else {
			chirp_stream_flush(s, stoptime);
			if(result > s->buffer_length) {
				s->buffer = realloc(s->buffer, result + 1);
				s->buffer_length = result + 1;
			}
		}
	}

	return result;
}

int chirp_stream_write(struct chirp_stream *s, const void *data, int length, time_t stoptime)
{
	if(s->mode != CHIRP_STREAM_WRITE) {
		errno = EINVAL;
		return -1;
	}

	int avail = s->buffer_length - s->buffer_valid;

	if(length > avail) {
		chirp_stream_flush(s, stoptime);
	}

	if(length >= s->buffer_length) {
		return chirp_client_putstream_write(s->client, data, length, stoptime);
	}

	memcpy(&s->buffer[s->buffer_valid], data, length);
	s->buffer_valid += length;

	return length;
}

int chirp_stream_flush(struct chirp_stream *s, time_t stoptime)
{
	if(s->mode == CHIRP_STREAM_READ)
		return 0;
	if(!s->buffer_valid)
		return 0;

	int result = chirp_client_putstream_write(s->client, s->buffer, s->buffer_valid, stoptime);

	s->buffer_valid = 0;

	return result;
}

static int chirp_stream_fill_buffer(struct chirp_stream *s, time_t stoptime)
{
	int avail = s->buffer_valid - s->buffer_position;
	int result;

	if(avail > 0)
		return avail;

	result = chirp_client_getstream_read(s->client, s->buffer, s->buffer_length, stoptime);
	if(result < 0)
		return result;

	s->buffer_valid = result;
	s->buffer_position = 0;

	return result;
}

int chirp_stream_readline(struct chirp_stream *s, char *line, int length, time_t stoptime)
{
	if(s->mode != CHIRP_STREAM_READ) {
		errno = EINVAL;
		return -1;
	}

	while(1) {
		while(length > 0 && (s->buffer_position < s->buffer_valid)) {
			*line = s->buffer[s->buffer_position];
			s->buffer_position++;
			if(*line == 10) {
				*line = 0;
				return 1;
			} else if(*line == 13) {
				continue;
			} else {
				line++;
				length--;
			}
		}
		if(length <= 0)
			break;
		if(chirp_stream_fill_buffer(s, stoptime) <= 0)
			break;
	}

	return 0;
}

int chirp_stream_read(struct chirp_stream *s, void *data, int length, time_t stoptime)
{
	int avail;
	int result;
	int actual;

	if(s->mode != CHIRP_STREAM_READ) {
		errno = EINVAL;
		return -1;
	}

	while(1) {
		avail = s->buffer_valid - s->buffer_position;

		if(avail <= 0) {
			result = chirp_stream_fill_buffer(s, stoptime);
			if(result <= 0)
				return result;
		} else {
			actual = MIN(avail, length);
			memcpy(data, &s->buffer[s->buffer_position], actual);
			s->buffer_position += actual;
			return actual;
		}
	}
}

int chirp_stream_close(struct chirp_stream *s, time_t stoptime)
{
	chirp_stream_flush(s, stoptime);
	chirp_client_disconnect(s->client);
	free(s->buffer);
	free(s);
	return 0;
}

/* vim: set noexpandtab tabstop=8: */
