/*
 * Copyright (C) 2022 The University of Notre Dame
 * This software is distributed under the GNU General Public License.
 * See the file COPYING for details.
*/

#include "buffer.h"
#include "debug.h"

#include <assert.h>
#include <errno.h>
#include <stdarg.h>
#include <stdio.h>
#include <string.h>

#define inuse(b)  ((size_t)(b->end - b->buf))
#define avail(b)  (b->len - inuse(b))

#define checkerror(b,err,expr) \
	do {\
		if ((err) == (expr)) {\
			if (b->abort_on_failure) {\
				fatal("[%s:%d]: %s", __FILE__, __LINE__, strerror(errno));\
			} else {\
				return -1;\
			}\
		}\
	} while (0)

void buffer_init(buffer_t * b)
{
	bzero(b->initial, sizeof(b->initial));
	b->buf = b->end = b->initial;
	b->len = sizeof(b->initial);
	b->ubuf.buf = NULL;
	b->ubuf.len = 0;
	b->end[0] = '\0'; /* initialize with empty string */
	b->max = 0;
	b->abort_on_failure = 0;
}

void buffer_ubuf(buffer_t * b, char *buf, size_t len)
{
	assert(b->buf == b->initial && b->buf == b->end);
	if (buf && len > sizeof(b->initial)) {
		b->buf = b->end = b->ubuf.buf = buf;
		b->len = b->ubuf.len = len;
		b->end[0] = '\0'; /* initialize with empty string */
	}
}

void buffer_max(buffer_t * b, size_t max)
{
	b->max = max;
}

void buffer_abortonfailure(buffer_t * b, int abortonfailure)
{
	b->abort_on_failure = abortonfailure;
}

void buffer_free(buffer_t * b)
{
	if (!(b->buf == b->ubuf.buf || b->buf == b->initial)) {
		free(b->buf);
	}
}

int buffer_seek(buffer_t * b, size_t pos) {
	assert(b);

	if (pos >= inuse(b)) {
		int rc = buffer_grow(b, pos - inuse(b) + 1);
		if (rc < 0) return rc;
		b->end = b->buf + pos;
		b->end[0] = '\0';
	} else {
		buffer_rewind(b, pos);
	}
	return 0;
}

int buffer_grow(buffer_t * b, size_t n)
{
	size_t used = inuse(b);
	size_t newlen = sizeof(b->initial); /* current buf is always at least as big as b->initial */

	/* simple solution to find next power of 2 */
	while (newlen < used+n) newlen <<= 1;

	/* too big? */
	if (0 < b->max && b->max < newlen) {
		if (used+n <= b->max) {
			/* This handles the case where b->max is not a power of 2. */
			newlen = b->max;
		} else {
			errno = ENOBUFS;
			checkerror(b, 0, 0);
		}
	}

	/* Keep using the initial buffer if possible */
	if (newlen <= b->len) return 0;

	if (b->buf == b->ubuf.buf || b->buf == b->initial) {
		char *new = malloc(newlen);
		checkerror(b, NULL, new);
		memcpy(new, b->buf, used);
		b->buf = new;
	} else {
		char *new = realloc(b->buf, newlen);
		checkerror(b, NULL, new);
		b->buf = new;
	}
	b->end = b->buf+used;
	b->end[0] = '\0';
	b->len = newlen;
	assert(avail(b) >= n);
	return 0;
}

int buffer_putvfstring(buffer_t * b, const char *format, va_list va)
{
	int rc;
	va_list va2;
	size_t used = inuse(b);

	va_copy(va2, va);
	rc = vsnprintf(b->end, avail(b), format, va2);
	va_end(va2);

	checkerror(b, -1, rc);
	/* N.B. vsnprintf rc does not include NUL byte */
	if (avail(b) <= (size_t)rc) {
		rc = buffer_grow(b, rc+1);
		if (rc == -1) return -1;
	} else {
		b->end += rc;
		assert(rc+used == inuse(b));
		assert(inuse(b) < b->len);
		return rc;
	}

	va_copy(va2, va);
	rc = vsnprintf(b->end, avail(b), format, va2);
	assert(rc >= 0);
	b->end += rc;
	assert(rc+used == inuse(b));
	assert(inuse(b) < b->len);
	va_end(va2);

	return rc;
}

int buffer_putfstring(buffer_t * b, const char *format, ...)
{
	va_list va;
	va_start(va, format);
	int rc = buffer_vprintf(b, format, va);
	va_end(va);
	return rc;
}

int buffer_putlstring(buffer_t * b, const char *str, size_t len)
{
	if (avail(b) <= len && buffer_grow(b, len+1) == -1) {
		return -1;
	}
	memcpy(b->end, str, len);
	b->end += len;
	b->end[0] = '\0';
	return (int)len;
}

const char *buffer_tolstring(buffer_t * b, size_t * size)
{
	if(size != NULL)
		*size = inuse(b);
	return b->buf;
}

size_t buffer_pos(buffer_t * b)
{
	return inuse(b);
}

void buffer_rewind(buffer_t * b, size_t n)
{
	assert(inuse(b) >= n);
	b->end = b->buf+n;
	b->end[0] = '\0';
}

int buffer_dupl(buffer_t *b, char **buf, size_t *l)
{
	size_t n = inuse(b);
	*buf = malloc(n+1); /* include NUL */
	checkerror(b, NULL, *buf);
	if (l)
		*l = n;
	memcpy(*buf, b->buf, n+1); /* include NUL */
	return 0;
}

/* vim: set noexpandtab tabstop=8: */
