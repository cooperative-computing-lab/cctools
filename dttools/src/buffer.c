/*
 * Copyright (C) 2010- The University of Notre Dame
 * This software is distributed under the GNU General Public License.
 * See the file COPYING for details.
*/

#include "buffer.h"

#include <assert.h>
#include <debug.h>
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
	if (buf && len >= sizeof(b->initial)) {
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

/* make room for at least n chars */
static int grow(buffer_t * b, size_t n)
{
	size_t newlen;
	size_t inuse;

	inuse = inuse(b);

	newlen = sizeof(b->initial); /* current buf is always at least as big as b->initial */
	/* simple solution to find next power of 2 */
	while (newlen < inuse+n) newlen <<= 1;

	if (b->max > 0 && newlen > b->max) {
		errno = ENOBUFS;
		checkerror(b, 0, 0);
	}

	if (b->buf == b->ubuf.buf || b->buf == b->initial) {
		char *new = malloc(newlen);
		checkerror(b, NULL, new);
		memcpy(new, b->buf, inuse);
		b->buf = new;
	} else {
		char *new = realloc(b->buf, newlen);
		checkerror(b, NULL, new);
		b->buf = new;
	}
	b->end = b->buf+inuse;
	b->len = newlen;
	return 0;
}

int buffer_vprintf(buffer_t * b, const char *format, va_list va)
{
	va_list va2;
	int rc;

	va_copy(va2, va);
	rc = vsnprintf(b->end, avail(b), format, va2);
	va_end(va2);

	checkerror(b, -1, rc);
	/* N.B. vsnprintf rc does not include NUL byte */
	if (((size_t)rc) >= avail(b))
		rc = grow(b, rc+1);
		if (rc == -1) return -1;
	else {
		b->end += rc;
		return 0;
	}

	va_copy(va2, va);
	rc = vsnprintf(b->end, avail(b), format, va2);
	assert(rc >= 0);
	b->end += rc;
	va_end(va2);

	return 0;
}

int buffer_printf(buffer_t * b, const char *format, ...)
{
	va_list va;
	va_start(va, format);
	int r = buffer_vprintf(b, format, va);
	va_end(va);
	return r;
}

int buffer_putlstring(buffer_t * b, const char *str, size_t len)
{
	if (avail(b) < len+1 && grow(b, len+1) == -1) {
		return -1;
	}
	strncpy(b->end, str, len);
	b->end += len;
	b->end[0] = '\0';
	return 0;
}

const char *buffer_tostring(buffer_t * b, size_t * size)
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

/* vim: set noexpandtab tabstop=4: */
