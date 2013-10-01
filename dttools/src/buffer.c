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
struct buffer_t {
	char *buf;
	char *end;
	size_t len; /* size of buffer */
	size_t max; /* maximum size of buffer */
	int abort_on_failure;

	char initial[1<<12];
	struct {
		char *buf;
		size_t len;
	} ubuf;
};

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

buffer_t *buffer_create(char *buf, size_t len, size_t max, int abort_on_failure)
{
	buffer_t *b = malloc(sizeof(buffer_t));
	if (b) {
		if (buf && len >= sizeof(b->initial)) {
			b->buf = b->end = b->ubuf.buf = buf;
			b->len = b->ubuf.len = len;
		} else {
			b->buf = b->end = b->initial;
			b->len = sizeof(b->initial);
			b->ubuf.buf = NULL;
			b->ubuf.len = 0;
		}
		b->max = max;
		b->abort_on_failure = abort_on_failure;
	} else if (abort_on_failure) {
		fatal("[%s:%d]: %s", __FILE__, __LINE__, strerror(errno));
	}
	return b;
}

void buffer_delete(buffer_t * b)
{
	if (!(b->buf == b->ubuf.buf || b->buf == b->initial)) {
		free(b->buf);
	}
	free(b);
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
