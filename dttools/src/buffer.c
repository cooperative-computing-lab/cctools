/*
Copyright (C) 2005- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "buffer.h"
#include "xmalloc.h"

#include <assert.h>
#include <stdarg.h>
#include <stdio.h>

struct buffer_t {
  char *buf;
  size_t size;
};

buffer_t *buffer_create (void)
{
  buffer_t *b = xxmalloc(sizeof(buffer_t));
  b->buf = NULL;
  b->size = 0;
  return b;
}

void buffer_delete (buffer_t *b)
{
  free(b->buf);
  free(b);
}

int buffer_vprep (buffer_t *b, const char *format, va_list ap)
{
  return vsnprintf(NULL, 0, format, ap);
}

void buffer_vprintf (buffer_t *b, const char *format, int size, va_list va)
{
  size_t osize = b->size;
  assert(size >= 0);
  b->size += size;
  b->buf = xxrealloc(b->buf, b->size+1); /* extra nul byte */
  vsnprintf(b->buf+osize, size+1, format, va);
}

void buffer_printf (buffer_t *b, const char *format, ...)
{
  int size;
  va_list va;
  va_start(va, format);
  size = buffer_vprep(b, format, va);
  va_end(va);
  va_start(va, format);
  buffer_vprintf(b, format, size, va);
  va_end(va);
}

const char *buffer_tostring (buffer_t *b, size_t *size)
{
  if (size != NULL) *size = b->size;
  return b->buf;
}
