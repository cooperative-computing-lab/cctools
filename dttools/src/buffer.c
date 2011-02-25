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

int buffer_vprintf (buffer_t *b, const char *format, va_list va)
{
  va_list va2;
  size_t osize = b->size;

  va_copy(va2, va);
  int n = vsnprintf(NULL, 0, format, va2);
  va_end(va2);

  if (n < 0) return -1;

  b->size += n;
  b->buf = xxrealloc(b->buf, b->size+1); /* extra nul byte */
  va_copy(va2, va);
  n = vsnprintf(b->buf+osize, n+1, format, va2);
  assert(n >= 0);
  va_end(va2);

  return 0;
}

int buffer_printf (buffer_t *b, const char *format, ...)
{
  va_list va;
  va_start(va, format);
  int r = buffer_vprintf(b, format, va);
  va_end(va);
  return r;
}

const char *buffer_tostring (buffer_t *b, size_t *size)
{
  if (size != NULL) *size = b->size;
  return b->buf;
}
