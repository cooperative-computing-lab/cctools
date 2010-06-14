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

CHIRP_FILE *cfs_fopen (const char *path, const char *mode)
{
  CHIRP_FILE *file;
  INT64_T fd;
  INT64_T flags = 0;

  if (strcmp(path, "/dev/null") == 0) return &CHIRP_FILE_NULL;

  if (strchr(mode, '+'))
    return (errno = ENOTSUP, NULL);
  if (strchr(mode, 'r'))
    flags |= O_RDONLY;
  else if (strchr(mode, 'w'))
    flags |= O_WRONLY | O_CREAT | O_TRUNC;
  else if (strchr(mode, 'a'))
    flags |= O_APPEND | O_CREAT;
  else
    return (errno = EINVAL, NULL);

  fd = cfs->open(path, flags, 0600);
  if (fd == -1)
    return NULL;

  file = xxmalloc(sizeof(CHIRP_FILE));
  file->b = buffer_create();
  file->fd = fd;
  file->offset = file->read_n = 0;
  file->error = 0;
  memset(file->read, '\0', sizeof(file->read));
  return file;
}

int cfs_fflush (CHIRP_FILE *file)
{
  size_t size;
  const char *content;

  if (file == &CHIRP_FILE_NULL) return 0;

  content = buffer_tostring(file->b, &size);

  while ((INT64_T) size > file->offset) /* finish all writes */
  {
    int w = cfs->pwrite(file->fd, content, size, file->offset);
    if (w == -1)
      return (file->error = EIO, EOF);
    file->offset += w;
  }
  return 0;
}

int cfs_fclose (CHIRP_FILE *file)
{
  if (cfs_fflush(file) != 0)
    return EOF;

  if (file == &CHIRP_FILE_NULL) return 0;

  buffer_delete(file->b);
  cfs->close(file->fd);
  free(file);

  return 0;
}

/* Easy fprintf wrapper for buffers. We actually write on close */
void cfs_fprintf( CHIRP_FILE *file, const char *format, ... )
{
  va_list va;
  int size;
  if (file == &CHIRP_FILE_NULL) return;
  va_start(va, format);
  size = buffer_vprep(file->b, format, va);
  va_end(va);
  va_start(va, format);
  buffer_vprintf(file->b, format, size, va);
  va_end(va);
}

size_t cfs_fwrite (const void *ptr, size_t size, size_t nitems, CHIRP_FILE *file)
{
  size_t bytes = 0, nbytes = size*nitems;
  for (; bytes < nbytes; bytes++)
    buffer_printf(file->b, "%c", (int) (((const char *)ptr)[bytes]));
  return nbytes;
}

/* WARNING: fread does not use the fgets buffer!! */
size_t cfs_fread (void *ptr, size_t size, size_t nitems, CHIRP_FILE *file)
{
  size_t nitems_read = 0;

  if (size == 0 || nitems == 0) return 0;

  while (nitems_read < nitems)
  {
    INT64_T t = cfs->pread(file->fd, ptr, size, file->offset);
    if (t == -1) return nitems_read;
    file->offset += t;
    ptr += size;
    nitems_read++;
  }
  return nitems_read;
}

char *cfs_fgets (char *s, int n, CHIRP_FILE *file)
{
  char *current = s;
  INT64_T i, empty = file->read_n == 0;

  if (file == &CHIRP_FILE_NULL) return NULL;

  for (i = 0; i < file->read_n; i++)
    if (i+2 >= n || file->read[i] == '\n') /* we got data now */
    {
      memcpy(s, file->read, i+1);
      s[i+1] = '\0';
      memmove(file->read, file->read+i+1, (file->read_n -= i+1));
      return s;
    }
  memcpy(current, file->read, i);
  current += i;
  n -= i;
  file->read_n = 0;

  i = cfs->pread(file->fd, file->read, CHIRP_FILESYSTEM_BUFFER-1, file->offset);
  if (i == -1)
    return (file->error = errno, NULL);
  else if (i == 0 && empty)
    return NULL;
  else if (i == 0)
    return s;

  file->read_n += i;
  file->offset += i;

  if (cfs_fgets(current, n, file) == NULL) /* some error */
    return NULL;
  else
    return s;
}

int cfs_ferror (CHIRP_FILE *file)
{
  return file->error;
}
