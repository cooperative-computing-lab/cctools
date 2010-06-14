
#ifndef BUFFER_H
#define BUFFER_H

#include <stdlib.h>
#include <stdarg.h>

typedef struct buffer_t buffer_t;

buffer_t *buffer_create (void);
void buffer_delete (buffer_t *b);
int buffer_vprep (buffer_t *b, const char *format, va_list ap);
void buffer_vprintf (buffer_t *b, const char *format, int size, va_list ap);
void buffer_printf (buffer_t *b, const char *format, ...);
const char *buffer_tostring (buffer_t *b, size_t *size);

#endif /* BUFFER_H */
