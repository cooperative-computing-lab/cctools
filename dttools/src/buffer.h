/*
Copyright (C) 2005- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef BUFFER_H
#define BUFFER_H

#include <stdlib.h>
#include <stdarg.h>

/** @file buffer.h String Buffer Operations.
    You can use the buffer in the same way you would print to a file. Use the
    buffer to do formatted printing. When you are done retrieve the final
    string using buffer_tostring.
*/

#if !(defined(__GNUC__) || defined(__clang__)) && !defined(__attribute__)
#define __attribute__(x) /* do nothing */
#endif

/** buffer_t is an opaque object representing a buffer. */
typedef struct buffer_t buffer_t;

/** Create a new buffer.
    @return A new empty buffer object. Returns NULL when out of memory.
  */
buffer_t *buffer_create(void);

/** Delete a buffer.
    @param b The buffer to free.
  */
void buffer_delete(buffer_t * b);

/** Print the formatted output to the buffer. The format string follows the
    same semantics as the UNIX vprintf function. buffer_vprintf does not call
    the variable argument macros va_(start|end) on ap.
    @param b The buffer to fill.
    @param format The format string.
    @param ap The variable argument list for the format string.
    @return Negative value on error.
  */

int buffer_vprintf(buffer_t * b, const char *format, va_list ap);

/** Appends the formatted output to the buffer. The format string follows the
    same semantics as the UNIX vprintf function.
    @param b The buffer to fill.
    @param format The format string.
    @param ... The variable arguments for the format string.
    @return Negative value on error.
  */
int buffer_printf(buffer_t * b, const char *format, ...)
__attribute__ (( format(printf,2,3) )) ;

/** Returns the buffer as a string. The string is no longer valid after
    deleting the buffer. A final ASCII NUL character is guaranteed to terminate
    the string.
    @param b The buffer.
    @param size The size of the string is placed in this variable. Can be NULL.
    @return The buffer as a string with a NUL terminator.
  */
const char *buffer_tostring(buffer_t * b, size_t * size);

#endif /* BUFFER_H */
