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

/** buffer_t is an opaque object representing a buffer. */
typedef struct buffer_t buffer_t;

/** Create a new buffer.
    @return A new empty buffer object. Returns NULL when out of memory.
  */
buffer_t *buffer_create (void);

/** Delete a buffer.
    @param b The buffer to free.
  */
void buffer_delete (buffer_t *b);

/** Determine the space needed to fill a buffer. This function is to be used
    in conjunction with buffer_vprintf. This procedure does not call any of
    the va_(start|end) macros. The integer size returned is to be passed
    to buffer_vprintf. This procedure does not actually change the buffer.
    @param b The buffer to fill.
    @param format The format string.
    @param ap The variable argument list for the format string.
    @return The number of bytes needed to save the buffer.
  */
int buffer_vprep (buffer_t *b, const char *format, va_list ap);

/** Print the formatted output to the buffer. The format string follows the
    same semantics as the UNIX vprintf function. The size argument is given
    by the buffer_vprep function. Neither buffer_vprep nor buffer_vprintf
    call the variable argument macros va_(start|end).
    @param b The buffer to fill.
    @param format The format string.
    @param size The integer size determined by buffer_vprep.
    @param ap The variable argument list for the format string.
  */
void buffer_vprintf (buffer_t *b, const char *format, int size, va_list ap);

/** Print the formatted output to the buffer. The format string follows the
    same semantics as the UNIX vprintf function.
    @param b The buffer to fill.
    @param format The format string.
    @param ... The variable arguments for the format string.
  */
void buffer_printf (buffer_t *b, const char *format, ...);

/** Returns the buffer as a string. The string is no longer valid after
    deleting the buffer. A final ASCII NUL character is guaranteed to terminate
    the string.
    @param b The buffer.
    @param size The size of the string is placed in this variable.
    @return The buffer as a string with a NUL terminator.
  */
const char *buffer_tostring (buffer_t *b, size_t *size);

#endif /* BUFFER_H */
