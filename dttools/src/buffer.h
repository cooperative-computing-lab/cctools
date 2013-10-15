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

typedef struct buffer {
	char *buf; /* buf points to the start of the buffer, which may be a buffer on the stack or heap */
	char *end; /* the current end of the buffer */
	size_t len; /* current size of buffer */
	size_t max; /* maximum size of buffer */
	int abort_on_failure; /* call debug.c fatal(...) on error instead of returning  */

	char initial[1<<12]; /* a reasonably sized buffer to use initially so we avoid (numerous) heap allocations */

	/* a user provided buffer which replaces initial if larger */
	struct {
		char *buf;
		size_t len;
	} ubuf;
} buffer_t;

/** Initialize a buffer.

	The buffer includes a reasonably sized starting buffer as part of its
	definition.  Usually this means for small strings being built, nothing is
	ever allocated on the heap. You can specify a larger starting buffer if
	this is inadequate.

    @param b The buffer to initialize.
  */
void buffer_init(buffer_t * b);

/** Use the provided buffer as a starting buffer.

	This function should only be called before any work is done on the buffer.
	The user buffer is only used if it is larger than the internal starting
	buffer.

    @param b   The buffer.
    @param buf A starting buffer to initially use to avoid allocating memory on the heap. (can be NULL)
    @param len The length of the buffer. (ignored if buf == NULL)
  */
void buffer_ubuf(buffer_t * b, char *buf, size_t len);

/** Set the maximum size of the buffer.

    @param b   The buffer.
    @param max The maximum amount of memory to allocate. (0 is unlimited)
  */
void buffer_max(buffer_t * b, size_t max);

/** Set the buffer to call fatal(...) on error instead of returning an error code.

    @param b                The buffer.
    @param abort_on_failure Kill the process on errors. (you no longer have to check returns)
  */
void buffer_abortonfailure(buffer_t * b, int abortonfailure);

/** Free any resources and memory in use by a buffer.
    @param b The buffer to free.
  */
void buffer_free(buffer_t * b);

/** Print the formatted output to the buffer. The format string follows the
    same semantics as the UNIX vprintf function. buffer_vprintf does not call
    the variable argument macros va_(start|end) on ap.
    @param b The buffer to fill.
    @param format The format string.
    @param ap The variable argument list for the format string.
    @return -1 on error.
  */

int buffer_vprintf(buffer_t * b, const char *format, va_list ap);

/** Appends the formatted output to the buffer. The format string follows the
    same semantics as the UNIX vprintf function.
    @param b The buffer to fill.
    @param format The format string.
    @param ... The variable arguments for the format string.
    @return -1 on error.
  */
int buffer_printf(buffer_t * b, const char *format, ...)
__attribute__ (( format(printf,2,3) )) ;

/** Appends the string to the end of the buffer.
    @param b The buffer to fill.
    @param str The string to append.
    @param len The length of the string.
    @return -1 on error.
  */
int buffer_putlstring(buffer_t * b, const char *str, size_t len);

/** Appends the string to the end of the buffer. Length derived via strlen.
    @param b The buffer to fill.
    @param str The string to append.
    @return -1 on error.
  */
#define buffer_putstring(b,s)  (buffer_putlstring(b,s,strlen(s)))

/** Appends the string literal to the end of the buffer. Length derived via sizeof.
    @param b The buffer to fill.
    @param l The literal string to append.
    @return -1 on error.
  */
#define buffer_putliteral(b,l)  (buffer_putlstring(b,l "",sizeof(l)-1))

/** Returns the buffer as a string. The string is no longer valid after
    deleting the buffer. A final ASCII NUL character is guaranteed to terminate
    the string.
    @param b The buffer.
    @param size The size of the string is placed in this variable. Can be NULL.
    @return The buffer as a string with a NUL terminator.
  */
const char *buffer_tostring(buffer_t * b, size_t * size);

/** Rewinds the buffer to position n.

    @param b The buffer.
    @param n The position to rewind to.
  */
void buffer_rewind(buffer_t * b, size_t n);

/** Get the current position in the buffer.

    @param b The buffer.
    @return The current position.
  */
size_t buffer_pos(buffer_t * b);

#endif /* BUFFER_H */
