/*
Copyright (C) 2005- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef BUFFER_H
#define BUFFER_H

#include <stdlib.h>
#include <stdarg.h>
#include <string.h>

/** @file buffer.h String Buffer Operations.
	You can use the buffer in the same way you would print to a file. Use the
	buffer to do formatted printing. When you are done retrieve the final
	string using buffer_tostring.
*/

#if !(defined(__GNUC__) || defined(__clang__)) && !defined(__attribute__)
#define __attribute__(x) /* do nothing */
#endif

#define BUFFER_INISIZ  (1<<12)

typedef struct buffer {
	char *buf; /* buf points to the start of the buffer, which may be a buffer on the stack or heap */
	char *end; /* the current end of the buffer */
	size_t len; /* current size of buffer */
	size_t max; /* maximum size of buffer */
	int abort_on_failure; /* call debug.c fatal(...) on error instead of returning  */

	/* a user provided buffer which replaces initial if larger */
	struct {
		char *buf;
		size_t len;
	} ubuf;
	char initial[BUFFER_INISIZ]; /* a reasonably sized buffer to use initially so we avoid (numerous) heap allocations */
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
	@param abortonfailure   Kill the process on errors. (you no longer have to check returns)
  */
void buffer_abortonfailure(buffer_t * b, int abortonfailure);

/** Free any resources and memory in use by a buffer.
	@param b The buffer to free.
  */
void buffer_free(buffer_t * b);

/** Make a heap allocated copy of the buffer.
	@param b The buffer to copy.
	@param buf A place to store the copy pointer.
	@param l The length of the string.
	@return -1 on error.
  */
int buffer_dupl(buffer_t *b, char **buf, size_t *l);

/** Make a heap allocated copy of the buffer.
	@param b The buffer to copy.
	@param buf A place to store the copy pointer.
	@return -1 on error.
  */
#define buffer_dup(b,buf) (buffer_dupl(b,buf,NULL))

/** Print the formatted output to the buffer. The format string follows the
	same semantics as the UNIX vprintf function. buffer_putvfstring does not call
	the variable argument macros va_(start|end) on ap.
	@param b The buffer to fill.
	@param format The format string.
	@param ap The variable argument list for the format string.
	@return bytes added (excluding NUL) or -1 on error.
  */

int buffer_putvfstring(buffer_t * b, const char *format, va_list ap);
#define buffer_vprintf buffer_putvfstring

/** Appends the formatted output to the buffer. The format string follows the
	same semantics as the UNIX vprintf function.
	@param b The buffer to fill.
	@param format The format string.
	@param ... The variable arguments for the format string.
	@return bytes added (excluding NUL) or -1 on error.
  */
int buffer_putfstring(buffer_t * b, const char *format, ...)
__attribute__ (( format(printf,2,3) )) ;
#define buffer_printf buffer_putfstring

/** Appends the string to the end of the buffer.
	@param b The buffer to fill.
	@param str The string to append.
	@param len The length of the string.
	@return bytes added (excluding NUL) or -1 on error.
  */
int buffer_putlstring(buffer_t * b, const char *str, size_t len);

/** Appends the string to the end of the buffer. Length derived via strlen.
	@param b The buffer to fill.
	@param s The string to append.
	@return bytes added (excluding NUL) or -1 on error.
  */
#define buffer_putstring(b,s)  (buffer_putlstring(b,s,strlen(s)))

/** Appends the string literal to the end of the buffer. Length derived via sizeof.
	@param b The buffer to fill.
	@param l The literal string to append.
	@return bytes added (excluding NUL) or -1 on error.
  */
#define buffer_putliteral(b,l)  (buffer_putlstring(b,l "",sizeof(l)-1))

/** Returns the buffer as a string. The string is no longer valid after
	deleting the buffer. A final ASCII NUL character is guaranteed to terminate
	the string.
	@param b The buffer.
	@param size The size of the string is placed in this variable. Can be NULL.
	@return The buffer as a string with a NUL terminator.
  */
const char *buffer_tolstring(buffer_t * b, size_t * size);

/** Returns the buffer as a string. The string is no longer valid after
	deleting the buffer. A final ASCII NUL character is guaranteed to terminate
	the string.
	@param b The buffer.
	@return The buffer as a string with a NUL terminator.
  */
#define buffer_tostring(b) buffer_tolstring(b, NULL)

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

/** Make room for at least n additional chars.
 *
 * @param b The buffer.
 * @param n Number of bytes to add to existing buffer.
 * @returns -1 on error.
 */
int buffer_grow(buffer_t * b, size_t n);

/** Seek to a position.
 *
 * Similar to @ref buffer_rewind(), but allows seeking past the end of the buffer.
 * Note that seeking beyond the currently allocated memory may result in a
 * buffer filled with garbage data (but still null-terminated).
 * @param b The buffer.
 * @param pos Absolute position to seek to.
 * @returns -1 on error.
 */
int buffer_seek(buffer_t * b, size_t pos);

/** Allocate a buffer named `name' on the stack of at most `size' bytes.
	Does not abort on failure, but hits the max size and drops further bytes
	written. You can turn on aborts on failure manually using
	buffer_abortonfailure or using BUFFER_STACK_ABORT. You do not need to call
	buffer_free(name) because nothing is ever allocated on the heap. This is
	defined as a macro.

	@param name The name of the buffer.
	@param size The maximum size of the buffer.
  */
#define BUFFER_STACK(name,size) \
	buffer_t name[1];\
	char name##_ubuf[size > BUFFER_INISIZ ? size : 1]; /* Unfortunately, we can't conditionally allocate this ubuf array. Use char[1] if less than BUFFER_INISIZ */\
	buffer_init(name);\
	buffer_max(name, size);\
	buffer_ubuf(name, name##_ubuf, size); /* if this is less than BUFFER_INISIZ, then B->initial is still used. */

/** Allocate a buffer named `name' on the stack of at most `size' bytes.
	This works the same as BUFFER_STACK but also sets the abort flag on the
	buffer. This is defined as a macro.

	@param name The name of the buffer.
	@param size The maximum size of the buffer.
  */
#define BUFFER_STACK_ABORT(name,size) \
	BUFFER_STACK(name,size);\
	buffer_abortonfailure(name, 1);

/** Allocate and print to a buffer named `name' on the stack of at most `size' bytes.
	This macro uses BUFFER_STACK to allocate the buffer. Variable arguments
	are passed to buffer_putfstring, starting with the format string.

	@param name The name of the buffer.
	@param size The maximum size of the buffer.
  */
#define BUFFER_STACK_PRINT(name,size,...) \
	BUFFER_STACK(name,size);\
	buffer_putfstring(name, __VA_ARGS__);

#endif /* BUFFER_H */
