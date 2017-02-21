/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2005- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include <sys/types.h>

#ifndef XXMALLOC_H
#define XXMALLOC_H

/** @file xxmalloc.h
Brittle memory allocation routines.
These routines may be used in place of <tt>malloc</tt> and <tt>strdup</tt>.
If they fail due to the (rare) possibility of heap exhaustion, they will
abort by calling @ref fatal with an appropriate error message.  Thus, the
caller of these routines need not continually check for a null pointer return.
*/

/** Allocate memory, or abort on failure.
@param nbytes The amount of memory to allocate.
@return On success, returns a valid pointer.  On failure, aborts by calling @ref fatal.
*/
void *xxmalloc(size_t nbytes);
void *xxcalloc(size_t nmemb, size_t size);
void *xxrealloc(void *ptr, size_t nbytes);

/** Duplicate string, or abort on failure.
@param str The string to duplicate.
@return On success, returns a valid pointer.  On failure, aborts by calling @ref fatal.
*/

char *xxstrdup(const char *str);

#endif
