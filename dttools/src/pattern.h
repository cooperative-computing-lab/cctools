/* Copyright (C) 2022 The University of Notre Dame
 * This software is distributed under the GNU General Public License.
 * See the file COPYING for details.
 */

#ifndef PATTERN_H
#define PATTERN_H

#include <stdarg.h>
#include <stddef.h>

/** @file pattern.h Pattern Matching Facilities.
 *
 * Lua 5.2 pattern matching. See Lua manual for patterns supported.
 *
 * Captures are passed through C varargs. String captures are heap
 * allocated and must be freed.
 *
 * Note: position captures are C offsets in the string (based 0).
 *
 * @return offset in str where match occurred or -1 if no match.
 * @see http://www.lua.org/manual/5.2/manual.html#6.4.1
 */

ptrdiff_t pattern_vmatch (const char *str, const char *patt, va_list va);
ptrdiff_t pattern_match (const char *str, const char *patt, ...);

#endif /* PATTERN_H */
