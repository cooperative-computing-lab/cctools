/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2005- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include <stdlib.h>

#ifndef STRINGTOOLS_H
#define STRINGTOOLS_H

#include "int_sizes.h"

typedef char *(*string_subst_lookup_t) (const char *name, void *arg);

char *escape_shell_string (const char *str);
void string_from_ip_address(const unsigned char *ip_addr_bytes, char *str);
int string_to_ip_address(const char *str, unsigned char *ip_addr_bytes);
int string_ip_subnet(const char *addr, char *subnet);
void string_chomp(char *str);
int whole_string_match_regex(const char *text, char *pattern);
int string_match_regex(const char *text, char *pattern);
int string_match(const char *pattern, const char *text);
char *string_front(const char *str, int max);
const char *string_back(const char *str, int max);
const char *string_basename(const char *str);
void string_dirname(const char *path, char *dir);
char *string_metric(double invalue, int power_needed, char *buffer);
INT64_T string_metric_parse(const char *str);
int string_time_parse(const char *str);
int string_split(char *str, int *argc, char ***argv);
int string_split_quotes(char *str, int *argc, char ***argv);
char *string_pad_right(char *str, int length);
char *string_pad_left(char *str, int length);
void string_cookie(char *str, int length);
char *string_subst(char *value, string_subst_lookup_t lookup, void *arg);

/** Appends second to first, both null terminated strings. Returns the new
  formed string. Both first and second are deallocated before the function
  returns (use string_combine_nofree if this is not desired). If the new string
  cannot be allocated (function returns NULL), then first and second are not modified.

  @param first Null terminated string.
  @param second Null terminated string.
  @return Null terminated string concatenating second to first.
  */
char *string_combine(char *first, char *second);
char *string_combine_multi(char *first, ...);
char *string_combine_nofree(const char *a, const char *b);
char *string_signal(int sig);
void string_split_path(const char *str, char *first, char *rest);
void string_split_multipath(const char *input, char *first, char *rest);
void string_collapse_path(const char *longpath, char *shortpath, int remove_dotdot);
void string_tolower(char *str);
void string_toupper(char *str);
int string_isspace(const char *str);
int string_is_integer(const char *str);
void string_replace_backslash_codes(const char *instr, char *outstr);
int string_equal(const char *str1, const char *str2);

int strpos(const char *str, char c);
int strrpos(const char *str, char c);
int getDateString(char *str);
int string_null_or_empty(const char *str);

/** Returns a heap allocated freeable string formatted using sprintf.
    @param fmt Format string passed to sprintf.
	@param ... Variable arguments passed to sprintf.
	@return The formatted string.
*/
char *string_format (const char *fmt, ...);

/** Writes a string formatted using snprintf. It is an error if the string is longer than the buffer provided.
  @param str Output string buffer, passed as first argument of snprintf.
  @param max Maximum number of characters to write to str, counting the final '\0'.
  @param fmt Format string passed to snprintf.
  @param ... Variable arguments passed to snprintf
  @return The number of character written, not counting the final '\0'.
 */

int string_nformat(char *str, const size_t max, const char *fmt, ...);


/** Returns a heap allocated freeable string for the current working directory.
	@return The current working directory.
 */
char *string_getcwd (void);

char *string_trim(char *s, int(func)(int));
char *string_trim_spaces(char *s);
char *string_trim_quotes(char *s);
int string_istrue(char *s);

#ifndef CCTOOLS_OPSYS_LINUX
char *strsep(char **stringp, const char *delim);
#endif

#endif
