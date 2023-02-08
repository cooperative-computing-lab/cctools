/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef STRINGTOOLS_H
#define STRINGTOOLS_H

#include <stdint.h>
#include <stdlib.h>
#include <time.h>
#include <stdarg.h>

typedef char *(*string_subst_lookup_t) (const char *name, void *arg);

/** Comparison function for an array of strings.
 * This is useful with qsort(3) and list_sort().
 */
int string_compare(const void *p1, const void *p2);

/** Takes a command string and escapes special characters in the Shell Command
  language. Mallocs space for new string and does not modify original string.
  Characters dollar-sign $, backtick `, backslash \, and double-quote " are
  escaped.
  @param str Command string presented to be escaped.
  @return String with special characters escaped.
  */
char *string_escape_shell (const char *str);

/** Takes a command string and escapes quote characters.
  Mallocs space for new string and does not modify original string.
  The resulting string is wrapped in double quotes,
  and will be subject to shell expansion.
  @param str Command string presented to be quoted.
  @return String with characters quoted.
  */
char *string_quote_shell (const char *str);

/** Takes a command string, escapes double quotes with another double 
  quote, and escapes single quotes with two additional single quotes.
  This is a utilized when putting wrapped and nested commands in Condor.
  @param str Command string presented to be escaped.
  @return String with special characters escaped.
  */
char *string_escape_condor( const char *str);

/** Escape special characters in a string with backslash.
@param s The null-terminated string to escape.
@param t A buffer for the new string.
@param specials A string indicating all special character to escape.
@param length The length of buffer t, including the null terminator.
@return True if t was long enough to encode the entire escaped string.
*/

int string_escape_chars( const char *s, char *t, const char *specials, int length );


void string_chomp(char *str);
int whole_string_match_regex(const char *text, const char *pattern);
int string_match_regex(const char *text, const char *pattern);
int string_match(const char *pattern, const char *text);
char *string_front(const char *str, int max);
const char *string_back(const char *str, int max);
char *string_metric(double value, int power_needed, char *buffer);
int64_t string_metric_parse(const char *str);
time_t string_time_parse(const char *str);
int string_split(char *str, int *argc, char ***argv);
int string_split_quotes(char *str, int *argc, char ***argv);
char *string_pad_right(char *str, unsigned int length);
char *string_pad_left(char *str, int length);
void string_cookie(char *str, int length);
char *string_subst(char *value, string_subst_lookup_t lookup, void *arg);
int string_prefix_is(const char *string, const char *prefix);
int string_suffix_is(const char *string, const char *suffix);

/** Appends second to first, both null terminated strings. Returns the new
  formed string. First argument is reallocated with realloc.
  @param first Null terminated string.
  @param second Null terminated string.
  @return Null terminated string concatenating second to first.
  */
char *string_combine(char *first, const char *second);
char *string_combine_multi(char *first, ...);
char *string_signal(int sig);
void string_tolower(char *str);
void string_toupper(char *str);
int string_isspace(const char *str);
int string_is_integer(const char *str, long long *integer_value );
int string_is_float(const char *str, double *double_value );
void string_replace_backslash_codes(const char *instr, char *outstr);

/** Replace instances of %% in a string with the string 'replace'.
  To escape this behavior, %%%% becomes %%.
  (Backslash it not used as the escape, as it would interfere with shell escapes.)
  This function works like realloc: the string str must be created by malloc
  and may be freed and reallocated.  Therefore, always invoke it like this:
  x = replace_percents(x,replace);
  @param str Base string to have percents replaced within.
  @param replace String used to replace %%.
  @return The base string with replacements.
  */
char *string_replace_percents( const char *str, const char *replace );
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
char *string_format (const char *fmt, ...)
__attribute__ (( format(printf,1,2) ));

/** Writes a string formatted using snprintf. It is an error if the string is longer than the buffer provided.
  @param str Output string buffer, passed as first argument of snprintf.
  @param max Maximum number of characters to write to str, counting the final '\0'.
  @param fmt Format string passed to snprintf.
  @param ... Variable arguments passed to snprintf
  @return The number of character written, not counting the final '\0'.
 */
int string_nformat(char *str, const size_t max, const char *fmt, ...);

char *string_trim(char *s, int(func)(int));
char *string_trim_spaces(char *s);
char *string_trim_quotes(char *s);

/** Converts a string to a boolean value. "true", "yes", and "N">0 are,
 * case-insensitive, true. Everything else (including NULL) is false.
 * @param str A boolean, possibly NULL, string.
 * @return True or false.
 */
int string_istrue(const char *str);

/**
Apply a wrapper to a given command. If the wrapper_command contains {}, do the substitution there. Otherwise, just append the command to the wrapper with an extra space.

Example:

<pre>
string_wrap_command( "ls -l", "strace -o trace" ) -> "strace -o trace ls -l"
string_wrap_command( "ls -l", "strace {} > output" ) -> "strace ls -la > output"
string_wrap_command( "ls -l", 0 ) -> "ls -l"
</pre>
@param command The original command.
@param wrapper_command The command to wrap around it.
@result The combined command, returned as a newly allocated string.
*/

char * string_wrap_command( const char *command, const char *wrapper_command );


#ifndef CCTOOLS_OPSYS_LINUX
char *strsep(char **stringp, const char *delim);
#endif

char *strnchr (const char *s, int c);

#endif
