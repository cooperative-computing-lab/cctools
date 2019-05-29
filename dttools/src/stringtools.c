/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2005- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "debug.h"
#include "random.h"
#include "stringtools.h"
#include "timestamp.h"
#include "xxmalloc.h"
#include "buffer.h"

#include <assert.h>
#include <ctype.h>
#include <errno.h>
#include <math.h>
#include <regex.h>
#include <signal.h>
#include <stdarg.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

int string_compare(const void *p1, const void *p2) {
	/* The actual arguments to this function are "pointers to
	 * pointers to char", but strcmp(3) arguments are "pointers
	 * to char", hence the following cast plus dereference */
	return strcmp(* (char * const *) p1, * (char * const *) p2);
}

/*
 * Based on opengroup.org's definition of the Shell Command Language (also gnu's)
 * In section 2.2.3 on Double-Quoted Strings, it indicates you only need to
 * escape dollar sign, backtick, and backslash. I also escape double quote as
 * we are adding and exterior double quote around the string.
 *
 * [ $ \ ` " ] Are always escaped.
 * */
char *string_escape_shell( const char *str )
{
	buffer_t B[1];
	buffer_init(B);
	buffer_abortonfailure(B, 1);

	const char *s;
	buffer_putliteral(B,"\"");
	for(s=str;*s;s++) {
		if(*s=='"' || *s=='\\' || *s=='$' || *s=='`')
			buffer_putliteral(B,"\\");
		buffer_putlstring(B,s,1);
	}
	buffer_putliteral(B,"\"");

	char *result;
	buffer_dup(B,&result);
	buffer_free(B);

	return result;
}

char *string_quote_shell(const char *str) {
	int backslashed = 0;
	buffer_t B[1];
	buffer_init(B);
	buffer_abortonfailure(B, 1);

	const char *s;
	buffer_putliteral(B, "\"");
	for (s=str; *s; s++) {
		if (backslashed) {
			backslashed = 0;
		} else {
			if (*s == '"')
				buffer_putliteral(B,"\\");
			else if (*s == '\\')
				backslashed = 1;
		}
		buffer_putlstring(B,s,1);
	}
	buffer_putliteral(B,"\"");

	char *result;
	buffer_dup(B,&result);
	buffer_free(B);

	return result;
}

/*
 * Based on HTCondor documentation:
 * -The white space characters of spaces or tabs delimit arguments.
 * -To embed white space characters of spaces or tabs within a single argument, 
 * surround the entire argument with single quote marks.
 * -To insert a literal single quote mark, escape it within an argument already 
 * delimited by single quote marks by adding another single quote mark.
 *
 * We surround the whole string with double quotes to enable quote escaping in
 * Condor. Then when a double quote is encountered we escape with another double
 * quote. When a single quote is encountered we add a single quote to enter 
 * 'single quote mode' and then escape the quote with another single quote. This
 * does not attempt to match single quotes or double quotes, just tries to escape 
 * every kind of quote.
 * */
char *string_escape_condor( const char *str )
{
	buffer_t B[1];
	buffer_init(B);
	buffer_abortonfailure(B, 1);

	const char *s;
	buffer_putliteral(B,"\"");
	for(s=str;*s;s++) {
		if(*s=='"')
			buffer_putliteral(B,"\"");
		if(*s=='\'')
			buffer_putliteral(B,"\'\'");
		buffer_putlstring(B,s,1);
	}
	buffer_putliteral(B," ");
	buffer_putliteral(B,"\"");

	char *result;
	buffer_dup(B,&result);
	buffer_free(B);

	return result;
}

void string_chomp(char *start)
{
	char *s = start;

	if(!s)
		return;
	if(!*s)
		return;

	while(*s) {
		s++;
	}

	s--;

	while(s >= start && (*s == '\n' || *s == '\r')) {
		*s = 0;
		s--;
	}
}

int whole_string_match_regex(const char *text, const char *pattern)
{
	char *new_pattern;
	int result;

	if(!pattern || !text)
		return 0;

	new_pattern = (char *) malloc(sizeof(char) * (strlen(pattern) + 3));
	if(!new_pattern)
		return 0;

	new_pattern[0] = '\0';
	if(*pattern != '^')
		strcat(new_pattern, "^");
	strcat(new_pattern, pattern);
	if(pattern[strlen(pattern) - 1] != '$')
		strcat(new_pattern, "$");

	result = string_match_regex(text, new_pattern);
	free(new_pattern);

	return result;
}


int string_match_regex(const char *text, const char *pattern)
{
	int ret = 0;
	regex_t re;

	if(!pattern || !text)
		return 0;
	if(regcomp(&re, pattern, REG_EXTENDED | REG_NOSUB) != 0) {
		return 0;
	}
	ret = regexec(&re, text, (size_t) 0, NULL, 0);
	regfree(&re);
	if(!ret)
		return 1;
	return 0;
}

int string_match(const char *pattern, const char *text)
{
	char *w;
	int headlen, taillen;

	w = strchr(pattern, '*');
	if(!w)
		return !strcmp(pattern, text);

	headlen = w - pattern;
	taillen = strlen(pattern) - headlen - 1;

	return !strncmp(pattern, text, headlen) && !strcmp(&pattern[headlen + 1], &text[strlen(text) - taillen]);
}

char *string_front(const char *str, int max)
{
	static char buffer[256];
	int length;

	length = strlen(str);
	if(length < max) {
		strcpy(buffer, str);
	} else {
		strncpy(buffer, str, max);
		buffer[max] = 0;
	}
	return buffer;
}

const char *string_back(const char *str, int max)
{
	int length;

	length = strlen(str);
	if(length < max) {
		return str;
	} else {
		return &str[length - max];
	}
}

char *string_metric(double value, int power_needed, char *buffer)
{
	static const char suffix[][3] = { "", " K", " M", " G", " T", " P" };
	static char localbuffer[100];

	double magnitude;

	if(power_needed == -1) {
		magnitude = floor(log(value)/log(1024.0));
	} else {
		magnitude = power_needed;
	}
	magnitude = fmin(fmax(magnitude, 0.0), (double)(sizeof(suffix)/sizeof(suffix[0])-1));

	if(!buffer)
		buffer = localbuffer;

	snprintf(buffer, sizeof(localbuffer), "%.1f%s", value / pow(1024.0, magnitude), suffix[(int)magnitude]);

	return buffer;
}

int64_t string_metric_parse(const char *str)
{
	int64_t result;
	char prefix;

	switch (sscanf(str, "%" SCNd64 " %c", &result, &prefix)) {
		case 2:
			switch (toupper((int) prefix)) {
				case 'P':
					return result << 50;
				case 'T':
					return result << 40;
				case 'G':
					return result << 30;
				case 'M':
					return result << 20;
				case 'K':
					return result << 10;
				default:
					return result;
			}
		case 1:
			return result;
		default:
			return errno = EINVAL, -1;
	}
}

time_t string_time_parse(const char *str)
{
	int64_t t;
	char mod;

	switch (sscanf(str, "%" SCNd64 " %c", &t, &mod)) {
		case 2:
			switch (tolower((int)mod)) {
				case 'd':
					return t * 60 * 60 * 24;
				case 'h':
					return t * 60 * 60;
				case 'm':
					return t * 60;
				case 's':
				default:
					return t;
			}
		case 1:
			return t;
		default:
			return errno = EINVAL, -1;
	}
}

/*
Split a string into words, recognizing only spaces.
You probably want to use string_split_quotes instead.
*/

int string_split(char *str, int *argc, char ***argv)
{
	*argc = 0;

	*argv = malloc((strlen(str) + 1) * sizeof(char *));
	if(!*argv)
		return 0;

	while(*str) {
		while(isspace((int) *str)) {
			str++;
		}
		(*argv)[(*argc)++] = str;
		while(*str && !isspace((int) *str)) {
			str++;
		}
		if(*str) {
			*str = 0;
			str++;
		}
	}

	(*argv)[*argc] = 0;

	return 1;
}

/*
Split a string into args, respecting backwhacks and quotes.
This is probably the one you want to use.
*/

int string_split_quotes(char *str, int *argc, char ***argv)
{
	*argc = 0;

	*argv = malloc((strlen(str) + 1) * sizeof(char *));
	if(!*argv)
		return 0;

	while(*str) {

		/* Skip over leading whitespace */

		while(isspace((int) *str)) {
			str++;
		}

		if(!*str)
			break;

		/* The token begins here. */
		(*argv)[(*argc)++] = str;

		/* Start advancing over tokens */
		while(*str) {
			if(*str == '\\') {
				/* If we are backwhacked, shift and continue */
				memmove(str, str + 1, strlen(str));
				if(*str)
					str++;
			} else if(isspace((int) *str)) {
				/* If we have found a delimiter, accept */
				*str = 0;
				str++;
				break;
			} else if(*str == '\'' || *str == '\"') {
				/* Upon finding a quote, we enter a new loop */
				char quote = *str;
				memmove(str, str + 1, strlen(str));
				while(*str) {
					if(*str == '\\') {
						/* Skip anything backwhacked */
						memmove(str, str + 1, strlen(str));
						if(*str)
							str++;
					} else if(*str == quote) {
						/* Shift and stop on a matching quote */
						memmove(str, str + 1, strlen(str));
						break;
					} else {
						/* Otherwise, keep going */
						str++;
					}
				}
			} else if(!*str) {
				/* If we have found the end, accept */
				break;
			} else {
				/* Otherwise, continue on */
				str++;
			}
		}
	}

	(*argv)[*argc] = 0;

	return 1;
}

char *string_pad_right(char *old, unsigned int length)
{
	unsigned int i;
	char *s = malloc(length + 1);
	if(!s)
		return 0;

	if(strlen(old) <= length) {
		strcpy(s, old);
		for(i = strlen(old); i < length; i++) {
			s[i] = ' ';
		}
	} else {
		strncpy(s, old, length);
	}
	s[length] = 0;
	return s;
}

char *string_pad_left(char *old, int length)
{
	int i;
	int slength;
	int offset;
	char *s;

	s = malloc(length + 1);
	if(!s)
		return 0;

	slength = strlen(old);
	offset = length - slength;

	for(i = 0; i < length; i++) {
		if(i < offset) {
			s[i] = ' ';
		} else {
			s[i] = old[i - offset];
		}
	}

	s[length] = 0;
	return s;
}

void string_cookie(char *s, int length)
{
	int i;
	random_init();

	for(i = 0; i < length; i++) {
		s[i] = random_int() % 26 + 'a';
	}

	s[length - 1] = 0;
}

char *string_subst(char *value, string_subst_lookup_t lookup, void *arg)
{
	char *subvalue, *newvalue;
	char *dollar, *ldelim, *rdelim;
	char oldrdelim;
	int length;

	while(1) {
		dollar = strchr(value, '$');
		if(!dollar)
			return value;

		while(dollar > value) {
			if(*(dollar - 1) == '\\') {
				dollar = strchr(dollar + 1, '$');
			} else if(*(dollar + 1) == '$') {
				*dollar = ' ';
				dollar = strchr(dollar + 2, '$');
			} else {
				break;
			}

			if(!dollar)
				return value;
		}

		ldelim = dollar + 1;
		if(*ldelim == '(') {
			rdelim = ldelim + 1;
			while(*rdelim != ')')
				rdelim++;
		} else if(*ldelim == '{') {
			rdelim = ldelim + 1;
			while(*rdelim != '}')
				rdelim++;
		} else {
			ldelim--;
			rdelim = ldelim + 1;
			while(isalnum((int) *rdelim) || *rdelim == '_')
				rdelim++;
		}

		oldrdelim = *rdelim;
		*rdelim = 0;

		subvalue = lookup(ldelim + 1, arg);
		if(!subvalue)
			subvalue = strdup("");

		*rdelim = oldrdelim;

		length = strlen(value) - (rdelim - dollar) + strlen(subvalue) + 1;
		newvalue = malloc(length);
		if(!newvalue) {
			free(subvalue);
			free(value);
			return 0;
		}

		if(ldelim != dollar)
			rdelim++;
		*dollar = 0;

		strcpy(newvalue, value);
		strcat(newvalue, subvalue);
		strcat(newvalue, rdelim);

		free(subvalue);
		free(value);

		value = newvalue;
	}
}


int string_prefix_is(const char *string, const char *prefix) {
	size_t n;

	if(!string || !prefix) return 0;

	if((n = strlen(prefix)) == 0) return 0;

	if(strncmp(string, prefix, n) == 0) return 1;

	return 0;
}

int string_suffix_is(const char *string, const char *suffix) {
	size_t n, m;

	if(!string || !suffix) return 0;

	if((n = strlen(suffix)) == 0) return 0;
	if((m = strlen(string)) < n)  return 0;

	if(strncmp((string + m - n), suffix, n) == 0) return 1;

	return 0;
}


/* This definition taken directly from the GNU C library */

#undef __strsep
#undef strsep

#ifndef CCTOOLS_OPSYS_DARWIN

char *strsep(char **stringp, const char *delim)
{
	char *begin, *end;

	begin = *stringp;
	if(begin == NULL)
		return NULL;

	/* A frequent case is when the delimiter string contains only one
	   character.  Here we don't need to call the expensive `strpbrk'
	   function and instead work using `strchr'.  */
	if(delim[0] == '\0' || delim[1] == '\0') {
		char ch = delim[0];

		if(ch == '\0')
			end = NULL;
		else {
			if(*begin == ch)
				end = begin;
			else if(*begin == '\0')
				end = NULL;
			else
				end = strchr(begin + 1, ch);
		}
	} else
		/* Find the end of the token.  */
		end = strpbrk(begin, delim);

	if(end) {
		/* Terminate the token and set *STRINGP past NUL character.  */
		*end++ = '\0';
		*stringp = end;
	} else
		/* No more delimiters; this is the last token.  */
		*stringp = NULL;

	return begin;
}

#endif

char *string_combine(char *a, const char *b)
{
	char *r = NULL;
	size_t a_len;

	if(!a) {
		if(!b) {
			return r;
		} else {
			return xxstrdup(b);
		}
	}

	if(!b) return a;

	a_len = strlen(a);
	r = realloc(a, (a_len + strlen(b) + 1) * sizeof(char));
	if(!r) {
		fatal("Cannot allocate memory for string concatenation.\n");
	}
	strcat(r, b);

	return r;
}

char *string_combine_multi(char *r, ...)
{
	char *n;
	va_list args;
	va_start(args, r);


	while((n = va_arg(args, char *))) {
		r = string_combine(r, n);
	}

	return r;

	va_end(args);
}

char *string_signal(int sig)
{
#ifdef HAS_STRSIGNAL
	return strsignal(sig);
#else
	return (char *) _sys_siglist[sig];
#endif
}

void string_tolower(char *s)
{
	while(*s) {
		*s = tolower((int) (*s));
		s++;
	}
}

void string_toupper(char *s)
{
	while(*s) {
		*s = toupper((int) (*s));
		s++;
	}
}

int string_is_integer( const char *s, long long *integer_value )
{
	char *endptr;
	*integer_value = strtoll(s,&endptr,10);
	return !*endptr;
}

int string_is_float( const char *s, double *double_value )
{
	char *endptr;
	*double_value = strtod(s,&endptr);
	return !*endptr;
}

int string_isspace(const char *s)
{
	while(*s) {
		if(!isspace((int) *s))
			return 0;
		s++;
	}

	return 1;
}

void string_replace_backslash_codes(const char *a, char *b)
{
	while(*a) {
		if(*a == '\\') {
			a++;
			char c;
			switch (*a) {
			case 'a':
				c = 7;
				break;	// bell
			case 'b':
				c = 8;
				break;	// backspace
			case 't':
				c = 9;
				break;	// tab
			case 'n':
				c = 10;
				break;	// newline
			case 'v':
				c = 11;
				break;	// vertical tab
			case 'f':
				c = 12;
				break;	// formfeed
			case 'r':
				c = 13;
				break;	// return
			default:
				c = *a;
				break;
			}
			*b++ = c;
			a++;
		} else {
			*b++ = *a++;
		}
	}

	*b = 0;
}

char *string_replace_percents( const char *str, const char *replace )
{
	/* Common case: do nothing if no percents. */
	if(!strchr(str,'%')) return xxstrdup(str);

	buffer_t buffer;
	buffer_init(&buffer);

	const char *s;
	for(s=str;*s;s++) {
		if(*s=='%' && *(s+1)=='%' ) {
			if( *(s+2)=='%' && *(s+3)=='%') {
				buffer_putlstring(&buffer,"%%",2);
				s+=3;
			} else {
				buffer_putstring(&buffer,replace);
				s++;
			}
		} else {
			buffer_putlstring(&buffer,s,1);
		}
	}

	char *result;
	buffer_dup(&buffer,&result);
	buffer_free(&buffer);

	return result;
}

int strpos(const char *str, char c)
{

	unsigned int i;
	if(str != NULL) {
		for(i = 0; i < strlen(str); i++) {
			if(str[i] == c)
				return i;
		}
	}
	return -1;
}


int strrpos(const char *str, char c)
{

	int i;
	if(str != NULL) {
		for(i = strlen(str) - 1; i >= 0; i--) {
			if(str[i] == c)
				return i;
		}
	}
	return -1;
}

int string_null_or_empty(const char *str)
{
	if(!str)
		return 1;
	if(!strncmp(str, "", 1))
		return 1;
	return 0;
}

int getDateString(char *str)
{

	int retval;
	char *Month[12] = { "Jan", "Feb", "Mar", "Apr", "May", "Jun",
		"Jul", "Aug", "Sep", "Oct", "Nov", "Dec"
	};

	struct tm *T = NULL;
	time_t Tval = 0;
	Tval = time(NULL);
	T = localtime(&Tval);
	if(T->tm_mday < 10)
		retval = sprintf(str, "%s0%d", Month[T->tm_mon], T->tm_mday);
	else
		retval = sprintf(str, "%s%d", Month[T->tm_mon], T->tm_mday);
	if(retval <= 4)
		return 0;
	else
		return 1;
}

char * string_format( const char *fmt, ... )
{
	va_list args;

	va_start(args,fmt);
	int n = vsnprintf(NULL, 0, fmt, args);
	va_end(args);

	if(n < 0)
		return NULL;

	char *str = xxmalloc((n + 1) * sizeof(char));
	va_start(args,fmt);
	n = vsnprintf(str, n + 1, fmt, args);
	va_end(args);

	assert(n >= 0);

	return str;
}

int string_nformat (char *str, const size_t max, const char *fmt, ...)
{
	va_list(va);
	va_start(va, fmt);
	size_t n = vsnprintf(str, max, fmt, va);
	va_end(va);

	if( max <= n )
		fatal("String '%30s...' is %zd (greater than the %zd limit).", str, n, max);

	return n;
}

char *string_trim(char *s, int func(int))
{
	char *p;

	/* Skip front */
	while (func(*s))
		s++;

	/* Skip back */
	p = s + strlen(s) - 1;
	while (func(*p))
		p--;

	/* Terminate string */
	*(p + 1) = 0;

	return s;
}

char *string_trim_spaces(char *s)
{
	return string_trim(s, isspace);
}

char *string_trim_quotes(char *s)
{
	char *front, *back;

	front = s;
	back  = s + strlen(s) - 1;

	while (*front == '\'' || *front == '"') {
		if (*back != *front)
			break;
		*back = 0;
		back--;
		front++;
	}

	return front;
}

int string_istrue(const char *str)
{
	if(str == NULL)
		str = "";
	return (strcasecmp(str, "true") == 0) || (strcasecmp(str, "yes") == 0) || (atoi(str) > 0);
}

int string_equal(const char *str1, const char *str2){
	return !strcmp(str1, str2);
}

char * string_wrap_command( const char *command, const char *wrapper_command )
{
	if(!wrapper_command) return xxstrdup(command);

	char * braces = strstr(wrapper_command,"{}");
	char * square = strstr(wrapper_command,"[]");
	char * new_command;

	if(braces) {
		new_command = xxstrdup(command);
	} else {
		new_command = string_escape_shell(command);
	}

	char * result = malloc(strlen(new_command)+strlen(wrapper_command)+16);

	if(braces) {
		strcpy(result,wrapper_command);
		result[braces-wrapper_command] = 0;
		strcat(result,new_command);
		strcat(result,braces+2);
	} else if(square) {
		strcpy(result,wrapper_command);
		result[square-wrapper_command] = 0;
		strcat(result,new_command);
		strcat(result,square+2);
	} else {
		strcpy(result,wrapper_command);
		strcat(result," /bin/sh -c ");
		strcat(result,new_command);
	}

	free(new_command);

	return result;
}

char *strnchr (const char *s, int c)
{
	char *next = strchr(s, c);
	if (next) next += 1;
	return next;
}

/* vim: set noexpandtab tabstop=4: */
