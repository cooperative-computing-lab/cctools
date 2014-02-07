/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2005- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "debug.h"
#include "stringtools.h"
#include "timestamp.h"
#include "xxmalloc.h"

#include <assert.h>
#include <errno.h>
#include <stdio.h>
#include <string.h>
#include <math.h>
#include <stdlib.h>
#include <ctype.h>
#include <stdarg.h>
#include <signal.h>
#include <regex.h>

#define STRINGTOOLS_BUFFER_SIZE 256
#define METRIC_POWER_COUNT 6

char *escape_shell_string(const char *str)
{
	if(str == NULL)
		str = "";
	char *escaped_string = malloc(strlen(str) * 3 + 1);
	if(escaped_string == NULL)
		return NULL;
	const char *old = str;
	char *current = escaped_string;
	strcpy(current, "'");
	current += 1;
	for(; *old; old++) {
		if(*old == '\'') {
			strcpy(current, "'\\''");
			current += 3;
		} else {
			*current = *old;
			current += 1;
		}
	}
	strcpy(current, "'");
	return escaped_string;
}

void string_from_ip_address(const unsigned char *bytes, char *str)
{
	sprintf(str, "%u.%u.%u.%u", (unsigned) bytes[0], (unsigned) bytes[1], (unsigned) bytes[2], (unsigned) bytes[3]);
}

int string_to_ip_address(const char *str, unsigned char *bytes)
{
	unsigned a, b, c, d;
	int fields;

	fields = sscanf(str, "%u.%u.%u.%u", &a, &b, &c, &d);
	if(fields != 4)
		return 0;

	if(a > 255 || b > 255 || c > 255 || d > 255)
		return 0;

	bytes[0] = a;
	bytes[1] = b;
	bytes[2] = c;
	bytes[3] = d;

	return 1;
}

int string_ip_subnet(const char *addr, char *subnet)
{
	unsigned bytes[4];
	int fields;

	fields = sscanf(addr, "%u.%u.%u.%u", &bytes[0], &bytes[1], &bytes[2], &bytes[3]);
	if(fields != 4)
		return 0;

	if(bytes[0] < 128) {
		sprintf(subnet, "%u", bytes[0]);
	} else if(bytes[0] < 192) {
		sprintf(subnet, "%u.%u", bytes[0], bytes[1]);
	} else {
		sprintf(subnet, "%u.%u.%u", bytes[0], bytes[1], bytes[2]);
	}

	return 1;
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

int whole_string_match_regex(const char *text, char *pattern)
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
		strncat(new_pattern, "^", 1);
	strncat(new_pattern, pattern, strlen(pattern));
	if(text[strlen(pattern) - 1] != '$')
		strncat(new_pattern, "$", 1);

	result = string_match_regex(text, new_pattern);
	free(new_pattern);

	return result;
}


int string_match_regex(const char *text, char *pattern)
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
	static char buffer[STRINGTOOLS_BUFFER_SIZE];
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

char *string_metric(double invalue, int power_needed, char *buffer)
{
	static char localbuffer[100];
	static char *suffix[METRIC_POWER_COUNT] = { " ", "K", "M", "G", "T", "P" };

	double value = invalue;
	int power = 0;

	if(power_needed == -1) {
		while((value >= 1000.0) && (power < (METRIC_POWER_COUNT - 1))) {
			value = value / 1024.0;
			power++;
		}
	} else {
		power = power_needed;
		value = value / (pow(2, 10 * power));
	}

	if(!buffer)
		buffer = localbuffer;

	sprintf(buffer, "%.1f %s", value, suffix[power]);

	return buffer;
}

INT64_T string_metric_parse(const char *str)
{
	INT64_T result, factor;
	char prefix;
	int fields;

	fields = sscanf(str, INT64_FORMAT "%c", &result, &prefix);
	if(fields == 1)
		return result;

	switch (toupper((int) prefix)) {
	case 'K':
		factor = 1024LL;
		break;
	case 'M':
		factor = 1024LL * 1024;
		break;
	case 'G':
		factor = 1024LL * 1024 * 1024;
		break;
	case 'T':
		factor = 1024LL * 1024 * 1024 * 1024;
		break;
	case 'P':
		factor = 1024LL * 1024 * 1024 * 1024 * 1024;
		break;
	default:
		factor = 0;
		break;
	}

	return result * factor;
}

int string_time_parse(const char *str)
{
	int value;
	char mod;

	if(sscanf(str, "%d%c", &value, &mod) == 2) {
		switch (mod) {
		case 's':
			return value;
		case 'm':
			return value * 60;
		case 'h':
			return value * 60 * 60;
		case 'd':
			return value * 60 * 60 * 24;
		}
	} else if(sscanf(str, "%d", &value) == 1) {
		return value;
	}

	return 0;
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
				memcpy(str, str + 1, strlen(str));
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
				memcpy(str, str + 1, strlen(str));
				while(*str) {
					if(*str == '\\') {
						/* Skip anything backwhacked */
						memcpy(str, str + 1, strlen(str));
						if(*str)
							str++;
					} else if(*str == quote) {
						/* Shift and stop on a matching quote */
						memcpy(str, str + 1, strlen(str));
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

	for(i = 0; i < length; i++) {
		s[i] = rand() % 26 + 'a';
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
	size_t a_len = strlen(a);

	if(a && b) {
		r = realloc(a, (a_len + strlen(b) + 1) * sizeof(char));
	}

	if(r)
		strcat(r, b);
	else
		fatal("Cannot allocate memory for string concatenation.\n");


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

int string_is_integer(const char *s)
{
	while(*s) {
		if(!isdigit((int) *s))
			return 0;
		s++;
	}
	return 1;
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

char *string_format(const char *fmt, ...)
{
	va_list va;

	va_start(va, fmt);
	int n = vsnprintf(NULL, 0, fmt, va);
	va_end(va);

	if(n < 0)
		return NULL;

	char *str = xxmalloc((n + 1) * sizeof(char));
	va_start(va, fmt);
	n = vsnprintf(str, n + 1, fmt, va);
	assert(n >= 0);
	va_end(va);

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

/* vim: set noexpandtab tabstop=4: */
