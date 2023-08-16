/*
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "url_encode.h"

#include <stdio.h>
#include <string.h>

void url_encode(const char *s, char *t, int length)
{
	if(s) {
		while(*s && length > 1) {
			if(*s <= 32 || *s == '%' || *s == '\\' || *s == '<' || *s == '>' || *s == '\'' || *s == '\"' || *s > 122) {
				if(length > 3) {
					snprintf(t, length, "%%%2X", *s);
					t += 3;
					length -= 3;
					s++;
				} else {
					break;
				}
			} else {
				*t++ = *s++;
				length--;
			}
		}
	}
	*t = 0;
}

void url_decode(const char *s, char *t, int length)
{
	while(*s && length > 1) {
		if(*s == '%') {
			unsigned int x;
			sscanf(s + 1, "%2x", &x);
			*t++ = x;
			s += 3;
		} else {
			*t++ = *s++;
		}
		length--;
	}
	*t = 0;
}

/* vim: set noexpandtab tabstop=8: */
