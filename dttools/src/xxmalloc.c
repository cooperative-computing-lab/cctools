/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "debug.h"
#include "xxmalloc.h"

#include <stdlib.h>
#include <string.h>

/*
Note that xmalloc is a common name, as it is suggested by
the Stevens Unix book.  So a to avoid conflicts with packages
such as readline, we use xxmalloc, but it means the same thing.
*/

void *xxmalloc(size_t nbytes)
{
	void *result = malloc(nbytes);
	if(result) {
		return result;
	} else {
		fatal("out of memory");
		return 0;
	}
}

char *xxstrdup(const char *str)
{
	void *result = strdup(str);
	if(result) {
		return result;
	} else {
		fatal("out of memory");
		return 0;
	}
}

void *xxrealloc(void *ptr, size_t nsize)
{
	void *result = realloc(ptr, nsize);
	if(nsize > 0 && result == NULL)
		fatal("out of memory");
	return result;
}

void *xxcalloc(size_t nmemb, size_t size) {
	void *result = calloc(nmemb, size);
	if (result) {
		return result;
	} else {
		fatal("out of memory");
		return NULL;
	}
}

/* vim: set noexpandtab tabstop=8: */
