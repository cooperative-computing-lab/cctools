/*
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "xxmalloc.h"

#include <assert.h>
#include <stddef.h>
#include <string.h>

/* Format:
   <NULL terminated array of char *>
   <size_t size of entire memory block>
   <data>
*/

#define DEFAULT_SIZE  (sizeof(char *) + sizeof(size_t))

char **string_array_new (void)
{
	char **data = (char **) xxrealloc(NULL, DEFAULT_SIZE);
	*data = NULL;
	size_t *length = (size_t *) data+1;
	*length = DEFAULT_SIZE;
	return data;
}

char **string_array_append (char **oarray, const char *str)
{
	char **narray, **tmp;
	for (tmp = oarray; *tmp; tmp++) ;
	tmp++; /* advance past NULL pointer */
	size_t olength = *((size_t *) tmp);
	size_t nlength = olength + strlen(str)+1 + sizeof(char *);
	narray = xxrealloc(oarray, nlength);
	ptrdiff_t offset = ((char *)narray)-((char *)oarray)+sizeof(char *); /* difference including extra pointer */
	for (tmp = narray; *tmp; tmp++)
		*tmp = ((char *)*tmp)+offset; /* correct the address */
	*tmp = (char *) (((char *)narray)+olength+sizeof(char *)); /* set to new string location */
	strcpy(*tmp, str);
	tmp++; /* now points to the old data length */
	memmove(((char *)tmp)+sizeof(char *), tmp, olength-(((char *)tmp)-((char *)narray))); /* careful with pointer arithmetic */
	*tmp = NULL; /* set NULL terminated final entry */
	tmp++;
	*((size_t *) tmp) = nlength; /* set the new length */
	return narray;
}

/* vim: set noexpandtab tabstop=8: */
