/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2005- The University of Notre Dame
This software is distributed under a BSD-style license.
See the file COPYING for details.
*/

#include "sort_dir.h"

#include <dirent.h>
#include <string.h>
#include <stdlib.h>
#include <dirent.h>

int sort_dir( const char *dirname, char ***list, int (*sort) ( const char *a, const char *b ) )
{
	DIR *dir=0;
	struct dirent *d;
	int size=10;
	int used=0;
	char *s;

	*list = malloc(size*sizeof(char*));
	if(!*list) goto failure;

	dir = opendir(dirname);
	if(!dir) goto failure;

	while( (d = readdir(dir)) ) {
		if(used>=size) {
			size *=2;
			*list = realloc(*list,sizeof(char*)*size);
			if(!*list) goto failure;
		}

		s = strdup(d->d_name);
		if(!s) goto failure;

		(*list)[used++] = s;
	}

	if(sort) {
		qsort( *list, used, sizeof(char*), (void*) sort );
	}

	closedir(dir);
	(*list)[used] = 0;
	return 1;

	failure:
	if(*list) {
		sort_dir_free(*list);
		*list = 0;
	} 
	if(dir) closedir(dir);
	return 0;
}

void sort_dir_free( char **list )
{
	int i;
	if(list) {
		for(i=0;list[i];i++) {
			if(list[i]) free(list[i]);
		}
		free(list);
	}
}

