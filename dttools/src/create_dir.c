/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2005- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include <unistd.h>
#include <string.h>
#include <errno.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <sys/types.h>

#include "debug.h"

int create_dir(const char *path, int mode)
{
	char *temp;
	char *delim;
	char oldchar;
	int result;

	temp = malloc(strlen(path) + 1);
	strcpy(temp, path);

	delim = temp;

	while((delim = strchr(delim, '/'))) {

		if(delim == temp) {
			delim++;
			continue;
		}

		oldchar = *delim;
		*delim = 0;

		result = mkdir(temp, mode);
		if(result != 0) {
			if(errno == EEXIST) {
				/* no problem, keep going */
			} else {
				free(temp);
				return 0;
			}
		} else {
			/* ok, made it successfully */
		}

		*delim = oldchar;
		delim++;
	}

	/* Now, last chance */

	result = mkdir(temp, mode);

	free(temp);

	if(result != 0) {
		if(errno == EEXIST) {
			return 1;
		} else {
			return 0;
		}
	} else {
		return 1;
	}
}
