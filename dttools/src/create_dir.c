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

int create_dir(const char *path, int mode)
{
	char *temp;
	char *current;

	current = temp = malloc(strlen(path) + 2);
	strcpy(temp, path);
	strcat(temp, "/");

	while((current = strchr(current, '/'))) {
		struct stat buf;
		char oldchar = *current;
		if (current == temp) {
		  current += 1;
		  continue;
		}
		*current = 0;
		if (stat(temp, &buf) == 0) {
			if (S_ISDIR(buf.st_mode)) {
				/* continue */
			} else {
				free(temp);
				errno = ENOTDIR;
				return 0;
			}
		} else if (errno == ENOENT && mkdir(temp, mode) == 0) {
			/* continue */
		} else {
			free(temp);
			return 0;
		}

		*current = oldchar;
		current += 1;
	}

	free(temp);
	return 1;
}

/* vim: set noexpandtab tabstop=4: */
