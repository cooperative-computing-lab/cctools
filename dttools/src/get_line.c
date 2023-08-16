/*
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "get_line.h"
#include "xxmalloc.h"

char *get_line(FILE * fp)
{
	static char buffer[LARGE_LINE_MAX];
	char *other = NULL;

	if(!fgets(buffer, LARGE_LINE_MAX, fp)) {
		return NULL;
	}

	/* If the main buffer is completely filled and there is more
	   to read... (second condition is for slackers who don't put newlines
	   at the end of their text files) */
	if(!strrchr(buffer, '\n') && strlen(buffer) == LARGE_LINE_MAX - 1) {
		int s = LARGE_LINE_MAX;

		/* ...use the heap buffer ("other"), doubling its size
		   repeatedly until it is filled. */
		do {
			char *tmp = realloc(other, 2 * s * sizeof(char));

			if(!tmp) {
				free(other);
				other = NULL;
				return NULL;
			} else {
				if(!other) {
					strncpy(tmp, buffer, LARGE_LINE_MAX);
				}

				other = tmp;
			}

			/* Reusing tmp as a return value check */
			tmp = fgets(other + s - 1, s + 1, fp);

			if(!tmp) {
				/* fgets failed because there is no more to
				   read (i.e., EOF), after a read has already
				   occurred. This shouldn't happen if the file
				   is properly delimited with newlines
				   (including one on the end of the file
				   itself). If you don't write newlines at the
				   end of your text files, you should be
				   ashamed of yourself!
				 */
				return other;
			}

			s *= 2;

		} while(!strrchr(other, '\n'));

		return other;

	} else {
		return xxstrdup(buffer);
	}
}

/* vim: set noexpandtab tabstop=8: */
