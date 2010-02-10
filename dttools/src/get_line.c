/*
Copyright (C) 2009- The University of Notre Dame
Originally written by Kevin Partington (27 January 2009)
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "get_line.h"

char * get_line( FILE *fp )
{
	static char *other = NULL;
	static char buffer[LINE_MAX];

	/* Free the other buffer, if we have used it. */
	if (other)
	{
		free(other);
		other = NULL;
	}

	if (!fgets(buffer, LINE_MAX, fp))
	{
		return NULL;
	}

	/* If the main buffer is completely filled and there is more
	   to read... (second condition is for slackers who don't put newlines
	   at the end of their text files) */
	if (!strrchr(buffer, '\n') && strlen(buffer) == LINE_MAX - 1)
	{
		int s = LINE_MAX;

		/* ...use the heap buffer ("other"), doubling its size
		   repeatedly until it is filled. */
		do {
			char *tmp = realloc(other, 2*s * sizeof(char));

			if (!tmp)
			{
				free(other);
				other = NULL;
				return NULL;
			}
			else
			{
				if (!other)
					strncpy(tmp, buffer, strlen(buffer));

				other = tmp;
			}

			/* Reusing tmp as a return value check */
			tmp = fgets(other + s - 1, s + 1, fp);

			if (!tmp)
			{
				/* fgets failed because there is no more to
				   read (i.e., EOF), after a read has already
				   occurred. This shouldn't happen if the file
				   is properly delimited with newlines
				   (including one on the end of the file
				   itself). If you don't write newlines at the
				   end of your text files, you should be
				   ashamed of yourself! -KP
				*/
				return other;
			}
			
			s *= 2;

		} while (!strrchr(other, '\n'));

		return other;

	}
	else
	{
		return buffer;
	}
}
