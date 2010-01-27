/*
Copyright (C) 2009- The University of Notre Dame
Originally written by Kevin Partington (27 January 2009)
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef __LINEREADER_H__
#define __LINEREADER_H__

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/** Read a line of any length from a file.
The function is invoked with a stack buffer ("buffer") of a set size.
It will try to populate the stack buffer as much as it can 
 **/
/** Delete a linked list.
Note that this function only deletes the list itself,
it does not delete the items referred to by the list.
@param list The list to delete.
*/
static char * get_line( FILE *fp, char *buffer, int size )
{
	static char *other = NULL;

	/* Free the other buffer, if we have used it. */
	if (other)
	{
		free(other);
		other = NULL;
	}

	if (!fgets(buffer, size, fp))
	{
		return NULL;
	}

	/* If the main buffer is completely filled and there is more
	   to read... */
	if (!strrchr(buffer, '\n'))
	{
		int s = size;

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
				other = tmp;
			}

			strncpy(other, buffer, strlen(buffer));
			fgets(other + s - 1, s + 1, fp);
			s *= 2;

		} while (!strrchr(other, '\n'));

		return other;
	}
	else
	{
		return buffer;
	}
}

#endif
