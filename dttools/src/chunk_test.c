/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2005- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "chunk.h"

int main(int argc, char **argv)
{
	char **files = malloc(2 * sizeof(*files));

	files[0] = malloc(8 * sizeof(*files[0]));
	strcpy(files[0], "chunk.c");

	files[1] = malloc(8 * sizeof(*files[0]));
	strcpy(files[1], "chunk.h");

	if (!chunk_concat("large_chunk.txt", (char **)files, 2, "> ", NULL))
	{
		fprintf(stderr, "chunk_test: chunk_concat failed\n");
		exit(1);
	}
	
	return 0;
}
