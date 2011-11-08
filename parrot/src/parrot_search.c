/*
Copyright (C) 2008- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "parrot_client.h"

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <string.h>
#include <unistd.h>

int main( int argc, char *argv[] )
{
	const char *paths = NULL;
	const char *pattern;

	if (argc == 2) {
		paths = ".";
		pattern = argv[1];
	} else if (argc == 3) {
		paths = argv[1];
		pattern = argv[2];
	} else {
		printf("use: parrot_search [path] <pattern>\n");
		exit(EXIT_FAILURE);
	}

	size_t len1 = 65536;
	char *buffer = malloc(len1);
	size_t len2 = 4096;
	struct stat *stats = malloc(len2*sizeof(struct stat));

    int result;
	while ((result = parrot_search(paths, pattern, buffer, len1, stats, len2, PFS_SEARCH_INCLUDEROOT|PFS_SEARCH_METADATA)) == -1 && errno == ERANGE) {
		len1 *= 2;
		buffer = realloc(buffer, len1);
		len2 *= 2;
		stats = realloc(stats, len2);
		if (buffer == NULL || stats == NULL) {
			fprintf(stderr, "not enough memory");
			exit(EXIT_FAILURE);
		}
	}

	if (result > 0) {
		char *element = buffer;
		while (1) {
			char *next = strchr(element, ':');
			if (next) {
				printf("%.*s\n", (int)(next-element), element);
				element = next+1;
			} else {
				printf("%s\n", element);
				break;
			}
		}
	} else if (result == -1) {
		fprintf(stderr, "%s: %s\n", argv[0], strerror(errno));
		exit(EXIT_FAILURE);
	}

	return 0;
}
