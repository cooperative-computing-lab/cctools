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
	const char *path;
	const char *pattern;

	if (argc == 2) {
		path = ".";
		pattern = argv[1];
	} else if (argc == 3) {
		path = argv[1];
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
	while ((result = parrot_search(path, pattern, buffer, len1, stats, len2)) == -1 && errno == ERANGE) {
		len1 *= 2;
		buffer = realloc(buffer, len1);
		len2 *= 2;
		stats = realloc(stats, len2);
		if (buffer == NULL || stats == NULL) {
			fprintf(stderr, "not enough memory");
			exit(EXIT_FAILURE);
		}
	}

	if (result >= 0) {
		int i = 0;
        int n = 0;
		while (buffer[i]) {
			printf("%s\n", &buffer[i]);
			i += strlen(&buffer[i])+1;
            n += 1;
		}
		assert(n == result);
	} else {
		fprintf(stderr, "%s: %s", argv[0], strerror(errno));
		exit(EXIT_FAILURE);
	}

	return 0;
}
