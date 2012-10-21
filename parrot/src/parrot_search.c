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

static char *strerrsource(int errsource) {
	switch (errsource) {
		case PFS_SEARCH_ERR_OPEN: return "Open";
		case PFS_SEARCH_ERR_READ: return "Read";
		case PFS_SEARCH_ERR_CLOSE: return "Close";
		case PFS_SEARCH_ERR_STAT: return "Stat";
		default: return "Unknown";
	}
}

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

	int flags = PFS_SEARCH_METADATA|PFS_SEARCH_INCLUDEROOT;
	SEARCH *s = opensearch(paths, pattern, flags);
	struct searchent *res;

	while ((res = readsearch(s)) != NULL) {

		if (res->err) {
			printf("%s error on %s: %s\n", strerrsource(res->errsource), res->path, strerror(res->err));
			continue;
		}

		printf("%-30s", res->path);

		if (flags & PFS_SEARCH_METADATA)
			printf("\t%-10zd\t%-10zd\n", res->info->st_size, res->info->st_ino);
		else
			printf("\n");
	}

	closesearch(s);

	return 0;
}
