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
	int flags = 0;
	char c;

        while((c = getopt(argc, argv, "+smi")) != (char) -1) {
                switch (c) {
                	case 's': 
				flags |= PFS_SEARCH_STOPATFIRST;
				break;
                	case 'i': 
				flags |= PFS_SEARCH_INCLUDEROOT;
				break;
                	case 'm': 
				flags |= PFS_SEARCH_METADATA;
				break;
		}
	}	

	if (argc-optind == 1) {
		paths = ".";
		pattern = argv[optind];
	} else if (argc-optind == 2) {
		paths = argv[optind];
		pattern = argv[optind+1];
	} else {
		printf("use: parrot_search [options] [path] <pattern>\n");
		exit(EXIT_FAILURE);
	}

	SEARCH *s = parrot_opensearch(paths, pattern, flags);
	if (!s) {
		fprintf(stdout, "could not search: %s\n", strerror(errno));
		exit(EXIT_FAILURE);
	}
	struct searchent *res;
	int i = 0;

	while ((res = parrot_readsearch(s)) != NULL) {

		if (res->err) {
			printf("%s error on %s: %s\n", strerrsource(res->errsource), res->path, strerror(res->err));
			continue;
		}

		i++;
		printf("%-30s", res->path);

		if (flags & PFS_SEARCH_METADATA)
			printf("\t%-10ld\t%-10ld\n", (long)res->info->st_size, (long)res->info->st_ino);
		else
			printf("\n");
	}

	if (i==0) printf("no results\n");

	parrot_closesearch(s);

	return 0;
}
