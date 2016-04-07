/*
  Copyright (C) 2016- The University of Notre Dame This software is
  distributed under the GNU General Public License.  See the file
  COPYING for details.
*/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>

#include "jx.h"
#include "jx_parse.h"
#include "jx_print.h"

int main(int argc, char **argv) {
	if(argc < 3) {
		fprintf(stderr, "Usage:\n%s expected-number-of-objects input-file\n", argv[0]);
		exit(1);
	}

	int n = atoi(argv[1]);
	const char *filename = argv[2];

	FILE *stream = fopen(filename, "r");

	if(!stream) {
		fprintf(stderr, "%s: Could not open file '%s' (%s)\n", argv[0], filename, strerror(errno));
		exit(1);
	}

	int count = 0;

	struct jx *j;
	while((j = jx_parse_stream(stream))) {
		char *str = jx_print_string(j);
		fprintf(stdout, "%s\n", str);

		jx_delete(j);
		free(str);

		count++;
	}

	if(count == n) {
		exit(0);
	} else {
		fprintf(stderr, "%s: Expected %d objects, got %d.\n", argv[0], n, count);
		exit(2);
	}
}

/* vim: set noexpandtab tabstop=4: */
