/*
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "parrot_client.h"

#include "debug.h"

#include <unistd.h>

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

int main( int argc, char *argv[] )
{
	const char *flags = NULL;
	const char *file = NULL;
	off_t size = 0;

	if (!(2 <= argc && argc <= 4)) {
		fprintf(stderr, "Use: %s <flags> [file [size]]\n", argv[0]);
		fprintf(stderr, "Debug flags are: ");
		debug_flags_print(stderr);
		fprintf(stderr, "\n");
		exit(EXIT_FAILURE);
	}

	flags = argv[1];
	if (argc >= 3)
		file = argv[2];
	if (argc >= 4)
		size = strtol(argv[3], NULL, 10);

	int result = parrot_debug(flags, file, size);
	if (result == -1) {
		fprintf(stderr, "debug: %s\n", strerror(errno));
		exit(EXIT_FAILURE);
	}
	return 0;
}

/* vim: set noexpandtab tabstop=4: */
