/*
  Copyright (C) 2022 The University of Notre Dame This software is
  distributed under the GNU General Public License.  See the file
  COPYING for details.


  Given a file, replace environment variable with actual values
  into new file. If no output-file is specified the original will
  be replaced.

*/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <ctype.h>

#include "debug.h"
#include "envtools.h"
#include "stringtools.h"
#include "xxmalloc.h"

void show_help(const char *exe) {
	fprintf(stderr, "Usage:\n%s input-file [output-file]\n", exe);
}

int main(int argc, char **argv) {

	if(argc < 2 || argc > 3) {
		show_help(argv[0]);
		fatal("ARGC %d: %s", argc, argv[1]);
		//exit(1);
	}

	const char *input = argv[1];

	char *output = NULL;
	if(argc > 2) {
		output  = xxstrdup(argv[2]);
	} else {
		output  = string_format("%s.XXXXXX",input);
		int output_fd = mkstemp(output);
		if (output_fd == -1){
			fatal("could not create `%s': %s", output, strerror(errno));
		}
		close(output_fd);
	}

	if (env_replace(input, output)){
		fatal("unable to write replaced variables from %s to %s", input, output);
	}

	return 0;
}

/* vim: set noexpandtab tabstop=8: */
