/*
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

/* Auxiliary functions for common processing of options */

#include <stdio.h>
#include <errno.h>
#include <string.h>

#include "getopt_aux.h"
#include "debug.h"

void opts_write_port_file(const char *port_file, const int port)
{
	if(port_file) {
		FILE *file = fopen(port_file,"w");

		if(!file)
			fatal("couldn't write to %s: %s\n", port_file, strerror(errno));

		fprintf(file, "%d\n", port);
		fclose(file);
	}
}

/* vim: set noexpandtab tabstop=8: */
