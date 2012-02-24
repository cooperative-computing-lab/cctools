/*
Copyright (C) 2008- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "int_sizes.h"
#include "stringtools.h"
#include "parrot_client.h"

#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <string.h>
#include <unistd.h>

int main( int argc, char *argv[] )
{
	INT64_T size;

	if(argc!=3) {
		printf("use: parrot_mkalloc <path> <size>\n");
		return 0;
	}

	size = string_metric_parse(argv[2]);

	if(parrot_mkalloc(argv[1],size,0777)==0) {
		return 0;
	} else {
		if(errno==ENOSYS || errno==EINVAL) {
			fprintf(stderr,"parrot_mkalloc: This filesystem does not support allocations.\n");
		} else {
			fprintf(stderr,"parrot_mkalloc: %s\n",strerror(errno));
		}
		return 1;
	}
}
