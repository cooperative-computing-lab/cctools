/*
Copyright (C) 2008- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "parrot_client.h"

#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <string.h>
#include <unistd.h>

int main( int argc, char *argv[] )
{
	const char *buf;
	int result;

	if(argc<2) {
		buf = NULL;
	} else {
		buf = argv[1];
	}

	if(argc>2 || (buf && buf[0]=='-')) {
		printf("use: parrot_timeout [time]\n");
		return 0;
	}

	result = parrot_timeout(buf);
	if(result < 0) {
		if(errno==ENOSYS || errno==EINVAL)
			fprintf(stderr,"timeout: This filesystem doesn't support parrot_timeout\n");
		else
			fprintf(stderr, "timeout: %s\n", strerror(errno));
		return 1;
	} else {
		printf("timeout set to %d seconds\n", result);
	}
	return 0;
}
