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
	const char *path;
	char buf[4096];
	int result;

	if(argc<2) {
		path = ".";
	} else {
		path = argv[1];
	}

	if(argc>2 || path[0]=='-') {
		printf("use: parrot_whoami [path]\n");
		return 0;
	}

	result = parrot_whoami(path,buf,sizeof(buf));
	if(result>=0) {
		buf[result] = 0;
		printf("%s\n",buf);
		return 0;
	} else {
		if(errno==ENOSYS || errno==EINVAL) {
			fprintf(stderr,"whoami: This filesystem doesn't report your subject name\n");
		} else {
			fprintf(stderr,"whoami: %s\n",strerror(errno));
		}
		return 1;
	}
}

/* vim: set noexpandtab tabstop=4: */
