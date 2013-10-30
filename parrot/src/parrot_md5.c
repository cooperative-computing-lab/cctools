/*
Copyright (C) 2008- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "md5.h"
#include "parrot_client.h"

#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>

int main( int argc, char *argv[] )
{
	int i;
	unsigned char digest[16];

	if(argc<2) {
		printf("use: parrot_md5 <file> ...\n");
		return 1;
	}

	for(i=1;i<argc;i++) {
		if(parrot_md5(argv[i],digest)>=0 || md5_file(argv[i],digest)) {
			printf("%s %s\n",md5_string(digest),argv[i]);
		} else {
			fprintf(stderr,"parrot_md5: %s: %s\n",argv[i],strerror(errno));
		}
	}

	return 0;
}

/* vim: set noexpandtab tabstop=4: */
