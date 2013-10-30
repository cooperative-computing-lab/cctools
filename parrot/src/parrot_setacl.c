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
	if(argc!=4) {
		printf("use: parrot_setacl <path> <subject> <rights>\n");
		return 0;
	}

	if(!strcmp(argv[3],"read")) argv[3] = "rl";
	if(!strcmp(argv[3],"write")) argv[3] = "rwld";
	if(!strcmp(argv[3],"admin")) argv[3] = "rwlda";
	if(!strcmp(argv[3],"none")) argv[3] = "-";

	if(parrot_setacl(argv[1],argv[2],argv[3])>=0) {
		return 0;
	} else {
		if(errno==ENOSYS || errno==EINVAL) {
			fprintf(stderr,"setacl: This filesystem does not support Parrot access controls.\n");
		} else {
			fprintf(stderr,"setacl: %s\n",strerror(errno));
		}
		return 1;
	}
}

/* vim: set noexpandtab tabstop=4: */
