/*
Copyright (C) 2008- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "chirp_reli.h"
#include "auth_all.h"
#include "debug.h"

#include <stdio.h>
#include <time.h>
#include <string.h>
#include <errno.h>
#include "md5.h"

int main( int argc, char *argv[] )
{
	if(argc!=3)
	{
		printf ("use: chirp_md5sum <hostname[:port]> <remote-file>\n");
		return 0;
	}
	
   auth_register_all();
	const char *host = argv[1];
	char *path = argv[2];
	INT64_T result;
	unsigned char digest[16];

	result = chirp_reli_md5(host,path,digest,time(0)+30);
	if(result>=0) {
		printf("%s\n",md5_string(digest));
	} else {
		printf("error: %s\n",strerror(errno));
	}

	return 0;
}
