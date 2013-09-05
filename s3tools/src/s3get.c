/*
Copyright (C) 2010- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "path.h"

#include "s3common.h"
#include "s3c_file.h"

int main(int argc, char** argv) {
	char remotename[FILENAME_MAX];

	s3_initialize(&argc, argv);
	
	if(argc < 2) {
		fprintf(stderr, "usage: s3get <bucket> <filename>\n");
		return -1;
	}
	fprintf(stderr, "checking bucket %s for file %s\n", argv[1], argv[2]);

	sprintf(remotename, "/%s", path_basename(argv[2]));
	s3_get_file(argv[2], NULL, remotename, argv[1], s3_userid(), s3_key());

	return 0;
}

