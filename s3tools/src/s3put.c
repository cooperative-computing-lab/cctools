/*
Copyright (C) 2010- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "path.h"

#include "s3common.h"
#include "s3c_file.h"

int main(int argc, char** argv) {
	char remotename[FILENAME_MAX];

	s3_initialize(&argc, argv);
	if(argc < 3) {
		fprintf(stderr, "usage: s3put <filename> <bucket>\n");
		return -1;
	}
	sprintf(remotename, "/%s", path_basename(argv[1]));

	s3_put_file(argv[1], remotename, argv[2], AMZ_PERM_PRIVATE, s3_userid(), s3_key());

	return 0;
}

