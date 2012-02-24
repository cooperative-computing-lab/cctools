/*
Copyright (C) 2010- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stringtools.h>

#include "s3common.h"
#include "s3c_bucket.h"

int main(int argc, char** argv) {
	s3_initialize(&argc, argv);
	if(argc < 2) {
		fprintf(stderr, "usage: s3rmdir <bucket>\n");
		return -1;
	}

	s3_rm_bucket(argv[1], s3_userid(), s3_key());

	return 0;
}

