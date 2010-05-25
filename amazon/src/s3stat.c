#include "s3client.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stringtools.h>

#include "s3passwd.h"

int main(int argc, char** argv) {
	struct s3_dirent_object d;
	char remotename[FILENAME_MAX];

	if(argc < 3) {
		fprintf(stderr, "usage: s3get <filename> <bucket>\n");
		return -1;
	}
	sprintf(remotename, "/%s", string_basename(argv[1]));

	s3_stat_file(remotename, argv[2], &d, userid, key);

	return 0;
}

