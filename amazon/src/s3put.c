#include "s3client.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stringtools.h>

char userid[] = "AKIAI2WCNJXC4FOVWZUQ";
char key[] = "T2YG2V9Dz5gSPRfnO9oIGA9mTFMFQRJYvkIimhzE";

int main(int argc, char** argv) {
	char remotename[FILENAME_MAX];

	if(argc < 3) {
		fprintf(stderr, "usage: s3put <filename> <bucket>\n");
		return -1;
	}
	sprintf(remotename, "/%s", string_basename(argv[1]));

	s3_put_file(argv[1], remotename, argv[2], AMZ_PERM_PRIVATE, userid, key);

	return 0;
}

