#include "s3client.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stringtools.h>

char userid[] = "AKIAI2WCNJXC4FOVWZUQ";
char key[] = "T2YG2V9Dz5gSPRfnO9oIGA9mTFMFQRJYvkIimhzE";

int main(int argc, char** argv) {
	if(argc < 2) {
		fprintf(stderr, "usage: s3rmdir <bucket>\n");
		return -1;
	}

	s3_rm_bucket(argv[1], userid, key);

	return 0;
}

