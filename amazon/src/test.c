#include <stdio.h>
#include <time.h>
#include <hmac.h>
#include <sha1.h>
#include <string.h>
#include <iconv.h>
#include <b64_encode.h>
#include "s3client.h"

char userid[] = "AKIAI2WCNJXC4FOVWZUQ";
char key[] = "T2YG2V9Dz5gSPRfnO9oIGA9mTFMFQRJYvkIimhzE";

int main(int argc, char** argv) {

	s3_get_file("/tmp/fileyvItaQ", NULL, "/hello_world.txt", "malbrec2", userid, key);
	return 0;
}

