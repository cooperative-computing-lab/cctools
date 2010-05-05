#include "s3client.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stringtools.h>

char userid[] = "AKIAI2WCNJXC4FOVWZUQ";
char key[] = "T2YG2V9Dz5gSPRfnO9oIGA9mTFMFQRJYvkIimhzE";

int main(int argc, char** argv) {
	struct hash_table *acls;
	char *id, *filename;
	struct s3_acl_object *acl;
	char remotename[FILENAME_MAX];

	if(argc < 2) {
		fprintf(stderr, "usage: s3mkdir <bucket>\n");
		return -1;
	}

	if(argc > 2) {
		sprintf(remotename, "/%s", argv[2]);
		filename = remotename;
	} else filename = NULL;

	acls = hash_table_create(0, NULL);
	s3_getacl(argv[1], filename, acls, userid, key);

	hash_table_firstkey(acls);
	while(hash_table_nextkey(acls, &id, (void**)&acl)) {
		switch(acl->acl_type) {
			case S3_ACL_ID:
				printf("%s\t", acl->display_name);
				break;
			case S3_ACL_EMAIL:
			case S3_ACL_URI:
				printf("%s\t", id);
		}

		if(acl->perm & S3_ACL_FULL_CONTROL)	printf("f");
		if(acl->perm & S3_ACL_READ)		printf("r");
		if(acl->perm & S3_ACL_WRITE)		printf("w");
		if(acl->perm & S3_ACL_READ_ACP)		printf("g");
		if(acl->perm & S3_ACL_WRITE_ACP)	printf("s");

		printf("\n");
	}

	return 0;
}

