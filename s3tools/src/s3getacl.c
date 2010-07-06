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
#include "s3c_acl.h"

int main(int argc, char** argv) {
	struct hash_table *acls;
	char *id, *filename;
	struct s3_acl_object *acl;
	char owner[1024];
	char remotename[FILENAME_MAX];

	s3_initialize(&argc, argv);

	if(argc < 2) {
		fprintf(stderr, "usage: s3getacl <bucket> [filename]\n");
		return -1;
	}

	if(argc > 2) {
		sprintf(remotename, "/%s", argv[2]);
		filename = remotename;
	} else filename = NULL;

	acls = hash_table_create(0, NULL);
	s3_getacl(argv[1], filename, owner, acls, s3_userid(), s3_key());

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

