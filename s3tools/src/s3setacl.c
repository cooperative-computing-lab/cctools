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
	struct s3_acl_object *acl;
	char *bucket, *filename, *handle, *aclstr, *id;
	char remotename[FILENAME_MAX];
	char owner[FILENAME_MAX];
	unsigned int i;
	unsigned char mask = 0;

	s3_initialize(&argc, argv);

	if(argc < 4) {
		fprintf(stderr, "usage: s3setacl <bucket> [filename] <email | display name> [+|-]<acls>\n");
		return -1;
	}

	bucket = argv[1];
	if(argc == 5) {
		sprintf(remotename, "/%s", argv[2]);
		filename = remotename;
		handle = argv[3];
		aclstr = argv[4];
	} else {
		filename = NULL;
		handle = argv[2];
		aclstr = argv[3];
	}

	acls = hash_table_create(0, NULL);
	s3_getacl(bucket, filename, owner, acls, s3_userid(), s3_key());

	hash_table_firstkey(acls);
	while(hash_table_nextkey(acls, &id, (void**)&acl)) {
		if(!strcmp(id, handle)) break;
	}
	if(!acl) acl = hash_table_lookup(acls, handle);

	if(!acl && !strchr(handle, '@')) {
		fprintf(stderr, "Error: invalid handle (%s)\n", handle);
		exit(0);
	}

	if(!acl && aclstr[0] != '-') {
		acl = malloc(sizeof(*acl));
		acl->acl_type = S3_ACL_EMAIL;
		acl->perm = 0;
		acl->display_name = NULL;
		hash_table_insert(acls, handle, acl);
	}
	if(!acl) return 0;

	fprintf(stderr, "aclstr: %s\n", aclstr);
	for(i = strcspn(aclstr, "frwgs"); i < strlen(aclstr); i++) {
		switch(aclstr[i]) {
			case 'f':	mask = mask | S3_ACL_FULL_CONTROL; break;
			case 'r':	mask = mask | S3_ACL_READ; break;
			case 'w':	mask = mask | S3_ACL_WRITE; break;
			case 'g':	mask = mask | S3_ACL_READ_ACP; break;
			case 's':	mask = mask | S3_ACL_WRITE_ACP; break;
		}
	}

	if(aclstr[0] == '+') {
		acl->perm = acl->perm | mask;
	} else if(aclstr[0] == '-') {
		acl->perm = acl->perm & ~mask;
	} else {
		acl->perm = mask;
	}

	s3_setacl(bucket, filename, owner, acls, s3_userid(), s3_key());

	return 0;
}


/* vim: set noexpandtab tabstop=4: */
