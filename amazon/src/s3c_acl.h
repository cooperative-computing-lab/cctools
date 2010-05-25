#ifndef S3C_ACL_H_
#define S3C_ACL_H_

#include <hash_table.h>
#include <stdlib.h>

#define S3_ACL_URI	1
#define S3_ACL_ID	2
#define S3_ACL_EMAIL	3

#define S3_ACL_FULL_CONTROL	0x01
#define S3_ACL_READ		0x02
#define S3_ACL_WRITE		0x04
#define S3_ACL_READ_ACP		0x08
#define S3_ACL_WRITE_ACP	0x10


struct s3_acl_object {
	char acl_type;
	char* display_name;
	char perm;
};

int s3_getacl(char* bucketname, char* filename, char *owner, struct hash_table* acls, const char* access_key_id, const char* access_key);
int s3_setacl(char* bucketname, char* filename, const char* owner, struct hash_table* acls, const char* access_key_id, const char* access_key);

#endif

