#ifndef S3_CLIENT_H_
#define S3_CLIENT_H_

#include <list.h>
#include <hash_table.h>
#include <stdlib.h>
#include <md5.h>
#include <time.h>

#define ACCESS_KEY_ID_LENGTH 21
#define ACCESS_KEY_LENGTH 41
#define AWS_CANONICAL_ID_LENGTH 65
#define MAX_KEY_LENGTH 1024

#define S3_ACL_URI	1
#define S3_ACL_ID	2
#define S3_ACL_EMAIL	3

#define S3_ACL_FULL_CONTROL	0x01
#define S3_ACL_READ		0x02
#define S3_ACL_WRITE		0x04
#define S3_ACL_READ_ACP		0x08
#define S3_ACL_WRITE_ACP	0x10

enum amz_header_type {
	AMZ_CUSTOM_HEADER,
	AMZ_HEADER_ACL,
	AMZ_HEADER_MFA
};

enum amz_base_perm {
	AMZ_PERM_PRIVATE,
	AMZ_PERM_PUBLIC_READ,
	AMZ_PERM_PUBLIC_WRITE,
	AMZ_PERM_AUTH_READ,
	AMZ_PERM_BUCKET_READ,
	AMZ_PERM_BUCKET_FULL
};

struct amz_header_object {
	int type;
	char *custom_type;
	char *value;
};

struct s3_acl_object {
	char acl_type;
	char* display_name;
	char perm;
};

struct amz_metadata_object {
	char type[MAX_KEY_LENGTH];
	char value[MAX_KEY_LENGTH];
};

struct s3_dirent_object {
	char key[MAX_KEY_LENGTH];
	time_t last_modified;
	char digest[MD5_DIGEST_LENGTH];
	int size;
	char owner[AWS_CANONICAL_ID_LENGTH];
	char* display_name;
	struct list *metadata;
};

int s3_mk_bucket(char* bucketname, enum amz_base_perm perms, const char* access_key_id, const char* access_key);
int s3_rm_bucket(char* bucketname, const char* access_key_id, const char* access_key);
int s3_ls_bucket(char* bucketname, struct list* dirent, const char* access_key_id, const char* access_key);


int s3_getacl(char* bucketname, char* filename, char *owner, struct hash_table* acls, const char* access_key_id, const char* access_key);
int s3_setacl(char* bucketname, char* filename, const char* owner, struct hash_table* acls, const char* access_key_id, const char* access_key);


int s3_put_file(const char* localname, char* remotename, char* bucketname, enum amz_base_perm perms, const char* access_key_id, const char* access_key);
int s3_get_file(const char* localname, struct s3_dirent_object *dirent, char* remotename, char* bucketname, const char* access_key_id, const char* access_key);
int s3_rm_file(char* filename, char* bucketname, const char* access_key_id, const char* access_key);
int s3_stat_file(char* filename, char* bucketname, struct s3_dirent_object *dirent, const char* access_key_id, const char* access_key);


#endif

