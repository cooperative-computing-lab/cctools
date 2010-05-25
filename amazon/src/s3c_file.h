#ifndef S3_CLIENT_H_
#define S3_CLIENT_H_

#include <list.h>
#include <hash_table.h>
#include <stdlib.h>
#include <md5.h>

#define ACCESS_KEY_ID_LENGTH 21
#define ACCESS_KEY_LENGTH 41
#define AWS_CANONICAL_ID_LENGTH 65
#define MAX_KEY_LENGTH 1024

struct s3_header {
	char key[MAX_KEY_LENGTH];
	char digest[MD5_DIGEST_LENGTH];
	int size;
	time_t last_modified;
	struct list *metadata;
};

int s3_put_file(const char* localname, char* remotename, char* bucketname, enum amz_base_perm perms, const char* access_key_id, const char* access_key);
int s3_get_file(const char* localname, struct s3_header *head, char* remotename, char* bucketname, const char* access_key_id, const char* access_key);
int s3_rm_file(char* filename, char* bucketname, const char* access_key_id, const char* access_key);
int s3_stat_file(char* filename, char* bucketname, struct s3_header *head, const char* access_key_id, const char* access_key);

#endif

