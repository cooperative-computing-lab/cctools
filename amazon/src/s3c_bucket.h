#ifndef S3C_BUCKET_H_
#define S3C_BUCKET_H_

#include <list.h>
#include "s3client.h"
#include "s3c_file.h"

enum amz_base_perm {
	AMZ_PERM_PRIVATE,
	AMZ_PERM_PUBLIC_READ,
	AMZ_PERM_PUBLIC_WRITE,
	AMZ_PERM_AUTH_READ,
	AMZ_PERM_BUCKET_READ,
	AMZ_PERM_BUCKET_FULL
};

int s3_mk_bucket(char* bucketname, enum amz_base_perm perms, const char* access_key_id, const char* access_key);
int s3_rm_bucket(char* bucketname, const char* access_key_id, const char* access_key);
int s3_ls_bucket(char* bucketname, struct list* dirent, const char* access_key_id, const char* access_key);

#endif

