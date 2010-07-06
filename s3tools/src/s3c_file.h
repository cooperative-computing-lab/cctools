/*
Copyright (C) 2010- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/
#ifndef S3_FILE_H_
#define S3_FILE_H_

#include <list.h>
#include <hash_table.h>
#include <stdlib.h>
#include "s3c_util.h"

#define ACCESS_KEY_ID_LENGTH 21
#define ACCESS_KEY_LENGTH 41
#define AWS_CANONICAL_ID_LENGTH 65
#define MAX_KEY_LENGTH 1024

int s3_put_file(const char* localname, char* remotename, char* bucketname, enum amz_base_perm perms, const char* access_key_id, const char* access_key);
int s3_get_file(const char* localname, struct s3_dirent_object *dirent, char* remotename, char* bucketname, const char* access_key_id, const char* access_key);
int s3_rm_file(char* filename, char* bucketname, const char* access_key_id, const char* access_key);
int s3_stat_file(char* filename, char* bucketname, struct s3_dirent_object *dirent, const char* access_key_id, const char* access_key);

#endif

