/*
Copyright (C) 2010- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/
#ifndef S3C_BUCKET_H_
#define S3C_BUCKET_H_

#include <list.h>
#include "s3c_util.h"
#include "s3c_file.h"


int s3_mk_bucket(char* bucketname, enum amz_base_perm perms, const char* access_key_id, const char* access_key);
int s3_rm_bucket(char* bucketname, const char* access_key_id, const char* access_key);
int s3_ls_bucket(char* bucketname, struct list* dirent, const char* access_key_id, const char* access_key);

#endif

