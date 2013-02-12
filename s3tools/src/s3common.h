/*
Copyright (C) 2010- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/
#ifndef S3COMMON_H_
#define S3COMMON_H_

#define DEFAULT_CONFIGFILE_NAME ".s3tools.conf"

const char* s3_userid();
const char* s3_key();

void s3_initialize(int* argc, char** argv);
int s3_register_userid(const char *userid, const char* key);
void s3_clear_userid();



#endif
