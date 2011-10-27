/*
Copyright (C) 2010- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/
#ifndef S3C_UTIL_H_
#define S3C_UTIL_H_

#include <md5.h>
#include <time.h>

#define ACCESS_KEY_ID_LENGTH 21
#define ACCESS_KEY_LENGTH 41
#define AWS_CANONICAL_ID_LENGTH 65
#define MAX_KEY_LENGTH 1024
#define HEADER_LINE_MAX 10240

enum s3_header_type {
	S3_HEADER_CUSTOM,
	S3_HEADER_AMZ_ACL,
	S3_HEADER_AMZ_MFA
};

struct s3_header_object {
	int type;
	char *custom_type;
	char *value;
};

struct amz_metadata_object {
	char type[MAX_KEY_LENGTH];
	char value[MAX_KEY_LENGTH];
};

enum amz_base_perm {
	AMZ_PERM_PRIVATE,
	AMZ_PERM_PUBLIC_READ,
	AMZ_PERM_PUBLIC_WRITE,
	AMZ_PERM_AUTH_READ,
	AMZ_PERM_BUCKET_READ,
	AMZ_PERM_BUCKET_FULL
};


enum s3_message_type {
	S3_MESG_GET,
	S3_MESG_POST,
	S3_MESG_PUT,
	S3_MESG_DELETE,
	S3_MESG_HEAD,
	S3_MESG_COPY
};

struct s3_message {
	enum s3_message_type type;
	char *path;
	char *bucket;
	char *content_md5;
	char *content_type;
	time_t date;
	struct list* amz_headers;
	int expect;

	int content_length;
	char authorization[55];
};

struct s3_dirent_object {
	char key[MAX_KEY_LENGTH];
	time_t last_modified;
	char digest[MD5_DIGEST_LENGTH];
	int size;
	char owner[AWS_CANONICAL_ID_LENGTH];
	char *display_name;
	struct list *metadata;
};

int s3_set_endpoint(const char *target);
struct s3_header_object* s3_new_header_object(enum s3_header_type type, const char* custom_type, const char* value);
const char * s3_get_header(enum s3_header_type type, const char* custom_type);
int s3_header_comp(const void *a, const void *b);
int sign_message(struct s3_message* mesg, const char* user, const char * key);
struct link * s3_send_message(struct s3_message *mesg, struct link *server, time_t stoptime);
int s3_message_to_string(struct s3_message *mesg, char** text);

#endif

