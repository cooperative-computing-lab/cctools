#ifndef S3C_UTIL_H_
#define S3C_UTIL_H_


#define ACCESS_KEY_ID_LENGTH 21
#define ACCESS_KEY_LENGTH 41
#define AWS_CANONICAL_ID_LENGTH 65
#define MAX_KEY_LENGTH 1024
#define HEADER_LINE_MAX 10240

enum amz_header_type {
	AMZ_CUSTOM_HEADER,
	AMZ_HEADER_ACL,
	AMZ_HEADER_MFA
};

struct amz_header_object {
	int type;
	char *custom_type;
	char *value;
};

struct amz_metadata_object {
	char type[MAX_KEY_LENGTH];
	char value[MAX_KEY_LENGTH];
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

struct amz_header_object* amz_new_header(enum amz_header_type type, const char* custom_type, const char* value) {
const char * amz_get_header(enum amz_header_type type, const char* custom_type) {
int amz_header_comp(const void *a, const void *b) {
int sign_message(struct s3_message* mesg, const char* user, const char * key) {
int s3_message_to_string(struct s3_message *mesg, char** text) {

#endif

