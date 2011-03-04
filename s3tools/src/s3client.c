#include <list.h>
#include <hash_table.h>
#include <link.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <hmac.h>
#include <math.h>
#include <b64_encode.h>
#include <sys/stat.h>
#include "s3client.h"

#define HEADER_LINE_MAX 10240

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

static char *s3_endpoint = "s3.amazonaws.com";
static char *s3_address  = "72.21.202.66";
static int   s3_timeout  = 60;


struct amz_header_object* amz_new_header(enum amz_header_type type, const char* custom_type, const char* value) {
	struct amz_header_object *amz;
	amz = malloc(sizeof(*amz));
	amz->type = type;
	if(type == AMZ_CUSTOM_HEADER ) {
		amz->custom_type = strdup(custom_type);
	} else amz->custom_type = NULL;

	amz->value = strdup(value);
	return amz;
}

const char * amz_get_header(enum amz_header_type type, const char* custom_type) {
	static const char amz_header_acl[] = "x-amz-acl";
	static const char amz_header_mfa[] = "x-amz-mfa";
	switch(type) {
		case AMZ_HEADER_ACL:
			return amz_header_acl;
		case AMZ_HEADER_MFA:
			return amz_header_mfa;
		default:
			return custom_type;
	}
}

int amz_header_comp(const void *a, const void *b) {
	const struct amz_header_object *one, *two;
	const char *header1, *header2;
	int result;
	one = (struct amz_header_object *)a;
	two = (struct amz_header_object *)b;

	header1 = amz_get_header(one->type, one->custom_type);
	header2 = amz_get_header(two->type, two->custom_type);

	result = strcmp(header1, header2);
	if(result) return result;
	return strcmp(one->value, two->value);
}

int sign_message(struct s3_message* mesg, const char* user, const char * key) {
	int sign_str_len = 0;
	char *sign_str;
	char date[1024];
	struct amz_header_object *amz;
	struct list *amz_headers;
	char digest[SHA1_DIGEST_LENGTH];
	char string[SHA1_DIGEST_LENGTH*2];
	int result;
	memset(digest, 0, SHA1_DIGEST_LENGTH);
	memset(string, 0, SHA1_DIGEST_LENGTH*2);

	switch(mesg->type) {
		case S3_MESG_GET: 
			sign_str_len += 4;
			break;
		case S3_MESG_POST: 
			sign_str_len += 5;
			break;
		case S3_MESG_PUT:
			sign_str_len += 4;
			break;
		case S3_MESG_DELETE:
			sign_str_len += 7;
			break;
		case S3_MESG_HEAD:
			sign_str_len += 5;
			break;
		case S3_MESG_COPY:
			sign_str_len += 4;
			break;
		default:
			return -1;
	}

	if(mesg->content_md5) sign_str_len += strlen(mesg->content_md5);
	sign_str_len += 1;
	if(mesg->content_type) sign_str_len += strlen(mesg->content_type);
	sign_str_len += 1;

	strftime(date, 1024, "%a, %d %b %Y %H:%M:%S %Z", gmtime(&mesg->date));
	sign_str_len += strlen(date) + 1;

	if(mesg->amz_headers) {	
		list_first_item(mesg->amz_headers);
		while( (amz = (struct amz_header_object *)list_next_item(mesg->amz_headers)) ) {
			if(amz->type < AMZ_CUSTOM_HEADER) continue;
			switch(amz->type) {
				case AMZ_CUSTOM_HEADER:
					if(!amz->custom_type) return -1;
					sign_str_len += strlen(amz->custom_type) + 1;
					break;
				default:
					sign_str_len += strlen(amz_get_header(amz->type, amz->custom_type)) + 1;
					break;
			}
			if(!amz->value) return -1;
			sign_str_len += strlen(amz->value) + 1;
		}
	}
	if(!mesg->bucket || !mesg->path) {
		return -1;
	}
	sign_str_len += 1 + strlen(mesg->bucket) + 1 + strlen(mesg->path) + 1;
	
	sign_str = malloc(sign_str_len);
	if(!sign_str) return -1;
	memset(sign_str, 0, sign_str_len);

	switch(mesg->type) {
		case S3_MESG_GET: 
			sprintf(sign_str, "GET\n");
			break;
		case S3_MESG_POST: 
			sprintf(sign_str, "POST\n");
			break;
		case S3_MESG_PUT:
			sprintf(sign_str, "PUT\n");
			break;
		case S3_MESG_DELETE:
			sprintf(sign_str, "DELETE\n");
			break;
		case S3_MESG_HEAD:
			sprintf(sign_str, "HEAD\n");
			break;
		case S3_MESG_COPY:
			sprintf(sign_str, "PUT\n");
			break;
	}

	if(mesg->content_md5) sprintf(sign_str, "%s%s\n", sign_str,mesg->content_md5);
			else sprintf(sign_str, "%s\n", sign_str);
	if(mesg->content_type) sprintf(sign_str, "%s%s\n", sign_str, mesg->content_type);
			else sprintf(sign_str, "%s\n", sign_str);
	sprintf(sign_str, "%s%s", sign_str, date);

	if(mesg->amz_headers) {
		amz_headers = list_sort(list_duplicate(mesg->amz_headers), &amz_header_comp);
		list_first_item(amz_headers);
		{	int c_type = -1;
			char* c_ctype = NULL;
			while( (amz = (struct amz_header_object *)list_next_item(amz_headers)) ) {
				if(amz->type < AMZ_CUSTOM_HEADER) continue;
				if(c_type != amz->type) {
					c_type = amz->type;
					c_ctype = amz->custom_type;
					sprintf(sign_str, "%s\n%s:%s", sign_str, amz_get_header(amz->type, amz->custom_type), amz->value);

				} else if(c_type == AMZ_CUSTOM_HEADER && strcmp(c_ctype, amz->custom_type)) {
					c_ctype = amz->custom_type;
					sprintf(sign_str, "%s\n%s:%s", sign_str, amz->custom_type, amz->value);
				} else {
					sprintf(sign_str, "%s,%s", sign_str, amz->value);
				}
			}
		}
		list_delete(amz_headers);
	}

	sprintf(sign_str, "%s\n/%s%s", sign_str, mesg->bucket, mesg->path);
	if((result = hmac_sha1(sign_str, strlen(sign_str), key, strlen(key), (unsigned char*)digest))) return result;

	hmac_sha1(sign_str, strlen(sign_str), key, strlen(key), (unsigned char*)digest);
	b64_encode(digest, SHA1_DIGEST_LENGTH, string, SHA1_DIGEST_LENGTH*2);

	sprintf(mesg->authorization, "AWS %s:%s", user, string);
	free(sign_str);
	return 0;
}


int s3_message_to_string(struct s3_message *mesg, char** text) {
	int msg_str_len = 0;
	char *msg_str = NULL;
	char date[1024];
	struct amz_header_object *amz;

	switch(mesg->type) {
		case S3_MESG_GET: 
			msg_str_len = 4 + 11;
			break;
		case S3_MESG_POST: 
			msg_str_len = 5 + 11;
			break;
		case S3_MESG_PUT:
			msg_str_len = 4 + 11;
			break;
		case S3_MESG_DELETE:
			msg_str_len = 7 + 11;
			break;
		case S3_MESG_HEAD:
			msg_str_len = 5 + 11;
			break;
		case S3_MESG_COPY:
			msg_str_len = 4 + 11;
			break;
		default:
			fprintf(stderr, "Invalid Message Type\n");
			return 0;
	}

	if(mesg->path) msg_str_len += strlen(mesg->path) + 1;
	else {
		fprintf(stderr, "no message path\n");
		return 0;
	}
	if(mesg->bucket) msg_str_len += 6 + strlen(mesg->bucket) + 1 + strlen(s3_endpoint) + 1;
	else {
		fprintf(stderr, "no message bucket\n");
		return 0;
	}
	strftime(date, 1024, "%a, %d %b %Y %H:%M:%S %Z", gmtime(&mesg->date));
	//strftime(date, 1024, "%c %Z", gmtime(&mesg->date));
	msg_str_len += 6 + strlen(date) + 2;

	if(mesg->content_type) msg_str_len += 14 + strlen(mesg->content_type) + 2;

	{	int len = mesg->content_length;
		do {
			msg_str_len += 1;
			len /= 10;
		} while(len);
	}
	msg_str_len += 18;

	if(mesg->content_md5) msg_str_len += 13 + strlen(mesg->content_md5) + 2;

	if(mesg->amz_headers) {
		list_first_item(mesg->amz_headers);
		while( (amz = (struct amz_header_object *)list_next_item(mesg->amz_headers)) ) {
			switch(amz->type) {
				case AMZ_CUSTOM_HEADER:
					if(!amz->custom_type) {
						fprintf(stderr, "no custom type defined for AMZ_CUSTOM_HEADER\n");
						return 0;
					}
					msg_str_len += strlen(amz->custom_type) + 2;
					break;
				default:
					msg_str_len += strlen(amz_get_header(amz->type, amz->custom_type)) + 2;
					break;
			}
			if(!amz->value) {
				fprintf(stderr, "no value for amz_header\n");
				return 0;
			}
			msg_str_len += strlen(amz->value) + 2;
		}
	}

	msg_str_len += 72; //Authorization

	if(mesg->expect) msg_str_len +=  22;

	msg_str = (char*)malloc(msg_str_len + 1);
	if(!msg_str) {
		fprintf(stderr, "malloc(%d) failed for msg_string\n", msg_str_len);
		return 0;
	}
	memset(msg_str, 0, msg_str_len);

	switch(mesg->type) {
		case S3_MESG_GET: 
			sprintf(msg_str, "GET ");
			break;
		case S3_MESG_POST: 
			sprintf(msg_str, "POST ");
			break;
		case S3_MESG_PUT:
			sprintf(msg_str, "PUT ");
			break;
		case S3_MESG_DELETE:
			sprintf(msg_str, "DELETE ");
			break;
		case S3_MESG_HEAD:
			sprintf(msg_str, "HEAD ");
			break;
		case S3_MESG_COPY:
			sprintf(msg_str, "PUT ");
			break;
	}

	sprintf(msg_str, "%s%s HTTP/1.1\r\n", msg_str, mesg->path);
	sprintf(msg_str, "%sHost: %s.%s\r\n", msg_str, mesg->bucket, s3_endpoint);
	sprintf(msg_str, "%sDate: %s\r\n", msg_str, date);
	if(mesg->content_type) sprintf(msg_str, "%sContent-Type: %s\r\n", msg_str, mesg->content_type);
	sprintf(msg_str, "%sContent-Length: %d\r\n", msg_str, mesg->content_length);
	if(mesg->content_md5) sprintf(msg_str, "%sContent-MD5: %s\r\n", msg_str, mesg->content_md5);

	if(mesg->amz_headers) {
		list_first_item(mesg->amz_headers);
		while( (amz = (struct amz_header_object *)list_next_item(mesg->amz_headers)) ) {
			switch(amz->type) {
				case AMZ_CUSTOM_HEADER:
					sprintf(msg_str, "%s%s: %s\r\n", msg_str, amz->custom_type, amz->value);
					break;
				default:
					sprintf(msg_str, "%s%s: %s\r\n", msg_str, amz_get_header(amz->type, amz->custom_type), amz->value);
					break;
			}
		}
	}
	sprintf(msg_str, "%sAuthorization: %s\r\n", msg_str, mesg->authorization);

	if(mesg->expect) sprintf(msg_str, "%sExpect: 100-continue\r\n", msg_str);
	sprintf(msg_str, "%s\r\n", msg_str);
	*text = msg_str;
	
	return msg_str_len;

}

int s3_mk_bucket(char* bucketname, enum amz_base_perm perms, const char* access_key_id, const char* access_key) {
	struct link* server;
	char path[] = "/";
	struct amz_header_object *amz;
	time_t stoptime = time(0)+s3_timeout;
	struct s3_message mesg;
	char * text;
	char response[HEADER_LINE_MAX];
	int length;

	if(!access_key_id || !access_key || !s3_endpoint) return -1;

	mesg.type = S3_MESG_PUT;
	mesg.path = path;
	mesg.bucket = bucketname;
	mesg.content_length = 0;
	mesg.content_type = NULL;
	mesg.content_md5 = NULL;
	mesg.date = time(0);
	mesg.expect = 0;

	switch(perms) {
		case AMZ_PERM_PRIVATE:      amz = amz_new_header(AMZ_HEADER_ACL, NULL, "private"); break;
		case AMZ_PERM_PUBLIC_READ:  amz = amz_new_header(AMZ_HEADER_ACL, NULL, "public-read"); break;
		case AMZ_PERM_PUBLIC_WRITE: amz = amz_new_header(AMZ_HEADER_ACL, NULL, "public-read-write"); break;
		case AMZ_PERM_AUTH_READ:    amz = amz_new_header(AMZ_HEADER_ACL, NULL, "authenticated-read"); break;
		case AMZ_PERM_BUCKET_READ:  amz = amz_new_header(AMZ_HEADER_ACL, NULL, "bucket-owner-read"); break;
		case AMZ_PERM_BUCKET_FULL:  amz = amz_new_header(AMZ_HEADER_ACL, NULL, "bucket-owner-full-control"); break;
		default: return -1;
	}
	mesg.amz_headers = list_create();
	list_push_tail(mesg.amz_headers, amz);


	sign_message(&mesg, access_key_id, access_key);
	length = s3_message_to_string(&mesg, &text);
	list_free(mesg.amz_headers);
	list_delete(mesg.amz_headers);

	server = link_connect(s3_address, 80, stoptime);
	if(!server) return -1;

	link_putlstring(server, text, length, stoptime);
	free(text);

	link_readline(server, response, HEADER_LINE_MAX, stoptime);
	if(strcmp(response, "HTTP/1.1 200 OK")) {
		// Error: transfer failed; close connection and return failure
		//fprintf(stderr, "Error: create bucket failed\nResponse: %s\n", response);
		link_close(server);
		return -1;
	}

	//fprintf(stderr, "Response:\n");
	do {
	//	fprintf(stderr, "\t%s\n", response);
		if(!strcmp(response, "Server: AmazonS3")) break;
	} while(link_readline(server, response, HEADER_LINE_MAX, stoptime));

	link_close(server);
	return 0;
}

int s3_rm_bucket(char* bucketname, const char* access_key_id, const char* access_key) {
	struct s3_message mesg;
	struct link* server;
	time_t stoptime = time(0)+s3_timeout;
	char response[HEADER_LINE_MAX];
	char path[] = "/";
	char * text;
	int length;

	if(!access_key_id || !access_key || !s3_endpoint) return -1;

	mesg.type = S3_MESG_DELETE;
	mesg.path = path;
	mesg.bucket = bucketname;
	mesg.content_type = NULL;
	mesg.content_md5 = NULL;
	mesg.content_length = 0;
	mesg.date = time(0);
	mesg.expect = 0;
	mesg.amz_headers = NULL;

	server = link_connect(s3_address, 80, stoptime);
	if(!server) return -1;

	sign_message(&mesg, access_key_id, access_key);
	length = s3_message_to_string(&mesg, &text);

	link_putlstring(server, text, length, stoptime);
	free(text);

	link_readline(server, response, HEADER_LINE_MAX, stoptime);
	if(strcmp(response, "HTTP/1.1 204 No Content")) {
		// Error: transfer failed; close connection and return failure
		//fprintf(stderr, "Error: delete bucket failed\nResponse: %s\n", response);
		link_close(server);
		return -1;
	}

	//fprintf(stderr, "Response:\n");
	do {
	//	fprintf(stderr, "\t%s\n", response);
		if(!strcmp(response, "Server: AmazonS3")) break;
	} while(link_readline(server, response, HEADER_LINE_MAX, stoptime));

	link_close(server);
	return 0;
}

int s3_ls_bucket(char* bucketname, struct list* dirents, const char* access_key_id, const char* access_key) {
	struct s3_message mesg;
	struct link* server;
	time_t stoptime = time(0)+s3_timeout;
	char response[HEADER_LINE_MAX];
	char path[HEADER_LINE_MAX];
	char * text;
	int length;
	char done = 0;

	if(!access_key_id || !access_key || !s3_endpoint) return -1;

	sprintf(path, "/");
	mesg.type = S3_MESG_GET;
	mesg.path = path;
	mesg.bucket = bucketname;
	mesg.content_type = NULL;
	mesg.content_md5 = NULL;
	mesg.content_length = 0;
	mesg.date = time(0);
	mesg.expect = 0;
	mesg.amz_headers = NULL;

	server = link_connect(s3_address, 80, stoptime);
	if(!server) return -1;


	do {
		char *buffer, *temp, *start, *end, *last;
		char trunc[25];
		int keys;
		sign_message(&mesg, access_key_id, access_key);
		length = s3_message_to_string(&mesg, &text);
	//	fprintf(stderr, "Request:\n%s\n", text);
		link_putlstring(server, text, length, stoptime);
		free(text);

		link_readline(server, response, HEADER_LINE_MAX, stoptime);

		if(strcmp(response, "HTTP/1.1 200 OK")) {
			// Error: transfer failed; close connection and return failure
			//fprintf(stderr, "Error: list bucket failed\nResponse: %s\n", response);
			link_close(server);
			return -1;
		}

		length = 0;
		do {
			if(!strncmp(response, "Content-Length:", 14)) sscanf(response, "Content-Length: %d", &length);
			if(!strcmp(response, "Transfer-Encoding: chunked")) length = 0;
			if(!strcmp(response, "Server: AmazonS3")) break;
		} while(link_readline(server, response, HEADER_LINE_MAX, stoptime));
		link_readline(server, response, HEADER_LINE_MAX, stoptime);

		if(length) {
			buffer = malloc(length+1);
			link_read(server, buffer, length, stoptime);
		} else {
			struct list *buf;
			int clen = 0;
			buf = list_create();
			do {
				link_readline(server, response, HEADER_LINE_MAX, stoptime);
				sscanf(response, "%x", &clen);
				//link_readline(server, response, HEADER_LINE_MAX, stoptime);
				if(clen) {
					buffer = malloc(clen+1);
					link_read(server, buffer, clen, stoptime);
					link_readline(server, response, HEADER_LINE_MAX, stoptime);
					list_push_tail(buf, buffer);
					length += clen;
				}
			} while(clen);
			buffer = malloc(length+1);
			buffer[0] = '\0';
			while((temp = list_pop_head(buf))) {
				sprintf(buffer, "%s%s", buffer, temp);
				free(temp);
			}
			list_delete(buf);
		}

		sscanf(buffer, "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\r\n<ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Name>%*[^<]</Name><Prefix></Prefix><Marker></Marker><MaxKeys>%d</MaxKeys><IsTruncated>%[^<]</IsTruncated>", &keys, trunc);
		if(!strcmp(trunc, "false")) done = 1;
		temp = buffer;
		while( (start = strstr(temp, "<Contents>")) ) {
			struct s3_dirent_object *d;
			struct tm date;
			char display_name[1024];
			end = strstr(start, "</Contents>");
			end[10] = '\0';
			temp = end + 11;
			d = malloc(sizeof(*d));
			date.tm_isdst = -1;
			sscanf(start, "<Contents><Key>%[^<]</Key><LastModified>%d-%d-%dT%d:%d:%d.%*dZ</LastModified><ETag>&quot;%[^&]&quot;</ETag><Size>%d</Size><Owner><ID>%[^<]</ID><DisplayName>%[^<]</DisplayName></Owner>", d->key, &date.tm_year, &date.tm_mon, &date.tm_mday, &date.tm_hour, &date.tm_min, &date.tm_sec, d->digest, &d->size, d->owner, display_name);
			d->display_name = strdup(display_name);
			date.tm_mon -= 1;
			d->last_modified = mktime(&date);
			list_push_tail(dirents, d);
			last = d->key;
		}
		free(buffer);

	} while(!done);

	link_close(server);
	return 0;
}

int s3_getacl(char* bucketname, char* filename, char* owner, struct hash_table* acls, const char* access_key_id, const char* access_key) {
	struct s3_message mesg;
	struct link* server;
	time_t stoptime = time(0)+s3_timeout;
	char path[HEADER_LINE_MAX];
	char response[HEADER_LINE_MAX];
	char * text;
	char * start;
	char * temp;
	int length;
 
	if(!s3_endpoint) return -1;
	if(filename) sprintf(path, "%s?acl", filename);
	else sprintf(path, "/?acl");

	mesg.type = S3_MESG_GET;
	mesg.path = path;
	mesg.bucket = bucketname;
	mesg.content_type = NULL;
	mesg.content_md5 = NULL;
	mesg.content_length = 0;
	mesg.date = time(0);
	mesg.expect = 0;
	mesg.amz_headers = NULL;

	server = link_connect(s3_address, 80, stoptime);
	if(!server) return -1;

	sign_message(&mesg, access_key_id, access_key);
	length = s3_message_to_string(&mesg, &text);

	link_putlstring(server, text, length, stoptime);
	free(text);

	link_readline(server, response, HEADER_LINE_MAX, stoptime);
	if(strcmp(response, "HTTP/1.1 200 OK")) {
		// Error: transfer failed; close connection and return failure
		//fprintf(stderr, "Error: request file failed\nResponse: %s\n", response);
		link_close(server);
		return -1;
	}

	do {
		if(!strncmp(response, "Content-Length:", 14)) sscanf(response, "Content-Length: %d", &length);
		if(!strcmp(response, "Transfer-Encoding: chunked")) length = 0;
		if(!strcmp(response, "Server: AmazonS3")) break;
	} while(link_readline(server, response, HEADER_LINE_MAX, stoptime));
	link_readline(server, response, HEADER_LINE_MAX, stoptime);

	if(length) {
		text = malloc(length+1);
		link_read(server, text, length, stoptime);
	} else {
		struct list *buf;
		char *temp;
		int clen = 0;
		buf = list_create();
		do {
			link_readline(server, response, HEADER_LINE_MAX, stoptime);
			sscanf(response, "%x", &clen);
			//link_readline(server, response, HEADER_LINE_MAX, stoptime);
			if(clen) {
				text = malloc(clen+1);
				link_read(server, text, clen, stoptime);
				link_readline(server, response, HEADER_LINE_MAX, stoptime);
				list_push_tail(buf, text);
				length += clen;
			}
		} while(clen);
		text = malloc(length+1);
		text[0] = '\0';
		while((temp = list_pop_head(buf))) {
			sprintf(text, "%s%s", text, temp);
			free(temp);
		}
		list_delete(buf);
	}
	link_close(server);

	if(owner) sscanf(strstr(text, "<Owner>"), "<Owner><ID>%[^<]</ID>", owner);
	temp = text;
	while( (start = strstr(temp, "<Grant>")) ) {
		char id[1024];
		char display_name[1024];
		char permission[1024];
		char type;
		struct s3_acl_object *acl;
		char *end;

		end = strstr(start, "</Grant>");
		end[7] = '\0';
		temp = end + 8;

		memset(display_name, 0, 1024);
		type = S3_ACL_ID;
		if( sscanf(start, "<Grant><Grantee %*[^>]><ID>%[^<]</ID><DisplayName>%[^<]</DisplayName></Grantee><Permission>%[^<]</Permission></Grantee>", id, display_name, permission) != 3 ) {
			type = S3_ACL_URI;
			sscanf(start, "<Grant><Grantee %*[^>]><URI>http://acs.amazonaws.com/groups/global/%[^<]</URI></Grantee><Permission>%[^<]</Permission></Grantee>", id, permission);
		}

		if( !(acl = hash_table_lookup(acls, id)) ) {
			acl = malloc(sizeof(*acl));
			acl->acl_type = type;
			if(*display_name) acl->display_name = strdup(display_name);
			else acl->display_name = NULL;
			acl->perm = 0;
			hash_table_insert(acls, id, acl);
		}

		if(!strcmp(permission, "FULL_CONTROL")) {
			acl->perm = acl->perm | S3_ACL_FULL_CONTROL;
		} else if(!strcmp(permission, "READ")) {
			acl->perm = acl->perm | S3_ACL_READ;
		} else if(!strcmp(permission, "WRITE")) {
			acl->perm = acl->perm | S3_ACL_WRITE;
		} else if(!strcmp(permission, "READ_ACP")) {
			acl->perm = acl->perm | S3_ACL_READ_ACP;
		} else if(!strcmp(permission, "WRITE_ACP")) {
			acl->perm = acl->perm | S3_ACL_WRITE_ACP;
		}
	}

	free(text);
	return 0;
}

// NOT IMPLEMENTED YET
int s3_setacl(char* bucketname, char *filename, const char* owner, struct hash_table* acls, const char* access_key_id, const char* access_key) {
	struct s3_message mesg;
	struct link* server;
	time_t stoptime = time(0)+s3_timeout;
	char path[HEADER_LINE_MAX];
	char response[HEADER_LINE_MAX];
	char * text;
	int length;
	char *id;
	struct s3_acl_object *acl;
 
	if(!s3_endpoint) return -1;
	if(filename) sprintf(path, "%s?acl", filename);
	else
	sprintf(path, "/?acl");
 

	mesg.content_length = 39 + 32 + strlen(owner) + 32;
	hash_table_firstkey(acls);
	while(hash_table_nextkey(acls, &id, (void**)&acl)) {
		int glength;

		switch(acl->acl_type) {
			case S3_ACL_URI:
				glength = 140+strlen(id);
				break;
			case S3_ACL_EMAIL:
				glength = 135+strlen(id);
				break;
			default:
				glength = 107+strlen(id);
		}

		if(acl->perm & S3_ACL_FULL_CONTROL)	mesg.content_length += 40 + glength + 12;
		if(acl->perm & S3_ACL_READ)		mesg.content_length += 40 + glength + 4;
		if(acl->perm & S3_ACL_WRITE)		mesg.content_length += 40 + glength + 5;
		if(acl->perm & S3_ACL_READ_ACP)		mesg.content_length += 40 + glength + 8;
		if(acl->perm & S3_ACL_WRITE_ACP)	mesg.content_length += 40 + glength + 9;
	}
	mesg.content_length += 43;

	mesg.type = S3_MESG_PUT;
	mesg.path = path;
	mesg.bucket = bucketname;
	mesg.content_type = NULL;
	mesg.content_md5 = NULL;
	mesg.date = time(0);
	mesg.expect = 0;
	mesg.amz_headers = NULL;

	server = link_connect(s3_address, 80, stoptime);
	if(!server) return -1;

	sign_message(&mesg, access_key_id, access_key);
	length = s3_message_to_string(&mesg, &text);

	fprintf(stderr, "Message:\n%s\n", text);
	link_putlstring(server, text, length, stoptime);
	free(text);

	link_putliteral(server, "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n", stoptime);
	link_putliteral(server, "<AccessControlPolicy><Owner><ID>", stoptime);
	link_putstring(server, owner, stoptime);
	link_putliteral(server, "</ID></Owner><AccessControlList>", stoptime);

	hash_table_firstkey(acls);
	while(hash_table_nextkey(acls, &id, (void**)&acl)) {
		char grantee[HEADER_LINE_MAX];
		char grant[HEADER_LINE_MAX];

		switch(acl->acl_type) {
			case S3_ACL_URI:
				sprintf(grantee, "<Grantee xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:type=\"Group\"><URI>http://acs.amazonaws.com/groups/global/%s</URI></Grantee>", id);
				break;
			case S3_ACL_EMAIL:
				sprintf(grantee, "<Grantee xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:type=\"AmazonCustomerByEmail\"><EmailAddress>%s</EmailAddress></Grantee>", id);
				break;
			default:
				sprintf(grantee, "<Grantee xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:type=\"CanonicalUser\"><ID>%s</ID></Grantee>", id);
		}

		if(acl->perm & S3_ACL_FULL_CONTROL) {
			link_putfstring(server, grant, "<Grant>%s<Permission>FULL_CONTROL</Permission></Grant>", stoptime, grantee);
		}
		if(acl->perm & S3_ACL_READ) {
			link_putfstring(server, grant, "<Grant>%s<Permission>READ</Permission></Grant>", stoptime, grantee);
		}
		if(acl->perm & S3_ACL_WRITE) {
			link_putfstring(server, grant, "<Grant>%s<Permission>WRITE</Permission></Grant>", stoptime, grantee);
		}
		if(acl->perm & S3_ACL_READ_ACP) {
			link_putfstring(server, grant, "<Grant>%s<Permission>READ_ACP</Permission></Grant>", stoptime, grantee);
		}
		if(acl->perm & S3_ACL_WRITE_ACP) {
			link_putfstring(server, grant, "<Grant>%s<Permission>WRITE_ACP</Permission></Grant>", stoptime, grantee);
		}
	}

	link_putliteral(server, "</AccessControlList></AccessControlPolicy>\n", stoptime);

	link_readline(server, response, HEADER_LINE_MAX, stoptime);
	if(strcmp(response, "HTTP/1.1 200 OK")) {
		// Error: transfer failed; close connection and return failure
		fprintf(stderr, "Error: send file failed\nResponse: %s\n", response);
		link_close(server);
		return -1;
	}

//	fprintf(stderr, "Response:\n");
	do {
//		fprintf(stderr, "\t%s\n", response);
		if(!strcmp(response, "Server: AmazonS3")) break;
	} while(link_readline(server, response, HEADER_LINE_MAX, stoptime));

	link_close(server);

	return 0;
}



int s3_put_file(const char* localname, char* remotename, char* bucketname, enum amz_base_perm perms, const char* access_key_id, const char* access_key) {
	struct link* server;
	struct amz_header_object *amz;
	time_t stoptime = time(0)+s3_timeout;
	struct s3_message mesg;
	struct stat st;
	FILE* infile;
	char response[HEADER_LINE_MAX];
	char * text;
	int length;

	if(!access_key_id || !access_key || !s3_endpoint) return -1;

	if(stat(localname, &st)) return -1;

	mesg.type = S3_MESG_PUT;
	mesg.path = remotename;
	mesg.bucket = bucketname;
	mesg.content_type = NULL;
	mesg.content_md5 = NULL;
	mesg.content_length = st.st_size;
	mesg.date = time(0);
	mesg.expect = 1;
	mesg.amz_headers = NULL;

	switch(perms) {
		case AMZ_PERM_PRIVATE:      amz = amz_new_header(AMZ_HEADER_ACL, NULL, "private"); break;
		case AMZ_PERM_PUBLIC_READ:  amz = amz_new_header(AMZ_HEADER_ACL, NULL, "public-read"); break;
		case AMZ_PERM_PUBLIC_WRITE: amz = amz_new_header(AMZ_HEADER_ACL, NULL, "public-read-write"); break;
		case AMZ_PERM_AUTH_READ:    amz = amz_new_header(AMZ_HEADER_ACL, NULL, "authenticated-read"); break;
		case AMZ_PERM_BUCKET_READ:  amz = amz_new_header(AMZ_HEADER_ACL, NULL, "bucket-owner-read"); break;
		case AMZ_PERM_BUCKET_FULL:  amz = amz_new_header(AMZ_HEADER_ACL, NULL, "bucket-owner-full-control"); break;
		default: return -1;
	}

	mesg.amz_headers = list_create();
	if(!mesg.amz_headers) return -1;
	list_push_tail(mesg.amz_headers, amz);

	sign_message(&mesg, access_key_id, access_key);
	length = s3_message_to_string(&mesg, &text);
	list_free(mesg.amz_headers);
	list_delete(mesg.amz_headers);

	server = link_connect(s3_address, 80, stoptime);
	if(!server) return -1;

	link_putlstring(server, text, length, stoptime);
	free(text);

	link_readline(server, response, HEADER_LINE_MAX, stoptime);
	if(strcmp(response, "HTTP/1.1 100 Continue")) {
		// Error: Invalid Headers; close connection and return failure
		link_close(server);
		return -1;
	}
	link_readline(server, response, HEADER_LINE_MAX, stoptime);

	infile = fopen(localname, "r");
	link_stream_from_file(server, infile, mesg.content_length, stoptime);
	fclose(infile);
	//link_putliteral(server, "\r\n\r\n", stoptime);

	link_readline(server, response, HEADER_LINE_MAX, stoptime);
	if(strcmp(response, "HTTP/1.1 200 OK")) {
		// Error: transfer failed; close connection and return failure
		//fprintf(stderr, "Error: send file failed\nResponse: %s\n", response);
		link_close(server);
		return -1;
	}

	//fprintf(stderr, "Response:\n");
	do {
	//	fprintf(stderr, "\t%s\n", response);
		if(!strcmp(response, "Server: AmazonS3")) break;
	} while(link_readline(server, response, HEADER_LINE_MAX, stoptime));

	link_close(server);

	return 0;
}

int s3_get_file(const char* localname, struct s3_dirent_object *dirent, char* remotename, char* bucketname, const char* access_key_id, const char* access_key) {
	struct s3_message mesg;
	struct link* server;
	time_t stoptime = time(0)+s3_timeout;
	char response[HEADER_LINE_MAX];
	char * text;
	int length;
	FILE* outfile;

	if(!access_key_id || !access_key || !s3_endpoint) return -1;

	mesg.type = S3_MESG_GET;
	mesg.path = remotename;
	mesg.bucket = bucketname;
	mesg.content_type = NULL;
	mesg.content_md5 = NULL;
	mesg.content_length = 0;
	mesg.date = time(0);
	mesg.expect = 0;
	mesg.amz_headers = NULL;

	server = link_connect(s3_address, 80, stoptime);
	if(!server) return -1;

	sign_message(&mesg, access_key_id, access_key);
	length = s3_message_to_string(&mesg, &text);
	//fprintf(stderr, "message: %s\n", text);

	link_putlstring(server, text, length, stoptime);
	free(text);

	link_readline(server, response, HEADER_LINE_MAX, stoptime);
	if(strcmp(response, "HTTP/1.1 200 OK")) {
		// Error: transfer failed; close connection and return failure
		//fprintf(stderr, "Error: request file failed\nResponse: %s\n", response);
		link_close(server);
		return -1;
	}

	//fprintf(stderr, "Response:\n");
	do {
	//	fprintf(stderr, "\t%s\n", response);
		if(!strncmp(response, "Content-Length:", 14)) {
			sscanf(response, "Content-Length: %d", &length);
		} else if(dirent && dirent->metadata && !strncmp(response, "x-amz-meta-", 11)) {
			struct amz_metadata_object *obj;
			obj = malloc(sizeof(*obj));
			sscanf(response, "x-amz-meta-%[^:]: %s", obj->type, obj->value);
			list_push_tail(dirent->metadata, obj);
		} else if(dirent && !strncmp(response, "Last-Modified:", 14)) {
			struct tm date;
			char date_str[1024];
			sscanf(response, "Last-Modified: %s", date_str);
			strptime(date_str, "%a, %d %b %Y %H:%M:%S %Z", &date);
			date.tm_isdst = -1;
			dirent->last_modified = mktime(&date);
		} else if(dirent && !strncmp(response, "ETag:", 5)) {
			sscanf(response, "ETag: \"%[^\"]\"", dirent->digest);
		}

		if(!strcmp(response, "Server: AmazonS3")) break;
	} while(link_readline(server, response, HEADER_LINE_MAX, stoptime));
	if(dirent) {
		dirent->size = length;
		sprintf(dirent->key, "%s", remotename);
		dirent->owner[0] = 0;
		dirent->display_name = 0;
	}

	link_readline(server, response, HEADER_LINE_MAX, stoptime);
	outfile = fopen(localname, "w");
	if(!outfile) {
		//fprintf(stderr, "error opening destination file\n");
		link_close(server);
		return -1;
	}
	link_stream_to_file(server, outfile, length, stoptime);
	fclose(outfile);

	link_close(server);
	return 0;
}

int s3_rm_file(char* filename, char* bucketname, const char* access_key_id, const char* access_key) {
	struct s3_message mesg;
	struct link* server;
	time_t stoptime = time(0)+s3_timeout;
	char response[HEADER_LINE_MAX];
	char * text;
	int length;

	if(!access_key_id || !access_key || !s3_endpoint) return -1;

	mesg.type = S3_MESG_DELETE;
	mesg.path = filename;
	mesg.bucket = bucketname;
	mesg.content_type = NULL;
	mesg.content_md5 = NULL;
	mesg.content_length = 0;
	mesg.date = time(0);
	mesg.expect = 0;
	mesg.amz_headers = NULL;

	server = link_connect(s3_address, 80, stoptime);
	if(!server) return -1;

	sign_message(&mesg, access_key_id, access_key);
	length = s3_message_to_string(&mesg, &text);

	link_putlstring(server, text, length, stoptime);
	free(text);

	link_readline(server, response, HEADER_LINE_MAX, stoptime);
	if(strcmp(response, "HTTP/1.1 204 No Content")) {
		// Error: transfer failed; close connection and return failure
		//fprintf(stderr, "Error: delete file failed\nResponse: %s\n", response);
		link_close(server);
		return -1;
	}

	//fprintf(stderr, "Response:\n");
	do {
	//	fprintf(stderr, "\t%s\n", response);
		if(!strcmp(response, "Server: AmazonS3")) break;
	} while(link_readline(server, response, HEADER_LINE_MAX, stoptime));

	link_close(server);
	return 0;
}

int s3_stat_file(char* filename, char* bucketname, struct s3_dirent_object* dirent, const char* access_key_id, const char* access_key) {
	struct s3_message mesg;
	struct link* server;
	time_t stoptime = time(0)+s3_timeout;
	char response[HEADER_LINE_MAX];
	char * text;
	int length;

	if(!access_key_id || !access_key || !s3_endpoint) return -1;

	mesg.type = S3_MESG_HEAD;
	mesg.path = filename;
	mesg.bucket = bucketname;
	mesg.content_type = NULL;
	mesg.content_md5 = NULL;
	mesg.content_length = 0;
	mesg.date = time(0);
	mesg.expect = 0;
	mesg.amz_headers = NULL;

	server = link_connect(s3_address, 80, stoptime);
	if(!server) return -1;

	sign_message(&mesg, access_key_id, access_key);
	length = s3_message_to_string(&mesg, &text);

	link_putlstring(server, text, length, stoptime);
	free(text);

	link_readline(server, response, HEADER_LINE_MAX, stoptime);
	if(strcmp(response, "HTTP/1.1 200 OK")) {
		// Error: transfer failed; close connection and return failure
		//fprintf(stderr, "Error: request file failed\nResponse: %s\n", response);
		link_close(server);
		return -1;
	}

	do {
		if(!strncmp(response, "Content-Length:", 14)) {
			sscanf(response, "Content-Length: %d", &length);
		} else if(dirent->metadata && !strncmp(response, "x-amz-meta-", 11)) {
			struct amz_metadata_object *obj;
			obj = malloc(sizeof(*obj));
			sscanf(response, "x-amz-meta-%[^:]: %s", obj->type, obj->value);
			list_push_tail(dirent->metadata, obj);
		} else if(!strncmp(response, "Last-Modified:", 14)) {
			struct tm date;
			char date_str[1024];
			sscanf(response, "Last-Modified: %s", date_str);
			strptime(date_str, "%a, %d %b %Y %H:%M:%S %Z", &date);
			date.tm_isdst = -1;
			dirent->last_modified = mktime(&date);
		} else if(!strncmp(response, "ETag:", 5)) {
			sscanf(response, "ETag: \"%[^\"]\"", dirent->digest);
		}

		if(!strcmp(response, "Server: AmazonS3")) break;
	} while(link_readline(server, response, HEADER_LINE_MAX, stoptime));
	dirent->size = length;
	sprintf(dirent->key, "%s", filename);
	dirent->owner[0] = 0;
	dirent->display_name = 0;

	link_close(server);
	return 0;
}

