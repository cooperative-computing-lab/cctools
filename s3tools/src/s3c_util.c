/*
Copyright (C) 2010- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <math.h>
#include <time.h>

#include <debug.h>
#include <link.h>
#include <list.h>
#include <hmac.h>
#include <b64_encode.h>
#include <domain_name_cache.h>

#include "s3c_util.h"

char s3_default_endpoint[] = "s3.amazonaws.com";
char *s3_endpoint = s3_default_endpoint;
int s3_timeout = 60;


int s3_set_endpoint(const char *target) {
	static char endpoint_address[DOMAIN_NAME_MAX];

	if(!target) return 0;
	if(!domain_name_cache_lookup(target, endpoint_address)) return 0;

	if(s3_endpoint && s3_endpoint != s3_default_endpoint) free(s3_endpoint);
	s3_endpoint = strdup(target);
	return 1;
}

struct s3_header_object* s3_new_header_object(enum s3_header_type type, const char* custom_type, const char* value) {
	struct s3_header_object *obj;
	obj = malloc(sizeof(*obj));
	obj->type = type;
	if(type == S3_HEADER_CUSTOM ) {
		obj->custom_type = strdup(custom_type);
	} else obj->custom_type = NULL;

	obj->value = strdup(value);
	return obj;
}


const char * s3_get_header_string(enum s3_header_type type, const char* custom_type) {
	static const char x_amz_acl[] = "x-amz-acl";
	static const char x_amz_mfa[] = "x-amz-mfa";

	switch(type) {
		case S3_HEADER_AMZ_ACL:
			return x_amz_acl;
		case S3_HEADER_AMZ_MFA:
			return x_amz_mfa;
		default:
			return custom_type;
	}
}

int s3_header_object_comp(const void *a, const void *b) {
	const struct s3_header_object *one, *two;
	const char *header1, *header2;
	int result;
	one = (struct s3_header_object *)a;
	two = (struct s3_header_object *)b;

	header1 = s3_get_header_string(one->type, one->custom_type);
	header2 = s3_get_header_string(two->type, two->custom_type);

	result = strcmp(header1, header2);
	if(result) return result;
	return strcmp(one->value, two->value);
}

int sign_message(struct s3_message* mesg, const char* user, const char * key) {
	int sign_str_len = 0;
	char *sign_str;
	char date[1024];
	struct s3_header_object *amz;
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
		while( (amz = (struct s3_header_object *)list_next_item(mesg->amz_headers)) ) {
			if(amz->type < S3_HEADER_CUSTOM) continue;
			switch(amz->type) {
				case S3_HEADER_CUSTOM:
					if(!amz->custom_type) return -1;
					sign_str_len += strlen(amz->custom_type) + 1;
					break;
				default:
					sign_str_len += strlen(s3_get_header_string(amz->type, amz->custom_type)) + 1;
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
		amz_headers = list_sort(list_duplicate(mesg->amz_headers), &s3_header_object_comp);
		list_first_item(amz_headers);
		{	int c_type = -1;
			char* c_ctype = NULL;
			while( (amz = (struct s3_header_object *)list_next_item(amz_headers)) ) {
				if(amz->type < S3_HEADER_CUSTOM) continue;
				if(c_type != amz->type) {
					c_type = amz->type;
					c_ctype = amz->custom_type;
					sprintf(sign_str, "%s\n%s:%s", sign_str, s3_get_header_string(amz->type, amz->custom_type), amz->value);

				} else if(c_type == S3_HEADER_CUSTOM && strcmp(c_ctype, amz->custom_type)) {
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


struct link * s3_send_message(struct s3_message *mesg, struct link *server, time_t stoptime) {
	char *message_text;
	char address[16];
	int message_length, data_sent;
	
	if(!server) {
		if(!domain_name_cache_lookup(s3_endpoint, address))
			return NULL;

		server = link_connect(address, 80, stoptime);
	}
	
	if(!server)
		return NULL;

	message_length = s3_message_to_string(mesg, &message_text);
	
	if(message_length <= 0) {
		link_close(server);
		return NULL;
	}
	
	data_sent = link_write(server, message_text, message_length, stoptime);
	debug(D_TCP, "S3 Message Sent:\n%s\n", message_text);
	free(message_text);
	
	if(data_sent < message_length) {
		link_close(server);
		server = NULL;
	}
	
	return server;
}

int s3_message_to_string(struct s3_message *mesg, char** text) {
	int msg_str_len = 0;
	char *msg_str = NULL;
	char date[1024];
	struct s3_header_object *amz;

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
		while( (amz = (struct s3_header_object *)list_next_item(mesg->amz_headers)) ) {
			switch(amz->type) {
				case S3_HEADER_CUSTOM:
					if(!amz->custom_type) {
						fprintf(stderr, "no custom type defined for S3_HEADER_CUSTOM\n");
						return 0;
					}
					msg_str_len += strlen(amz->custom_type) + 2;
					break;
				default:
					msg_str_len += strlen(s3_get_header_string(amz->type, amz->custom_type)) + 2;
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
		while( (amz = (struct s3_header_object *)list_next_item(mesg->amz_headers)) ) {
			switch(amz->type) {
				case S3_HEADER_CUSTOM:
					sprintf(msg_str, "%s%s: %s\r\n", msg_str, amz->custom_type, amz->value);
					break;
				default:
					sprintf(msg_str, "%s%s: %s\r\n", msg_str, s3_get_header_string(amz->type, amz->custom_type), amz->value);
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

