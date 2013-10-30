/*
Copyright (C) 2010- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/
#include <list.h>
#include <link.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <sys/stat.h>

#include "s3c_util.h"
#include "s3c_file.h"

extern char *s3_endpoint;
extern char *s3_address;
extern int s3_timeout;


int s3_put_file(const char* localname, char* remotename, char* bucketname, enum amz_base_perm perms, const char* access_key_id, const char* access_key) {
	struct link* server;
	struct s3_header_object *head;
	time_t stoptime = time(0)+s3_timeout;
	struct s3_message mesg;
	struct stat st;
	FILE* infile;
	char response[HEADER_LINE_MAX];
	//char * text;
	//int length;

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
		case AMZ_PERM_PRIVATE:      head = s3_new_header_object(S3_HEADER_AMZ_ACL, NULL, "private"); break;
		case AMZ_PERM_PUBLIC_READ:  head = s3_new_header_object(S3_HEADER_AMZ_ACL, NULL, "public-read"); break;
		case AMZ_PERM_PUBLIC_WRITE: head = s3_new_header_object(S3_HEADER_AMZ_ACL, NULL, "public-read-write"); break;
		case AMZ_PERM_AUTH_READ:    head = s3_new_header_object(S3_HEADER_AMZ_ACL, NULL, "authenticated-read"); break;
		case AMZ_PERM_BUCKET_READ:  head = s3_new_header_object(S3_HEADER_AMZ_ACL, NULL, "bucket-owner-read"); break;
		case AMZ_PERM_BUCKET_FULL:  head = s3_new_header_object(S3_HEADER_AMZ_ACL, NULL, "bucket-owner-full-control"); break;
		default: return -1;
	}

	mesg.amz_headers = list_create();
	if(!mesg.amz_headers) return -1;
	list_push_tail(mesg.amz_headers, head);

	sign_message(&mesg, access_key_id, access_key);
	server = s3_send_message(&mesg, NULL, stoptime);
	//length = s3_message_to_string(&mesg, &text);
	list_free(mesg.amz_headers);
	list_delete(mesg.amz_headers);

	if(!server)
		return -1;

	//server = link_connect(s3_address, 80, stoptime);
	//if(!server) return -1;

	//link_putlstring(server, text, length, stoptime);
	//free(text);

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

	link_readline(server, response, HEADER_LINE_MAX, stoptime);
	if(strcmp(response, "HTTP/1.1 200 OK")) {
		// Error: transfer failed; close connection and return failure
		//fprintf(stderr, "Error: send file failed\nResponse: %s\n", response);
		link_close(server);
		return -1;
	}

	do {
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
	//char * text;
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

	//server = link_connect(s3_address, 80, stoptime);

	sign_message(&mesg, access_key_id, access_key);
	server = s3_send_message(&mesg, NULL, stoptime);
	if(!server)
		return -1;
	
	//length = s3_message_to_string(&mesg, &text);

	//link_putlstring(server, text, length, stoptime);
	//free(text);

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
	//char * text;
	//int length;

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

	//server = link_connect(s3_address, 80, stoptime);

	sign_message(&mesg, access_key_id, access_key);
	server = s3_send_message(&mesg, NULL, stoptime);
	if(!server)
		return -1;

	//length = s3_message_to_string(&mesg, &text);

	//link_putlstring(server, text, length, stoptime);
	//free(text);

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
	//char * text;
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

	//server = link_connect(s3_address, 80, stoptime);

	sign_message(&mesg, access_key_id, access_key);
	server = s3_send_message(&mesg, NULL, stoptime);
	if(!server)
		return -1;

	//length = s3_message_to_string(&mesg, &text);

	//link_putlstring(server, text, length, stoptime);
	//free(text);

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

	link_close(server);
	return 0;
}


/* vim: set noexpandtab tabstop=4: */
