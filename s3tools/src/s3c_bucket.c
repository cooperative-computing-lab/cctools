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

#include "s3c_bucket.h"
#include "s3c_file.h"
#include "s3c_util.h"

extern const char *s3_endpoint;
extern const char *s3_address;
extern int s3_timeout;

int s3_mk_bucket(char* bucketname, enum amz_base_perm perms, const char* access_key_id, const char* access_key) {
	struct link* server;
	char path[] = "/";
	struct s3_header_object *head;
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
		case AMZ_PERM_PRIVATE:      head = s3_new_header_object(S3_HEADER_AMZ_ACL, NULL, "private"); break;
		case AMZ_PERM_PUBLIC_READ:  head = s3_new_header_object(S3_HEADER_AMZ_ACL, NULL, "public-read"); break;
		case AMZ_PERM_PUBLIC_WRITE: head = s3_new_header_object(S3_HEADER_AMZ_ACL, NULL, "public-read-write"); break;
		case AMZ_PERM_AUTH_READ:    head = s3_new_header_object(S3_HEADER_AMZ_ACL, NULL, "authenticated-read"); break;
		case AMZ_PERM_BUCKET_READ:  head = s3_new_header_object(S3_HEADER_AMZ_ACL, NULL, "bucket-owner-read"); break;
		case AMZ_PERM_BUCKET_FULL:  head = s3_new_header_object(S3_HEADER_AMZ_ACL, NULL, "bucket-owner-full-control"); break;
		default: return -1;
	}
	mesg.amz_headers = list_create();
	list_push_tail(mesg.amz_headers, head);


	sign_message(&mesg, access_key_id, access_key);
	length = s3_message_to_string(&mesg, &text);
	list_free(mesg.amz_headers);
	list_delete(mesg.amz_headers);

	server = link_connect(s3_address, 80, stoptime);
	if(!server) return -1;

	link_write(server, text, length, stoptime);
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

	link_write(server, text, length, stoptime);
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
		link_write(server, text, length, stoptime);
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
			struct s3_dirent_object *dirent;
			struct tm date;
			char display_name[1024];
			end = strstr(start, "</Contents>");
			end[10] = '\0';
			temp = end + 11;
			dirent = malloc(sizeof(*dirent));
			date.tm_isdst = -1;
			sscanf(strstr(start, "<Key>"), "<Key>%[^<]</Key>", dirent->key);
			sscanf(strstr(start, "<LastModified>"), "<LastModified>%d-%d-%dT%d:%d:%d.%*dZ</LastModified>", &date.tm_year, &date.tm_mon, &date.tm_mday, &date.tm_hour, &date.tm_min, &date.tm_sec);
			sscanf(strstr(start, "<ETag>"), "<ETag>&quot;%[^&]&quot;</ETag>", dirent->digest);
			sscanf(strstr(start, "<Size>"), "<Size>%d</Size>", &dirent->size);
			sscanf(strstr(start, "<Owner>"), "<Owner><ID>%*[^<]</ID><DisplayName>%[^<]</DisplayName></Owner>", display_name);
			if(strlen(display_name)) dirent->display_name = strdup(display_name);
			date.tm_mon -= 1;
			dirent->last_modified = mktime(&date);
			list_push_tail(dirents, dirent);
			last = dirent->key;
		}
		free(buffer);

	} while(!done);

	link_close(server);
	return 0;
}

