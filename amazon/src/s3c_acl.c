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

	link_write(server, text, length, stoptime);
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
	link_write(server, text, length, stoptime);
	free(text);

	link_write(server, "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n", 39, stoptime);
	link_write(server, "<AccessControlPolicy><Owner><ID>", 32, stoptime);
	link_write(server, owner, strlen(owner), stoptime);
	link_write(server, "</ID></Owner><AccessControlList>", 32, stoptime);

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
			sprintf(grant, "<Grant>%s<Permission>FULL_CONTROL</Permission></Grant>", grantee);
			link_write(server, grant, strlen(grant), stoptime);
		}
		if(acl->perm & S3_ACL_READ) {
			sprintf(grant, "<Grant>%s<Permission>READ</Permission></Grant>", grantee);
			link_write(server, grant, strlen(grant), stoptime);
		}
		if(acl->perm & S3_ACL_WRITE) {
			sprintf(grant, "<Grant>%s<Permission>WRITE</Permission></Grant>", grantee);
			link_write(server, grant, strlen(grant), stoptime);
		}
		if(acl->perm & S3_ACL_READ_ACP) {
			sprintf(grant, "<Grant>%s<Permission>READ_ACP</Permission></Grant>", grantee);
			link_write(server, grant, strlen(grant), stoptime);
		}
		if(acl->perm & S3_ACL_WRITE_ACP) {
			sprintf(grant, "<Grant>%s<Permission>WRITE_ACP</Permission></Grant>", grantee);
			link_write(server, grant, strlen(grant), stoptime);
		}
	}

	link_write(server, "</AccessControlList></AccessControlPolicy>\n", 43, stoptime);

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

