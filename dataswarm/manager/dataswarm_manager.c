/*
Copyright (C) 2020- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <getopt.h>
#include <errno.h>

#include "link.h"
#include "jx.h"
#include "jx_print.h"
#include "jx_parse.h"
#include "debug.h"
#include "stringtools.h"
#include "cctools.h"
#include "hash_table.h"

#include "dataswarm_worker.h"
#include "dataswarm_client.h"

struct hash_table *worker_table = 0;
struct hash_table *client_table = 0;
struct link *manager_link = 0;

int connect_timeout = 5;
int stall_timeout = 30;

int send_string_message( struct link *l, const char *str, int length, time_t stoptime )
{
	char lenstr[16];
	sprintf(lenstr,"%d\n",length);
	int lenstrlen = strlen(lenstr);
	int result = link_write(l,lenstr,lenstrlen,stoptime);
	if(result!=lenstrlen) return 0;
	result = link_write(l,str,length,stoptime);
	return result==length;
}

char * recv_string_message( struct link *l, time_t stoptime )
{
	char lenstr[16];
	int result = link_readline(l,lenstr,sizeof(lenstr),stoptime);
	if(!result) return 0;

	int length = atoi(lenstr);
	char *str = malloc(length);
	result = link_read(l,str,length,stoptime);
	if(result!=length) {
		free(str);
		return 0;
	}
	return str;
}

int send_json_message( struct link *l, struct jx *j, time_t stoptime )
{
	char *str = jx_print_string(j);
	int result = send_string_message(l,str,strlen(str),stoptime);
	free(str);
	return result;
}

struct jx * recv_json_message( struct link *l, time_t stoptime )
{
	char *str = recv_string_message(l,stoptime);
	if(!str) return 0;
	struct jx *j = jx_parse_string(str);
	free(str);
	return j;
}

void process_files()
{
}

void process_tasks()
{
}

void handle_connect_message( struct link *manager_link, time_t stoptime )
{
	struct link *l;

	while((l = link_accept(manager_link,stoptime))) {
		struct jx *msg = recv_json_message(l,stoptime);
		if(!msg) {
			link_close(l);
			break;
		}

		char addr[LINK_ADDRESS_MAX];
		int port;

		link_address_remote(l,addr,&port);
		char *key = string_format("%s:%d",addr,port);

		printf("new connection from %s:%d\n",addr,port);

		const char *type = jx_lookup_string(msg,"type");

		if(!strcmp(type,"worker")) {
			printf("new worker from %s:%d\n",addr,port);	
			struct dataswarm_worker *w = dataswarm_worker_create(l);
			hash_table_insert(worker_table,key,w);
		} else if(!strcmp(type,"client")) {
			printf("new client from %s:%d\n",addr,port);	
			struct dataswarm_client *c = dataswarm_client_create(l);
			hash_table_insert(client_table,key,c);
		} else {
			/* invalid type? */
		}

		jx_delete(msg);
	}
}

void handle_client_message( struct dataswarm_client *c, time_t stoptime )
{
	struct jx *msg = recv_json_message(c->link,stoptime);
	if(!msg) return;

	const char *action = jx_lookup_string(msg,"action");
	if(!strcmp(action,"task_submit")) {
		/* */
	} else if(!strcmp(action,"file_submit")) {
		/* */
	} else {
		/* send back an invalid command response */
	}	
}

void handle_worker_message( struct dataswarm_worker *w, time_t stoptime )
{
	struct jx *msg = recv_json_message(w->link,stoptime);
	const char *action = jx_lookup_string(msg,"action");
	if(!strcmp(action,"task_change")) {
		/* */
	} else if(!strcmp(action,"file_change")) {
		/* */
	} else if(!strcmp(action,"status")) {
		/* */
	} else {
		/* send back an invalid command response */
	}
}

int handle_messages( int msec )
{
	int n = hash_table_size(client_table) + hash_table_size(worker_table) + 1;

	struct link_info *table = malloc(sizeof(struct link_info)*(n+1));

	table[0].link = manager_link;
	table[0].events = LINK_READ;
	table[0].revents = 0;

	char *key;
	struct dataswarm_worker *w;
	struct dataswarm_client *c;

	n = 1;

	hash_table_firstkey(client_table);
	while(hash_table_nextkey(client_table, &key, (void **) &c)) {
		table[n].link = c->link;
		table[n].events = LINK_READ;
		table[n].revents = 0;
		n++;
	}

	hash_table_firstkey(worker_table);
	while(hash_table_nextkey(worker_table, &key, (void **) &w)) {
		table[n].link = w->link;
		table[n].events = LINK_READ;
		table[n].revents = 0;
		n++;
	}

	link_poll(table,n,msec);

	int i;
	for(i=0;i<n;i++) {
		if(table[i].revents&LINK_READ) {
			struct link *l = table[i].link;
			if(i==0) {
				handle_connect_message(l,time(0)+connect_timeout);
			} else if((c=hash_table_lookup(client_table,key))) {
				handle_client_message(c,time(0)+stall_timeout);
			} else if((w==hash_table_lookup(worker_table,key))) {
				handle_worker_message(w,time(0)+stall_timeout);
			}

		}
	}

	free(table);

	return n;
}

void server_main_loop()
{
	while(1) {
		handle_messages(100);
		process_files();
		process_tasks();
	}
}

static const struct option long_options[] = 
{
	{"port", required_argument, 0, 'p'},
	{"debug", required_argument, 0, 'd'},
	{"debug-file", required_argument, 0, 'o'},
	{"help", no_argument, 0, 'h' },
	{"version", no_argument, 0, 'v' }
};

static void show_help( const char *cmd )
{
	printf("use: %s [options]\n",cmd);
	printf("where options are:\n");
	printf("-p,--port=<port>          Port number to listen on.\n");
	printf("-d,--debug=<subsys>       Enable debugging for this subsystem.\n");
	printf("-o,--debug-file=<file>    Send debugging output to this file.\n");
	printf("-h,--help                 Show this help string\n");
	printf("-v,--version              Show version string\n");	
}

int main(int argc, char *argv[])
{
	int port = 0;

	int c;
        while((c = getopt_long(argc, argv, "p:N:s:d:o:hv", long_options, 0))!=-1) {

		switch(c) {
			case 'd':
				debug_flags_set(optarg);
				break;
			case 'o':
				debug_config_file(optarg);
				break;
			case 'p':
				port = atoi(optarg);
				break;
			case 'v':
	                        cctools_version_print(stdout, argv[0]);
				return 0;
				break;
			default:
			case 'h':
				show_help(argv[0]);
				return 0;
				break;
		}
	}


	manager_link = link_serve(port);
	if(!manager_link) {
		printf("could not serve on port %d: %s\n", port,strerror(errno));
		return 1;
	}
	
	char addr[LINK_ADDRESS_MAX];
	link_address_local(manager_link,addr,&port);
	printf("listening on port %d...\n",port);

	worker_table = hash_table_create(0,0);
	client_table = hash_table_create(0,0);

	server_main_loop();

	printf("server shutting down.\n");

	return 0;
}

