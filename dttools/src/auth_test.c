/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2005- The University of Notre Dame
This software is distributed under a BSD-style license.
See the file COPYING for details.
*/

#include "auth.h"
#include "link.h"
#include "getopt.h"
#include "debug.h"
#include "domain_name_cache.h"
#include "auth_all.h"
#include "stringtools.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>

static void show_use( const char *cmd ) 
{
	fprintf(stderr,"Use: %s [options]\n",cmd);
	fprintf(stderr,"Where options are:\n");
	fprintf(stderr," -a <type> Allow this auth type\n");
	fprintf(stderr," -p <num>  Port number\n");
	fprintf(stderr," -r <host> Remote host\n");
	fprintf(stderr," -d <flag> Debugging\n");
	fprintf(stderr,"Where debug flags arg: ");
	debug_flags_print(stderr);
	fprintf(stderr,"\n");
}


int main( int argc, char *argv[] )
{
	struct link *link, *master;
	char *subject=0, *type=0;
	time_t stoptime;
	char line[1024];
	char c;
	int portnum=30000;
	char *hostname=0;
	int timeout=30;

	debug_config(argv[0]);

	while((c=getopt(argc,argv,"a:p:r:d:o:O:"))!=(char)-1) {
		switch(c) {
			case 'p':
				portnum = atoi(optarg);
				break;
			case 'r':
				hostname = optarg;
				break;
			case 'd':
				debug_flags_set(optarg);
				break;
			case 'o':
				debug_config_file(optarg);
				break;
			case 'O':
				debug_config_file_size(string_metric_parse(optarg));
				break;
			case 'a':
				if(!auth_register_byname(optarg)) fatal("couldn't register %s authentication",optarg);
				break;
			default:
				show_use(argv[0]);
				exit(1);
		}
	}

	if(hostname) {
		char addr[LINK_ADDRESS_MAX];

		stoptime = time(0)+timeout;

		if(!domain_name_cache_lookup(hostname,addr)) fatal("unknown host name: %s",hostname);

		link = link_connect(addr,portnum,stoptime);
		if(!link) fatal("couldn't connect to %s:%d: %s",hostname,portnum,strerror(errno));

		if(auth_assert(link,&type,&subject,stoptime)) {
			printf("server thinks I am %s %s\n",type,subject);
			if(link_readline(link,line,sizeof(line),stoptime)) {
				printf("got message: %s\n",line);
			} else {
				printf("lost connection!\n");
			}
		} else {
			printf("unable to authenticate.\n");
		}

		link_close(link);

	} else {
		stoptime = time(0)+timeout;

		master = link_serve(portnum);
		if(!master) fatal("couldn't serve port %d: %s\n",portnum,strerror(errno));

		while(time(0)<stoptime) {
			link = link_accept(master,stoptime);
			if(!link) continue;

			if(auth_accept(link,&type,&subject,stoptime)) {
				time_t t = time(0);
				sprintf(line,"Hello %s:%s, it is now %s",type,subject,ctime(&t));
				link_write(link,line,strlen(line),stoptime);
			} else {
				printf("couldn't auth accept\n");
			} 
			link_close(link);
		}
	}

	return 0;
}


