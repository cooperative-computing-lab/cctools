/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2005- The University of Notre Dame
This software is distributed under a BSD-style license.
See the file COPYING for details.
*/

#include <stdio.h>
#include <errno.h>
#include <string.h>
#include <stdlib.h>
#include <dirent.h>
#include <time.h>

#include "chirp_reli.h"

#include "debug.h"
#include "auth_all.h"
#include "stringtools.h"
#include "xmalloc.h"
#include "full_io.h"

static int timeout=3600;

static void show_version( const char *cmd )
{
	printf("%s version %d.%d.%d built by %s@%s on %s at %s\n",cmd,CCTOOLS_VERSION_MAJOR,CCTOOLS_VERSION_MINOR,CCTOOLS_VERSION_MICRO,BUILD_USER,BUILD_HOST,__DATE__,__TIME__);
}

static void show_help( const char *cmd )
{
	printf("use: %s [options] <hostname[:port]> <remote-file> <local-file>\n",cmd);
	printf("where options are:\n");
	printf(" -a <flag>  Require this authentication mode.\n");
	printf(" -d <flag>  Enable debugging for this subsystem.\n");
	printf(" -t <time>  Timeout for failure. (default is %ds)\n",timeout);
	printf(" -v         Show program version.\n");
	printf(" -h         This message.\n");
}
 
int main( int argc, char *argv[] )
{	
	int did_explicit_auth = 0;
	const char *hostname, *source_file, *target_file;
	time_t stoptime;
	FILE *file;
	INT64_T result;
	char c;

	debug_config(argv[0]);

	while((c=getopt(argc,argv,"a:d:t:vh"))!=(char)-1) {
		switch(c) {
			case 'a':
				auth_register_byname(optarg);
				did_explicit_auth = 1;
				break;
			case 'd':
				debug_flags_set(optarg);
				break;
			case 't':
				timeout = string_time_parse(optarg);
				break;
			case 'v':
				show_version(argv[0]);
				exit(0);
				break;
			case 'h':
				show_help(argv[0]);
				exit(0);
				break;

		}
	}
 
	if(!did_explicit_auth) auth_register_all();

	if( (argc-optind)<3 ) {
		show_help(argv[0]);
		exit(0);
	}

	hostname = argv[optind];
	source_file = argv[optind+1];
	target_file = argv[optind+2];

	stoptime = time(0) + timeout;

	if(!strcmp(target_file,"-")) {
		file = stdout;
	} else {
		file = fopen(target_file,"w");
		if(!file) {
			fprintf(stderr,"couldn't open %s: %s\n",target_file,strerror(errno));
			return 1;
		}
	}

	result = chirp_reli_getfile(hostname,source_file,file,stoptime);
	if(result<0) {
		fprintf(stderr,"couldn't get %s:%s: %s\n",hostname,source_file,strerror(errno));
		return 1;
	} else {
		return 0;
	}
}

