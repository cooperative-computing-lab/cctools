/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2005- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include <stdio.h>
#include <errno.h>
#include <string.h>
#include <stdlib.h>
#include <dirent.h>
#include <time.h>
#include <sys/stat.h>

#include "chirp_reli.h"
#include "chirp_recursive.h"
#include "chirp_stream.h"

#include "debug.h"
#include "auth_all.h"
#include "stringtools.h"
#include "xmalloc.h"
#include "full_io.h"

#if CCTOOLS_OPSYS_CYGWIN || CCTOOLS_OPSYS_DARWIN || CCTOOLS_OPSYS_FREEBSD
#define fopen64 fopen
#define open64 open
#define lseek64 lseek
#define stat64 stat
#define fstat64 fstat
#define lstat64 lstat
#define fseeko64 fseeko
#endif

static int timeout=3600;
static int buffer_size=65536;

static void show_version( const char *cmd )
{
	printf("%s version %d.%d.%d built by %s@%s on %s at %s\n",cmd,CCTOOLS_VERSION_MAJOR,CCTOOLS_VERSION_MINOR,CCTOOLS_VERSION_MICRO,BUILD_USER,BUILD_HOST,__DATE__,__TIME__);
}

static void show_help( const char *cmd )
{
	printf("use: %s [options] <local-file> <hostname[:port]> <remote-file>\n",cmd);
	printf("where options are:\n");
	printf(" -a <flag>  Require this authentication mode.\n");
	printf(" -b <size>  Set transfer buffer size. (default is %d bytes)\n",buffer_size);
	printf(" -d <flag>  Enable debugging for this subsystem.\n");
	printf(" -f         Follow input file like tail -f.\n");
	printf(" -t <time>  Timeout for failure. (default is %ds)\n",timeout);
	printf(" -v         Show program version.\n");
	printf(" -h         This message.\n");
}

int main( int argc, char *argv[] )
{	
	int did_explicit_auth = 0;
	int follow_mode = 0;
	int whole_file_mode = 1;
	const char *hostname, *source_file, *target_file;
	time_t stoptime;
	FILE *file;
	char c;

	debug_config(argv[0]);

	while((c=getopt(argc,argv,"a:b:d:ft:vh"))!=(char)-1) {
		switch(c) {
			case 'a':
				auth_register_byname(optarg);
				did_explicit_auth = 1;
				break;
			case 'b':
				buffer_size = atoi(optarg);
				break;
			case 'd':
				debug_flags_set(optarg);
				break;
			case 'f':
				follow_mode = 1;
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

	source_file = argv[optind];
	hostname = argv[optind+1];
	target_file = argv[optind+2];
	stoptime = time(0) + timeout;

	if(!strcmp(source_file,"-")) {
		file = stdin;
        source_file = "/dev/stdin";
	} else {
		file = fopen(source_file,"r");
		if(!file) {
			fprintf(stderr,"couldn't open %s: %s\n",source_file,strerror(errno));
			return 1;
		}
	}

	if(follow_mode) whole_file_mode = 0;

	if(whole_file_mode) {
		return chirp_recursive_put(hostname,source_file,target_file,stoptime);
	} else {
		struct chirp_stream *stream;
		char *buffer = xxmalloc(buffer_size);
		INT64_T ractual, wactual;

		stream = chirp_stream_open(hostname,target_file,CHIRP_STREAM_WRITE,stoptime);
		if(!stream) {
			fprintf(stderr,"couldn't open %s for writing: %s\n",target_file,strerror(errno));
			return 1;
		}

		while(1) {
			ractual = full_fread(file,buffer,buffer_size);
			if(ractual==0) {
				if(follow_mode) {
					debug(D_DEBUG,"waiting for more data...");
					sleep(1);
					continue;
				} else {
					break;
				}
			}
			wactual = chirp_stream_write(stream,buffer,ractual,stoptime);
			if(wactual!=ractual) {
				fprintf(stderr,"couldn't write to %s: %s\n",target_file,strerror(errno));
				return 1;
			}
		}
		chirp_stream_close(stream,stoptime);
		return 0;
	}
}
