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
#include <sys/types.h>
#include <dirent.h>

#include "chirp_reli.h"

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
static int buffer_size=1048576;

static void show_version( const char *cmd )
{
	printf("%s version %d.%d.%d built by %s@%s on %s at %s\n",cmd,CCTOOLS_VERSION_MAJOR,CCTOOLS_VERSION_MINOR,CCTOOLS_VERSION_MICRO,BUILD_USER,BUILD_HOST,__DATE__,__TIME__);
}

static void show_help( const char *cmd )
{
	printf("use: %s [options] <local-directory> <hostname[:port]> <remote-directory>\n",cmd);
	printf("where options are:\n");
	printf(" -a <flag>  Require this authentication mode.\n");
	printf(" -b <size>  Set transfer buffer size. (default is %d bytes)\n",buffer_size);
	printf(" -d <flag>  Enable debugging for this subsystem.\n");
	printf(" -t <time>  Timeout for failure. (default is %ds)\n",timeout);
	printf(" -w	    wait for new data at src after all preexisting data is sent\n");
	printf(" -x	    remove the files after they have been migrated");
	printf(" -v         Show program version.\n");
	printf(" -h         This message.\n");
}

//3 arguments after options,  arg1 src dir, arg2 hostname, arg3 target dir 
int main( int argc, char *argv[] )
{	
	INT64_T result;
	int did_explicit_auth = 0;
	int forever = 0;
	int rm = 0;
	int missed_some = 0;
	const char *hostname, *source_dir, *target_dir;
	char src[512], dest[512];
	time_t stoptime;
	DIR *dir;
	struct dirent *entry;
	struct chirp_stat buf;
	FILE *file;
	char c;

	debug_config(argv[0]);

	while((c=getopt(argc,argv,"a:b:d:t:vhwx"))!=(char)-1) {
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
			case 'w':
				forever = 1;
				break;
			case 'x':
				rm = 1;
				break;
		}
	}
 
	while(1) {
		if(!did_explicit_auth) auth_register_all();

		if( (argc-optind)<3 ) {
			show_help(argv[0]);
			exit(0);
		}

		source_dir = argv[optind];
		hostname = argv[optind+1];
		target_dir = argv[optind+2];
		stoptime = time(0) + timeout;


		if(chirp_reli_stat(hostname, target_dir, &buf, stoptime)) {
			result = chirp_reli_mkdir(hostname, target_dir, 0700, stoptime);
			if(result) {
				fprintf(stderr, "couldn't create%s: %s\n",target_dir, strerror(errno));
				return 1;
			}
		}

		while(1) {
			stoptime = time(0) + timeout;
			dir = opendir(source_dir);
			if (!dir) {
				fprintf(stderr, "couldn't open %s: %s\n",source_dir,strerror(errno));
				return 1;
			}
			entry = readdir(dir);
			while(entry) {
				if(strcmp(entry->d_name, ".")&&strcmp(entry->d_name, "..")) {
					strcpy(src, source_dir);
					strcpy(dest, target_dir);
					strcat(dest, "/");
					strcat(dest, entry->d_name);
					strcat(src, "/");
					strcat(src, entry->d_name);
					file = fopen(src, "r");
					if (file) {
						struct stat64 info;
						fstat64(fileno(file),&info);
						fprintf(stderr, "Trying to put %s in %s:%s\n", src, hostname, dest);
						if(chirp_reli_putfile(hostname,dest,file,info.st_mode,info.st_size,stoptime)!=info.st_size) {
							fprintf(stderr,"couldn't put %s: %s\n",entry->d_name,strerror(errno));
							missed_some++;
						} else if (rm) remove(src);		
						fclose(file);
					} else {
						fprintf(stderr, "file open failed for some reason\n");
						fprintf(stderr, "error is: %s\n", strerror(errno));
					}
				}
				entry = readdir(dir);
			}
			closedir(dir);
			if (!forever && !missed_some) break;
		}
		if (!forever) break;
	}
	
	return 0;
}
