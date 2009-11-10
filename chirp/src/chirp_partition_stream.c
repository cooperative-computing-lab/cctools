/*
Copyright (C) 2008- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <string.h>

#include <stdio.h>
#include <errno.h>
#include <string.h>
#include <stdlib.h>
#include <dirent.h>
#include <time.h>
#include <sys/stat.h>

#include "chirp_reli.h"
#include "chirp_stream.h"

#include "debug.h"
#include "auth_all.h"
#include "stringtools.h"
#include "xmalloc.h"

static int timeout=3600;
static int buffer_size=1048576;
int MAX_LINE_LEN=65537;

static void show_version( const char *cmd )
{
	printf("%s version %d.%d.%d built by %s@%s on %s at %s\n",cmd,CCTOOLS_VERSION_MAJOR,CCTOOLS_VERSION_MINOR,CCTOOLS_VERSION_MICRO,BUILD_USER,BUILD_HOST,__DATE__,__TIME__);
}

static void show_help( const char *cmd )
{
	printf("use: %s [options] <local-file> <hostname[:port]> <remote-file> [<hostname[:port]> <remote-file> ...]\n",cmd);
	printf("where options are:\n");
	printf(" -a <flag>  Require this authentication mode.\n");
	printf(" -b <size>  Set transfer buffer size. (default is %d bytes)\n",buffer_size);
	printf(" -d <flag>  Enable debugging for this subsystem.\n");
	printf(" -t <time>  Timeout for failure. (default is %ds)\n",timeout);
	printf(" -v         Show program version.\n");
	printf(" -h         This message.\n");
   
}

int main(int argc, char *argv[]) {


    int did_explicit_auth = 0;
    time_t stoptime;
    char c;
    int i, srcindex, numparts;
    char instring[MAX_LINE_LEN];
    FILE* input;
    struct chirp_stream *outputs[argc-2];
    INT64_T offsets[argc-2];
    INT64_T linelen, written;

    
    debug_config(argv[0]);
    
    while((c=getopt(argc,argv,"a:b:d:t:vh"))!=(char)-1) {
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
	    
	}
    }

    if(!did_explicit_auth) auth_register_all();

    if( (argc-optind)<3 ) {
	show_help(argv[0]);
	exit(0);
    }
    
    srcindex=optind;
    numparts=(argc-optind-1)/2;
    stoptime = time(0) + timeout;    

    if(!(input = fopen(argv[srcindex], "r"))) {fprintf(stderr,"%s: %s\n",argv[srcindex],strerror(errno)); return 1; }

    for(i=0 ; i<numparts; i++) {
	outputs[i]=chirp_stream_open(argv[srcindex+(2*i)+1],argv[srcindex+(2*i)+2],CHIRP_STREAM_WRITE,stoptime);
	if(!(outputs[i])) {fprintf(stderr,"couldn't open %s for writing on %s: %s\n",argv[srcindex+(2*i)+2],argv[srcindex+(2*i)+1],strerror(errno)); return 1; }
	offsets[i]=0;
    }

    i=0;
    fgets(instring,MAX_LINE_LEN,input);
    while(!feof(input)) {
	linelen=strlen(instring);

	written = chirp_stream_write(outputs[i],instring,linelen,stoptime);
	if(written != linelen) {
	    fprintf(stderr,"%s/%s: %s\n",argv[srcindex+(2*i)+1],argv[srcindex+(2*i)+2],strerror(errno)); return 1;
	}
	
	i=(i+1)%numparts;
	fgets(instring,MAX_LINE_LEN,input);
    }
    
    for(i=0 ; i<numparts; i++) {
	chirp_stream_flush(outputs[i],stoptime);
	chirp_stream_close(outputs[i],stoptime);
    }

    return 0;
}
