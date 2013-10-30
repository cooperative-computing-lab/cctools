/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2005- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "ast.h"
#include "ast_print.h"
#include "ast_execute.h"
#include "parser.h"
#include "variable.h"
#include "ftsh_error.h"
#include "multi_fork.h"

#include "cctools.h"
#include "debug.h"
#include "xxmalloc.h"
#include "stringtools.h"
#include "macros.h"

#include <stdio.h>
#include <getopt.h>
#include <errno.h>
#include <string.h>
#include <stdlib.h>
#include <signal.h>

int ftsh_expmin=1;
int ftsh_expmax=3600;
int ftsh_expfactor=2;
int ftsh_exprand=1;

static void null_handler( int sig )
{
}

static void show_help( char *cmd )
{
	cctools_version_print(stderr, cmd);
	fprintf(stderr,"\
Use: ftsh [options] <program> [arg1] [arg2]\n\
Where options are:\n\
  -f <file>  Log file.\n\
             Default is the standard error.\n\
             Overrides environment variable FTSH_LOG_FILE.\n\
  -l <level> Log level. Default is '10'.\n\
             0  = log nothing\n\
             10 = log failed commands\n\
             20 = log all commands\n\
             30 = log program structures\n\
             40 = log process and signal activity\n\
             Overrides environment variable FTSH_LOG_LEVEL.\n\
  -D         Log time values in decimal format.\n\
             Overrides environment variable FTSH_LOG_DECIMAL.\n\
  -t <secs>  Kill timeout.  Default is '30'.\n\
             Number of seconds between soft kill and hard kill.\n\
             Overrides environment variable FTSH_KILL_TIMEOUT.\n\
  -k <mode>  Kill mode.  Default is 'strong'.\n\
             May be 'weak' or 'strong'.\n\
             Overrides environment variable FTSH_KILL_MODE.\n\
  -p or -n   Parse and print program, but do not execute.\n\
  -P         Parse and print program, including parser debug log.\n\
  -v         Show version string.\n\
  -h         Show this help screen.\n\
");

}

static int ftsh_main( int argc, char *argv[] )
{
	struct ast_group *g;
	int parse_mode = 0;
	int parse_debug_mode = 0;
	char *log_file = 0;
	int log_level = 10;
	int log_decimal = 0;
	int kill_timeout = 30;
	char *kill_mode = "strong";
	FILE *stream;
	char *s;
    signed char c;
	char env[1024];
	int result;
	struct sigaction sa;
	sigset_t ss;

	/* We want to be woken, not aborted, on these signals. */
	sigemptyset(&ss);
	sa.sa_handler = null_handler;
	sa.sa_mask = ss;
	sa.sa_flags = 0;
	sigaction(SIGCHLD,&sa,0);
	sigaction(SIGALRM,&sa,0);

	random_init();

	/* First, get settings from the environment */

	log_file = getenv("FTSH_LOG_FILE");

	s = getenv("FTSH_LOG_LEVEL");
	if(s) log_level = atoi(s);

	s = getenv("FTSH_LOG_DECIMAL");
	if(s) log_decimal = 1;

	s = getenv("FTSH_KILL_TIMEOUT");
	if(s) kill_timeout = atoi(s);
		
	s = getenv("FTSH_KILL_MODE");
	if(s) kill_mode = s;

	s = getenv("FTSH_EXPMIN");
	if(s) ftsh_expmin=atoi(s);

	s = getenv("FTSH_EXPMAX");
	if(s) ftsh_expmax=atoi(s);

	s = getenv("FTSH_EXPFACTOR");
	if(s) ftsh_expfactor=atoi(s);

	s = getenv("FTSH_EXPRAND");
	if(s) ftsh_exprand=atoi(s);


	/* Now, process the arguments and let them override the environment settings */

	while( (c=getopt(argc,argv,"+f:l:t:Dk:npPvh")) > -1 ) {
		switch(c) {
			case 'f':
				log_file = optarg;
				break;
			case 'l':
				log_level = atoi(optarg);
				break;
			case 't':
				if(getenv("FTSH_KILL_TIMEOUT")) {
					kill_timeout = MIN(kill_timeout,atoi(optarg));
				} else {
					kill_timeout = atoi(optarg);
				}
				break;
			case 'D':
				log_decimal = 1;
				break;
			case 'k':
				kill_mode = optarg;
				break;
			case 'n':
			case 'p':
				parse_mode = 1;
				break;
			case 'P':
				parse_mode = 1;
				parse_debug_mode = 1;
				break;
			case 'v':
				cctools_version_print(stderr, argv[0]);
				return 1;
				break;
			case 'h':
			default:
				show_help(argv[0]);
				return 1;
				break;
		}
	}

	cctools_version_debug(D_DEBUG, argv[0]);

	if(optind>=argc) {
		show_help(argv[0]);
		return 1;
	}

	/* Reset the environment for my children */

	sprintf(env,"FTSH_VERSION=%d.%d.%d",CCTOOLS_VERSION_MAJOR,CCTOOLS_VERSION_MINOR,CCTOOLS_VERSION_MICRO);
	putenv(xxstrdup(env));

	if(log_file) {
		sprintf(env,"FTSH_LOG_FILE=%s",log_file);
		putenv(xxstrdup(env));
	}

	sprintf(env,"FTSH_LOG_LEVEL=%d",log_level);
	putenv(xxstrdup(env));

	if(log_decimal) {
		sprintf(env,"FTSH_LOG_DECIMAL=");
		putenv(xxstrdup(env));
	}

	sprintf(env,"FTSH_KILL_TIMEOUT=%d",MAX(kill_timeout-5,0));
	putenv(xxstrdup(env));

	sprintf(env,"FTSH_KILL_MODE=%s",kill_mode);
	putenv(xxstrdup(env));

	sprintf(env,"FTSH_VERSION_MAJOR=%d",CCTOOLS_VERSION_MAJOR);
	putenv(xxstrdup(env));

	sprintf(env,"FTSH_VERSION_MINOR=%d",CCTOOLS_VERSION_MINOR);
	putenv(xxstrdup(env));

	sprintf(env,"FTSH_VERSION_MICRO=%d",CCTOOLS_VERSION_MICRO);
	putenv(xxstrdup(env));

	/* Now, initialize my systems */

	if(log_file) {
       		stream = fopen(log_file,"a");
		if(!stream) ftsh_fatal(0,"couldn't open log file %s: %s",log_file,strerror(errno));
		ftsh_error_stream(stream);
	}

	ftsh_error_name(argv[optind]);
	ftsh_error_level(log_level);
	ftsh_error_decimal_time(log_decimal);
	multi_fork_kill_timeout = kill_timeout;

	if(!strcmp(kill_mode,"weak")) {
		multi_fork_kill_mode = MULTI_FORK_KILL_MODE_WEAK;
	} else if(!strcmp(kill_mode,"strong")) {
		multi_fork_kill_mode = MULTI_FORK_KILL_MODE_STRONG;
	} else {
		ftsh_fatal(0,"The kill mode must be either 'weak' or 'strong'");
	}

	stream = fopen(argv[optind],"r");
	if(!stream) ftsh_fatal(0,"couldn't open program %s: %s",argv[optind],strerror(errno));

	result = variable_frame_push(0,argc-optind,&argv[optind]);
	if(!result) ftsh_fatal(0,"couldn't set up arguments: %s",strerror(errno));

	/* Finally, parse and execute the program. */

	g = parse_file(stream,parse_debug_mode);
	if(!g) return 1;

	if(parse_mode) {
		ast_group_print(stdout,g,-1);
		return 0;
	} else {
		if(ast_program_execute(g,0)) {
			ftsh_error(FTSH_ERROR_STRUCTURE,0,"script succeeded");
			return 0;
		} else {
			ftsh_error(FTSH_ERROR_FAILURE,0,"script failed");
			return 1;
		}
	}
}

int main( int argc, char *argv[] )
{
	/*
	If we are started from the shell, argv is as usual.
	If we get started as a #! hack, then all of the args
	are in argv[1], and the rest of the args go in argv[2..n].
	*/

	if( argc>1 && argv[1][0]=='-' && strchr(argv[1],' ') ) {
		int tmp_argc, new_argc;
		char **tmp_argv, **new_argv;
		int i;

		/* Split the first argument into pieces */
		string_split_quotes(xxstrdup(argv[1]),&tmp_argc,&tmp_argv);

		/* Allocate a new argument array */
		new_argc = tmp_argc + argc;
		new_argv = xxmalloc( sizeof(char*) * (new_argc+1) );

		/* Copy all of the pieces into the new arg array */
		new_argv[0] = argv[0];


		for( i=0; i<tmp_argc; i++ ) {
			new_argv[1+i] = tmp_argv[i];
			printf("argv[%d] = %s\n",i+1,new_argv[1+i]);
		}

		/* Add a double-dash to prevent ftsh from considering the rest as option arguments. */
		new_argv[tmp_argc+1] = "--";
		printf("argv[%d] = %s\n",tmp_argc+1,new_argv[tmp_argc+1]);

		for( i=2; i<argc; i++ ) {
			new_argv[tmp_argc+i] = argv[i];
			printf("argv[%d] = %s\n",tmp_argc+i,new_argv[tmp_argc+i]);
		}

		new_argv[new_argc] = 0;

		return ftsh_main(new_argc,new_argv);
	} else {
		return ftsh_main(argc,argv);
	}
}


/* vim: set noexpandtab tabstop=4: */
