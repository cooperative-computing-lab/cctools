/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2005- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "ftsh_error.h"

#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <sys/time.h>
#include <unistd.h>
#include <string.h>

static FILE * error_stream = 0;
static int error_level = FTSH_ERROR_FAILURE;
static const char * error_name = "unknown";
static int decimal_time = 0;

void ftsh_error_name( const char *name )
{
	error_name = name;
}

void ftsh_error_level( int level )
{
	error_level = level;
}

void ftsh_error_stream( FILE *stream )
{
	error_stream = stream;
}

void ftsh_error_decimal_time( int onoff )
{
	decimal_time = onoff;
}

static char * make_prefix( int line )
{
	static char txt[1024];
	struct timeval tv;
	char *c;

	gettimeofday(&tv,0);

	if(decimal_time) {
		sprintf(txt,"%d.%06d [%d] %s:%d",(int)tv.tv_sec,(int)tv.tv_usec,(int)getpid(),error_name,line);
	} else {
		time_t t = tv.tv_sec;
		sprintf(txt,"%s[%d] %s:%d",ctime(&t),(int)getpid(),error_name,line);
		c = strchr(txt,'\n');
		if(c) *c = ' ';
	}

	return txt;
}

static void do_error( int level, int line, const char *fmt, va_list args )
{
	if(!error_stream) {
		error_stream = stderr;
	}
	if(error_level>=level) {
		fprintf(error_stream,"%s ",make_prefix(line));
		vfprintf(error_stream,fmt,args);
		fprintf(error_stream,"\n");
		fflush(error_stream);
	}
}

void ftsh_error( int level, int line, const char *fmt, ... )
{
	va_list args;
	va_start(args,fmt);
	do_error(level,line,fmt,args);
	va_end(args);
}

void ftsh_fatal( int line, const char *fmt, ... )
{
	va_list args;
	va_start(args,fmt);
	do_error(error_level,line,fmt,args);
	exit(1);
	va_end(args);
}

/* vim: set noexpandtab tabstop=4: */
