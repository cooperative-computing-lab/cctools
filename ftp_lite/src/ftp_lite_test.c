/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include <stdio.h>
#include <errno.h>
#include <string.h>
#include <stdlib.h>

#include "copy_stream.h"
#include "debug.h"
#include "ftp_lite.h"

#define LINE_MAX 1024
#define BUFFER_SIZE 32768

static void do_open( const char *x, const char *y );
static void do_close( const char *x, const char *y );
static void do_get( const char *x, const char *y );
static void do_put( const char *x, const char *y );
static void do_ls( const char *x, const char *y );
static void do_mv( const char *x, const char *y );
static void do_rm( const char *x, const char *y );
static void do_mkdir( const char *x, const char *y );
static void do_rmdir( const char *x, const char *y );
static void do_size( const char *x, const char *y );
static void do_cd( const char *x, const char *y );
static void do_nop( const char *x, const char *y );
static void do_help( const char *x, const char *y );
static void do_quit( const char *x, const char *y );

static struct ftp_lite_server *server=0;

struct command {
	char *name;
	int must_be_open;
	int minargs, maxargs;
	char *help;
	void (*handler) ( const char *arg1, const char *arg2 );
};

static struct command list[] =
{
{"open",    0, 1, 2, "<host> [port]",            do_open},
{"close",   1, 0, 0, "",                         do_close},
{"get",	    1, 1, 2, "<remotefile> [localfile]", do_get},
{"put",	    1, 1, 2, "<localfile> [remotefile]", do_put},
{"ls",      1, 0, 1, "[remotepath]",             do_ls},
{"rename",  1, 2, 2, "<oldname> <newname>",      do_mv},
{"mv",      1, 2, 2, "<oldname> <newname>",      do_mv},
{"rm",	    1, 1, 1, "<file>",                   do_rm},
{"del",	    1, 1, 1, "<file>",                   do_rm},
{"delete",  1, 1, 1, "<file>",                   do_rm},
{"mkdir",   1, 1, 1, "<dir>",                    do_mkdir},
{"rmdir",   1, 1, 1, "<dir>",                    do_rmdir},
{"size",    1, 1, 1, "<file>",                   do_size},
{"cd",      1, 1, 1, "<path>",                   do_cd},
{"cwd",     1, 1, 1, "<path>",                   do_cd},
{"chdir",   1, 1, 1, "<path>",                   do_cd},
{"nop",     1, 0, 0, "",                         do_nop},
{"noop",    1, 0, 0, "",                         do_nop},
{"help",    0, 0, 0, "",                         do_help},
{"quit",    0, 0, 0, "",                         do_quit},
{0,0,0,0,0,0},
};

int main( int argc, char *argv[] )
{
	char line[LINE_MAX];
	char command[LINE_MAX];
	char arg1[LINE_MAX];
	char arg2[LINE_MAX];
	int fields;
	int i;

	debug_config(argv[0]);
	debug_flags_set("ftp");

	if(argc>1) {
		do_open(argv[1],argv[2]);
	}

	while(1) {
		fprintf(stdout,"ftp_lite> ");
		fflush(stdout);

		if(!fgets(line,LINE_MAX,stdin)) break;

		command[0] = 0;
		arg1[0] = 0;
		arg2[0] = 0;

		fields = sscanf(line,"%s %s %s",command,arg1,arg2);
		if(fields==0) continue;

		for(i=0;list[i].name;i++) {
			if(!strcmp(list[i].name,command)) {
				if(!server && list[i].must_be_open) {
					printf("not connected\n");
					break;
				} else if( ((fields-1)>=list[i].minargs) && ((fields-1)<=list[i].maxargs ) ) {
					list[i].handler(arg1,arg2);
					break;
				} else {
					printf("use: %s %s\n",command,list[i].help);
					break;
				}
			}
		}

		if(!list[i].name) {
			printf("unknown command: %s  (try 'help')\n",command);
		}
	}

	return 0;
}

static void do_open( const char *host, const char *textport )
{
	int port;

	if(textport) {
		port = atoi(textport);
	} else {
		port = 0;
	}

	server = ftp_lite_open_and_auth(host,port);
	if(!server) {
		printf("couldn't login to %s: %s\n",host,strerror(errno));
		return;
	}

	printf("connected to %s\n",host);
}

static void do_close( const char *x, const char *y )
{
	if(!server) {
		printf("not connected\n");
	} else {
		ftp_lite_close(server);
		printf("disconnected\n");
		server = 0;
	}
}

static void do_get( const char *rfile, const char *lfile )
{
	FILE *file, *data;
	ftp_lite_size_t length;

	if(!lfile[0]) lfile=rfile;

	file = fopen(lfile,"wb+");
	if(!file) {
		printf("couldn't open local file %s: %s\n",lfile,strerror(errno));
		return;
	}

	data = ftp_lite_get(server,rfile,0);
	if(!data) {
		printf("couldn't open remote file %s: %s\n",rfile,strerror(errno));
		fclose(file);
		return;
	}

	length = copy_stream_to_stream(data,file);
	if(length>=0) {
		printf("got %lld bytes\n",length);
	} else {
		printf("couldn't get file: %s\n",strerror(errno));
	}
	fclose(data);
	ftp_lite_done(server);
	fclose(file);
}

static void do_put( const char *lfile, const char *rfile )
{
	FILE *file, *data;
	ftp_lite_size_t length;

	if(!rfile[0]) rfile = lfile;

	file = fopen(lfile,"rb");
	if(!file) {
		printf("couldn't open local file %s: %s\n",lfile,strerror(errno));
		return;
	}

	data = ftp_lite_put(server,rfile,0,FTP_LITE_WHOLE_FILE);
	if(!data) {
		printf("couldn't open remote file %s: %s\n",rfile,strerror(errno));
		fclose(file);
		return;
	}

	length = copy_stream_to_stream(file,data);
	if(length>=0) {
		printf("put %lld bytes\n",length);
	} else {
		printf("couldn't put file: %s\n",strerror(errno));
	}
	fclose(data);
	ftp_lite_done(server);
	fclose(file);
}

static void do_ls( const char *path, const char *y )
{
	FILE *data;
	ftp_lite_size_t length;

	if(!path[0]) path = ".";

	data = ftp_lite_list(server,path);
	if(!data) {
		printf("couldn't open remote path %s: %s\n",path,strerror(errno));
		return;
	}

	length = copy_stream_to_stream(data,stdout);
	if(length<0) {
		printf("couldn't read list: %s\n",strerror(errno));
	}
	fclose(data);
	ftp_lite_done(server);
}

static void do_rm( const char *path, const char *x )
{
	if(!ftp_lite_delete(server,path)) {
		printf("couldn't delete %s: %s\n",path,strerror(errno));
	} else {
		printf("deleted %s\n",path);
	}
}

static void do_rmdir( const char *path, const char *x )
{
	if(!ftp_lite_delete_dir(server,path)) {
		printf("couldn't delete %s: %s\n",path,strerror(errno));
	} else {
		printf("deleted %s\n",path);
	}
}

static void do_mkdir( const char *path, const char *x )
{
	if(!ftp_lite_make_dir(server,path)) {
		printf("couldn't create %s: %s\n",path,strerror(errno));
	} else {
		printf("created %s\n",path);
	}
}

static void do_mv( const char *oldpath, const char *newpath )
{
	if(!ftp_lite_rename(server,oldpath,newpath)) {
		printf("couldn't rename: %s\n",strerror(errno));
	} else {
		printf("renamed %s to %s\n",oldpath,newpath);
	}
}

static void do_cd( const char *path, const char *x )
{
	if(!ftp_lite_change_dir(server,path)) {
		printf("couldn't change dir: %s\n",strerror(errno));
	} else {
		printf("current dir is now %s\n",path);
	}
}

static void do_nop( const char *path, const char *x )
{
	if(!ftp_lite_nop(server)) {
		printf("couldn't nop: %s\n",strerror(errno));
	} else {
		printf("nop successful\n");
	}
}

static void do_size( const char *path, const char *x )
{
	ftp_lite_size_t size;

	size = ftp_lite_size(server,path);
	if(size<0) {
		printf("couldn't examine %s: %s\n",path,strerror(errno));
	} else {
		printf("%s is %lld bytes\n",path,(long long)size);
	}
}

static void do_quit( const char *x, const char *y )
{
	exit(0);
}

static void do_help( const char *x, const char *y )
{
	int i;

	printf("Available commands:\n");
	for(i=0;list[i].name;i++) {
		printf("%s\t%s\n",list[i].name,list[i].help);
	}
}


/* vim: set noexpandtab tabstop=8: */
