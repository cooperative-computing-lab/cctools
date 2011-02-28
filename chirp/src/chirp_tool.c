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
#include <termios.h>
#include <unistd.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <ctype.h>

#include "chirp_reli.h"
#include "chirp_recursive.h"
#include "chirp_protocol.h"
#include "chirp_acl.h"
#include "chirp_group.h"
#include "chirp_matrix.h"

#include "timestamp.h"
#include "debug.h"
#include "auth_all.h"
#include "stringtools.h"
#include "xmalloc.h"
#include "list.h"
#include "domain_name_cache.h"
#include "md5.h"

#ifdef HAS_LIBREADLINE
#include "readline/readline.h"
#include "readline/history.h"
#endif

#if CCTOOLS_OPSYS_CYGWIN || CCTOOLS_OPSYS_DARWIN || CCTOOLS_OPSYS_FREEBSD
#define fopen64 fopen
#define open64 open
#define lseek64 lseek
#define stat64 stat
#define fstat64 fstat
#define lstat64 lstat
#define fseeko64 fseeko
#endif

#define BUFFER_SIZE 32768

static INT64_T do_open( 	int argc, char **argv );
static INT64_T do_close( 	int argc, char **argv );
static INT64_T do_get( 	int argc, char **argv );
static INT64_T do_put( 	int argc, char **argv );
static INT64_T do_cat(      int argc, char **argv );
static INT64_T do_cd( 	int argc, char **argv );
static INT64_T do_lcd( 	int argc, char **argv );
static INT64_T do_pwd( 	int argc, char **argv );
static INT64_T do_lpwd( 	int argc, char **argv );
static INT64_T do_getacl( 	int argc, char **argv );
static INT64_T do_setacl( 	int argc, char **argv );
static INT64_T do_resetacl( int argc, char **argv );
static INT64_T do_ls( 	int argc, char **argv );
static INT64_T do_mv( 	int argc, char **argv );
static INT64_T do_rm( 	int argc, char **argv );
static INT64_T do_mkdir( 	int argc, char **argv );
static INT64_T do_rmdir( 	int argc, char **argv );
static INT64_T do_stat( 	int argc, char **argv );
static INT64_T do_statfs( 	int argc, char **argv );
static INT64_T do_chmod(	int argc, char **argv );
static INT64_T do_timeout( 	int argc, char **argv );
static INT64_T do_debug( 	int argc, char **argv );
static INT64_T do_help( 	int argc, char **argv );
static INT64_T do_quit( 	int argc, char **argv );
static INT64_T do_whoami( 	 int argc, char **argv );
static INT64_T do_whoareyou( int argc, char **argv );
static INT64_T do_job_run( int argc, char **argv );
static INT64_T do_job_start( int argc, char **argv );
static INT64_T do_job_exec(  int argc, char **argv );
static INT64_T do_job_wait(  int argc, char **argv );
static INT64_T do_job_list(  int argc, char **argv );
static INT64_T do_job_kill(  int argc, char **argv );
static INT64_T do_job_remove(int argc, char **argv );
static INT64_T do_md5( 	 int argc, char **argv );
static INT64_T do_localpath(  int argc, char **argv );
static INT64_T do_audit( 	 int argc, char **argv );
static INT64_T do_thirdput(  int argc, char **argv );
static INT64_T do_mkalloc(   int argc, char **argv );
static INT64_T do_lsalloc(   int argc, char **argv );
static INT64_T do_group_create(     int argc, char **argv );
static INT64_T do_group_add(        int argc, char **argv);
static INT64_T do_group_remove(     int argc, char **argv);
static INT64_T do_group_list(       int argc, char** argv);
static INT64_T do_group_policy( int argc, char** argv);
static INT64_T do_matrix_create(    int argc, char **argv );
static INT64_T do_matrix_list(      int argc, char **argv );
static INT64_T do_matrix_delete(    int argc, char **argv );

static int process_command( int argc, char **argv );

static int timeout=3600;
static time_t stoptime=0;
static char current_host[CHIRP_PATH_MAX] = {0};
static char current_local_dir[CHIRP_PATH_MAX];
static char current_remote_dir[CHIRP_PATH_MAX];
int interactive_mode=0;

static int long_information = 0;

struct command {
	char *name;
	int must_be_open;
	int minargs, maxargs;
	char *help;
	INT64_T (*handler) ( int argc, char **argv);
};

static struct command list[] =
{
{"open",    0, 1, 1, "<host>",                   do_open},
{"close",   1, 0, 0, "",                         do_close},
{"whoami",  1, 0, 0, "",                         do_whoami},
{"whoareyou",1, 1, 1, "<hostname>", do_whoareyou},
{"cat",     1, 1, 100, "<file> [file2] [file3] ...", do_cat},
{"cd",      1, 1, 1, "<remotedir>",              do_cd},
{"lcd",     0, 1, 1, "<localdir>",               do_lcd},
{"pwd",     1, 0, 0, "",                         do_pwd},
{"lpwd",    0, 0, 0, "",                         do_lpwd},
{"get",	    1, 1, 2, "<remotefile> [localfile]", do_get},
{"put",	    1, 1, 2, "<localfile> [remotefile]", do_put},
{"thirdput",1, 3, 3, "<file> <3rdhost> <3rdfile>",do_thirdput},
{"getacl",  1, 0, 1, "[remotepath]",             do_getacl},
{"listacl", 1, 0, 1, "[remotepath]",             do_getacl},
{"setacl",  1, 3, 3, "<remotepath> <user> <rwldax>",do_setacl},
{"resetacl",1, 2, 2, "<remotepath> <rwldax>",do_resetacl},
{"ls",      1, 0, 2, "[-la] [remotepath]",       do_ls},
{"mv",      1, 2, 2, "<oldname> <newname>",      do_mv},
{"rm",	    1, 1, 1, "<file>",                   do_rm},
{"mkdir",   1, 1, 2, "[-p] <dir>",               do_mkdir},
{"rmdir",   1, 1, 1, "<dir>",                    do_rmdir},
{"stat",    1, 1, 1, "<file>",                   do_stat},
{"df",      1, 0, 1, "[-k|-m|-g|-t]",         	 do_statfs},
{"chmod",   1, 2, 2, "<mode> <path>",		 do_chmod},
{"md5",     1, 1, 1,  "<path>",                  do_md5},
{"localpath",  1, 0, 1, "[remotepath]",          do_localpath},
{"audit",   1, 0, 1,  "[-r]",                    do_audit},
{"lsalloc", 1, 0, 1, "[path]",                   do_lsalloc},
{"mkalloc", 1, 2, 2, "<path> <size>",            do_mkalloc},
{"job_run", 1, 1, 100, "<command> [args] [<stdin] [>stdout] [>&stderr]", do_job_run},
{"job_exec",1, 4, 100, "<infile> <outfile> <errfile> <command> [args]",do_job_exec},
{"job_start",1, 4, 100, "<infile> <outfile> <errfile> <command> [args]",do_job_start},
{"job_list",1, 0, 0,  "",                        do_job_list},
{"job_wait",1, 1, 1,  "<jobid>",                  do_job_wait},
{"job_kill",1, 1, 1,  "<jobid>",                 do_job_kill},
{"job_remove",1, 1, 1,  "<jobid>",               do_job_remove},
{"group_create", 1, 1, 1,  "<groupname>", do_group_create},
{"group_add", 1, 2, 2,  "<groupname> <username>", do_group_add},
{"group_remove", 1, 2, 2,  "<groupname> <username>", do_group_remove},
{"group_list", 1, 1, 1,  "<groupname>", do_group_list},
{"group_policy", 1, 1, 5, "-f <file cache duration> -d <decision cache duration> <groupname>", do_group_policy},
{"matrix_create", 1, 4, 4, "<path> <width> <height> <nhosts>", do_matrix_create},
{"matrix_list",   1, 1, 1, "<path>", do_matrix_list},
{"matrix_delete", 1, 1, 1, "<path>", do_matrix_delete},
{"timeout", 0, 1, 1, "<seconds>",                do_timeout},
{"debug",   0, 0, 1, "[subsystem]",              do_debug},
{"help",    0, 0, 0, "",                         do_help},
{"exit",    0, 0, 0, "",                         do_quit},
{"quit",    0, 0, 0, "",                         do_quit},
{0,0,0,0,0},
};

static void show_version( const char *cmd )
{
	printf("%s version %d.%d.%d built by %s@%s on %s at %s\n",cmd,CCTOOLS_VERSION_MAJOR,CCTOOLS_VERSION_MINOR,CCTOOLS_VERSION_MICRO,BUILD_USER,BUILD_HOST,__DATE__,__TIME__);
}

static void show_help( const char *cmd )
{
	printf("use: %s [options] [hostname] [command]\n",cmd);
	printf("where options are:\n");
	printf(" -a <flag>  Require this authentication mode.\n");
	printf(" -d <flag>  Enable debugging for this subsystem.\n");
	printf(" -q         Quiet mode; supress messages and table headers.\n");
	printf(" -t <time>  Set remote operation timeout.\n");
	printf(" -v         Show program version.\n");
	printf(" -h         This message.\n");
	printf(" -l         Long transfer information.\n");
}
 

int main( int argc, char *argv[] )
{	
	char *temp;
	int did_explicit_auth = 0;
	char prompt[CHIRP_LINE_MAX];
	char line[CHIRP_LINE_MAX];
	char **user_argv=0;
	int user_argc;
	char c;
	int result = 0;

	debug_config(argv[0]);

	while((c=getopt(argc,argv,"+a:d:vhlt:"))!=(char)-1) {
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
		        case 'l':
			        long_information = 1;
				break;

		}
	}
 
	if(!did_explicit_auth) auth_register_all();
	getcwd(current_local_dir,CHIRP_PATH_MAX);

	interactive_mode = isatty(0);

	if(optind<argc) {
		stoptime = time(0) + timeout;
		if(do_open(1,&argv[optind-1])) {
			fprintf(stderr,"couldn't open %s: %s\n",argv[optind],strerror(errno));
			return 1;
		}
	}

	if((argc-optind)>1) {
		return !process_command(argc-optind-1,&argv[optind+1]);
	}

	while(1) {
		if(interactive_mode) {
			sprintf(prompt," chirp:%s:%s> ",current_host,current_remote_dir);
		} else {
			prompt[0] = 0;
		}

#ifdef HAS_LIBREADLINE
		temp = readline(prompt);		
		if(!temp) break;
		strcpy(line,temp);
		free(temp);
#else
		printf("%s",prompt);
		fflush(stdout);
		if(!fgets(line,CHIRP_LINE_MAX,stdin)) break;
#endif

		if(!line[0]) continue;

        if (!interactive_mode && (temp = strchr(line, '#'))) { /* comment? */
			for (temp--; temp > line && isspace((int)*temp); temp--)
              ; /* preceding space? */
			if (temp <= line) continue; /* else not comment */
		}

#ifdef HAS_LIBREADLINE
		add_history(line);
#endif

		{
			char *start = line, *last = strlen(line)+line;
			while (*start != '\0') { /* process compound commands */
				char *end = strchr(start, ';');
				while (end != NULL && end != start && *(end-1) == '\\')
					end = strchr(end+1, ';');
				if (end == NULL) end = start+strlen(start);
                *end = '\0';

				if(user_argv) free(user_argv);
				string_split(start,&user_argc,&user_argv);
				if(user_argc==0) {
					start++;
					continue;
				}
				result = process_command(user_argc,user_argv);
				start = end == last ? last : end+1;
			}
		}
		if(!interactive_mode && !result) break;
	}

	if(result) {
		return 0;
	} else {
		return 1;
	}
}

static int process_command( int argc, char **argv )
{
	int i;

	for(i=0;list[i].name;i++) {
		if(!strcmp(list[i].name,argv[0])) {
			if(!current_host[0] && list[i].must_be_open) {
				printf("not connected\n");
				return 0;
			} else if( ((argc-1)>=list[i].minargs) && ((argc-1)<=list[i].maxargs ) ) {
				stoptime = time(0)+timeout;
				if(list[i].handler(argc,argv)<0) {
					fprintf(stderr,"couldn't %s: %s\n",argv[0],strerror(errno));
					return 0;
				} else {
					return 1;
				}
			} else {
				printf("use: %s %s\n",argv[0],list[i].help);
				return 0;
			}
		}
	}

	printf("unknown command: %s  (try 'help')\n",argv[0]);
	return 0;
}

static INT64_T do_open( int argc, char **argv )
{
	char subject[CHIRP_LINE_MAX];

	do_close(0,0);

	if(chirp_reli_whoami(argv[1],subject,sizeof(subject),stoptime)>=0) {
		strcpy(current_host,argv[1]);
		strcpy(current_remote_dir,"/");
		if(interactive_mode) printf("connected to %s as %s\n",current_host,subject);
		return 0;
	} else {
		return -1;
	}
}

static INT64_T do_close( int argc, char **argv )
{
	current_host[0] = 0;
	current_remote_dir[0] = 0;
	return 0;
}

static void complete_local_path( const char *file, char *full_path )
{
	char temp[CHIRP_PATH_MAX];
	if(file[0]!='/') {
		sprintf(temp,"%s/%s",current_local_dir,file);
	} else {
		strcpy(temp,file);
	}
	string_collapse_path(temp,full_path,1);
}

static void complete_remote_path( const char *file, char *full_path )
{
	char temp[CHIRP_PATH_MAX];
	if(file[0]!='/') {
		sprintf(temp,"%s/%s",current_remote_dir,file);
	} else {
		strcpy(temp,file);
	}
	string_collapse_path(temp,full_path,1);
}

static INT64_T do_cat( int argc, char **argv )
{
	INT64_T actual;
	int i;

	for(i=1;i<argc;i++) {
		char full_path[CHIRP_PATH_MAX];
		complete_remote_path(argv[i],full_path);
		actual = chirp_reli_getfile(current_host,full_path,stdout,stoptime);
		if(actual<0) {
			fprintf(stderr,"%s: %s\n",full_path,strerror(errno));
		}
	}

	return 0;
}

static INT64_T do_cd( int argc, char **argv )
{
	char full_path[CHIRP_PATH_MAX];
	struct chirp_stat info;
	complete_remote_path(argv[1],full_path);
	if(chirp_reli_stat(current_host,full_path,&info,stoptime)<0) {
		return -1;
	} else {
		if(S_ISDIR(info.cst_mode)) {
			string_collapse_path(full_path,current_remote_dir,1);
			return 0;
		} else {
			errno = ENOTDIR;
			return -1;
		}
	}
}

static INT64_T do_lcd( int argc, char **argv )
{
	char full_path[CHIRP_PATH_MAX];
	complete_local_path(argv[1],full_path);
	if(chdir(full_path)==0) {
		getcwd(current_local_dir,CHIRP_PATH_MAX);
		return 0;
	} else {
		return -1;
	}
}

static INT64_T do_pwd( int argc, char **argv )
{
	printf("%s\n",current_remote_dir);
	return 0;
}

static INT64_T do_lpwd( int argc, char **argv )
{
	printf("%s\n",current_local_dir);
	return 0;
}

static INT64_T do_get( int argc, char **argv )
{
	char target_full_path[CHIRP_PATH_MAX];
	char source_full_path[CHIRP_PATH_MAX];
	timestamp_t start, stop;
	double elapsed;
	INT64_T result;

	if(!argv[2]) argv[2]=(char*)string_basename(argv[1]);

	complete_remote_path(argv[1],source_full_path);
	complete_local_path(argv[2],target_full_path);

	start = timestamp_get();
	result = chirp_recursive_get(current_host,source_full_path,target_full_path,stoptime);
	stop = timestamp_get();

	elapsed = (stop-start)/1000000.0;

	if(result>0) {
		printf("%sB read in %.2fs ",string_metric(result,-1,0),elapsed);
		printf("(%sB/s)\n",string_metric(result/elapsed,-1,0));
	}

	return result;
}

static INT64_T do_put( int argc, char **argv )
{
	char target_full_path[CHIRP_PATH_MAX];
	char source_full_path[CHIRP_PATH_MAX];
	timestamp_t start, stop;
	double elapsed;
	INT64_T result;

	if(!argv[2]) argv[2]=(char*)string_basename(argv[1]);

	complete_local_path(argv[1],source_full_path);
	complete_remote_path(argv[2],target_full_path);

	start = timestamp_get();
	result = chirp_recursive_put(current_host,source_full_path,target_full_path,stoptime);
	stop = timestamp_get();

	elapsed = (stop-start)/1000000.0;

	if(result>0) {
		printf("%sB written in %.2fs ",string_metric(result,-1,0),elapsed);
		printf("(%sB/s)\n",string_metric(result/elapsed,-1,0));
	}

	return result;
}

static INT64_T do_setacl( int argc, char **argv )
{
	char full_path[CHIRP_PATH_MAX];
	complete_remote_path(argv[1],full_path);

	if(!strcmp(argv[3],"read")) argv[3] = "rl";
	if(!strcmp(argv[3],"write")) argv[3] = "rwld";
	if(!strcmp(argv[3],"admin")) argv[3] = "rwldva";
	if(!strcmp(argv[3],"reserve")) argv[3] = "lv";
	if(!strcmp(argv[3],"none")) argv[3] = ".";

	return chirp_reli_setacl(current_host,full_path,argv[2],argv[3],stoptime);
}

static INT64_T do_resetacl( int argc, char **argv )
{
	char full_path[CHIRP_PATH_MAX];
	complete_remote_path(argv[1],full_path);

	if(!strcmp(argv[2],"read")) argv[2] = "rl";
	if(!strcmp(argv[2],"write")) argv[2] = "rwld";
	if(!strcmp(argv[2],"admin")) argv[2] = "rwldva";
	if(!strcmp(argv[2],"reserve")) argv[2] = "lv";
	if(!strcmp(argv[2],"none")) argv[2] = ".";

	return chirp_reli_resetacl(current_host,full_path,argv[2],stoptime);
}

static void print_one_acl( const char *line, void *stream )
{
	fprintf(stream,"%s\n",line);
}

static INT64_T do_getacl( int argc, char **argv )
{
	char full_path[CHIRP_PATH_MAX];

	if(!argv[1]) argv[1] = ".";
	complete_remote_path(argv[1],full_path);

	return chirp_reli_getacl(current_host,full_path,print_one_acl,stdout,stoptime);
}

static int ls_all_mode = 0;

static void long_ls_callback( const char *name, struct chirp_stat *info, void *arg )
{
	char timestr[1024];

	if(name[0]=='.' && !ls_all_mode) return;

	time_t t = info->cst_ctime;
	time_t current = time(0);

	if((current-t)>(60*60*24*365)) {
		strftime(timestr,sizeof(timestr),"%b %d  %Y",localtime(&t));
	} else {
		strftime(timestr,sizeof(timestr),"%b %d %H:%M",localtime(&t));
	}

	printf("%c%c%c%c%c%c%c%c%c%c %4lld %8lld %8lld %8lld %s %s\n",
		S_ISDIR(info->cst_mode) ? 'd' : '-',
		info->cst_mode & 0400 ? 'r' : '-',
		info->cst_mode & 0200 ? 'w' : '-',
		info->cst_mode & 0100 ? 'x' : '-',
		info->cst_mode & 0040 ? 'r' : '-',
		info->cst_mode & 0020 ? 'w' : '-',
		info->cst_mode & 0010 ? 'x' : '-',
		info->cst_mode & 0004 ? 'r' : '-',
		info->cst_mode & 0002 ? 'w' : '-',
		info->cst_mode & 0001 ? 'x' : '-',
		info->cst_nlink,
		info->cst_uid,
		info->cst_gid,
		info->cst_size,
		timestr,
		name);
}

static void ls_callback( const char *name, void *arg )
{
	if(name[0]=='.' && !ls_all_mode) return;
	printf("%s\n",name);
}

static INT64_T do_ls( int argc, char **argv )
{
	char full_path[CHIRP_PATH_MAX];
	int long_mode = 0;

	const char *options = argv[1];
	const char *file = argv[2];

	if(!options) options="";
	if(!file) file="";

	if(options[0]!='-') {
		file = options;
		options = "-";
	}

	ls_all_mode = 0;

	options++;
	while(*options) {
		switch(*options) {
			case 'l':
				long_mode = 1;
				break;
			case 'a':
				ls_all_mode = 1;
				break;
			default:
				printf("ls: unknown option: %c\n",*options);
				errno = EINVAL;
				return -1;
		}
		options++;
	}

	if(!file[0]) file = ".";
	complete_remote_path(file,full_path);

	struct chirp_dir *dir;
	struct chirp_dirent *d;

	dir = chirp_reli_opendir(current_host,full_path,stoptime);
	if(dir) {
		while((d=chirp_reli_readdir(dir))) {
			if(long_mode) {
				long_ls_callback(d->name,&d->info,0);
			} else {
				ls_callback(d->name,0);
			}
		}
		chirp_reli_closedir(dir);
		return 0;
	} else {
		return -1;
	}
}

static INT64_T do_rm( int argc, char **argv )
{
	char full_path[CHIRP_PATH_MAX];
	complete_remote_path(argv[1],full_path);
	return chirp_reli_rmall(current_host,full_path,stoptime);
}

static INT64_T do_rmdir( int argc, char **argv )
{
	char full_path[CHIRP_PATH_MAX];
	complete_remote_path(argv[1],full_path);
	return chirp_reli_rmdir(current_host,full_path,stoptime);
}

static INT64_T do_mkdir( int argc, char **argv )
{
	char full_path[CHIRP_PATH_MAX];
	int result;
	int create_parents = (argc == 3 && strcmp(argv[1], "-p") == 0);

	if (create_parents) {
		complete_remote_path(argv[2],full_path);
		result = chirp_reli_mkdir_recursive(current_host,full_path,0777,stoptime);
	}
	else {
		complete_remote_path(argv[1],full_path);
		result = chirp_reli_mkdir(current_host,full_path,0777,stoptime);
	}

	if(result<0 && errno==EEXIST) result = 0;

	return result;
}

static INT64_T do_stat( int argc, char **argv )
{
	char full_path[CHIRP_PATH_MAX];
	struct chirp_stat info;
	time_t t;

	complete_remote_path(argv[1],full_path);

	if(chirp_reli_stat(current_host,full_path,&info,stoptime)<0) {
		return -1;
	} else {
		printf("device:  %lld\n",info.cst_dev);
		printf("inode:   %lld\n",info.cst_ino);
		printf("mode:    %04llo\n",info.cst_mode);
		printf("nlink:   %lld\n",info.cst_nlink);
		printf("uid:     %lld\n",info.cst_uid);
		printf("gid:     %lld\n",info.cst_gid);
		printf("rdevice: %lld\n",info.cst_rdev);
		printf("size:    %lld\n",info.cst_size);
		printf("blksize: %lld\n",info.cst_blksize);
		printf("blocks:  %lld\n",info.cst_blocks);
		t = info.cst_atime;
		printf("atime:   %s",ctime(&t));
		t = info.cst_mtime;
		printf("mtime:   %s",ctime(&t));
		t = info.cst_ctime;
		printf("ctime:   %s",ctime(&t));
		return 0;
	}
 }

static INT64_T do_statfs( int argc, char **argv )
{
	struct chirp_statfs info;
	int metric_power = -1;

	if(argc>1) {
		if(!strcmp(argv[1],"-k")) {
			metric_power = 1;
		} else if(!strcmp(argv[1],"-m")) {
			metric_power = 2;
		} else if(!strcmp(argv[1],"-g")) {
			metric_power = 3;
		} else if(!strcmp(argv[1],"-t")) {
			metric_power = 4;
		} else {
			errno = EINVAL;
			return -1;
		}
	}

	if(chirp_reli_statfs(current_host,"/",&info,stoptime)<0) {
		return -1;
	} else {
		printf("/\n");
		printf("%sB TOTAL\n",string_metric(info.f_blocks*info.f_bsize,metric_power,0));
		printf("%sB INUSE\n",string_metric((info.f_blocks-info.f_bfree)*info.f_bsize,metric_power,0));
		return 0;
	}
}

static INT64_T do_mv( int argc, char **argv )
{
	char old_full_path[CHIRP_PATH_MAX];
	char new_full_path[CHIRP_PATH_MAX];
	complete_remote_path(argv[1],old_full_path);
	complete_remote_path(argv[2],new_full_path);
	return chirp_reli_rename(current_host,old_full_path,new_full_path,stoptime);
}

static INT64_T do_chmod( int argc, char **argv )
{
	char full_path[CHIRP_PATH_MAX];
	unsigned mode;
	sscanf(argv[1],"%o",&mode);
	complete_remote_path(argv[2],full_path);
	return chirp_reli_chmod(current_host,full_path,mode,stoptime);
}

static INT64_T do_debug( int argc, char **argv )
{
	if(argv[1]) {
		if(debug_flags_set(argv[1])) {
			return 0;
		} else {
			printf("Valid debugging flags are: ");
			debug_flags_print(stdout);
			printf("\n");
			errno = EINVAL;
			return -1;
		}
	} else {
		debug_flags_clear();
		return 0;
	}	
	
}

static INT64_T do_whoami( int argc, char **argv )
{
	char name[CHIRP_LINE_MAX];
	int result;

	result = chirp_reli_whoami(current_host,name,sizeof(name),stoptime);
	if(result>=0) {
		name[result] = 0;
		printf("%s\n",name);
	}
	return result;
}

static INT64_T do_whoareyou( int argc, char **argv )
{
	char name[CHIRP_LINE_MAX];
	int result;

	result = chirp_reli_whoareyou(current_host,argv[1],name,sizeof(name),
					stoptime);
	if(result>=0) {
		name[result] = 0;
		printf("%s\n",name);
	}
	return result;
}

static INT64_T last_job_started = 0;

static INT64_T do_job_start( int argc, char **argv )
{
	char infile[CHIRP_LINE_MAX];
	char outfile[CHIRP_LINE_MAX];
	char errfile[CHIRP_LINE_MAX];
	char cmd[CHIRP_LINE_MAX];
	int i;
	INT64_T jobid;

	if(!strcmp(argv[1],"-")) {
		strcpy(infile,argv[1]);
	} else {
		complete_remote_path(argv[1],infile);
	}

	if(!strcmp(argv[2],"-")) {
		strcpy(outfile,argv[2]);
	} else {
		complete_remote_path(argv[2],outfile);
	}

	if(!strcmp(argv[3],"-")) {
		strcpy(errfile,argv[3]);
	} else {
		complete_remote_path(argv[3],errfile);
	}

	if(argv[4][0]=='@') {
		strcpy(cmd,argv[4]);
	} else {
		complete_remote_path(argv[4],cmd);
	}

	for(i=5;i<argc;i++) {
		strcat(cmd," ");
		strcat(cmd,argv[i]);
	}

	int result = 1;

	jobid = chirp_reli_job_begin(current_host,current_remote_dir,infile,outfile,errfile,cmd,stoptime);
	if(jobid>0) {
		printf("jobid %lld created\n",jobid);
		if(chirp_reli_job_commit(current_host,jobid,stoptime)==0) {
			printf("jobid %lld submitted.\n",jobid);
			last_job_started = jobid;
			result = 0;
		} else {
			printf("jobid %lld could not be started: %s\n",jobid,strerror(errno));
		}
	} else {
		printf("job could not be created: %s\n",strerror(errno));
	}

	return result;
}

static INT64_T do_job_wait_jobid( INT64_T jobid )
{
	struct chirp_job_state status;

	while(1) {
		if(chirp_reli_job_wait(current_host,jobid,&status,300,stoptime)==0) {
			switch(status.state) {
				case CHIRP_JOB_STATE_BEGIN:
				case CHIRP_JOB_STATE_IDLE:
				case CHIRP_JOB_STATE_RUNNING:
				case CHIRP_JOB_STATE_SUSPENDED:
					continue;
				
			case CHIRP_JOB_STATE_COMPLETE:
					printf("jobid %lld completed with exit code %d\n",jobid,status.exit_code);
					break;

				case CHIRP_JOB_STATE_KILLED:
				case CHIRP_JOB_STATE_FAILED:
				default:
					printf("jobid %lld %s\n",jobid,chirp_job_state_string(status.state));
					break;
			}
			chirp_reli_job_remove(current_host,jobid,stoptime);
			printf("jobid %lld removed.\n",jobid);
			break;
		} else {
			printf("jobid %lld error: %s\n",jobid,strerror(errno));
			break;
		}
	}
	return 0;
}

static INT64_T do_job_wait( int argc, char **argv )
{
	return do_job_wait_jobid(atoll(argv[1]));
}

static INT64_T do_job_exec( int argc, char **argv )
{
	if(do_job_start(argc,argv)==0) {
		do_job_wait_jobid(last_job_started);
	}
	return 0;
}

static INT64_T do_job_kill( int argc, char **argv )
{
	return chirp_reli_job_kill(current_host,atoll(argv[1]),stoptime);
}

static INT64_T do_job_remove( int argc, char **argv )
{
	return chirp_reli_job_remove(current_host,atoll(argv[1]),stoptime);
}

static void job_list_callback( struct chirp_job_state *state, void *arg )
{
	char submit_date[16];
	char submit_time[16];
	int run_time = 0;

	strftime(submit_date,sizeof(submit_date),"%m/%d/%Y",localtime(&state->submit_time));
	strftime(submit_time,sizeof(submit_time),"%T",localtime(&state->submit_time));

	if(state->start_time>0) {
		if(state->stop_time>0) {
			run_time = state->stop_time-state->start_time;
		} else {
			run_time = time(0)-state->start_time;
		}
	} else {
		run_time = 0;
	}

	if(run_time<0) run_time = 0;

	int hours = run_time/3600;
	int minutes = (run_time%3600)/60;
	int seconds = (run_time%60);

	printf("%-8lld %10s %8s %02d:%02d:%02d %-8s %3d %-16s %s\n",state->jobid,submit_date,submit_time,hours,minutes,seconds,chirp_job_state_string(state->state),state->exit_code,state->owner,state->command);
}

static INT64_T do_job_list( int argc, char ** argv )
{
	printf("JOBID    SUB_DATE   SUB_TIME RUNTIME  STATE   EXIT OWNER            COMMAND\n");
	return chirp_reli_job_list(current_host,job_list_callback,stdout,stoptime);
}

static INT64_T do_job_run( int argc, char ** argv )
{
	char infile[CHIRP_LINE_MAX];
	char outfile[CHIRP_LINE_MAX];
	char errfile[CHIRP_LINE_MAX];
	char cmd[CHIRP_LINE_MAX];
	char args[CHIRP_LINE_MAX];
	char *newargv[6];
	int i;

	strcpy(infile,"-");
	strcpy(outfile,"stdout.txt");
	strcpy(errfile,"stderr.txt");
	strcpy(cmd,argv[1]);
	args[0] = 0;

	for(i=2;i<argc;i++) {
		if(argv[i][0]=='<') {
			strcpy(infile,&argv[i][1]);
		} else if(argv[i][0]=='>') {
			if(argv[i][1]=='&') {
				strcpy(errfile,&argv[i][2]);
			} else {
				strcpy(outfile,&argv[i][1]);
			}
		} else {
			strcat(args,argv[i]);
			strcat(args," ");
		}
	}

	newargv[1] = infile;
	newargv[2] = outfile;
	newargv[3] = errfile;
	newargv[4] = cmd;
	newargv[5] = args;

	do_job_exec(6,newargv);
	
	if(!strcmp(outfile,"stdout.txt")) {
		char full_path[CHIRP_PATH_MAX];
		complete_remote_path(outfile,full_path);
		chirp_reli_getfile(current_host,full_path,stdout,stoptime);
	}

	if(!strcmp(errfile,"stderr.txt")) {
		char full_path[CHIRP_PATH_MAX];
		complete_remote_path(errfile,full_path);
		chirp_reli_getfile(current_host,full_path,stdout,stoptime);
	}

	return 0;
}

static INT64_T do_md5( int argc, char **argv )
{
	unsigned char digest[16];
	char full_path[CHIRP_LINE_MAX];
	int result;

	complete_remote_path(argv[1],full_path);

	result = chirp_reli_md5(current_host,full_path,digest,stoptime);
	if(result>0) printf("%s %s\n",md5_string(digest),full_path);
	return result;
}

static INT64_T do_localpath( int argc, char **argv )
{
	char localpath[CHIRP_LINE_MAX];
	char full_path[CHIRP_LINE_MAX];
	int result;

	if(argc==2) {
		complete_remote_path(argv[1],full_path);
	} else {
		complete_remote_path(".",full_path);
	}
	
	result = chirp_reli_localpath(current_host,full_path,localpath,sizeof(localpath),stoptime);
	if(result>=0) {
		localpath[result] = 0;
		printf("%s\n",localpath);
	}
	return result;
}

static INT64_T do_audit( int argc, char **argv )
{
	struct chirp_audit *list;
	int result;
	int i;
	int raw_mode=0;
	
	if(argc>1) {
		if(!strcmp(argv[1],"-r")) {
			raw_mode = 1;
		} else {
			printf("audit: unknown option: %s\n",argv[1]);
			return -1;
		}
	}

	result = chirp_reli_audit(current_host,"/",&list,stoptime);
	if(result>=0) {
		if(!raw_mode) printf("   FILES     DIRS      DATA OWNER\n");
		for(i=0;i<result;i++) {
			if(raw_mode) {
				printf("%lld %lld %lld %s\n",list[i].nfiles,list[i].ndirs,list[i].nbytes,list[i].name);
			} else {
				printf("%8lld %8lld %8sB %s\n",list[i].nfiles,list[i].ndirs,string_metric(list[i].nbytes,-1,0),list[i].name);
			}
		}
		free(list);
	}

	return result;
}

static INT64_T do_timeout( int argc, char **argv )
{
	timeout = atoi(argv[1]);
	printf("Timeout is %d seconds.\n",timeout);
	return 0;
}

static INT64_T do_quit( int argc, char **argv )
{
	exit(0);
	return -1;
}

static INT64_T do_help( int argc, char **argv )
{
	int i;
	printf("Commands:\n");
	for(i=0;list[i].name;i++) {
		printf("%-12s %s\n",list[i].name,list[i].help);
	}
	printf("\nDebugging subsystems are:\n");
	debug_flags_print(stdout);
	printf("\n");
	return 0;
}

static INT64_T do_thirdput( int argc, char **argv )
{
	INT64_T result;
	char full_path[CHIRP_PATH_MAX];
	char remote_path[CHIRP_PATH_MAX];
	time_t stop,start;

	complete_remote_path(argv[1],full_path);
	sprintf(remote_path,"/%s",argv[3]);


	start = time(0);	
	result = chirp_reli_thirdput(current_host,full_path,argv[2],remote_path,stoptime);
	stop = time(0);
	if(stop==start) stop++;

	if(result>0) {
		printf("%lld bytes transferred in %d seconds ",result,(int)(stop-start));
		printf("(%.1lfMB/s)\n",result/(double)(stop-start)/1024.0/1024.0);
	}

	return result;
}

static INT64_T do_mkalloc( int argc, char **argv )
{
	char full_path[CHIRP_PATH_MAX];
	complete_remote_path(argv[1],full_path);	
	return chirp_reli_mkalloc(current_host,full_path,string_metric_parse(argv[2]),0700,stoptime);
}

static INT64_T do_lsalloc( int argc, char **argv )
{
	char full_path[CHIRP_PATH_MAX];
	char alloc_path[CHIRP_PATH_MAX];
	INT64_T total, inuse;
	int result;

	if(argc!=2) argv[1] = ".";

	complete_remote_path(argv[1],full_path);	

	result = chirp_reli_lsalloc(current_host,full_path,alloc_path,&total,&inuse,stoptime);

	if(result>=0) {
		printf("%s\n",alloc_path);
		printf("%sB TOTAL\n",string_metric(total,-1,0));
		printf("%sB INUSE\n",string_metric(inuse,-1,0));
	}

	return result;
}

static INT64_T do_group_create(int argc, char** argv)
{
  char full_path[CHIRP_PATH_MAX];
  int result = 0;

  if (argc < 2)
    return -1;

  complete_remote_path(argv[1], full_path);

  result = chirp_reli_group_create(current_host, full_path, stoptime);

  if (result > 0)
    {
      printf("group:%s created\n", full_path);
    }

  return result;
}

static INT64_T do_group_add( int argc, char** argv )
{
  char full_path[CHIRP_PATH_MAX];
  int result = 0;

  if (argc < 3)
    return -1;

  complete_remote_path(argv[1], full_path);
     
  result = chirp_reli_group_add(current_host, full_path, argv[2], stoptime);

  if (result == 0)
    {
      printf("user %s added to group %s\n",  argv[2], full_path);
    }

  else if (result == 1)
    {
      printf("user %s already exists in group %s\n", argv[2], full_path);
    }
  return result;
}

static INT64_T do_group_remove( int argc, char** argv )
{
  char full_path[CHIRP_PATH_MAX];
  INT64_T result = 0;

  if (argc < 3)
    return -1;

  complete_remote_path(argv[1], full_path);
     
  result = chirp_reli_group_remove(current_host, full_path, argv[2], stoptime);

  if (result == 0)
    {
      printf("user %s deleted from group %s\n", argv[2], full_path);
      return 1;
    }
  else if (!errno)
    {
      printf("user %s not found in group %s\n", argv[2], full_path);
      return 0;
    }
  else 
    {
      strerror(errno);
    }
  return result;
}

static void show_one_user( const char *name, void *stream )
{
	fprintf(stream,"%s\n",name);
}

static INT64_T do_group_list( int argc, char** argv )
{
	static char full_path[CHIRP_PATH_MAX];

	complete_remote_path(argv[1], full_path);

	return chirp_reli_group_list(current_host,full_path,show_one_user,stdout,stoptime);
}

static INT64_T do_group_policy( int argc, char** argv )
{
  static char full_path[CHIRP_PATH_MAX];
  int result;
  int file_duration = 0;
  int dec_duration = 0;
  char c;

  optind = opterr = 0;
  while((c=getopt(argc,argv,"+d:f:D:F")) != (char)-1) {
          switch(c) {
                  case 'd':
                          dec_duration = atoi(optarg);
                          break;
                  case 'D':
                          dec_duration = atoi(optarg);
                          break;
                  case 'f':
                          file_duration = atoi(optarg);
                          break;
                  case 'F':
                          file_duration = atoi(optarg);
                          break;
    }
  }

  if (argv[optind] == NULL)  /* no group name specified */
    {
      errno = EINVAL;
      return -1;
    }
  complete_remote_path(argv[optind], full_path);

  result = chirp_reli_group_policy_set(current_host, full_path, file_duration, dec_duration, stoptime);

  return result;
}

static INT64_T do_matrix_create( int argc, char **argv )
{
	char path[CHIRP_PATH_MAX];
	struct chirp_matrix *m;

	complete_remote_path(argv[1],path);

	m = chirp_matrix_create(current_host,path,atoi(argv[2]),atoi(argv[3]),sizeof(double),atoi(argv[4]),stoptime);
	if(m) {
		chirp_matrix_close(m,stoptime);
		return 0;
	} else {
		return 1;
	}
}

static INT64_T do_matrix_list( int argc, char **argv )
{
	char path[CHIRP_PATH_MAX];
	struct chirp_matrix *m;

	complete_remote_path(argv[1],path);

	m = chirp_matrix_open(current_host,path,stoptime);
	if(m) {
		printf("host:   %s\n",current_host);
		printf("path:   %s\n",path);
		printf("width:  %d\n",chirp_matrix_width(m));
		printf("height: %d\n",chirp_matrix_height(m));
		printf("esize:  %d\n",chirp_matrix_element_size(m));
		printf("nhosts: %d\n",chirp_matrix_nhosts(m));
		printf("nfiles: %d\n",chirp_matrix_nfiles(m));
		chirp_matrix_close(m,stoptime);
		return 0;
	} else {
		return 1;
	}
}

static INT64_T do_matrix_delete( int argc, char **argv )
{
	char path[CHIRP_PATH_MAX];
	complete_remote_path(argv[1],path);
	return chirp_matrix_delete(current_host,path,stoptime);	
}

