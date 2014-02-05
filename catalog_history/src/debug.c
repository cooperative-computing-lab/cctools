/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2005- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "debug.h"

#include "full_io.h"
#include "stringtools.h"
#include "xxmalloc.h"

#include <unistd.h>
#include <fcntl.h>

#include <sys/stat.h>
#include <sys/time.h>

#include <assert.h>
#include <errno.h>
#include <signal.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

static int debug_fd = STDERR_FILENO;
static char *debug_file = NULL;
static int debug_file_size = 1<<20;
static const char *program_name = "";
static INT64_T debug_flags = D_NOTICE;
static pid_t(*debug_getpid) () = getpid;

struct flag_info {
	const char *name;
	INT64_T flag;
};

static struct flag_info table[] = {
	{"syscall", D_SYSCALL},
	{"notice", D_NOTICE},
	{"channel", D_CHANNEL},
	{"process", D_PROCESS},
	{"resolve", D_RESOLVE},
	{"libcall", D_LIBCALL},
	{"tcp", D_TCP},
	{"dns", D_DNS},
	{"auth", D_AUTH},
	{"local", D_LOCAL},
	{"http", D_HTTP},
	{"ftp", D_FTP},
	{"nest", D_NEST},
	{"chirp", D_CHIRP},
	{"cvmfs", D_CVMFS},
	{"multi", D_MULTI},
	{"dcap", D_DCAP},
	{"rfio", D_RFIO},
	{"glite", D_GLITE},
	{"lfc", D_LFC},
	{"gfal", D_GFAL},
	{"grow", D_GROW},
	{"pstree", D_PSTREE},
	{"alloc", D_ALLOC},
	{"cache", D_CACHE},
	{"poll", D_POLL},
	{"hdfs", D_HDFS},
	{"bxgrid", D_BXGRID},
	{"debug", D_DEBUG},
	{"login", D_LOGIN},
	{"irods", D_IRODS},
	{"wq", D_WQ},
	{"mpi", D_MPI},
	{"user", D_USER},
	{"xrootd", D_XROOTD},
	{"remote", D_REMOTE},
	{"batch", D_BATCH},
	{"rmonitor", D_RMON},
	{"all", D_ALL},
	{"time", 0},		/* backwards compatibility */
	{"pid", 0},		/* backwards compatibility */
	{0, 0}
};

struct fatal_callback {
	void (*callback) ();
	struct fatal_callback *next;
};

struct fatal_callback *fatal_callback_list = 0;


int debug_flags_set(const char *flagname)
{
	struct flag_info *i;

	if(!strcmp(flagname,"clear")) {
		debug_flags_clear();
		return 1;
	}

	for(i = table; i->name; i++) {
		if(!strcmp(flagname, i->name)) {
			debug_flags |= i->flag;
			return 1;
		}
	}

	return 0;
}

void debug_flags_print(FILE * stream)
{
	int i;

	for(i = 0; table[i].name; i++) {
		fprintf(stream, "%s ", table[i].name);
	}
}

void debug_set_flag_name(INT64_T flag, const char *name)
{
	struct flag_info *i;

	for(i = table; i->name; i++) {
		if(i->flag & flag) {
			i->name = name;
			break;
		}
	}
}

static const char *flag_to_name(INT64_T flag)
{
	struct flag_info *i;

	for(i = table; i->name; i++) {
		if(i->flag & flag)
			return i->name;
	}

	return "debug";
}

static void do_debug(int is_fatal, INT64_T flags, const char *fmt, va_list args)
{
	char newfmt[65536];
	char buffer[65536];
	int length;

	struct timeval tv;
	struct tm *tm;

	gettimeofday(&tv, 0);
	tm = localtime(&tv.tv_sec);

	snprintf(newfmt, 65536, "%04d/%02d/%02d %02d:%02d:%02d.%02ld [%d] %s: %s: %s", tm->tm_year + 1900, tm->tm_mon + 1, tm->tm_mday, tm->tm_hour, tm->tm_min, tm->tm_sec, (long) tv.tv_usec / 10000, (int) debug_getpid(), program_name,
		is_fatal ? "fatal " : flag_to_name(flags), fmt);

	vsnprintf(buffer, 65536, newfmt, args);
	string_chomp(buffer);
	strcat(buffer, "\n");
	length = strlen(buffer);

	if(debug_file) {
		struct stat info;

		fstat(debug_fd, &info);
		if(info.st_size >= debug_file_size && debug_file_size != 0) {
			close(debug_fd);

			if(stat(debug_file, &info) == 0) {
				if(info.st_size >= debug_file_size) {
					char *newname = malloc(strlen(debug_file) + 5);
					sprintf(newname, "%s.old", debug_file);
					rename(debug_file, newname);
					free(newname);
				}
			}

			debug_fd = open(debug_file, O_CREAT | O_TRUNC | O_WRONLY, 0660);
			if(debug_fd == -1){
				debug_fd = STDERR_FILENO;
				fatal("could not open log file `%s': %s", debug_file, strerror(errno));
			}
		}
	}

	full_write(debug_fd, buffer, length);
}

void debug(INT64_T flags, const char *fmt, ...)
{
	va_list args;
	va_start(args, fmt);

	if(flags & debug_flags) {
		int save_errno = errno;
		do_debug(0, flags, fmt, args);
		errno = save_errno;
	}

	va_end(args);
}

void vdebug(INT64_T flags, const char *fmt, va_list args)
{
	if(flags & debug_flags) {
		int save_errno = errno;
		do_debug(0, flags, fmt, args);
		errno = save_errno;
	}
}

void warn(INT64_T flags, const char *fmt, ...)
{
	va_list args;
	va_start(args, fmt);

        int save_errno = errno;
        do_debug(0, flags, fmt, args);
        errno = save_errno;

	va_end(args);
}

void fatal(const char *fmt, ...)
{
	struct fatal_callback *f;
	va_list args;
	va_start(args, fmt);

	do_debug(1, 0, fmt, args);

	for(f = fatal_callback_list; f; f = f->next) {
		f->callback();
	}

	while(1) {
		kill(getpid(), SIGTERM);
		kill(getpid(), SIGKILL);
	}

	va_end(args);
}

void debug_config_fatal(void (*callback) ())
{
	struct fatal_callback *f;
	f = xxmalloc(sizeof(*f));
	f->callback = callback;
	f->next = fatal_callback_list;
	fatal_callback_list = f;
}

void debug_config(const char *name)
{
	const char *n = strdup(name);

	program_name = strrchr(n, '/');
	if(program_name) {
		program_name++;
	} else {
		program_name = n;
	}
}

void debug_config_file(const char *f)
{
	free(debug_file);
	debug_file = NULL;
	if(f) {
		if(*f == '/'){
			debug_file = strdup(f);
		} else {
			char path[8192];
			if(getcwd(path, sizeof(path)) == NULL)
				assert(0);
			assert(strlen(path) + strlen(f) + 1 < 8192);
			strcat(path, "/");
			strcat(path, f);
			debug_file = strdup(path);
		}
		debug_fd = open(debug_file, O_CREAT | O_APPEND | O_WRONLY, 0660);
		if (debug_fd == -1){
			debug_fd = STDERR_FILENO;
			fatal("could not access log file `%s' for writing: %s", debug_file, strerror(errno));
		}
	} else {
		if (debug_fd != STDERR_FILENO){
			close(debug_fd); /* we opened some file */
		}
		debug_fd = STDERR_FILENO;
	}
}

void debug_config_file_size( size_t size )
{
	debug_file_size = size;
}

void debug_config_getpid(pid_t(*getpidfunc) ())
{
	debug_getpid = getpidfunc;
}

INT64_T debug_flags_clear()
{
	INT64_T result = debug_flags;
	debug_flags = 0;
	return result;
}

void debug_flags_restore(INT64_T fl)
{
	debug_flags = fl;
}

/* vim: set noexpandtab tabstop=4: */
