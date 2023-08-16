/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "debug.h"

#include "buffer.h"
#include "path.h"
#include "xxmalloc.h"

#include <sys/time.h>

#include <ctype.h>
#include <errno.h>
#include <limits.h>
#include <signal.h>
#include <stdarg.h>
#include <string.h>
#include <time.h>

#include <stdio.h>

extern void debug_stderr_write (int64_t flags, const char *str);
extern void debug_stdout_write (int64_t flags, const char *str);

extern void debug_file_write (int64_t flags, const char *str);
extern void debug_file_size (off_t size);
extern int debug_file_path (const char *path);
extern void debug_file_rename (const char *suffix);
extern int debug_file_reopen (void);
extern int debug_file_close (void);

static void (*debug_write) (int64_t flags, const char *str) = debug_stderr_write;
static pid_t (*debug_getpid) (void) = getpid;
static char debug_program_name[PATH_MAX];
static int64_t debug_flags = D_NOTICE|D_ERROR|D_FATAL;

struct flag_info {
	const char *name;
	int64_t flag;
};

static struct flag_info table[] = {
	/* info, the default, is not shown here */
	{"fatal", D_FATAL},
	{"error", D_ERROR},
	{"notice", D_NOTICE},
	{"debug", D_DEBUG},

	/* subsystems */
	{"syscall", D_SYSCALL},
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
	{"login", D_LOGIN},
	{"irods", D_IRODS},
	{"wq", D_WQ},
	{"mpi", D_MPI},
	{"user", D_USER},
	{"xrootd", D_XROOTD},
	{"remote", D_REMOTE},
	{"batch", D_BATCH},
	{"rmonitor", D_RMON},
	{"makeflow", D_MAKEFLOW},
	{"makeflow_run", D_MAKEFLOW_RUN},
	{"makeflow_alloc", D_MAKEFLOW_ALLOC},
	{"makeflow_lexer",    D_MAKEFLOW_LEXER},
	{"makeflow_parser",   D_MAKEFLOW_PARSER},
	{"makeflow_hook",   D_MAKEFLOW_HOOK},
	{"ext", D_EXT},
	{"rmonitor", D_RMON},
	{"confuga", D_CONFUGA},
	{"vine", D_VINE},
	{"tlq", D_TLQ},
	{"jx", D_JX},
	{"ssl", D_SSL},
	{"all", D_ALL},
	{"bucketing", D_BUCKETING},
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

	fprintf(stream, "clear (unsets all flags)");

	for(i = 0; table[i].name; i++) {
		fprintf(stream, ", %s", table[i].name);
	}
}

void debug_set_flag_name(int64_t flag, const char *name)
{
	struct flag_info *i;

	for(i = table; i->name; i++) {
		if(i->flag & flag) {
			i->name = name;
			break;
		}
	}
}

static const char *debug_flags_to_name(int64_t flags)
{
	struct flag_info *i;

	for(i = table; i->name; i++) {
		if(i->flag & flags)
			return i->name;
	}

	return "debug";
}

static void do_debug(int64_t flags, const char *fmt, va_list args)
{
	buffer_t B;
	char ubuf[1<<16];

	buffer_init(&B);
	buffer_ubuf(&B, ubuf, sizeof(ubuf));
	buffer_max(&B, sizeof(ubuf));

	if (debug_write == debug_file_write || debug_write == debug_stderr_write || debug_write == debug_stdout_write) {
		struct timeval tv;
		struct tm *tm;
		gettimeofday(&tv, 0);
		tm = localtime(&tv.tv_sec);

		buffer_putfstring(&B, "%04d/%02d/%02d %02d:%02d:%02d.%02ld ", tm->tm_year + 1900, tm->tm_mon + 1, tm->tm_mday, tm->tm_hour, tm->tm_min, tm->tm_sec, (long) tv.tv_usec / 10000);
		buffer_putfstring(&B, "%s[%d] ", debug_program_name, getpid());
	}
	/* Parrot prints debug messages for children: */
	if (getpid() != debug_getpid()) {
		buffer_putfstring(&B, "<child:%d> ", (int)debug_getpid());
	}
	buffer_putfstring(&B, "%s: ", debug_flags_to_name(flags));

	buffer_putvfstring(&B, fmt, args);
	while(isspace(buffer_tostring(&B)[buffer_pos(&B)-1]))
		buffer_rewind(&B, buffer_pos(&B)-1); /* chomp whitespace */
	buffer_putliteral(&B, "\n");

	debug_write(flags, buffer_tostring(&B));

	if(debug_write != debug_stderr_write && (flags & (D_ERROR | D_NOTICE | D_FATAL))) {
		debug_stderr_write(flags, buffer_tostring(&B));
	}

	buffer_free(&B);
}

void debug(int64_t flags, const char *fmt, ...)
{
	if(flags & debug_flags) {
		va_list args;
		int save_errno = errno;
		va_start(args, fmt);
		do_debug(flags, fmt, args);
		va_end(args);
		errno = save_errno;
	}
}

void vdebug(int64_t flags, const char *fmt, va_list args)
{
	if(flags & debug_flags) {
		int save_errno = errno;
		do_debug(flags, fmt, args);
		errno = save_errno;
	}
}

void warn(int64_t flags, const char *fmt, ...)
{
	va_list args;

	int save_errno = errno;
	va_start(args, fmt);
	do_debug(flags|D_ERROR, fmt, args);
	va_end(args);
	errno = save_errno;
}

void notice(int64_t flags, const char *fmt, ...)
{
	va_list args;

	int save_errno = errno;
	va_start(args, fmt);
	do_debug(flags|D_NOTICE, fmt, args);
	va_end(args);
	errno = save_errno;
}

void fatal(const char *fmt, ...)
{
	struct fatal_callback *f;
	va_list args;

	va_start(args, fmt);
	do_debug(D_FATAL, fmt, args);
	va_end(args);

	for(f = fatal_callback_list; f; f = f->next) {
		f->callback();
	}

	while(1) {
		raise(SIGTERM);
		raise(SIGKILL);
	}
}

void debug_config_fatal(void (*callback) ())
{
	struct fatal_callback *f;
	f = xxmalloc(sizeof(*f));
	f->callback = callback;
	f->next = fatal_callback_list;
	fatal_callback_list = f;
}

int debug_config_file_e (const char *path)
{
	if(path == NULL || strcmp(path, ":stderr") == 0) {
		debug_write = debug_stderr_write;
		return 0;
	} else if(strcmp(path, ":stdout") == 0) {
		debug_write = debug_stdout_write;
		return 0;
	} else {
		debug_write = debug_file_write;
		return debug_file_path(path);
	}
}

void debug_config_file (const char *path)
{
	if (debug_config_file_e(path) == -1) {
		fprintf(stderr, "could not set debug file '%s': %s", path, strerror(errno));
		exit(EXIT_FAILURE);
	}
}

void debug_config (const char *name)
{
	strncpy(debug_program_name, path_basename(name), sizeof(debug_program_name)-1);
}

void debug_config_file_size (off_t size)
{
	debug_file_size(size);
}

void debug_config_getpid (pid_t (*getpidf)(void))
{
	debug_getpid = getpidf;
}

int64_t debug_flags_clear()
{
	int64_t result = debug_flags;
	debug_flags = 0;
	return result;
}

void debug_flags_restore(int64_t fl)
{
	debug_flags = fl;
}

void debug_rename(const char *suffix)
{
	debug_file_rename(suffix);
}

void debug_reopen(void)
{
	if (debug_file_reopen() == -1)
		fatal("could not reopen debug log: %s", strerror(errno));
}

void debug_close(void)
{
	debug_file_close();
}

/* vim: set noexpandtab tabstop=8: */
