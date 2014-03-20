/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2005- The University of Notre Dame
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

extern void debug_stderr_write (INT64_T flags, const char *str);

extern void debug_file_write (INT64_T flags, const char *str);
extern void debug_file_size (off_t size);
extern void debug_file_path (const char *path);
extern void debug_file_rename (const char *suffix);

#ifdef HAS_SYSLOG_H
extern void debug_syslog_write (INT64_T flags, const char *str);
extern void debug_syslog_config (const char *name);
#endif

#ifdef HAS_SYSTEMD_JOURNAL_H
extern void debug_journal_write (INT64_T flags, const char *str);
#endif


static void (*debug_write) (INT64_T flags, const char *str) = debug_stderr_write;
static pid_t (*debug_getpid) (void) = getpid;
static char debug_program_name[PATH_MAX];
static INT64_T debug_flags = D_NOTICE|D_ERROR|D_FATAL;

struct flag_info {
	const char *name;
	INT64_T flag;
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

static const char *debug_flags_to_name(INT64_T flags)
{
	struct flag_info *i;

	for(i = table; i->name; i++) {
		if(i->flag & flags)
			return i->name;
	}

	return "debug";
}

static void do_debug(INT64_T flags, const char *fmt, va_list args)
{
	buffer_t B;
	char ubuf[1<<16];

	buffer_init(&B);
	buffer_ubuf(&B, ubuf, sizeof(ubuf));
	buffer_max(&B, sizeof(ubuf));

	if (debug_write == debug_file_write || debug_write == debug_stderr_write) {
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
	while(isspace(buffer_tostring(&B, NULL)[buffer_pos(&B)-1]))
		buffer_rewind(&B, buffer_pos(&B)-1); /* chomp whitespace */
	buffer_putliteral(&B, "\n");

	debug_write(flags, buffer_tostring(&B, NULL));

	buffer_free(&B);
}

void debug(INT64_T flags, const char *fmt, ...)
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

void vdebug(INT64_T flags, const char *fmt, va_list args)
{
	if(flags & debug_flags) {
		int save_errno = errno;
		do_debug(flags, fmt, args);
		errno = save_errno;
	}
}

void warn(INT64_T flags, const char *fmt, ...)
{
	va_list args;

	int save_errno = errno;
	va_start(args, fmt);
	do_debug(flags|D_ERROR, fmt, args);
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
		raise(SIGQUIT); /* dump core */
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

void debug_config_file (const char *path)
{
	if(path == NULL || strcmp(path, ":stderr") == 0) {
		debug_write = debug_stderr_write;
	} else if (strcmp(path, ":syslog") == 0) {
#ifdef HAS_SYSLOG_H
		debug_write = debug_syslog_write;
		debug_syslog_config(debug_program_name);
#else
		fprintf(stderr, "syslog is not available");
		exit(EXIT_FAILURE);
#endif
	} else if (strcmp(path, ":journal") == 0) {
#ifdef HAS_SYSTEMD_JOURNAL_H
		debug_write = debug_journal_write;
#else
		fprintf(stderr, "systemd journal is not available");
		exit(EXIT_FAILURE);
#endif
	} else {
		debug_write = debug_file_write;
		debug_file_path(path);
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

void debug_rename(const char *suffix)
{
	debug_file_rename(suffix);
}

/* vim: set noexpandtab tabstop=4: */
