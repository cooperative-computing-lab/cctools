/*
  Copyright (C) 2013- The University of Notre Dame
  This software is distributed under the GNU General Public License.
  See the file COPYING for details.
*/

/* We need RTLD_NEXT for find the libc implementation of fork(),
 * vfork(), etc., but RTLD_NEXT is not POSIX. However, the BSDs
 * have it by the default, and glibc needs _GNU_SOURCE defined.
 * */

#if defined(__linux__) && !defined(_GNU_SOURCE)
#define _GNU_SOURCE // Aaaaaah!!
#endif

#include <stdio.h>
#include <stdlib.h>
#include <stdarg.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <signal.h>
#include <fcntl.h>
#include <dlfcn.h>
#include <sys/wait.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/ipc.h>
#include <sys/sem.h>
#include <limits.h>

#ifdef __linux__
#include <sched.h>
#endif

#include "debug.h"
//#define debug fprintf
//#define D_RMON stderr

#include "rmonitor_helper_comm.h"

#define BUFFER_MAX 1024

// XXX This is a quick hack to get through the build on Cygwin.
// It appears thaqt RTLD_NEXT does not exist on Cygwin.
// Can this module work on that operating system?

#if defined(CCTOOLS_OPSYS_CYGWIN) && !defined(RTLD_NEXT)
#define RTLD_NEXT 0
#endif

#define PUSH_ERRNO { int last_errno = errno; errno = 0;
#define POP_ERRNO(msg) msg.error = errno; if(!errno){ errno = last_errno; } }

pid_t fork()
{
	pid_t pid;
	typeof(fork) *original_fork = dlsym(RTLD_NEXT, "fork");

	debug(D_RMON, "fork from %d.\n", getpid());
	pid = original_fork();

	if(!pid)
	{
		struct rmonitor_msg msg;
		msg.type   = BRANCH;

		/* We only send a message from the child, thus error is always zero. */
		msg.error  = 0;
		msg.origin = getpid();
		msg.data.p = getppid();

		send_monitor_msg(&msg);
	}

	return pid;
}

pid_t __fork()
{
	return fork();
}

pid_t vfork()
{
	return fork();
}

pid_t __vfork()
{
	return fork();
}


int chdir(const char *path)
{
	int   status;
	typeof(chdir) *original_chdir = dlsym(RTLD_NEXT, "chdir");

	debug(D_RMON, "chdir from %d.\n", getpid());
	status = original_chdir(path);

	if(status == 0)
	{
		struct rmonitor_msg msg;
		char  *newpath = getcwd(NULL, 0);

		msg.type   = CHDIR;

		/* We only send a message when cwd actually changes, so errno is always 0. */
		msg.error  = 0;
		msg.origin = getpid();
		strcpy(msg.data.s, newpath);
		free(newpath);

		send_monitor_msg(&msg);
	}

	return status;
}

int fchdir(int fd)
{
	int   status;
	typeof(fchdir) *original_fchdir = dlsym(RTLD_NEXT, "fchdir");

	debug(D_RMON, "fchdir from %d.\n", getpid());
	status = original_fchdir(fd);

	if(status == 0)
	{
		struct rmonitor_msg msg;
		char  *newpath = getcwd(NULL, 0);

		msg.type   = CHDIR;

		/* We only send a message when cwd actually changes, so errno is always 0. */
		msg.error  = 0;
		msg.origin = getpid();
		strcpy(msg.data.s, newpath);
		free(newpath);

		send_monitor_msg(&msg);
	}

	return status;
}

static int open_for_writing(int fd) {
	int flags, access_mode;

	flags       = fcntl(fd, F_GETFL);

	if(flags == -1) {
		/* if error, assume output. */
		return 1;
	}

	access_mode = flags & O_ACCMODE;
	return (access_mode != O_RDONLY);
}

FILE *fopen(const char *path, const char *mode)
{
	struct rmonitor_msg msg;

	FILE *file;
	typeof(fopen) *original_fopen = dlsym(RTLD_NEXT, "fopen");

	debug(D_RMON, "fopen %s mode %s from %d.\n", path, mode, getpid());

	PUSH_ERRNO
		file = original_fopen(path, mode);
	POP_ERRNO(msg)

	/* With ENOENT we do not send a message, simply to reduce spam. */
	if(msg.error == ENOENT)
		return file;

	/* Consider file as input by default. */
	msg.type   = OPEN_INPUT;

	if(file && open_for_writing(fileno(file))) {
		msg.type   = OPEN_OUTPUT;
	}

	msg.origin = getpid();
	strcpy(msg.data.s, path);

	send_monitor_msg(&msg);

	return file;
}

int open(const char *path, int flags, ...)
{
	struct rmonitor_msg msg;

	va_list ap;
	int     fd;
	int     mode;

	typeof(open) *original_open = dlsym(RTLD_NEXT, "open");

	va_start(ap, flags);
	mode = va_arg(ap, int);
	va_end(ap);

	debug(D_RMON, "open %s from %d.\n", path, getpid());

	PUSH_ERRNO
		fd = original_open(path, flags, mode);
	POP_ERRNO(msg)

	/* With ENOENT we do not send a message, simply to reduce spam. */
	if(msg.error == ENOENT)
		return fd;

	/* Consider file as input by default. */
	msg.type   = OPEN_INPUT;

	if(fd > -1 && open_for_writing(fd)) {
		msg.type   = OPEN_OUTPUT;
	}

	msg.origin = getpid();
	strcpy(msg.data.s, path);

	send_monitor_msg(&msg);

	return fd;
}

#if defined(__linux__) && defined(__USE_LARGEFILE64)
FILE *fopen64(const char *path, const char *mode)
{
	struct rmonitor_msg msg;

	FILE *file;
	typeof(fopen64) *original_fopen64 = dlsym(RTLD_NEXT, "fopen64");

	debug(D_RMON, "fopen64 %s mode %s from %d.\n", path, mode, getpid());

	PUSH_ERRNO
		file = original_fopen64(path, mode);
	POP_ERRNO(msg)

	/* With ENOENT we do not send a message, simply to reduce spam. */
	if(msg.error == ENOENT)
		return file;

	/* Consider file as input by default. */
	msg.type   = OPEN_INPUT;

	if(file && open_for_writing(fileno(file))) {
		msg.type   = OPEN_OUTPUT;
	}

	msg.origin = getpid();
	strcpy(msg.data.s, path);

	send_monitor_msg(&msg);

	return file;
}

int open64(const char *path, int flags, ...)
{
	struct rmonitor_msg msg;

	va_list ap;
	int     fd;
	int     mode;

	typeof(open64) *original_open64 = dlsym(RTLD_NEXT, "open64");

	va_start(ap, flags);
	mode = va_arg(ap, int);
	va_end(ap);

	debug(D_RMON, "open64 %s from %d.\n", path, getpid());

	PUSH_ERRNO
		fd = original_open64(path, flags, mode);
	POP_ERRNO(msg)

	/* With ENOENT we do not send a message, simply to reduce spam. */
	if(msg.error == ENOENT)
		return fd;

	/* Consider file as input by default. */
	msg.type   = OPEN_INPUT;

	if(fd > -1 && open_for_writing(fd)) {
		msg.type   = OPEN_OUTPUT;
	}

	msg.origin = getpid();
	strcpy(msg.data.s, path);

	send_monitor_msg(&msg);

	return fd;
}
#endif /* defined linux && __USE_LARGEFILE64 */

ssize_t write(int fd, const void *buf, size_t count)
{
	struct rmonitor_msg msg;
	msg.type   = WRITE;
	msg.origin = getpid();

	typeof(write) *original_write = dlsym(RTLD_NEXT, "write");

	ssize_t real_count;
	PUSH_ERRNO
		real_count = original_write(fd, buf, count);
	POP_ERRNO(msg)

	msg.data.n = real_count;
	send_monitor_msg(&msg);

	return real_count;
}

void wakeup_pselect_from_exit(int signum)
{
	if(signum == SIGCONT)
		signal(SIGCONT, SIG_DFL);
}

void exit_wrapper_preamble(int status)
{
	static int did_exit_wrapper = 0;

	if(did_exit_wrapper)
		return;

	did_exit_wrapper = 1;

	sigset_t set_cont, set_prev;
	void (*prev_handler)(int signum);
	struct timespec timeout = {.tv_sec = 2, .tv_nsec = 0};

	debug(D_RMON, "%s from %d.\n", str_msgtype(END_WAIT), getpid());

	prev_handler = signal(SIGCONT, wakeup_pselect_from_exit);
	sigemptyset(&set_cont);
	sigaddset(&set_cont, SIGCONT);
	sigprocmask(SIG_BLOCK, &set_cont, &set_prev); //Adds SIGCONT to blocked signals.

	struct rmonitor_msg msg;
	msg.type   = END_WAIT;
	msg.error  = 0;
	msg.origin = getpid();
	msg.data.n = status;

	send_monitor_msg(&msg);

	/* Wait at most timeout for monitor to send SIGCONT */
	debug(D_RMON, "Waiting for monitoring: %d.\n", getpid());
	pselect(0, NULL, NULL, NULL, &timeout, &set_prev);
	signal(SIGCONT, prev_handler);
	sigprocmask(SIG_SETMASK, &set_prev, NULL);

	debug(D_RMON, "Continue with %s: %d.\n", str_msgtype(END_WAIT), getpid());
}

void end_wrapper_epilogue(void)
{
	debug(D_RMON, "%s from %d.\n", str_msgtype(END), getpid());

	struct rmonitor_msg msg;
	msg.type   = END;
	msg.error  = 0;
	msg.origin = getpid();
	msg.data.p = getpid();

	send_monitor_msg(&msg);
}


void exit(int status)
{
	exit_wrapper_preamble(status);
	end_wrapper_epilogue();

	debug(D_RMON, "%d about to call exit()\n", getpid());

	typeof(exit) *original_exit = dlsym(RTLD_NEXT, "exit");
	original_exit(status);

	/* we exited in the above line. The next line is to make the compiler
	   happy with noreturn warnings. */

	exit(status);
}

void _exit(int status)
{

	/* We may get two END messages, from exit and _exit, but the second
	   will be ignored as the processes would no longer in the
	   monitoring tables. */

	exit_wrapper_preamble(status);
	end_wrapper_epilogue();

	debug(D_RMON, "%d about to call _exit()\n", getpid());

	typeof(_exit) *original_exit = dlsym(RTLD_NEXT, "_exit");
	original_exit(status);

	/* we exit in the above line. The next line is to make the compiler
	   happy with noreturn warnings. */

	_exit(status);
}

pid_t waitpid(pid_t pid, int *status, int options)
{
	int status_; //status might be NULL, thus we use status_ to retrive the state.
	pid_t pidb;
	typeof(waitpid) *original_waitpid = dlsym(RTLD_NEXT, "waitpid");

	debug(D_RMON, "waiting from %d for %d.\n", getpid(), pid);
	pidb = original_waitpid(pid, &status_, options);

	if(WIFEXITED(status_) || WIFSIGNALED(status_))
	{
		struct rmonitor_msg msg;
		msg.type   = WAIT;
		msg.error  = 0;          /* send message only on success, so error is 0. */
		msg.origin = getpid();
		msg.data.p = pidb;

		send_monitor_msg(&msg);
	}

	if(status)
		*status = status_;

	return pidb;
}

pid_t wait(int *status)
{
	return waitpid(-1, status, 0);
}


/* wrap main ensures exit_wrapper_preamble runs, and thus monitoring
is done at least once */

#if defined(__clang__) || defined(__GNUC__)
void __attribute__((destructor)) init() {
	/* we use default status of 0, since if command did not call exit
	 * explicitely, that is the default. */
	exit_wrapper_preamble(0);
}

#endif

/* vim: set noexpandtab tabstop=4: */
