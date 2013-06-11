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
//#define D_DEBUG stderr

#include "rmonitor_helper_comm.h"

#define BUFFER_MAX 1024

// XXX This is a quick hack to get through the build on Cygwin.
// It appears thaqt RTLD_NEXT does not exist on Cygwin.
// Can this module work on that operating system?

#if defined(CCTOOLS_OPSYS_CYGWIN) && !defined(RTLD_NEXT)
#define RTLD_NEXT 0
#endif

pid_t fork()
{
	pid_t pid;
	typeof(fork) *original_fork = dlsym(RTLD_NEXT, "fork");

	debug(D_DEBUG, "fork from %d.\n", getpid());
	pid = original_fork();

	if(!pid)
	{
		struct monitor_msg msg;

		msg.type   = BRANCH;
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

	debug(D_DEBUG, "chdir from %d.\n", getpid());
	status = original_chdir(path);

	if(status == 0)
	{
		struct monitor_msg msg;
		char  *newpath = getcwd(NULL, 0);

		msg.type   = CHDIR;
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

	debug(D_DEBUG, "fchdir from %d.\n", getpid());
	status = original_fchdir(fd);

	if(status == 0)
	{
		struct monitor_msg msg;
		char  *newpath = getcwd(NULL, 0);

		msg.type   = CHDIR;
		msg.origin = getpid();
		strcpy(msg.data.s, newpath);
		free(newpath);

		send_monitor_msg(&msg);
	}

	return status;
}

FILE *fopen(const char *path, const char *mode)
{
	FILE *file;
	typeof(fopen) *original_fopen = dlsym(RTLD_NEXT, "fopen");

	debug(D_DEBUG, "fopen from %d.\n", getpid());
	file = original_fopen(path, mode);

	if(file)
	{
		struct monitor_msg msg;

		msg.type   = OPEN;
		msg.origin = getpid();
		strcpy(msg.data.s, path);

		send_monitor_msg(&msg);
	}

	return file;
}

int open(const char *path, int flags, ...)
{
	va_list ap;
	int     fd;
	int     mode;

	typeof(open) *original_open = dlsym(RTLD_NEXT, "open");

	va_start(ap, flags);
	mode = va_arg(ap, int);
	va_end(ap);

	debug(D_DEBUG, "open from %d.\n", getpid());
	fd = original_open(path, flags, mode);

	if(fd > -1)
	{
		struct monitor_msg msg;

		msg.type   = OPEN;
		msg.origin = getpid();
		strcpy(msg.data.s, path);

		send_monitor_msg(&msg);
	}

	return fd;
}

void wakeup_pselect_from_exit(int signum)
{
	if(signum == SIGCONT)
		signal(SIGCONT, SIG_DFL);
}

void exit_wrapper_preamble(void)
{
	sigset_t set_cont, set_prev;
	void (*prev_handler)(int signum);
	struct timespec timeout = {.tv_sec = 0, .tv_nsec = 500000}; 

	debug(D_DEBUG, "%s from %d.\n", str_msgtype(END_WAIT), getpid());

	prev_handler = signal(SIGCONT, wakeup_pselect_from_exit);
	sigemptyset(&set_cont);
	sigaddset(&set_cont, SIGCONT);
	sigprocmask(SIG_BLOCK, &set_cont, &set_prev); //Adds SIGCONT to blocked signals.

	struct monitor_msg msg;
	msg.type   = END_WAIT;
	msg.origin = getpid();
	msg.data.p = getpid();

	send_monitor_msg(&msg);

	/* Wait at most timeout for monitor to send SIGCONT */
	debug(D_DEBUG, "Waiting for monitoring: %d.\n", getpid());
	pselect(0, NULL, NULL, NULL, &timeout, &set_prev);
	signal(SIGCONT, prev_handler);
	sigprocmask(SIG_SETMASK, &set_prev, NULL); 

	debug(D_DEBUG, "Continue with %s: %d.\n", str_msgtype(END_WAIT), getpid());
}

void end_wrapper_epilogue(void)
{
	debug(D_DEBUG, "%s from %d.\n", str_msgtype(END), getpid());

	struct monitor_msg msg;
	msg.type   = END;
	msg.origin = getpid();
	msg.data.p = getpid();

	send_monitor_msg(&msg);
}


void exit(int status)
{
	exit_wrapper_preamble();
	end_wrapper_epilogue();

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

	exit_wrapper_preamble();
	end_wrapper_epilogue();

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

	debug(D_DEBUG, "waiting from %d for %d.\n", getpid(), pid);
	pidb = original_waitpid(pid, &status_, options);

	if(WIFEXITED(status_) || WIFSIGNALED(status_))
	{
		struct monitor_msg msg;
		msg.type   = END;
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


/* wrap main ensuring exit_wrapper_preamble for one final monitoring
   checks gets called at least once */

#if defined(__clang__) || defined(__GNUC__)
void __attribute__((destructor)) init() {
	exit_wrapper_preamble();
}
#endif
