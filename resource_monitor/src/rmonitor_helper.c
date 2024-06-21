/*
  Copyright (C) 2022 The University of Notre Dame
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

#include <assert.h>
#include <dlfcn.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <signal.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ipc.h>
#include <sys/sem.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#ifdef __linux__
#include <sched.h>
#endif

#include "itable.h"
#include "timestamp.h"

#define CCTOOLS_HELPER_DEBUG_MESSAGES 0

#define D_RMON stderr
#define debug \
	if (CCTOOLS_HELPER_DEBUG_MESSAGES) \
	fprintf

#include "rmonitor_helper_comm.h"

#define BUFFER_MAX 1024

#define PUSH_ERRNO \
	{ \
		int last_errno = errno; \
		errno = 0;
#define POP_ERRNO(msg) \
	msg.error = errno; \
	if (!errno) { \
		errno = last_errno; \
	} \
	}

#define START(msg) \
	{ \
		if (msg.type == RX || msg.type == TX) \
			msg.start = timestamp_get(); \
		PUSH_ERRNO
#define END(msg) \
	POP_ERRNO(msg) \
	if (msg.type == RX || msg.type == TX) \
		msg.end = timestamp_get(); \
	}

static struct itable *family_of_fd = NULL;
static uint64_t start_time = 0;
static uint64_t end_time = 0;

static int stop_short_running = 0; /* Stop processes that run for less than RESOURCE_MONITOR_SHORT_TIME seconds. */

#define declare_original_dlsym(name) __typeof__(name) *original_##name;
#define define_original_dlsym(name) original_##name = dlsym(RTLD_NEXT, #name);

declare_original_dlsym(fork);
declare_original_dlsym(chdir);
declare_original_dlsym(fchdir);
declare_original_dlsym(close);
declare_original_dlsym(open);
declare_original_dlsym(socket);
declare_original_dlsym(write);
declare_original_dlsym(read);
declare_original_dlsym(recv);
declare_original_dlsym(recvfrom);
declare_original_dlsym(send);
declare_original_dlsym(sendmsg);
declare_original_dlsym(recvmsg);
declare_original_dlsym(exit);
declare_original_dlsym(_exit);
declare_original_dlsym(waitpid);

#if defined(__linux__) && defined(__USE_LARGEFILE64)
declare_original_dlsym(open64);
#endif

static int initializing_helper = 0;

void rmonitor_helper_initialize()
{

	if (initializing_helper)
		return;

	initializing_helper = 1;

	define_original_dlsym(fork);
	define_original_dlsym(chdir);
	define_original_dlsym(fchdir);
	define_original_dlsym(close);
	define_original_dlsym(open);
	define_original_dlsym(socket);
	define_original_dlsym(write);
	define_original_dlsym(read);
	define_original_dlsym(recv);
	define_original_dlsym(recvfrom);
	define_original_dlsym(send);
	define_original_dlsym(sendmsg);
	define_original_dlsym(recvmsg);
	define_original_dlsym(exit);
	define_original_dlsym(_exit);
	define_original_dlsym(waitpid);

#if defined(__linux__) && defined(__USE_LARGEFILE64)
	define_original_dlsym(open64);
#endif

	if (!family_of_fd) {
		family_of_fd = itable_create(8);
	}

	if (getenv(RESOURCE_MONITOR_HELPER_STOP_SHORT)) {
		stop_short_running = 1;
	} else {
		stop_short_running = 0;
	}

	initializing_helper = 0;
}

int is_root_process()
{
	const char *pid_s = getenv(RESOURCE_MONITOR_ROOT_PROCESS);
	return (pid_s && atoi(pid_s) == getpid());
}

pid_t fork()
{
	pid_t pid;

	if (!original_fork) {
		rmonitor_helper_initialize();
		assert(original_fork);
	}

	debug(D_RMON, "fork from %d.\n", getpid());
	pid = original_fork();

	if (!pid) {
		char start_tmp[256];
		snprintf(start_tmp, 256, "%" PRId64, timestamp_get());
		setenv(RESOURCE_MONITOR_PROCESS_START, start_tmp, 1);

		struct rmonitor_msg msg;
		msg.type = BRANCH;

		/* We only send a message from the child, thus error is always zero. */
		msg.error = 0;
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
	int status;

	if (!original_chdir) {
		return syscall(SYS_chdir, path);
	}

	debug(D_RMON, "chdir from %d.\n", getpid());
	status = original_chdir(path);

	if (status == 0) {
		struct rmonitor_msg msg;
		char *newpath = getcwd(NULL, 0);

		msg.type = CHDIR;

		/* We only send a message when cwd actually changes, so errno is always 0. */
		msg.error = 0;
		msg.origin = getpid();
		strcpy(msg.data.s, newpath);
		free(newpath);

		send_monitor_msg(&msg);
	}

	return status;
}

int fchdir(int fd)
{
	int status;
	if (!original_fchdir) {
		return syscall(SYS_fchdir, fd);
	}

	debug(D_RMON, "fchdir from %d.\n", getpid());
	status = original_fchdir(fd);

	if (status == 0) {
		struct rmonitor_msg msg;
		char *newpath = getcwd(NULL, 0);

		msg.type = CHDIR;

		/* We only send a message when cwd actually changes, so errno is always 0. */
		msg.error = 0;
		msg.origin = getpid();
		strcpy(msg.data.s, newpath);
		free(newpath);

		send_monitor_msg(&msg);
	}

	return status;
}

int close(int fd)
{
	if (!original_close) {
		return syscall(SYS_close, fd);
	}

	if (family_of_fd) {
		itable_remove(family_of_fd, fd);
	}

	int status = original_close(fd);

	return status;
}

static int open_for_writing(int fd)
{
	int flags, access_mode;

	flags = fcntl(fd, F_GETFL);

	if (flags == -1) {
		/* if error, assume output. */
		return 1;
	}

	access_mode = flags & O_ACCMODE;
	return (access_mode != O_RDONLY);
}

int open(const char *path, int flags, ...)
{
	struct rmonitor_msg msg;

	va_list ap;
	int fd;
	int mode;

	va_start(ap, flags);
	mode = va_arg(ap, int);
	va_end(ap);

	if (!original_open) {
		return syscall(SYS_open, path, flags, mode);
	}

	debug(D_RMON, "open %s from %d.\n", path, getpid());

	START(msg)
	fd = original_open(path, flags, mode);
	END(msg)

	/* With ENOENT we do not send a message, simply to reduce spam. */
	if (msg.error == ENOENT)
		return fd;

	/* Consider file as input by default. */
	msg.type = OPEN_INPUT;

	if (fd > -1 && open_for_writing(fd)) {
		msg.type = OPEN_OUTPUT;
	}

	msg.origin = getpid();
	strcpy(msg.data.s, path);

	send_monitor_msg(&msg);

	return fd;
}

#if defined(__linux__) && defined(__USE_LARGEFILE64)

int open64(const char *path, int flags, ...)
{
	struct rmonitor_msg msg;

	va_list ap;
	int fd;
	int mode;

	va_start(ap, flags);
	mode = va_arg(ap, int);
	va_end(ap);

	if (!original_open64) {
		return syscall(SYS_open, path, flags | O_LARGEFILE, mode);
	}

	debug(D_RMON, "open64 %s from %d.\n", path, getpid());

	START(msg)
	fd = original_open64(path, flags, mode);
	END(msg)

	/* With ENOENT we do not send a message, simply to reduce spam. */
	if (msg.error == ENOENT)
		return fd;

	/* Consider file as input by default. */
	msg.type = OPEN_INPUT;

	if (fd > -1 && open_for_writing(fd)) {
		msg.type = OPEN_OUTPUT;
	}

	msg.origin = getpid();
	strcpy(msg.data.s, path);

	send_monitor_msg(&msg);

	return fd;
}
#endif /* defined linux && __USE_LARGEFILE64 */

int socket(int domain, int type, int protocol)
{
	int fd;

	if (!original_socket) {
		rmonitor_helper_initialize();
		assert(original_socket);
	}

	fd = original_socket(domain, type, protocol);

	if (fd > -1 && (domain != AF_LOCAL || domain != AF_NETLINK)) {
		itable_insert(family_of_fd, fd, (void **)1);
	} else {
		itable_remove(family_of_fd, fd);
	}

	return fd;
}

ssize_t write(int fd, const void *buf, size_t count)
{
	struct rmonitor_msg msg;

	if (!original_write) {
		return syscall(SYS_write, fd, buf, count);
	}

	msg.origin = getpid();

	if (family_of_fd && itable_lookup(family_of_fd, fd)) {
		msg.type = TX;
	} else {
		msg.type = WRITE;
	}

	ssize_t real_count;
	START(msg)
	real_count = original_write(fd, buf, count);
	END(msg)

	msg.data.n = real_count;
	send_monitor_msg(&msg);

	return real_count;
}

ssize_t read(int fd, void *buf, size_t count)
{
	struct rmonitor_msg msg;

	if (!original_read) {
		return syscall(SYS_read, fd, buf, count);
	}

	msg.origin = getpid();

	if (family_of_fd && itable_lookup(family_of_fd, fd)) {
		msg.type = RX;
	} else {
		msg.type = READ;
	}

	ssize_t real_count;
	START(msg)
	real_count = original_read(fd, buf, count);
	END(msg)

	msg.data.n = real_count;
	send_monitor_msg(&msg);

	return real_count;
}

ssize_t recv(int fd, void *buf, size_t count, int flags)
{
	struct rmonitor_msg msg;

	if (!original_recv) {
		rmonitor_helper_initialize();
		assert(original_recv);
	}

	msg.type = RX;
	msg.origin = getpid();

	ssize_t real_count;
	START(msg)
	real_count = original_recv(fd, buf, count, flags);
	END(msg)

	msg.data.n = real_count;
	send_monitor_msg(&msg);

	return real_count;
}

ssize_t recvfrom(int fd, void *buf, size_t count, int flags, struct sockaddr *src, socklen_t *addrlen)
{
	struct rmonitor_msg msg;

	if (!original_recvfrom) {
		rmonitor_helper_initialize();
		assert(original_recvfrom);
	}

	msg.type = RX;
	msg.origin = getpid();

	ssize_t real_count;
	START(msg)
	real_count = original_recvfrom(fd, buf, count, flags, src, addrlen);
	END(msg)

	msg.data.n = real_count;
	send_monitor_msg(&msg);

	return real_count;
}

ssize_t send(int fd, const void *buf, size_t count, int flags)
{
	struct rmonitor_msg msg;

	if (!original_send) {
		rmonitor_helper_initialize();
		assert(original_send);
	}

	msg.type = TX;
	msg.origin = getpid();

	ssize_t real_count;
	START(msg)
	real_count = original_send(fd, buf, count, flags);
	END(msg)

	msg.data.n = real_count;
	send_monitor_msg(&msg);

	return real_count;
}

ssize_t sendmsg(int fd, const struct msghdr *mg, int flags)
{
	struct rmonitor_msg msg;

	if (!original_sendmsg) {
		rmonitor_helper_initialize();
		assert(original_sendmsg);
	}

	msg.type = TX;
	msg.origin = getpid();

	ssize_t real_count;
	START(msg)
	real_count = original_sendmsg(fd, mg, flags);
	END(msg)

	msg.data.n = real_count;
	send_monitor_msg(&msg);

	return real_count;
}

ssize_t recvmsg(int fd, struct msghdr *mg, int flags)
{
	struct rmonitor_msg msg;

	if (!original_recvmsg) {
		rmonitor_helper_initialize();
		assert(original_recvmsg);
	}

	msg.type = RX;
	msg.origin = getpid();

	ssize_t real_count;
	START(msg)
	real_count = original_recvmsg(fd, mg, flags);
	END(msg)

	msg.data.n = real_count;
	send_monitor_msg(&msg);

	return real_count;
}

/* dummy handler. In RH5, SIGCONT is ignored by sigprocmask/sigtimedwait,
 * unless handler is different than SIG_IGN. */
void exit_signal_handler(int signum)
{
	return;
}

void exit_wrapper_preamble(int status)
{
	static int did_exit_wrapper = 0;

	if (did_exit_wrapper)
		return;

	did_exit_wrapper = 1;

	sigset_t all_signals;
	sigset_t old_signals;

	sigfillset(&all_signals);
	struct timespec timeout = {.tv_sec = 10, .tv_nsec = 0};

	debug(D_RMON, "%s from %d.\n", str_msgtype(END_WAIT), getpid());

	char *start_tmp = getenv(RESOURCE_MONITOR_PROCESS_START);
	start_time = start_tmp ? atoll(start_tmp) : 0;
	end_time = timestamp_get();

	struct rmonitor_msg msg;
	msg.type = END_WAIT;
	msg.error = 0;
	msg.origin = getpid();
	msg.data.n = status;
	msg.start = start_time;
	msg.end = end_time;
	;

	sighandler_t old_handler = signal(SIGCONT, exit_signal_handler);

	int short_process = 0;
	if (is_root_process()) {
		// root process is never considered a short running process
		short_process = 0;
	} else if (stop_short_running) {
		// we are stopping all processes, so no process is considered short running
		short_process = 0;
	} else if (end_time < (start_time + RESOURCE_MONITOR_SHORT_TIME)) {
		// process ran for less than RESOURCE_MONITOR_SHORT_TIME, so it is
		// considered short running.
		short_process = 1;
	} else {
		// anything else is not considered a short running process
		short_process = 0;
	}

	// If not short running, stop the process for examination.
	int blocking_signals = 0;
	if (!short_process) {
		if (sigprocmask(SIG_SETMASK, &all_signals, &old_signals) != -1) {
			blocking_signals = 1;
		}
	}

	send_monitor_msg(&msg);

	if (blocking_signals) {
		debug(D_RMON, "Waiting for monitoring: %d.\n", getpid());
		sigtimedwait(&all_signals, NULL, &timeout);
		sigprocmask(SIG_SETMASK, &old_signals, NULL);
		signal(SIGCONT, old_handler);
	} else {
		signal(SIGCONT, old_handler);
	}

	debug(D_RMON, "Continue with %s: %d.\n", str_msgtype(END_WAIT), getpid());
}

void end_wrapper_epilogue(void)
{
	debug(D_RMON, "%s from %d.\n", str_msgtype(END), getpid());

	struct rmonitor_msg msg;
	msg.type = END;
	msg.error = 0;
	msg.origin = getpid();
	msg.data.p = getpid();
	msg.start = start_time;
	msg.end = end_time;
	;

	send_monitor_msg(&msg);
}

void exit(int status)
{
	if (!original_exit) {
		syscall(SYS_exit, status);
	}

	exit_wrapper_preamble(status);
	end_wrapper_epilogue();

	debug(D_RMON, "%d about to call exit()\n", getpid());

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

	if (!original_exit) {
		syscall(SYS_exit, status);
	}

	exit_wrapper_preamble(status);
	end_wrapper_epilogue();

	debug(D_RMON, "%d about to call _exit()\n", getpid());

	original_exit(status);

	/* we exit in the above line. The next line is to make the compiler
	   happy with noreturn warnings. */

	_exit(status);
}

pid_t waitpid(pid_t pid, int *status, int options)
{
	int status_; // status might be NULL, thus we use status_ to retrive the state.
	pid_t pidb;

	if (!original_waitpid) {
		rmonitor_helper_initialize();
		assert(original_waitpid);
	}

	debug(D_RMON, "waiting from %d for %d.\n", getpid(), pid);
	pidb = original_waitpid(pid, &status_, options);

	if (WIFEXITED(status_) || WIFSIGNALED(status_)) {
		struct rmonitor_msg msg;
		msg.type = WAIT;
		msg.error = 0; /* send message only on success, so error is 0. */
		msg.origin = getpid();
		msg.data.p = pidb;

		send_monitor_msg(&msg);
	}

	if (status)
		*status = status_;

	return pidb;
}

pid_t wait(int *status)
{
	return waitpid(-1, status, 0);
}

#if defined(__clang__) || defined(__GNUC__)

void __attribute__((constructor)) init()
{
	/* find the dlsym values when loading the library. */
	rmonitor_helper_initialize();
}

/* wrap main ensures exit_wrapper_preamble runs, and thus monitoring
is done at least once */
void __attribute__((destructor)) fini()
{
	/* we use default status of 0, since if command did not call exit
	 * explicitely, that is the default. */
	exit_wrapper_preamble(0);
}

#endif

/* vim: set noexpandtab tabstop=4: */
