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
#define _GNU_SOURCE
#endif

#include <arpa/inet.h>
#include <assert.h>
#include <dlfcn.h>
#include <errno.h>
#include <stdio.h>
#include <string.h>
#include <sys/file.h>
#include <sys/syscall.h>
#include <sys/wait.h>
#include <unistd.h>

#define CCTOOLS_HELPER_DEBUG_MESSAGES 0

#define D_RMON stderr
#define debug \
	if (CCTOOLS_HELPER_DEBUG_MESSAGES) \
	fprintf

#define BUFFER_MAX 1024

#define RESOURCE_MONITOR_PIDS_FILE "CCTOOLS_RESOURCE_MONITOR_PIDS_FILE"

#define declare_original_dlsym(name) __typeof__(name) *original_##name;
#define define_original_dlsym(name) original_##name = dlsym(RTLD_NEXT, #name);

declare_original_dlsym(fork);
declare_original_dlsym(exit);
declare_original_dlsym(_exit);

static int initializing_helper = 0;

void rmonitor_helper_initialize()
{

	debug(D_RMON, "initializing fork wrapper\n");

	if (initializing_helper)
		return;

	initializing_helper = 1;

	define_original_dlsym(fork);
	define_original_dlsym(exit);
	define_original_dlsym(_exit);

	initializing_helper = 0;
}

void write_to_file_of_pids(pid_t pid)
{
	char *file_of_pids = getenv(RESOURCE_MONITOR_PIDS_FILE);

	if (file_of_pids) {
		int fd = open(file_of_pids, O_WRONLY | O_APPEND | O_CREAT | O_DSYNC, 0660);
		if (fd == -1) {
			debug(D_RMON, "error opening %s: %s\n", RESOURCE_MONITOR_PIDS_FILE, strerror(errno));
			return;
		}

		/* ensure pid is written as a 32bit number, network order */
		uint32_t b = htonl((uint32_t)pid);

		int ld = flock(fd, LOCK_EX);
		if (ld == -1) {
			debug(D_RMON, "error locking %s: %s\n", RESOURCE_MONITOR_PIDS_FILE, strerror(errno));
			return;
		}

		int count = write(fd, &b, sizeof(b));
		flock(fd, LOCK_UN);

		if (count == -1) {
			debug(D_RMON, "error writing to %s: %s\n", RESOURCE_MONITOR_PIDS_FILE, strerror(errno));
		}
	}
}

pid_t fork()
{
	pid_t pid;

	if (!original_fork) {
		rmonitor_helper_initialize();
		assert(original_fork);
	}

	pid = original_fork();

	if (pid > 0) {
		debug(D_RMON, "fork from %d -> %d\n", getpid(), pid);
		write_to_file_of_pids(pid);
	} else if (pid < 0) {
		debug(D_RMON, "fork error: %s\n", strerror(errno));
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

int exit_wrapper(int status)
{
	static int did_exit_wrapper = 0;

	if (did_exit_wrapper) {
		return status;
	}

	did_exit_wrapper = 1;

	pid_t pid = getpid();

	debug(D_RMON, "exit from %d\n", pid);
	write_to_file_of_pids(-pid);

	return status;
}

void exit(int status)
{
	if (!original_exit) {
		syscall(SYS_exit, status);
	}

	exit_wrapper(status);
	original_exit(status);

	/* we exited in the above line. The next line is to make the compiler
	   happy with noreturn warnings. */
	exit(status);
}

void _exit(int status)
{
	if (!original_exit) {
		syscall(SYS_exit, status);
	}

	exit_wrapper(status);
	original_exit(status);

	/* we exited in the above line. The next line is to make the compiler
	   happy with noreturn warnings. */
	exit(status);
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
	exit_wrapper(0);
}

#endif

/* vim: set noexpandtab tabstop=4: */
