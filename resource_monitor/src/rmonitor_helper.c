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
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>
#include <dlfcn.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/ipc.h>
#include <sys/sem.h>
#include <limits.h>

#ifdef __linux__
#include <sched.h>
#endif

#include "debug.h"

#include "rmonitor_helper_comm.h"

#define BUFFER_MAX 1024

pid_t waitpid(pid_t pid, int *status, int options)
{
	pid_t pidb;
	typeof(waitpid) *original_waitpid = dlsym(RTLD_NEXT, "waitpid");

	debug(D_DEBUG, "waiting from %d.\n", getpid());
	pidb = original_waitpid(pid, status, options);

	if(WIFEXITED(status) || WIFSIGNALED(status))
	{
		struct monitor_msg msg;
		msg.type   = END;
		msg.origin = getpid();
		msg.data.p = pidb;

		send_monitor_msg(&msg);
	}

	return pid;
}

pid_t wait(int *status)
{
	return waitpid(-1, status, 0);
}

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

