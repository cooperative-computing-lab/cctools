/*
Copyright (C) 2008- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

/*
This helper library works alongside the main parrot code in
order to avoid operations that are complex or unsupported by Parrot.
Parrot locates the helper library and uses the LD_PRELOAD linker
feature to force it into dynamically linked programs.  It can
then be used to fix a few problems that are more easy to solve
from the user side.
*/

#include <unistd.h>
#include <sys/types.h>
#include <syscall.h>
#include <time.h>
#include <sys/time.h>

/*
An ugly bug in Linux makes it impossible to ptrace across vfork
reliably, so the helper library converts vforks into forks,
which can be supported.  Other hacks may be added as necessary.
*/

pid_t vfork()
{
	return fork();
}

pid_t __vfork()
{
	return fork();
}

/*
Linux has a special fast path for a few time related system calls.
The standard library implementations of gettimeofday, time, and
clock_gettime work by simply reading a word out of a segment (VDSO)
specially mapped between the kernel and all processes.
These three functions un-do this optimization and force the calls
to be real system calls instead, which allows parrot to play games
with time, as needed.

Note that the helper is only activated in special cases (like time warp mode)
so that not all programs will pay this performance penalty.
*/

// Thanks, glibc...
#if defined(__GLIBC__) && \
		((__GLIBC__ << 16) + __GLIBC_MINOR__ <= (2 << 16) + 30) && \
		(defined(_BSD_SOURCE) || defined(_DEFAULT_SOURCE))
	typedef struct timezone * _cctools_timezone_ptr_t;
#else
	typedef void * _cctools_timezone_ptr_t;
#endif

int gettimeofday( struct timeval *tv, _cctools_timezone_ptr_t tz )
{
	return syscall(SYS_gettimeofday,tv,tz);
}

time_t time( time_t *t )
{
	time_t result = syscall(SYS_time);
	if(t) *t = result;
	return result;
}

int clock_gettime(clockid_t clk_id, struct timespec *tp)
{
	return syscall(SYS_clock_gettime,clk_id,tp);
}

/*
Some applications do not deal with all of the valid behaviors
of the write() system call.  (Yes, really.)  write() is allowed
to return fewer bytes than actually requested, leaving the application
responsible for retrying the operation.  This condition is slightly
more common in Parrot than in Linux, and thus tends to trigger
bugs in applications.  The solution here is to modify the program's
definition of write to retry automatically.
*/

ssize_t write( int fd, const void *vbuffer, size_t length )
{
	ssize_t total = 0;
	ssize_t actual = 0;
	const char *buffer = vbuffer;

	while(length>0) {
		actual = syscall(SYS_write,fd,buffer,length);
		if(actual<=0) break;

		total += actual;
		buffer += actual;
		length -= actual;
	}

	if(total>0) {
		return total;
	} else {
		return actual;
	}
}


/* vim: set noexpandtab tabstop=4: */
