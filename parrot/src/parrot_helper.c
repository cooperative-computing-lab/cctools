/*
Copyright (C) 2022 The University of Notre Dame
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

#if defined(__GLIBC__) && \
		((__GLIBC__ << 16) + __GLIBC_MINOR__ <= (2 << 16) + 30) && \
		(defined(_BSD_SOURCE) || defined(_DEFAULT_SOURCE))
	typedef struct timezone * _cctools_timezone_ptr_t;
#else
	typedef void * _cctools_timezone_ptr_t;
#endif
/*
The `gettimeofday()` function (and especially the second argument) seem
to have been deprecated for a while. In v2.31 glibc recently changed
the way it's defined in sys/time.h. Previously, the choice of feature
macros would result in `tz` being decalred as either a `struct timezone *`
or a `void *`. The `struct timezone *` form appears to be an older (possibly
BSD or SysV derived?) feature, while Linux declares a `void *` expects `NULL`.
Apparently glibc finally dropped support for the former mode. Unfortunately,
this a problem for Parrot. Just from the distros we build for (and a bleeding
edge one like Arch or the Nix environment from #2327), I saw several
different behaviors:
+ `__USE_BSD` switches to the `struct timezone *` form. This is an internal
  macro, turned on via `_BSD_SOURCE`. `_BSD_SOURCE` and `_SVID_SOURCE` are
  deprecated aliases for `_DEFAULT_SOURCE`.
+ `__USE_MISC` instead. This is turned on by either `_BSD_SOURCE` or
  `_SVID_SOURCE` (`_DEFAULT_SOURCE` on newer versions).
+ No feature macros at all. Current/future glibc versions only allow
  the `void *` form.

To complicate things, defining `_GNU_SOURCE` (and possibly `-std=`) can imply
features. Thus depending on glibc version our default `CFLAGS` resulted in the
`struct timezone *` form on some platforms, and `void *` on others.

We detect this glibc quirk here, and while ugly this is the only way I could
find to fix this that doesn't have the chance to introduce weird type
mismatches. Trying to work around the glibc headers by compiling with
different features defined or by avoiding certain headers might get things
to compile in the short term, but risks creating a mismatch if a feature macro
changes the typedefs e.g. `off_t` can vary in width depending on which
features are defined. A mismatch between header typedefs would be terrible
to debug, as we'd see either stack corruption or silent changes
to function arguments.

Thanks, glibc....
*/

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
