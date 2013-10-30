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

/*
An ugly bug in Linux makes it impossible to ptrace across vfork
reliably, so the helper library converts vforks into forks,
which can be supported.  Other hacks may be added as necessary.
*/

#include <unistd.h>
#include <sys/types.h>
#include <syscall.h>

pid_t vfork()
{
	return fork();
}

pid_t __vfork()
{
	return fork();
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
