/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include <signal.h>
#include <unistd.h>
#include <sys/wait.h>
#include <sys/types.h>
#include <string.h>
#include <errno.h>

#include "full_io.h"
#include "get_canonical_path.h"

/*
This is a little strange, but bear with me.
What we want to do is get the absolutely canonical
path name for a given path that we have constructed.
The most efficient way is through the OS's getcwd mechanism.
However, we don't want to change our own cwd, as we
may not be able to get it back.  So, fork a process,
attempt to cwd, then get the cwd and write it back on
a pipe.  Expensive but reliable.

This function exists to handle a very narrow use case within
the chirp_server.  Most of the time getcwd() is what you want
to use.
*/

int get_canonical_path(const char *path, char *canonical, int length)
{
	pid_t pid;
	int fds[2];
	int result = -1;
	int actual;
	int save_errno = 0;

	signal(SIGPIPE, SIG_IGN);

	if(pipe(fds) != 0)
		return 0;

	pid = fork();
	if(pid == 0) {
		close(fds[0]);
		result = chdir(path);
		if(result != 0)
			_exit(errno);

		if(getcwd(canonical, length)) {
			full_write(fds[1], canonical, strlen(canonical));
			_exit(0);
		} else {
			_exit(errno);
		}
	} else if(pid > 0) {
		int status;
		int value;
		pid_t gotpid;

		close(fds[1]);

		while(1) {
			gotpid = waitpid(pid, &status, 0);
			if(gotpid >= 0) {
				break;
			} else {
				if(errno == EINTR) {
					continue;
				} else {
					break;
				}
			}
		}

		if(gotpid == pid && WIFEXITED(status)) {
			value = WEXITSTATUS(status);
			if(value == 0) {
				actual = full_read(fds[0], canonical, length);
				if(actual > 0) {
					canonical[actual] = 0;
					result = actual;
				} else {
					save_errno = EACCES;
					result = -1;
				}
			} else {
				save_errno = value;
				result = -1;
			}
		} else {
			save_errno = EACCES;
			result = -1;
		}
		close(fds[0]);
	} else {
		save_errno = errno;
		close(fds[0]);
		close(fds[1]);
		result = -1;
	}

	if(save_errno)
		errno = save_errno;
	return result;
}

/* vim: set noexpandtab tabstop=8: */
