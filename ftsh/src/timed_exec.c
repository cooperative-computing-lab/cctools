/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2005- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "timed_exec.h"
#include "ftsh_error.h"
#include "stringtools.h"
#include "full_io.h"
#include "multi_fork.h"

#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <signal.h>
#include <sys/wait.h>
#include <time.h>
#include <stdio.h>
#include <errno.h>
#include <string.h>
#include <fcntl.h>

timed_exec_t timed_exec( int line, const char *path, char **argv, int fds[3], pid_t *pid, int *status, time_t stoptime )
{
	int fresult;
	int pfds[2];
	int child_errno;
	int actual;
	struct multi_fork_status s;

	actual = pipe(pfds);
	if(actual!=0) return TIMED_EXEC_NOEXEC;

	fresult = multi_fork(1,&s,stoptime,line);
	if(fresult>=0) {
		/* Move our standard I/O streams into the expected places. */
		/* It seems that cygwin doesn't like dup2 on the same fd. */

		int i, maxfd;

		for( i=0; i<=2; i++ ) {
			if( fds[i]!=i ) {
				if( dup2(fds[i],i) != i ) {
					ftsh_error(FTSH_ERROR_PROCESS,line,"failure to dup2(%d,%d): %s\n",fds[i],i,strerror(errno));
					goto done;
				}
			}
		}

		/* Close all of the file descriptors that we don't need. */

		maxfd = sysconf( _SC_OPEN_MAX );
		if(maxfd<=0) maxfd = 255;
		for(i=3;i<maxfd;i++) {
			if(i==pfds[1]) continue;
			close(i);
		}

		/* Set the pipe to automatically close after exec. */

		if( fcntl(pfds[1],F_SETFD,FD_CLOEXEC)==0 ) {
			setsid();
			execvp(path,argv);
		}

		/*
		If anything goes wrong, write the errno to the pipe,
		where the parent process can collect and print it.
		*/

		done:
		child_errno = errno;
		full_write(pfds[1],&child_errno,sizeof(child_errno));
		_exit(1);

	} else {

		/*
	        Now clear the pipe.  If it contains an int, then the process
	        forked, but was unable to exec.  Set the reason appropriately.
	        Otherwise, live with what we have.
		*/

		close(pfds[1]);
		actual = full_read(pfds[0],&child_errno,sizeof(int));
		close(pfds[0]);
	
		*status = s.status;
		*pid = s.pid;

		if(actual==sizeof(int)) {
			return TIMED_EXEC_NOEXEC;
		} else if(fresult==MULTI_FORK_SUCCESS) {
			return TIMED_EXEC_SUCCESS;
		} else if(fresult==MULTI_FORK_TIMEOUT) {
			return TIMED_EXEC_TIMEOUT;
		} else {
			return TIMED_EXEC_FAILURE;
		}
	}
}



/* vim: set noexpandtab tabstop=4: */
