/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2005- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "multi_fork.h"
#include "ftsh_error.h"
#include "stringtools.h"
#include "cancel.h"
#include "macros.h"

#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <string.h>
#include <sys/wait.h>
#include <signal.h>

int multi_fork_kill_timeout = 30;
int multi_fork_kill_mode = MULTI_FORK_KILL_MODE_STRONG;

/*
Fork off n processes without any fault-tolerance.
*/

static int multi_start( int n, struct multi_fork_status *p, time_t stoptime, int line )
{
	int i;
	pid_t pid;

	for(i=0;i<n;i++) {
		if(cancel_pending()) return MULTI_FORK_FAILURE;
		if(stoptime && (time(0)>stoptime)) return MULTI_FORK_TIMEOUT;
		pid = fork();
		if(pid==0) {
			return i;
		} else if(pid>0) {
			ftsh_error(FTSH_ERROR_PROCESS,line,"started new process %d",pid);
			p[i].pid = pid;
			p[i].state = MULTI_FORK_STATE_RUNNING;
		} else {
			ftsh_error(FTSH_ERROR_FAILURE,line,"couldn't create new process: %s\n",strerror(errno));
			return MULTI_FORK_FAILURE;
		}
	}

	return MULTI_FORK_SUCCESS;
}

/*
Wait for these n processes to complete,
allowing for a timeout or an incoming cancel signal, if requested.
*/

static int multi_wait( int n, struct multi_fork_status *p, time_t stoptime, int line, int stop_on_failure )
{
	int status;
	int interval;
	int i;
	pid_t pid;
	int total;

	while(1) {
		total=0;

		for(i=0;i<n;i++) {
			if( p[i].state==MULTI_FORK_STATE_GRAVE ) {
				total++;
			}
		}

		if(total>=n) return MULTI_FORK_SUCCESS;
		if(stop_on_failure && cancel_pending()) return MULTI_FORK_FAILURE;

		if(stoptime) {
			interval = stoptime-time(0);
			if(interval<=0) {
				return MULTI_FORK_TIMEOUT;
			} else {
				alarm(interval);
			}
		} else {
			/* Although we hope that this algorithm is correct, there are many ways to get it wrong, so regardless, bail out every 10 seconds and reconsider. */
			alarm(10);
		}

		pid = waitpid(-1,&status,0);
		if(pid>0) {
			ftsh_error(FTSH_ERROR_PROCESS,line,"process %d has completed",pid);
			for(i=0;i<n;i++) {
				if( p[i].state==MULTI_FORK_STATE_RUNNING && p[i].pid==pid ) {
					p[i].status = status;
					p[i].state = MULTI_FORK_STATE_GRAVE;
					if(WIFEXITED(status)&&(WEXITSTATUS(status)==0)) {
						break;
					} else if(stop_on_failure) {
						return MULTI_FORK_FAILURE;
					} else {
						break;
					}
				}
			}
		}
	}
}

/*
Attempt to kill a set of running processes.
First, send a gentle signal to all, then wait
to see if they exit voluntarily.  After that,
start killing forcibly.  If the kill mode is
strong, then keep killing every five seconds
until they exit.  If not, assume they are dead.
*/

static void multi_kill( int n, struct multi_fork_status *p, time_t stoptime, int line )
{
	int i;

	for( i=0; i<n; i++ ) {
		if(p[i].state==MULTI_FORK_STATE_CRADLE) {
			p[i].state = MULTI_FORK_STATE_GRAVE;
		} else if(p[i].state==MULTI_FORK_STATE_RUNNING) {
			ftsh_error(FTSH_ERROR_PROCESS,line,"sending SIGTERM to process %d",p[i].pid);
			kill(p[i].pid,SIGTERM);
			kill(-p[i].pid,SIGTERM);
		}
	}

	multi_wait(n,p,time(0)+multi_fork_kill_timeout,line,0);

	while(1) {
		int total=0;
		for( i=0; i<n; i++ ) {
			if(p[i].state==MULTI_FORK_STATE_RUNNING) {
				ftsh_error(FTSH_ERROR_PROCESS,line,"%d: sending SIGKILL to process %d",i,p[i].pid);
				kill(p[i].pid,SIGKILL);
				kill(-p[i].pid,SIGKILL);
				total++;
			}
		}
		if( total==0 ) break;
		if( multi_fork_kill_mode==MULTI_FORK_KILL_MODE_WEAK ) break;
		multi_wait(n,p,time(0)+5,line,0);
	}
}

int multi_fork( int n, struct multi_fork_status *p, time_t stoptime, int line )
{
	int i, result;

	for( i=0; i<n; i++ ) {
		p[i].state = MULTI_FORK_STATE_CRADLE;
	}

	cancel_hold();

	result = multi_start(n,p,stoptime,line);
	if(result==MULTI_FORK_SUCCESS) {
		result = multi_wait(n,p,stoptime,line,1);
		if(result!=MULTI_FORK_SUCCESS) {
			multi_kill(n,p,stoptime,line);
		}
	}

	cancel_release();

	return result;
}


/* vim: set noexpandtab tabstop=4: */
