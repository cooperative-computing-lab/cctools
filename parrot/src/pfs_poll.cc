/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2005- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "pfs_critical.h"
#include "pfs_paranoia.h"
#include "pfs_poll.h"
#include "pfs_process.h"

extern "C" {
#include "macros.h"
#include "debug.h"
}

#include <unistd.h>

#include <fcntl.h>
#include <poll.h>

#include <assert.h>
#include <errno.h>
#include <signal.h>
#include <stdio.h>
#include <string.h>

#define POLL_TIME_MAX 1

#define POLL_TABLE_MAX 4096
#define SLEEP_TABLE_MAX 4096

struct poll_entry {
	int fd;
	pid_t pid;
	int flags;
};

struct sleep_entry {
	struct timeval stoptime;
	pid_t pid;
};

static struct poll_entry poll_table[POLL_TABLE_MAX];
static struct sleep_entry sleep_table[SLEEP_TABLE_MAX];

static int poll_table_size = 0;
static int sleep_table_size = 0;
static int poll_abort_now = 0;

extern void install_handler( int sig, void (*handler)(int sig));
extern void handle_sigchld( int sig );

void pfs_poll_abort()
{
	poll_abort_now = 1;
}

void pfs_poll_init()
{
	int i;
	for(i=0;i<POLL_TABLE_MAX;i++) {
		poll_table[i].pid = -1;
	}
	for(i=0;i<SLEEP_TABLE_MAX;i++) {
		sleep_table[i].pid = -1;
	}
}

void pfs_poll_clear( int pid )
{
	int i;
	for(i=0;i<poll_table_size;i++) {
		if(poll_table[i].pid==pid) poll_table[i].pid = -1;
	}
	for(i=0;i<sleep_table_size;i++) {
		if(sleep_table[i].pid==pid) sleep_table[i].pid = -1;
	}
}

void pfs_poll_sleep()
{
	struct pollfd fds[POLL_TABLE_MAX];
	int n = 0;
	struct timeval curtime;
	struct timeval stoptime;
	struct timespec sleeptime;
	int i;

	poll_abort_now = 0;

	gettimeofday(&curtime,0);
	stoptime = curtime;
	stoptime.tv_sec += POLL_TIME_MAX;

	for(i=0;i<poll_table_size;i++) {
		struct poll_entry *p = &poll_table[i];
		if(p->pid>=0) {
			fds[n].fd = p->fd;
			fds[n].events = 0;
			fds[n].revents = 0;
			if(p->flags&PFS_POLL_READ) fds[n].events |= POLLIN;
			if(p->flags&PFS_POLL_WRITE) fds[n].events |= POLLOUT;
			if(p->flags&PFS_POLL_EXCEPT) fds[n].events |= POLLERR;
			n += 1;
		}
	}
	// Also poll watchdog fd, if necessary.
	pid_t pfs_watchdog_fd = -1;
	if ((pfs_watchdog_fd = pfs_paranoia_monitor_fd()) > 0) {
		fds[n].fd = pfs_watchdog_fd;
		fds[n].events = POLLIN;
		fds[n].revents = 0;
		n += 1;
	}

	for(i=0;i<sleep_table_size;i++) {
		struct sleep_entry *s = &sleep_table[i];
		if(s->pid>=0) {
			if(timercmp(&stoptime,&s->stoptime,>)) {
				stoptime = s->stoptime;
			}
		}
	}

	sleeptime.tv_sec = stoptime.tv_sec - curtime.tv_sec;
	sleeptime.tv_nsec = 1000 * (stoptime.tv_usec - curtime.tv_usec);

	while(sleeptime.tv_nsec<0) {
		sleeptime.tv_nsec += 1000000000;
		sleeptime.tv_sec -= 1;
	}

	if(sleeptime.tv_sec<0 || poll_abort_now) {
		sleeptime.tv_sec = 0;
		sleeptime.tv_nsec = 0;
	}

	sigset_t sigmask;
	sigemptyset(&sigmask);
	sigaddset(&sigmask, SIGPIPE);

	int result = ppoll(fds, n, &sleeptime, &sigmask);

	if(result>0) {
		/* Note: fds[n-1] is always watchdog if pfs_watchdog_fd > 0 */
		if ((pfs_watchdog_fd > 0) && (fds[n-1].revents & POLLIN)) {
			debug(D_NOTICE,"watchdog died unexpectedly; killing everyone.");
			pfs_process_kill_everyone(SIGKILL);
			// Note - above does not return.
		}
		for(i = 0; i < n; i++) {
			if(fds[i].revents) {
				int j;
				char event[64] = "";

				if(fds[i].revents & POLLIN)
					strcat(event, "|POLLIN");
				if(fds[i].revents & POLLOUT)
					strcat(event, "|POLLOUT");
				if(fds[i].revents & POLLERR)
					strcat(event, "|POLLERR");
				if(fds[i].revents & POLLHUP)
					strcat(event, "|POLLHUP");
				if(fds[i].revents & POLLNVAL)
					strcat(event, "|POLLNVAL");
				assert(strlen(event));
				debug(D_DEBUG, "poll: got event %s on fd %d", event+1, fds[i].fd);

				for(j = 0; j < poll_table_size; j++) {
					struct poll_entry *p = &poll_table[j];
					pid_t pid = p->pid;
					int fd = p->fd;
					if(pid >= 0 && fd == fds[i].fd) {
						debug(D_POLL,"waking pid %d because of fd %d",pid,fd);
						pfs_poll_clear(pid);
						pfs_process_wake(pid);
					}
				}
			}
		}
	} else if(result==0) {
		// select timed out, which should never happen, except
		// that it does when the jvm linked with hdfs sets up its
		// signal handlers to avoid sigchld.  In that case, re-install
		install_handler(SIGCHLD,handle_sigchld);

		gettimeofday(&curtime,0);

		for(i=0;i<sleep_table_size;i++) {
			struct sleep_entry *s = &sleep_table[i];
			pid_t pid = s->pid;
			if(pid>=0) {
				if(timercmp(&curtime,&s->stoptime,>)) {
					debug(D_POLL,"waking pid %d because time expired",pid);
					pfs_poll_clear(pid);
					pfs_process_wake(pid);
				}
			}
		}
	} else if(errno==EBADF) {
		debug(D_POLL,"select returned EBADF, which really shouldn't happen.");
		debug(D_POLL,"waking up all processes to clean up and try again.");

		for(i=0;i<poll_table_size;i++) {
			struct poll_entry *p = &poll_table[i];
			pid_t pid = p->pid;
			if(pid>=0) {
				debug(D_POLL,"waking pid %d",pid);
				pfs_poll_clear(pid);
				pfs_process_wake(pid);
			}
		}
	}
}

void pfs_poll_wakeon( int fd, int flags )
{
	struct poll_entry *p;
	int i;

	debug(D_POLL,"wake on fd %d flags %s",fd,pfs_poll_string(flags));

	for(i=0;i<POLL_TABLE_MAX;i++) {
		p = &poll_table[i];
		if(p->pid<0) {
			p->fd = fd;
			p->pid = pfs_process_getpid();
			p->flags = flags;
			if(i>=poll_table_size) poll_table_size=i+1;
			return;
		}
	}

	fatal("ran out of poll table space!");
}

void pfs_poll_wakein( struct timeval tv )
{
	struct sleep_entry *s;
	int i;

	debug(D_POLL,"wake in time %d.%06d",(int)tv.tv_sec,(int)tv.tv_usec);

	for(i=0;i<SLEEP_TABLE_MAX;i++) {
		s = &sleep_table[i];
		if(s->pid<0) {
			s->pid = pfs_process_getpid();
			gettimeofday(&s->stoptime,0);
			s->stoptime.tv_sec += tv.tv_sec;
			s->stoptime.tv_usec += tv.tv_usec;
			while(s->stoptime.tv_usec>1000000) {
				s->stoptime.tv_sec++;
				s->stoptime.tv_usec-=1000000;
			}
			if(i>=sleep_table_size) sleep_table_size=i+1;
			return;
		}
	}

	fatal("ran out of sleep table space!");
}

char * pfs_poll_string( int flags )
{
	static char str[4];
	if(flags&PFS_POLL_READ) {
		str[0] = 'r';
	} else {
		str[0] = '-';
	}
	if(flags&PFS_POLL_WRITE) {
		str[1] = 'w';
	} else {
		str[1] = '-';
	}
	if(flags&PFS_POLL_EXCEPT) {
		str[2] = 'e';
	} else {
		str[2] = '-';
	}
	str[3] = 0;
	return str;
}

/* vim: set noexpandtab tabstop=4: */
