/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2005- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "pfs_poll.h"
#include "pfs_process.h"
#include "pfs_critical.h"
#include "pfs_paranoia.h"

extern "C" {
#include "macros.h"
#include "debug.h"
}
#include <string.h>
#include <errno.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/select.h>
#include <stdio.h>
#include <signal.h>
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

extern void install_handler(int sig, void (*handler) (int sig));
extern void handle_sigchld(int sig);

void pfs_poll_abort()
{
	poll_abort_now = 1;
}

void pfs_poll_init()
{
	int i;
	for(i = 0; i < POLL_TABLE_MAX; i++) {
		poll_table[i].pid = -1;
	}
	for(i = 0; i < SLEEP_TABLE_MAX; i++) {
		sleep_table[i].pid = -1;
	}
}

void pfs_poll_clear(int pid)
{
	int i;
	for(i = 0; i < poll_table_size; i++) {
		if(poll_table[i].pid == pid)
			poll_table[i].pid = -1;
	}
	for(i = 0; i < sleep_table_size; i++) {
		if(sleep_table[i].pid == pid)
			sleep_table[i].pid = -1;
	}
}

void pfs_poll_sleep()
{
	struct timeval curtime;
	struct timeval stoptime;
	struct timespec sleeptime;
	fd_set rfds, wfds, efds;
	struct sleep_entry *s;
	struct poll_entry *p;
	int maxfd = 0, i;
	int result;

	FD_ZERO(&rfds);
	FD_ZERO(&wfds);
	FD_ZERO(&efds);

	poll_abort_now = 0;

	gettimeofday(&curtime, 0);
	stoptime = curtime;
	stoptime.tv_sec += POLL_TIME_MAX;

	for(i = 0; i < poll_table_size; i++) {
		p = &poll_table[i];
		if(p->pid >= 0) {
			maxfd = MAX(p->fd + 1, maxfd);
			if(p->flags & PFS_POLL_READ)
				FD_SET(p->fd, &rfds);
			if(p->flags & PFS_POLL_WRITE)
				FD_SET(p->fd, &wfds);
			if(p->flags & PFS_POLL_EXCEPT)
				FD_SET(p->fd, &efds);
		}
	}
	// Also poll watchdog fd, if necessary.
	pid_t pfs_watchdog_fd = -1;
	if((pfs_watchdog_fd = pfs_paranoia_monitor_fd()) > 0) {
		maxfd = MAX(pfs_watchdog_fd, maxfd);
		FD_SET(pfs_watchdog_fd, &rfds);
	}

	for(i = 0; i < sleep_table_size; i++) {
		s = &sleep_table[i];
		if(s->pid >= 0) {
			if(timercmp(&stoptime, &s->stoptime, >)) {
				stoptime = s->stoptime;
			}
		}
	}

	sleeptime.tv_sec = stoptime.tv_sec - curtime.tv_sec;
	sleeptime.tv_nsec = 1000 * (stoptime.tv_usec - curtime.tv_usec);

	while(sleeptime.tv_nsec < 0) {
		sleeptime.tv_nsec += 1000000000;
		sleeptime.tv_sec -= 1;
	}

	if(sleeptime.tv_sec < 0 || poll_abort_now) {
		sleeptime.tv_sec = 0;
		sleeptime.tv_nsec = 0;
	}

	sigset_t childmask;
	sigemptyset(&childmask);
	sigaddset(&childmask, SIGPIPE);

	result = pselect(maxfd, &rfds, &wfds, &efds, &sleeptime, &childmask);

	if(result > 0) {
		if((pfs_watchdog_fd > 0) && FD_ISSET(pfs_watchdog_fd, &rfds)) {
			debug(D_NOTICE, "watchdog died unexpectedly; killing everyone.");
			pfs_process_kill_everyone(SIGKILL);
			// Note - above does not return.
		}
		for(i = 0; i < poll_table_size; i++) {
			p = &poll_table[i];
			if(p->pid >= 0) {
				if((p->flags & PFS_POLL_READ && FD_ISSET(p->fd, &rfds)) || (p->flags & PFS_POLL_WRITE && FD_ISSET(p->fd, &wfds)) || (p->flags & PFS_POLL_EXCEPT && FD_ISSET(p->fd, &efds))
					) {
					pid_t pid = p->pid;
					debug(D_POLL, "waking pid %d because of fd %d", pid, p->fd);
					pfs_poll_clear(pid);
					pfs_process_wake(pid);
				}
			}
		}
	} else if(result == 0) {
		// select timed out, which should never happen, except
		// that it does when the jvm linked with hdfs sets up its
		// signal handlers to avoid sigchld.  In that case, re-install
		install_handler(SIGCHLD, handle_sigchld);

		gettimeofday(&curtime, 0);

		for(i = 0; i < sleep_table_size; i++) {
			s = &sleep_table[i];
			if(s->pid >= 0) {
				if(timercmp(&curtime, &s->stoptime, >)) {
					pid_t pid = s->pid;
					debug(D_POLL, "waking pid %d because time expired", pid);
					pfs_poll_clear(pid);
					pfs_process_wake(pid);
				}
			}
		}
	} else if(errno == EBADF) {
		debug(D_POLL, "select returned EBADF, which really shouldn't happen.");
		debug(D_POLL, "waking up all processes to clean up and try again.");

		for(i = 0; i < poll_table_size; i++) {
			p = &poll_table[i];
			if(p->pid >= 0) {
				pid_t pid = p->pid;
				debug(D_POLL, "waking pid %d", pid);
				pfs_poll_clear(pid);
				pfs_process_wake(pid);
			}
		}
	}

}

void pfs_poll_wakeon(int fd, int flags)
{
	struct poll_entry *p;
	int i;

	debug(D_POLL, "wake on fd %d flags %s", fd, pfs_poll_string(flags));

	for(i = 0; i < POLL_TABLE_MAX; i++) {
		p = &poll_table[i];
		if(p->pid < 0) {
			p->fd = fd;
			p->pid = pfs_process_getpid();
			p->flags = flags;
			if(i >= poll_table_size)
				poll_table_size = i + 1;
			return;
		}
	}

	fatal("ran out of poll table space!");
}

void pfs_poll_wakein(struct timeval tv)
{
	struct sleep_entry *s;
	int i;

	debug(D_POLL, "wake in time %d.%06d", tv.tv_sec, tv.tv_usec);

	for(i = 0; i < SLEEP_TABLE_MAX; i++) {
		s = &sleep_table[i];
		if(s->pid < 0) {
			s->pid = pfs_process_getpid();
			gettimeofday(&s->stoptime, 0);
			s->stoptime.tv_sec += tv.tv_sec;
			s->stoptime.tv_usec += tv.tv_usec;
			while(s->stoptime.tv_usec > 1000000) {
				s->stoptime.tv_sec++;
				s->stoptime.tv_usec -= 1000000;
			}
			if(i >= sleep_table_size)
				sleep_table_size = i + 1;
			return;
		}
	}

	fatal("ran out of sleep table space!");
}

char *pfs_poll_string(int flags)
{
	static char str[4];
	if(flags & PFS_POLL_READ) {
		str[0] = 'r';
	} else {
		str[0] = '-';
	}
	if(flags & PFS_POLL_WRITE) {
		str[1] = 'w';
	} else {
		str[1] = '-';
	}
	if(flags & PFS_POLL_EXCEPT) {
		str[2] = 'e';
	} else {
		str[2] = '-';
	}
	str[3] = 0;
	return str;
}
