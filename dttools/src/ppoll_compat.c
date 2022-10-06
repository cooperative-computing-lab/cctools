/*
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include <assert.h>
#include <signal.h>
#include <poll.h>
#include <time.h>
#include <errno.h>

#include "ppoll_compat.h"

static void noop(int sig) {}

int ppoll_compat(struct pollfd fds[], nfds_t nfds, int stoptime) {
	assert(fds);
	sigset_t mask;
	sigemptyset(&mask);
	int timeout = stoptime - time(NULL);
	if (timeout < 0) return 0;

#ifdef HAS_PPOLL
	struct timespec s;
	s.tv_nsec = 0;
	s.tv_sec = timeout;
	return ppoll(fds, nfds, &s, &mask);
#else
	sigset_t origmask;
	sigprocmask(SIG_SETMASK, &mask, &origmask);
	int rc = poll(fds, nfds, timeout*1000);
	int saved_errno = errno;
	sigprocmask(SIG_SETMASK, &origmask, NULL);
	errno = saved_errno;
	return rc;
#endif
}

void ppoll_compat_set_up_sigchld(void) {
	sigset_t mask;
	sigemptyset(&mask);
	sigaddset(&mask, SIGCHLD);
	sigprocmask(SIG_BLOCK, &mask, NULL);
	signal(SIGCHLD, noop);
}
