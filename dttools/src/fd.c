/*
 * Copyright (C) 2022 The University of Notre Dame
 * This software is distributed under the GNU General Public License.
 * See the file COPYING for details.
*/

#include "debug.h"

#include <unistd.h>

#include <fcntl.h>

#include <sys/stat.h>

#include <errno.h>
#include <limits.h>
#include <stdlib.h>
#include <string.h>

#if defined(OPEN_MAX)
#  define FD_MAX  OPEN_MAX
#elif defined(_POSIX_OPEN_MAX)
#  define FD_MAX  _POSIX_OPEN_MAX
#else
#  define FD_MAX  256  /* guess */
#endif

int fd_max (void)
{
	errno = 0;
	int max = (int) sysconf(_SC_OPEN_MAX);
	if (max == -1) {
		if (errno == 0) {
			max = FD_MAX;
		} else {
			debug(D_DEBUG, "sysconf(_SC_OPEN_MAX) error: %s", strerror(errno));
			exit(EXIT_FAILURE);
		}
	}
	return max;
}

int fd_nonstd_close (void)
{
	int fd, max = fd_max();
	for (fd = STDERR_FILENO+1; fd < max; fd++) {
		if (close(fd) == -1 && errno != EBADF) {
			debug(D_DEBUG, "could not close open file descriptor: %s", strerror(errno));
			return errno;
		}
	}
	return 0;
}

int fd_null (int fd, int oflag)
{
	int rc;
	int fdn;
start:
	fdn = open("/dev/null", oflag);
	if (fdn == -1) {
		if (errno == EINTR)
			goto start;
		else
			return -1;
	}
	if (dup2(fdn, fd) == -1) {
		rc = errno;
		goto out;
	}
	rc = 0;
out:
	while (close(fdn) == -1 && errno == EINTR) ; /* nothing we can do if this fails any other way */
	return rc;
}

/* vim: set noexpandtab tabstop=8: */
