#include "daemon.h"

#include "debug.h"

#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>

#include <errno.h>
#include <limits.h>
#include <stdlib.h>
#include <string.h>

#if defined(OPEN_MAX)
#define DAEMON_FD_MAX  OPEN_MAX
#elif defined(_POSIX_OPEN_MAX)
#define DAEMON_FD_MAX  _POSIX_OPEN_MAX
#else
#define DAEMON_FD_MAX  0
#endif

#define OPEN_MAX_GUESS  256

static int fd_max (void)
{
	errno = 0;
	int max = (int) sysconf(_SC_OPEN_MAX);
	if (max == -1) {
		if (errno == 0) {
			if (DAEMON_FD_MAX == 0) { /* indefinite limit */
				max = OPEN_MAX_GUESS;
			} else {
				max = DAEMON_FD_MAX;
			}
		} else {
			debug(D_DEBUG, "sysconf(_SC_OPEN_MAX) error: %s", strerror(errno));
			exit(EXIT_FAILURE);
		}
	}
	return max;
}

void daemonize (int cdroot)
{
	/* Become seesion leader and lose controlling terminal */
	pid_t pid = fork();
	if (pid < 0) {
		debug(D_DEBUG, "could not fork: %s", strerror(errno));
		exit(EXIT_FAILURE);
	} else if (pid > 0) {
		exit(EXIT_SUCCESS); /* exit parent */
	}

	pid_t group = setsid();
	if (group == (pid_t) -1) {
		debug(D_DEBUG, "could not create session: %s", strerror(errno));
		exit(EXIT_FAILURE);
	}

	/* Second fork ensures process cannot acquire controlling terminal */
	pid = fork();
	if (pid < 0) {
		debug(D_DEBUG, "could not fork: %s", strerror(errno));
		exit(EXIT_FAILURE);
	} else if (pid > 0) {
		exit(EXIT_SUCCESS); /* exit parent */
	}

	if (cdroot){
		int status = chdir("/");
		if (status == -1) {
			debug(D_DEBUG, "could not chdir to `/': %s", strerror(errno));
			exit(EXIT_FAILURE);
		}
	}

	umask(0);

	int fd, max = fd_max();
	for (fd = STDERR_FILENO+1; fd < max; fd++) {
		if (close(fd) == -1 && errno != EBADF) {
			debug(D_DEBUG, "could not close open file descriptor: %s", strerror(errno));
			exit(EXIT_FAILURE);
		}
	}

    FILE *file0 = freopen("/dev/null", O_RDONLY, stdin);
    if (file0 == NULL) {
        debug(D_DEBUG, "could not reopen stdin: %s", strerror(errno));
        exit(EXIT_FAILURE);
    }
    FILE *file1 = freopen("/dev/null", O_WRONLY, stdout);
    if (file1 == NULL) {
        debug(D_DEBUG, "could not reopen stdout: %s", strerror(errno));
        exit(EXIT_FAILURE);
    }
    FILE *file2 = freopen("/dev/null", O_WRONLY, stderr);
    if (file2 == NULL) {
        debug(D_DEBUG, "could not reopen stderr: %s", strerror(errno));
        exit(EXIT_FAILURE);
    }
}
