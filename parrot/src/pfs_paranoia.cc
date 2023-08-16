/*
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "pfs_paranoia.h"

#include <sys/types.h>
#include <stdlib.h>
#include <signal.h>
#include <errno.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/wait.h>
#include <unistd.h>
#include <string.h>

extern "C" {
#include "debug.h"
}

// Some global nasties
static int paranoia_mode = 0;
static pid_t *shared_table = NULL;
static int to_parent_fd = -1;
static int from_parent_fd = -1;
static int to_watchdog_fd = -1;
static int from_watchdog_fd = -1;
static pid_t watchdog_pid = -1;

// TODO: dynamically determine the maximum number of PIDs.
static const unsigned long max_pids = 128 * 1024 - 1;

static int pfs_paranoia_killall()
{
	pid_t *pid_ptr = shared_table;
	while((*pid_ptr) != 0) {
		if((*pid_ptr != 1) && (kill(*pid_ptr, SIGKILL) == -1)) {
			debug(D_NOTICE, "unable to kill process %d due to parrot death", *pid_ptr);
		}
		debug(D_PROCESS, "killing %d due to parrot death", *pid_ptr);
		pid_ptr++;
	}
	return 0;
}

static int pfs_paranoia_watchdog()
{
	fd_set fds;
	FD_ZERO(&fds);
	FD_SET(from_parent_fd, &fds);
	int ret;
	char value;

	do {
		while(-1 == (ret = select(from_parent_fd + 1, &fds, NULL, NULL, 0))) {
			if(errno != EINTR) {
				// TODO: log
				abort();
				break;
			}
		}
		if(1 == (ret = read(from_parent_fd, &value, 1))) {
			// Indication from parent to exit cleanly;
			_exit(0);
		} else if(ret == 0) {
			// Parent died unexpectedly; kill children.
			pfs_paranoia_killall();
			_exit(1);
			// Remaining cases, ret == -1, so errno is set.
		} else if(errno == EAGAIN) {
			continue;
		} else if(errno == EINTR) {
			continue;
		} else {
			// TODO: log
			abort();
		}
	} while(1);
}

#define SET_FLAG(fd, new_flag, fail_target) \
{ \
  flags = fcntl(fd, F_GETFL); \
  if (flags == -1) { \
	goto fail_target; \
  } \
  if (fcntl(fd, F_SETFL, flags | new_flag)) { \
	goto fail_target; \
  } \
}

int pfs_paranoia_setup(void)
{
	int flags;
	int tmp_pipe[2];
	pid_t pid;

	// Open a file appropriate for mmap
	char shared_mmap_template[] = "/tmp/shared_proc_tableXXXXXX";
	int shared_mmap_fd = mkstemp(shared_mmap_template);
	if(shared_mmap_fd == -1) {
		debug(D_NOTICE, "unable to create temporary file for mmap");
		goto fail_mkstemp;
	}
	if(unlink(shared_mmap_template) == -1) {
		debug(D_NOTICE, "unable to unlink temporary file for mmap");
		goto fail_mkstemp;
	}
	SET_FLAG(shared_mmap_fd, FD_CLOEXEC, fail_mkstemp);
	if(ftruncate(shared_mmap_fd, (max_pids + 1) * sizeof(pid_t)) == -1) {
		debug(D_NOTICE, "failed to resize shared mmap file");
		goto fail_mkstemp;
	}
	// Map out a shared memory segment for the child process.
	shared_table = (pid_t *) mmap(NULL, (max_pids + 1) * sizeof(pid_t), PROT_READ | PROT_WRITE, MAP_SHARED, shared_mmap_fd, 0);
	if(shared_table == MAP_FAILED) {
		debug(D_NOTICE, "failed to create mmap for watchdog (errno=%d, %s)", errno, strerror(errno));
		goto fail_unmap;
	}
	memset(shared_table, '\0', (max_pids + 1) * sizeof(pid_t));

	// Create two pipe pairs for communication
	if(pipe(tmp_pipe)) {
		// TODO: log error
		goto fail_pipe1;
	}
	SET_FLAG(tmp_pipe[0], FD_CLOEXEC | O_NONBLOCK, fail_pipe1);
	SET_FLAG(tmp_pipe[1], FD_CLOEXEC | O_NONBLOCK, fail_pipe1);
	from_watchdog_fd = tmp_pipe[0];
	to_parent_fd = tmp_pipe[1];
	if(pipe(tmp_pipe)) {
		// TODO: log error
		goto fail_pipe2;
	}
	SET_FLAG(tmp_pipe[0], FD_CLOEXEC | O_NONBLOCK, fail_pipe2);
	SET_FLAG(tmp_pipe[1], FD_CLOEXEC | O_NONBLOCK, fail_pipe2);
	from_parent_fd = tmp_pipe[0];
	to_watchdog_fd = tmp_pipe[1];

	debug(D_PROCESS, "about to fork watchdog process");
	// fork
	pid = fork();
	watchdog_pid = pid;
	if(pid == 0) {		// child
		// Close FDs not used;
		close(to_watchdog_fd);
		close(from_watchdog_fd);
		pfs_paranoia_watchdog();
		// Above shouldnt exit, but just in case...
		abort();
	} else if(pid == -1) {
		// TODO: log error
		goto fail_fork;
	}
	// Close FDs not used
	close(to_parent_fd);
	to_parent_fd = -1;
	close(from_parent_fd);
	from_parent_fd = -1;

	paranoia_mode = 1;
	return pid;

	// Resource cleanup.
	// Sigh - don't you wish you could toss a C++ exception here?
	  fail_fork:
	if(to_watchdog_fd != -1)
		close(to_watchdog_fd);
	if(from_watchdog_fd != -1)
		close(from_watchdog_fd);
	  fail_pipe2:
	if(to_parent_fd != -1)
		close(to_parent_fd);
	if(from_parent_fd != -1)
		close(from_parent_fd);
	  fail_pipe1:
	munmap(shared_table, (max_pids + 1) / sizeof(pid_t));
	  fail_unmap:
	close(shared_mmap_fd);
	  fail_mkstemp:
	return -1;
}

int pfs_paranoia_monitor_fd(void)
{
	return from_watchdog_fd;
}

int pfs_paranoia_cleanup(void)
{
	if(paranoia_mode == 0)
		return -1;

	int ret;
	char value = 'A';

	while(-1 == (ret = write(to_watchdog_fd, &value, 1))) {
		if(errno == EINTR) {
			continue;
		} else if((errno == EAGAIN) || (errno == EWOULDBLOCK)) {
			continue;
		} else {
			// TODO: log
			return 1;
		}
	}

	munmap(shared_table, (max_pids + 1) / sizeof(pid_t));
	shared_table = NULL;

	// Wait for watchdog to cleanup.
	int status = 0;
	pid_t result = waitpid(watchdog_pid, &status, 0);
	if(result < 0) {
		// TODO: log
		return 1;
	}

	return 0;
}

int pfs_paranoia_add_pid(pid_t pid)
{
	if(paranoia_mode == 0)
		return -1;

	pid_t *pid_ptr = shared_table;
	unsigned long pid_count = 0;
	debug(D_PROCESS, "shared table %p", shared_table);
	debug(D_PROCESS, "initial value %d", *pid_ptr);
	while((*pid_ptr != 0) && (*pid_ptr != 1)) {
		debug(D_PROCESS, "pid count %lu", pid_count);
		pid_count++;
		pid_ptr++;
	}
	if(pid_count >= max_pids) {
		// TODO: log
		abort();
	}
	*pid_ptr = pid;
	return 0;
}

int pfs_paranoia_delete_pid(pid_t pid)
{
	if(paranoia_mode == 0)
		return -1;

	pid_t *pid_ptr = shared_table;
	while((*pid_ptr != 0) && (*pid_ptr != pid)) {
		pid_ptr++;
	}
	if(*pid_ptr == pid) {
		*pid_ptr = 1;
		return 0;
	} else {
		// TODO: log
		return pid;
	}
}

int pfs_paranoia_payload(void)
{
	if(to_watchdog_fd >= 0) {
		close(to_watchdog_fd);
		to_watchdog_fd = -1;
	}
	if(from_watchdog_fd >= 0) {
		close(from_watchdog_fd);
		from_watchdog_fd = -1;
	}
	return 0;
}

/* vim: set noexpandtab tabstop=8: */
