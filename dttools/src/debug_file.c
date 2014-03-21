/*
Copyright (C) 2014- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "debug.h"

#include "full_io.h"
#include "path.h"

#include <unistd.h>
#include <fcntl.h>

#include <sys/stat.h>

#include <errno.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static int file_fd = -1;
static char file_path[PATH_MAX];
static off_t file_size_max = 1<<20;

static void reopen (void)
{
	file_fd = open(file_path, O_CREAT|O_APPEND|O_WRONLY, 0660);
	if (file_fd == -1){
		fprintf(stderr, "could not access log file `%s' for writing: %s\n", file_path, strerror(errno));
		exit(EXIT_FAILURE);
	}
}

void debug_file_write (INT64_T flags, const char *str)
{
	if (file_size_max > 0) {
		struct stat info;
		int rc = fstat(file_fd, &info);
		if (rc == -1 && errno == EBADF) {
			/* This may happen when a daemonize function like the one in
			 * daemon.h closes all open fds. */
			reopen();
			rc = fstat(file_fd, &info);
		}
		if (rc == 0) {
			if (info.st_size >= file_size_max) {
				close(file_fd);
				if(stat(file_path, &info) == 0 && info.st_size >= file_size_max) {
					char old[PATH_MAX] = "";
					snprintf(old, sizeof(old)-1, "%s.old", file_path);
					rename(file_path, old);
				}
				reopen();
			}
		} else {
			fprintf(stderr, "couldn't stat debug file: %s\n", strerror(errno));
			abort();
		}
	}

	full_write(file_fd, str, strlen(str));
}

void debug_file_path (const char *path)
{
	path_absolute(path, file_path, 0);
	close(file_fd);
	reopen();
}

void debug_file_size (off_t size)
{
	file_size_max = size;
}

void debug_file_rename (const char *suffix)
{
	if (strlen(file_path)) {
		char old[PATH_MAX] = "";

		close(file_fd);
		snprintf(old, sizeof(old)-1, "%s.%s", file_path, suffix);
		rename(file_path, old);
		reopen();
	}
}

/* vim: set noexpandtab tabstop=4: */
