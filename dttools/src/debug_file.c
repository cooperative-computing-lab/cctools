/*
Copyright (C) 2014- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "catch.h"
#include "debug.h"
#include "full_io.h"
#include "stringtools.h"
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
static struct stat file_stat;
static char file_path[PATH_MAX];
/* Default size of 0 will not roll over debug info. */
static off_t file_size_max = 0;

/* define custom debug for catch */
#undef debug
#define debug(l,fmt,...) fprintf(stderr, "%s: " fmt "\n", #l, __VA_ARGS__);

int debug_file_reopen (void)
{
	int rc;
	if (strlen(file_path)) {
		int flags;
		close(file_fd);
		CATCHUNIX(file_fd = open(file_path, O_CREAT|O_APPEND|O_WRONLY|O_NOCTTY, S_IRUSR|S_IWUSR|S_IRGRP|S_IWGRP));
		CATCHUNIX(flags = fcntl(file_fd, F_GETFD));
		flags |= FD_CLOEXEC;
		CATCHUNIX(fcntl(file_fd, F_SETFD, flags));
		CATCHUNIX(fstat(file_fd, &file_stat)); /* save inode */
		/* canonicalize the debug_file path for future operations */
		{
			char tmp[PATH_MAX] = "";
			CATCHUNIX(realpath(file_path, tmp) == NULL ? -1 : 0);
			memcpy(file_path, tmp, sizeof(file_path));
		}
	}
	rc = 0;
	goto out;
out:
	return RCUNIX(rc);
}

void debug_file_write (INT64_T flags, const char *str)
{
	int rc;

	/* Beware this code is racey and debug messages may be lost during
	 * rotations. Two processes rotating logs at the same time may create two
	 * new logs. The stat on the filename and inode comparison catches this
	 * after one lost debug message.
	 */

	if (file_size_max > 0) {
		struct stat info;
		rc = stat(file_path, &info);
		if (rc == 0) {
			if (info.st_size >= file_size_max) {
				char old[PATH_MAX];
				string_nformat(old, sizeof(old), "%s.old", file_path);
				rename(file_path, old);
				debug_file_reopen();
			} else if (info.st_ino != file_stat.st_ino) {
				/* another process rotated the log */
				debug_file_reopen();
			}
		} else {
			fprintf(stderr, "couldn't stat debug file: %s\n", strerror(errno));
			abort();
		}
	}

	rc = full_write(file_fd, str, strlen(str));
	if (rc == -1) {
		fprintf(stderr, "couldn't write to debug file: %s\n", strerror(errno));
		abort();
	}
}

int debug_file_path (const char *path)
{
	strncpy(file_path, path, sizeof(file_path)-1);
	return debug_file_reopen();
}

void debug_file_size (off_t size)
{
	file_size_max = size;
}

void debug_file_rename (const char *suffix)
{
	if (strlen(file_path)) {
		char old[PATH_MAX] = "";

		string_nformat(old, sizeof(old), "%s.%s", file_path, suffix);
		rename(file_path, old);
		debug_file_reopen();
	}
}

/* vim: set noexpandtab tabstop=4: */
