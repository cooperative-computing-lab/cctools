/*
Copyright (C) 2009- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "timer.h"

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

enum {
	OP_STAT,
	OP_OPEN,
	OP_READ,
	OP_CLOSE,
	NOPS
};

#define BUFFER_SIZE 8192

const char *OP_STRINGS[NOPS] = { "stat ", "open ", "read ", "close" };

static void show_help(const char *cmd)
{
	printf("Use: %s <path> <runs>\n", cmd);
}

static void do_stat(const char *path)
{
	struct stat buf;
	int result;

	timer_start(OP_STAT); 
	result = stat(path, &buf);
	timer_stop(OP_STAT);

	if (result < 0) {
		printf("could not stat %s: %s\n", path, strerror(errno));
		exit(EXIT_FAILURE);
	}
}

static void do_open(const char *path, int *fd)
{
	timer_start(OP_OPEN); 
	*fd = open(path, O_RDONLY);
	timer_stop(OP_OPEN);

	if (*fd < 0) {
		printf("could not open %s: %s\n", path, strerror(errno));
		exit(EXIT_FAILURE);
	}
}

static void do_read(int fd)
{
	char buffer[BUFFER_SIZE];
	int result;

	timer_start(OP_READ);
	result = read(fd, buffer, BUFFER_SIZE);
	timer_stop(OP_READ);

	if (result < 0) {
		printf("could not read: %s\n", strerror(errno));
		exit(EXIT_FAILURE);
	}
}

static void do_close(int fd)
{
	timer_start(OP_CLOSE); 
	close(fd);
	timer_stop(OP_CLOSE);
}

int main(int argc, char *argv[])
{
	char *path;
	int i, fd, runs;

	if (argc != 3) {
		show_help(argv[0]);
		return (EXIT_FAILURE);
	}

	path = argv[1];
	runs = atoi(argv[2]);

	timer_init(NOPS, OP_STRINGS);

	do_stat(path); 
	timer_reset(OP_STAT);

	for (i = 0; i < runs; i++) {
		do_stat(path);
		do_open(path, &fd);
		do_read(fd);
		do_close(fd);
	}

	timer_print_summary(0);
	timer_destroy();

	return (EXIT_SUCCESS);
}

/*
 * vim: sts=8 sw=8 ts=8 ft=c
 */
