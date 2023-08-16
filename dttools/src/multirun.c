/*
Copyright (C) 2004-2005 Douglas Thain and The University of Notre Dame
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

/*
Run a series of commands in parallel processes, substituting
a parameter in each command, and tagging the output.
Each parameter is substituted into the command using the
print %s syntax.  The degree of parallelism is controlled
using the command line.

Example use:
	multirun -p 10 "scp file %s:file" host1 host2 host3
	multirun -d "chirp %s getacl" host1 host2 host3
*/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <unistd.h>
#include <signal.h>
#include <errno.h>
#include <sys/wait.h>

#include "cctools.h"

static int debug = 0;
static char *name;
static const char *fileprefix = 0;
static pid_t child_pid = 0;

#define BUFFER_SIZE 65536

FILE *fopen_process(const char *cmd)
{
	int result;
	int fds[2];

	result = pipe(fds);
	if(result < 0)
		return 0;

	child_pid = fork();
	if(child_pid < 0) {
		close(fds[0]);
		close(fds[1]);
		return 0;
	}

	if(child_pid > 0) {
		close(fds[1]);
		return fdopen(fds[0], "r");
	} else {
		dup2(fds[1], 1);
		dup2(fds[1], 2);
		close(fds[0]);
		close(fds[1]);
		exit(system(cmd));
		return 0;
	}
}

int agent(char *param, char *format, int timeout)
{
	char buffer[BUFFER_SIZE + 1];
	FILE *child;
	char filename[1024];
	FILE *outfile;

	/* If a fileprefix has been given, write to that file */
	if(fileprefix) {
		sprintf(filename, "%s.%s", fileprefix, param);
		outfile = fopen(filename, "w");
		if(!outfile) {
			fprintf(stderr, "%s: Couldn't open %s: %s\n", param, filename, strerror(errno));
			return 1;
		}
	} else {
		outfile = stdout;
	}

	sprintf(buffer, format, param);

	child = fopen_process(buffer);
	if(!child) {
		fprintf(stderr, "%s: Unable to execute %s\n", param, buffer);
		return 0;
	}

	name = param;

	if(timeout > 0)
		alarm(timeout);

	while(fgets(buffer, BUFFER_SIZE, child)) {
		if(fileprefix) {
			fprintf(outfile, "%s", buffer);
		} else {
			fprintf(outfile, "%s: %s", param, buffer);
		}
	}

	fclose(child);

	exit(0);
}

void shutdown_handler()
{
	if(child_pid > 0) {
		kill(child_pid, SIGTERM);
		sleep(1);
		kill(child_pid, SIGKILL);
	}
	exit(0);
}

void alarm_handler()
{
	fprintf(stderr, "%s: timeout\n", name);
	shutdown_handler();
}

void sigterm_handler()
{
	fprintf(stderr, "%s: aborting\n", name);
	shutdown_handler();
}

static void ignore_signal(int sig)
{
}

static void use(char *program)
{
	fprintf(stderr, "Use: %s [options] <command> [params]\n", program);
	fprintf(stderr, "Options are:\n");
	fprintf(stderr, "\t-t <seconds>   Set timeout for each child process. (default is none)\n");
	fprintf(stderr, "\t-p <processes> Set the maximum number of concurrent jobs (default 5)\n");
	fprintf(stderr, "\t-f <prefix>    Send each output to file named prefix.param\n");
	fprintf(stderr, "\t-d             Debug mode\n");
	fprintf(stderr, "\t-v             Show version\n");
	exit(0);
}

int main(int argc, char *argv[])
{
	char *command = 0;
	char *param = 0;
	int started = 0, running = 0, done = 0;
	int timeout = 0;
	int limit = 5;
	int i = 0;
	int result;

	if(argc < 2)
		use(argv[0]);

	for(i = 1; i < argc; i++) {
		if(argv[i][0] == '-') {
			switch (argv[i][1]) {
			case 't':
				if(++i == argc)
					use(argv[0]);
				timeout = atoi(argv[i]);
				break;

			case 'p':
				if(++i == argc)
					use(argv[0]);
				limit = atoi(argv[i]);
				break;

			case 'f':
				if(++i == argc)
					use(argv[0]);
				fileprefix = argv[i];
				break;

			case 'd':
				debug = 1;
				break;

			case 'v':
				cctools_version_print(stdout, argv[0]);
				exit(0);
				break;

			default:
				use(argv[0]);
				break;
			}
		} else if(command == 0) {
			command = argv[i++];
			break;
		}
	}

	if(command == 0) {
		fprintf(stderr, "No command specified.\n");
		return -1;
	}

	signal(SIGPIPE, ignore_signal);
	signal(SIGCHLD, ignore_signal);
	signal(SIGALRM, alarm_handler);
	signal(SIGTERM, sigterm_handler);
	signal(SIGQUIT, sigterm_handler);
	signal(SIGABRT, sigterm_handler);
	signal(SIGINT, sigterm_handler);

	while(1) {
		if(i < argc) {
			param = argv[i++];

			result = fork();
			if(result < 0) {
				fprintf(stderr, "Unable to fork: %s\n", strerror(errno));
				return -1;
			} else if(result == 0) {
				agent(param, command, timeout);
				exit(0);
			} else {
				started++;
				running++;
			}
		}

		if(debug)
			fprintf(stderr, "multirun: %d started, %d running, %d done        \r", started, running, done);
		fflush(stdout);

		if(running == 0) {
			if(debug)
				fprintf(stderr, "\nmultirun: done\n");
			return 0;
		}

		if(running >= limit || i >= argc) {
			wait(0);
			running--;
			done++;
		}
	}
}

/* vim: set noexpandtab tabstop=8: */
