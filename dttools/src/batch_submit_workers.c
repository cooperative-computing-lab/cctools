/*
Copyright (C) 2010- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "batch_job.h"
#include "copy_stream.h"
#include "debug.h"
#include "stringtools.h"
#include "xmalloc.h"

#include <errno.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

static int find_executable(const char *exe_name, const char *env_path_var, char *exe_path)
{
	char *env_paths;
	char *cur_path;

	if (!getenv(env_path_var)) return 0;

	env_paths = xstrdup(getenv(env_path_var));

	for (cur_path = strtok(env_paths, ":"); cur_path; cur_path = strtok(NULL, ":")) {
		sprintf(exe_path, "%s/%s", cur_path, exe_name);
		if (access(exe_path, R_OK | X_OK) == 0) break;
	}

	free(env_paths);
	return cur_path != NULL;
}

static void show_help(const char *cmd)
{
	printf("Use: %s [options] <hostname> <port> <count>\n", cmd);
	printf("where batch options are:\n");
	printf("  -d <subsystem> Enable debugging for this subsystem.\n");
	printf("  -S <scratch>   Scratch directory. (default is /tmp/${USER}-workers)\n");
	printf("  -T <type>      Batch system type: unix, condor, sge, workqueue, xgrid. (default is unix)\n");
	printf("  -W <path>      Path to worker executable.\n");
	printf("  -h             Show this screen.\n");
	printf("\n");
	printf("where worker options are:\n");
	printf("  -a             Enable auto master selection mode.\n");
	printf("  -N <name>      Preferred master name.\n");
	printf("  -t <time>      Abort after this amount of idle time.\n");
}

int main(int argc, char *argv[])
{
	int i, c;
	int count = 0;
	int port;
	char scratch_dir[PATH_MAX] = "";
	char worker_path[PATH_MAX] = "";
	char worker_args[PATH_MAX] = "";
	char *hostname;
	int batch_queue_type = BATCH_QUEUE_TYPE_UNIX;
	struct batch_queue *q;
	FILE *ifs, *ofs;

	while ((c = getopt(argc, argv, "d:T:W:aN:t:h")) >= 0) {
		switch (c) {
			case 'd':
				debug_flags_set(optarg);
				break;
			case 'T':
				batch_queue_type = batch_queue_type_from_string(optarg);
				if (batch_queue_type == BATCH_QUEUE_TYPE_UNKNOWN) {
					fprintf(stderr, "unknown batch queue type: %s\n", optarg);
					return EXIT_FAILURE;
				}
				break;
			case 'W':
				strncpy(worker_path, optarg, PATH_MAX);
				break;
			case 'a':
				strncat(worker_args, " -a ", PATH_MAX);
				break;
			case 'N':
				strncat(worker_args, " -N ", PATH_MAX);
				strncat(worker_args, optarg, PATH_MAX);
				break;
			case 't':
				strncat(worker_args, " -t ", PATH_MAX);
				strncat(worker_args, optarg, PATH_MAX);
				break;
			case 'h':
			default:
				show_help(argv[0]);
				return EXIT_FAILURE;
				break;
		}
	}

	if ((argc - optind) != 3) {
		fprintf(stderr, "invalid number of arguments\n");
		show_help(argv[0]);
	}

	hostname = argv[optind];
	port = strtol(argv[optind + 1], NULL, 10);
	count = strtol(argv[optind + 2], NULL, 10);

	if (strlen(worker_path) > 0) {
		if (access(worker_path, R_OK | X_OK) < 0) {
			fprintf(stderr, "inaccessible worker specified: %s\n", worker_path);
			return EXIT_FAILURE;
		}
	} else {
		if (!find_executable("worker", "PATH", worker_path)) {
			fprintf(stderr, "please add worker to your PATH or specify it explicitly.\n");
			return EXIT_FAILURE;
		}
	}
	debug(D_DEBUG, "worker path: %s", worker_path);

	if (strlen(scratch_dir) <= 0) {
		if (batch_queue_type == BATCH_QUEUE_TYPE_CONDOR) {
			snprintf(scratch_dir, PATH_MAX, "/tmp/%s-workers", getenv("USER"));
		} else {
			snprintf(scratch_dir, PATH_MAX, "%s-workers", getenv("USER"));
		}
	}
	mkdir(scratch_dir, 0755);
	if (chdir(scratch_dir) < 0) {
		fprintf(stderr, "unable to cd into scratch directory %s: %s\n", scratch_dir, strerror(errno));
		return EXIT_FAILURE;
	}
	debug(D_DEBUG, "scratch dir: %s", scratch_dir);

	ifs = fopen(worker_path, "r");
	if (!ifs) {
		fprintf(stderr, "unable to open %s for reading: %s", worker_path, strerror(errno));
		return EXIT_FAILURE;
	}
	ofs = fopen("worker", "w+");
	if (!ofs) {
		fprintf(stderr, "unable to open %s/worker for writing: %s", scratch_dir, strerror(errno));
		return EXIT_FAILURE;
	}
	copy_stream_to_stream(ifs, ofs);
	fclose(ifs);
	fclose(ofs);
	chmod("worker", 0777);

	q = batch_queue_create(batch_queue_type);
	if (!q) fatal("unable to create batch_queue of type: %s", batch_queue_type_to_string(batch_queue_type));

	for (i = 0; i < count; i++) {
		char command[PATH_MAX];

		snprintf(command, PATH_MAX, "./%s %s %s %d", string_basename(worker_path), worker_args, hostname, port);
		debug(D_DEBUG, "submitting worker %d: %s\n", i + 1, command);
		batch_job_submit_simple(q, command, string_basename(worker_path), NULL);
	}

	batch_queue_delete(q);

	return EXIT_SUCCESS;
}

/*
 * vim: sts=8 sw=8 ts=8 ft=c
 */
