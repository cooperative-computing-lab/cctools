/*
Copyright (C) 2010- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "batch_job.h"
#include "copy_stream.h"
#include "debug.h"
#include "envtools.h"
#include "stringtools.h"
#include "xmalloc.h"
#include "itable.h"
#include "create_dir.h"
#include "delete_dir.h"

#include <errno.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <signal.h>

static int abort_flag = 0;

static void handle_abort( int sig ) {
	abort_flag = 1;
}

static void show_help(const char *cmd)
{
	printf("Use: %s [options] <count>\n", cmd);
	printf("where batch options are:\n");
	printf("  -d <subsystem> Enable debugging for this subsystem.\n");
	printf("  -S <scratch>   Scratch directory. (default is /tmp/${USER}-workers)\n");
	printf("  -T <type>      Batch system type: unix, condor, sge, workqueue, xgrid. (default is unix)\n");
	printf("  -r <count>     Number of attemps to retry if failed to submit a worker.\n");
	printf("  -W <path>      Path to worker executable.\n");
	printf("  -h             Show this screen.\n");
	printf("\n");
	printf("where worker options are:\n");
	printf(" -a              Enable auto mode. In this mode the worker would ask a catalog server for available masters.\n");
	printf("  -t <time>      Abort after this amount of idle time.\n");
	printf("  -C <catalog>   Set catalog server to <catalog>. Format: HOSTNAME:PORT \n");
	printf("  -N <project>   Name of a preferred project. A worker can have multiple preferred projects.\n");
	printf("  -s             Run as a shared worker. By default the worker would only work on preferred projects.\n");
	printf("  -o <file>      Send debugging to this file.\n");
}

int main(int argc, char *argv[])
{
	int i, c;
	int count = 0;
        int retry_count = 20;
	char scratch_dir[PATH_MAX] = "";
	char worker_path[PATH_MAX] = "";
	char worker_args[PATH_MAX] = "";
	int batch_queue_type = BATCH_QUEUE_TYPE_LOCAL;
        char command[PATH_MAX];
	batch_job_id_t jobid;
	struct batch_queue *q;
	FILE *ifs, *ofs;
	struct itable *remote_job_table;
        int auto_worker = 0;

	while ((c = getopt(argc, argv, "aC:d:hN:t:T:sS:W:")) >= 0) {
		switch (c) {
                        case 'a':
                                strcat(worker_args, " -a");
                                auto_worker = 1;
                                break;
                        case 'C':
                                strcat(worker_args, " -C ");
                                strcat(worker_args, optarg);
                                break;
                        case 'N':
                                strcat(worker_args, " -N ");
                                strcat(worker_args, optarg);
                                break;
                        case 's':
                                strcat(worker_args, " -s");
                                break;
                        case 't':
                                strcat(worker_args, " -t ");
                                strcat(worker_args, optarg);
                                break;
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
			case 'S':
				strncpy(scratch_dir, optarg, PATH_MAX);
				break;
			case 'r':
				retry_count = atoi(optarg);
				break;
			case 'h':
			default:
				show_help(argv[0]);
				return EXIT_FAILURE;
				break;
		}
	}

        if(!auto_worker) {
                if ((argc - optind) != 3) {
                        fprintf(stderr, "invalid number of arguments\n");
                        show_help(argv[0]);
                        return EXIT_FAILURE;
                }
                // Add host name
                strcat(worker_args, " ");
                strcat(worker_args, argv[optind]);
                // Add port
                strcat(worker_args, " ");
                strcat(worker_args, argv[optind+1]);
                // Number of workers to submit
                count = strtol(argv[optind+2], NULL, 10);
        } else {
                if ((argc - optind) != 1) {
                        fprintf(stderr, "invalid number of arguments\n");
                        show_help(argv[0]);
                        return EXIT_FAILURE;
                }
                count = strtol(argv[optind], NULL, 10);
        }

	signal(SIGINT,handle_abort);
	signal(SIGQUIT,handle_abort);
	signal(SIGTERM,handle_abort);

        // Locate the worker program
	if (strlen(worker_path) > 0) {
		if (access(worker_path, R_OK | X_OK) < 0) {
			fprintf(stderr, "Inaccessible worker specified: %s\n", worker_path);
			return EXIT_FAILURE;
		}
	} else {
		if (!find_executable("worker", "PATH", worker_path, PATH_MAX)) {
			fprintf(stderr, "Please add worker to your PATH or specify it explicitly.\n");
			return EXIT_FAILURE;
		}
	}
	debug(D_DEBUG, "worker path: %s", worker_path);

        // Create a tmp directory to hold all workers' runtime information
	if (strlen(scratch_dir) <= 0) {
		if (batch_queue_type == BATCH_QUEUE_TYPE_CONDOR) {
			snprintf(scratch_dir, PATH_MAX, "/tmp/%s-workers/%ld", getenv("USER"), (long)time(0));
		} else {
			snprintf(scratch_dir, PATH_MAX, "%s-workers/%ld", getenv("USER"), (long)time(0));
		}
	}
	create_dir(scratch_dir, 0755);
	debug(D_DEBUG, "scratch dir: %s", scratch_dir);

        // Copy the worker program to the tmp directory and we will enter that
        // directory afterwards. We have to copy the worker program to a local
        // filesystem (other than afs, etc.) because condor might not be able
        // to access your shared file system
        char new_worker_path[PATH_MAX];
        snprintf(new_worker_path, PATH_MAX, "%s/worker", scratch_dir);
	ifs = fopen(worker_path, "r");
	if (!ifs) {
		fprintf(stderr, "Unable to open %s for reading: %s\n", worker_path, strerror(errno));
		return EXIT_FAILURE;
	}
	ofs = fopen(new_worker_path, "w+");
	if (!ofs) {
		fprintf(stderr, "Unable to open %s/worker for writing: %s", scratch_dir, strerror(errno));
		return EXIT_FAILURE;
	}
	copy_stream_to_stream(ifs, ofs);
	fclose(ifs);
	fclose(ofs);
	chmod(new_worker_path, 0777);

	if (chdir(scratch_dir) < 0) {
		fprintf(stderr, "Unable to cd into scratch directory %s: %s\n", scratch_dir, strerror(errno));
		return EXIT_FAILURE;
	}

	q = batch_queue_create(batch_queue_type);
	if (!q) fatal("Unable to create batch_queue of type: %s", batch_queue_type_to_string(batch_queue_type));


	remote_job_table = itable_create(0);
        snprintf(command, PATH_MAX, "./%s %s", string_basename(worker_path), worker_args);
	for (i = 0; i < count; i++) {
		debug(D_DEBUG, "Submitting worker %d: %s\n", i + 1, command);
		jobid = batch_job_submit_simple(q, command, string_basename(worker_path), NULL);
                if(jobid >= 0) {
			itable_insert(remote_job_table, jobid, NULL);
                } else {
                        retry_count--;
                        if(!retry_count) {
		                fprintf(stderr, "Retry max reached. Stop submitting more workers..\n");
                                break;
                        }

		        fprintf(stderr, "Failed to submit the %dth job: %s. Will retry it.\n", i+1, command);
                        i--;
                }
	}
        printf("%d workers are submitted successfully.\n", i);

        struct batch_job_info info;
        while(!abort_flag) {
                jobid = batch_job_wait_timeout(q, &info, time(0)+5);
                if(jobid >= 0) {
			itable_remove(remote_job_table,jobid);
		        jobid = batch_job_submit_simple(q, command, string_basename(worker_path), NULL);
                        if(jobid >= 0) {
                                itable_insert(remote_job_table, jobid, NULL);
                        }
                }
        }

        // abort all jobs
        void *x;
        UINT64_T key;
	itable_firstkey(remote_job_table);
	while(itable_nextkey(remote_job_table, &key, &x)) {
		printf("batch_submit_workers: aborting remote job %llu\n", key);
		batch_job_remove(q, key);
	}

        delete_dir(scratch_dir);
	batch_queue_delete(q);

	return EXIT_SUCCESS;
}

/*
 * vim: sts=8 sw=8 ts=8 ft=c
 */
