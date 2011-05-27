/*
Copyright (C) 2010- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "batch_job.h"
#include "hash_table.h"
#include "copy_stream.h"
#include "debug.h"
#include "envtools.h"
#include "stringtools.h"
#include "xmalloc.h"
#include "itable.h"
#include "create_dir.h"
#include "delete_dir.h"
#include "macros.h"

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
static char worker_cmd[PATH_MAX] = "";
static char worker_path[PATH_MAX] = "";
static char worker_args[PATH_MAX] = "";
static struct batch_queue *q;
static struct itable *remote_job_table = NULL;
static int retry_count = 20;

static void handle_abort( int sig ) {
	abort_flag = 1;
}

struct worker_status {
	int batch_job_id;
	char status;
};
	
static int submit_workers(int count){
	int i;
	batch_job_id_t jobid;

	for (i = 0; i < count; i++) {
		debug(D_DEBUG, "Submitting worker %d: %s\n", i + 1, worker_cmd);
		jobid = batch_job_submit_simple(q, worker_cmd, string_basename(worker_path), NULL);
		if(jobid >= 0) {
			itable_insert(remote_job_table, jobid, NULL);
        } else {
			retry_count--;
			if(!retry_count) {
				fprintf(stderr, "Retry max reached. Stop submitting more workers..\n");
				break;
			}
		    fprintf(stderr, "Failed to submit the %dth job: %s. Will retry it.\n", i+1, worker_cmd);
			i--;
        }
	}
	return i;
}

static void remove_workers(struct itable *jobs) {
	void *x;
	UINT64_T key;

	itable_firstkey(jobs);
	while(itable_nextkey(jobs, &key, &x)) {
		// The key is the job id
		printf("work_queue_pool: aborting remote job %llu\n", key);
		batch_job_remove(q, key);
		itable_remove(remote_job_table, key);
	}
}

static void check_jobs_status_condor(struct itable **running_jobs, struct itable **idle_jobs, struct itable **bad_jobs) {
	FILE *fp;
	char line[128];
	/** Sample condor_q formatting. Source: https://condor-wiki.cs.wisc.edu/index.cgi/wiki?p=HowToWriteaCondorqWrapper
	char *cmd = "condor_q \
				-format '%4d.' ClusterId \
				-format '%-3d ' ProcId \
				-format '%-14s ' Owner \
				-format '%-11s ' 'formatTime(QDate,\"%m/%d %H:%M\")' \
				-format '%3d+' 'int(RemoteUserCpu/(60*60*24))' \
				-format '%02d:' 'int((RemoteUserCpu-(int(RemoteUserCpu/(60*60*24))*60*60*24))/(60*60))' \
				-format '%02d:' 'int((RemoteUserCpu-(int(RemoteUserCpu/(60*60))*60*60))/(60))' \
				-format '%02d ' 'int(RemoteUserCpu-(int(RemoteUserCpu/60)*60))' \
				-format '%-2s ' 'ifThenElse(JobStatus==0,\"U\",ifThenElse(JobStatus==1,\"I\",ifThenElse(JobStatus==2,\"R\",ifThenElse(JobStatus==3,\"X\",ifThenElse(JobStatus==4,\"C\",ifThenElse(JobStatus==5,\"H\",ifThenElse(JobStatus==6,\"E\",string(JobStatus))))))))' \
				-format '\%-3d ' JobPrio \
				-format '\%-4.1f ' ImageSize/1024.0 \
				-format '\%-18.18s' 'strcat(Cmd,\" \",Arguments)' \
				-format '\n' Owner";
	*/

	// We don't need the ProcId because ClusterId is the unique job id for each condor task in our case.
	char *cmd = "condor_q \
				-format '%4d\t' ClusterId \
				-format '%-2s ' 'ifThenElse(JobStatus==0,\"U\",ifThenElse(JobStatus==1,\"I\",ifThenElse(JobStatus==2,\"R\",ifThenElse(JobStatus==3,\"X\",ifThenElse(JobStatus==4,\"C\",ifThenElse(JobStatus==5,\"H\",ifThenElse(JobStatus==6,\"E\",string(JobStatus))))))))' \
				-format '\n' Owner";

	fp = popen(cmd, "r");

	// Parse condor_q result
	int jobid;
	char status;
	char hash_key[128];
	struct worker_status *ws;
	struct hash_table *all_job_status;

	all_job_status = hash_table_create(0,0);
	while(fgets(line, sizeof(line), fp)) {
		if(sscanf(line, "%d\t%c\n", &jobid, &status) == 2 ) {
			ws = (struct worker_status *)malloc(sizeof(struct worker_status));
			if(!ws) {
				fprintf(stderr,"Cannot record status for job %d (%s)", jobid, strerror(errno));
			}
			ws->batch_job_id = jobid;
			ws->status = status;

			sprintf(hash_key, "%d", jobid);
			hash_table_insert(all_job_status, hash_key, ws);
		} else {
			fprintf(stderr,"Invalid line found in condor_q output: %s", line);
		}
	}
	pclose(fp);

	/**
	char *key;
	int count=0;
	hash_table_firstkey(all_job_status);
	while(hash_table_nextkey(all_job_status,&key,(void**)&ws)) {
		printf("%d\t%c\n", ws->batch_job_id, ws->status);
		count++;
	}
	printf("%d jobs processed\n", count);
	*/


	void *x;
	UINT64_T ikey;
	struct itable *running_job_table;
	struct itable *idle_job_table;
	struct itable *bad_job_table;

	running_job_table = itable_create(0);
	idle_job_table = itable_create(0);
	bad_job_table = itable_create(0);

	itable_firstkey(remote_job_table);
	while(itable_nextkey(remote_job_table, &ikey, &x)) {
		sprintf(hash_key, "%d", (int)ikey);
		ws = hash_table_lookup(all_job_status, hash_key);
		if(ws) {
			//printf("%d\t%c\n", ws->batch_job_id, ws->status);
			if(ws->status == 'R') {
				itable_insert(running_job_table, ikey, NULL);
			} else if (ws->status == 'I') {
				itable_insert(idle_job_table, ikey, NULL);
			} else {
				itable_insert(bad_job_table, ikey, NULL);
			}
		} 
	}

	*running_jobs = running_job_table;
	*idle_jobs = idle_job_table;
	*bad_jobs= bad_job_table;

	// Remove hash table
	char *key;
	hash_table_firstkey(all_job_status);
	while(hash_table_nextkey(all_job_status, &key,(void**)&ws)) {
		free(ws);
	}
	hash_table_delete(all_job_status);
}

static int fix_running_job_number_condor(int goal) {
	struct itable *running_jobs;
	struct itable *idle_jobs;
	struct itable *bad_jobs;
	void *x;
	UINT64_T key;

	int running, idle, bad;
	int goal_achieved = 0;
	int count, extra;

	extra = MAX(10, goal*0.20);
	count = submit_workers(extra);
	printf("%d extra workers are submitted successfully.\n", count);

	while(!abort_flag) {
		check_jobs_status_condor(&running_jobs, &idle_jobs, &bad_jobs);
		running = itable_size(running_jobs);
		idle = itable_size(idle_jobs);
		bad = itable_size(bad_jobs);
		printf("Running Jobs: %d, Idle Jobs: %d, Bad Jobs: %d\n", running, idle, bad);
		
		count = running - goal;
		if(count >= 0) {
			// Remove excessive jobs
			struct itable *excessive_running_jobs = itable_create(0);
			itable_firstkey(running_jobs);
			while(itable_nextkey(running_jobs, &key, &x) && count) {
				// The key is the job id
				itable_insert(excessive_running_jobs, key, x);
				count--;
			}

			remove_workers(excessive_running_jobs);
			remove_workers(idle_jobs);
			remove_workers(bad_jobs);

			itable_delete(excessive_running_jobs);
			goal_achieved = 1;
			break;
		}

		count = goal+extra-running-idle;
		if(count > 0) {
			// submit more
			count = submit_workers(count);
			printf("%d extra workers are submitted successfully.\n", count);
		}

		// We can delete these hash tables directly because the data (e->value) of each entry is NULL
		itable_delete(running_jobs);
		itable_delete(idle_jobs);
		itable_delete(bad_jobs);

		sleep(3);
	}

	return goal_achieved;
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
	printf("  -a             Enable auto mode. In this mode the worker would ask a catalog server for available masters.\n");
	printf("  -t <time>      Abort after this amount of idle time.\n");
	printf("  -C <catalog>   Set catalog server to <catalog>. Format: HOSTNAME:PORT \n");
	printf("  -N <project>   Name of a preferred project. A worker can have multiple preferred projects.\n");
	printf("  -s             Run as a shared worker. By default the worker would only work on preferred projects.\n");
	printf("  -o <file>      Send debugging to this file.\n");
}

int main(int argc, char *argv[])
{
	int c;
	int goal = 0;
	char scratch_dir[PATH_MAX] = "";
	int batch_queue_type = BATCH_QUEUE_TYPE_LOCAL;
	batch_job_id_t jobid;
	FILE *ifs, *ofs;
	int auto_worker = 0;
	int fix_worker_number = 0;

	while ((c = getopt(argc, argv, "aC:d:fhN:r:sS:t:T:W:")) >= 0) {
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
			case 'f':
				fix_worker_number = 1;
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
		goal = strtol(argv[optind+2], NULL, 10);
	} else {
		if ((argc - optind) != 1) {
			fprintf(stderr, "invalid number of arguments\n");
			show_help(argv[0]);
			return EXIT_FAILURE;
		}
        goal = strtol(argv[optind], NULL, 10);
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


	snprintf(worker_cmd, PATH_MAX, "./%s %s", string_basename(worker_path), worker_args);
	remote_job_table = itable_create(0);

	int count = submit_workers(goal);
	printf("%d workers are submitted successfully.\n", count);

	if(fix_worker_number) {
		if(batch_queue_type == BATCH_QUEUE_TYPE_CONDOR) {
			fix_running_job_number_condor(goal);
			delete_dir(scratch_dir);
			exit(0);
		}
	}


	// Maintain a number of workers
	struct batch_job_info info;
	while(!abort_flag) {
		jobid = batch_job_wait_timeout(q, &info, time(0)+5);
		if(jobid >= 0) {
			itable_remove(remote_job_table,jobid);
			jobid = batch_job_submit_simple(q, worker_cmd, string_basename(worker_path), NULL);
			if(jobid >= 0) {
					itable_insert(remote_job_table, jobid, NULL);
			}
		}
	}

	// Abort all jobs
	remove_workers(remote_job_table);

    delete_dir(scratch_dir);
	batch_queue_delete(q);

	return EXIT_SUCCESS;
}

/*
 * vim: sts=4 sw=4 ts=4 ft=c
 */
