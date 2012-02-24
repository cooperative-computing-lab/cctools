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
#include "xxmalloc.h"
#include "itable.h"
#include "create_dir.h"
#include "delete_dir.h"
#include "macros.h"
#include "catalog_query.h"
#include "catalog_server.h"
#include "work_queue_catalog.h"
#include "list.h"

#include <errno.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <signal.h>

#define WORKERS_PER_JOB_MAX 50
#define EXTRA_WORKERS_MAX 20
#define EXTRA_WORKERS_PERCENTAGE 0.2

static int abort_flag = 0;
static struct batch_queue *q;
static struct itable *remote_job_table = NULL;
static struct hash_table *processed_masters;
static int retry_count = 20;

struct list *get_matched_masters(const char *catalog_host, int catalog_port, struct list *regex_list);
void process_matched_masters(struct list *matched_masters);
static int submit_workers(char *cmd, char *input_files, int count);

struct worker_status {
	int batch_job_id;
	char status;
};

static void handle_abort(int sig)
{
	abort_flag = 1;
}

static int locate_executable(char *name, char *path)
{
	if(strlen(path) > 0) {
		if(access(path, R_OK | X_OK) < 0) {
			fprintf(stderr, "Inaccessible %s specified: %s\n", name, path);
			return 0;
		}
	} else {
		if(!find_executable(name, "PATH", path, PATH_MAX)) {
			fprintf(stderr, "Please add %s to your PATH or specify it explicitly.\n", name);
			return 0;
		}
	}
	debug(D_DEBUG, "%s path: %s", name, path);
	return 1;
}

static int copy_executable(char *current_path, char *new_path)
{
	FILE *ifs, *ofs;

	ifs = fopen(current_path, "r");
	if(ifs == NULL) {
		fprintf(stderr, "Unable to open %s for reading: %s\n", current_path, strerror(errno));
		return 0;
	}
	ofs = fopen(new_path, "w+");
	if(ofs == NULL) {
		fprintf(stderr, "Unable to open %s for writing: %s", new_path, strerror(errno));
		fclose(ifs);
		return 0;
	}
	copy_stream_to_stream(ifs, ofs);
	fclose(ifs);
	fclose(ofs);
	chmod(new_path, 0777);

	return 1;
}

// TODO: // This is an experimental feature!!
void start_serving_masters(const char *catalog_host, int catalog_port, struct list *regex_list)
{
	struct list *matched_masters;

	while(!abort_flag) {
		matched_masters = get_masters_from_catalog(catalog_host, catalog_port, regex_list);
		debug(D_WQ, "Matching masters:\n");
		debug_print_masters(matched_masters);
		process_matched_masters(matched_masters);
		sleep(6);
	}
}

static void master_to_hash_key(struct work_queue_master *m, char *key)
{
	sprintf(key, "%s-%d-%llu", m->addr, m->port, m->start_time);
}

struct processed_master {
	char *master_hash_key;
	int hit;
};

// TODO: This is an experimental feature!!
void process_matched_masters(struct list *matched_masters)
{
	struct work_queue_master *m;
	char cmd[PATH_MAX] = "";
	char input_files[PATH_MAX] = "";
	char key[WORK_QUEUE_CATALOG_LINE_MAX];

	if(!matched_masters)
		return;

	char *tmp_key;
	struct processed_master *tmp_pm;
	hash_table_firstkey(processed_masters);
	while(hash_table_nextkey(processed_masters ,&tmp_key, (void **) &tmp_pm)) {
		tmp_pm->hit = 0;
	}

	list_first_item(matched_masters);
	while((m = (struct work_queue_master *) list_next_item(matched_masters))) {
		struct processed_master *pm;
		master_to_hash_key(m, key);
		if((pm = hash_table_lookup(processed_masters, key)) == 0) {

			snprintf(cmd, PATH_MAX, "./work_queue_worker -a -N %s", m->proj);
			snprintf(input_files, PATH_MAX, "work_queue_worker");

			int num_of_workers = 15;
			submit_workers(cmd, input_files, num_of_workers);
			printf("%d workers has been submitted for master: %s@%s:%d\n", num_of_workers, m->proj, m->addr, m->port);

		   	pm = (struct processed_master *)malloc(sizeof(*pm));
			if(pm == NULL) {
				fprintf(stderr, "Cannot allocate memory to record processed masters!\n");
				exit(1);
			}
			pm->master_hash_key = strdup(key);
			pm->hit = 1;
			hash_table_insert(processed_masters, key, pm);
		} else {
			debug(D_WQ, "Project %s@%s:%d has been processed. Skipping ...\n", m->proj, m->addr, m->port);
			pm->hit = 1;
		}
	}

	debug(D_WQ, "Processed masters list size: %d\n", hash_table_size(processed_masters));
	hash_table_firstkey(processed_masters);
	while(hash_table_nextkey(processed_masters, &tmp_key, (void **) &tmp_pm)) {
		if(tmp_pm) {
			if(tmp_pm->hit == 0) {
				tmp_pm = hash_table_remove(processed_masters, tmp_key);
				if(tmp_pm) {
					debug(D_WQ, "Removed %s from the processed masters list.\n", tmp_pm->master_hash_key);
					free(tmp_pm->master_hash_key);
					free(tmp_pm);
				} else {
					fprintf(stderr, "Error: failed to remove %s from the processed masters list.\n", tmp_key);
					exit(1);
				}
			}
		} else {
			fprintf(stderr, "Error: processed masters list contains invalid information.\n");
			exit(1);
		}
	}
}

int get_master_capacity(const char *catalog_host, int catalog_port, const char *proj)
{
	struct catalog_query *q;
	struct nvpair *nv;
	time_t stoptime;
	int capacity = 0;

	stoptime = time(0) + 5;

	q = catalog_query_create(catalog_host, catalog_port, stoptime);
	if(!q) {
		fprintf(stderr, "Failed to query catalog server at %s:%d\n", catalog_host, catalog_port);
		return 0;
	}

	while((nv = catalog_query_read(q, stoptime))) {
		if(strcmp(nvpair_lookup_string(nv, "type"), CATALOG_TYPE_WORK_QUEUE_MASTER) == 0) {
			if(strcmp(nvpair_lookup_string(nv, proj), proj) == 0) {
				capacity = nvpair_lookup_integer(nv, "capacity");
				nvpair_delete(nv);
				break;
			}
		}
		nvpair_delete(nv);
	}

	// Must delete the query otherwise it would occupy 1 tcp connection forever!
	catalog_query_delete(q);
	return capacity;
}

static int submit_workers(char *cmd, char *input_files, int count)
{
	int i;
	batch_job_id_t jobid;

	for(i = 0; i < count; i++) {
		debug(D_DEBUG, "Submitting job %d: %s\n", i + 1, cmd);
		jobid = batch_job_submit_simple(q, cmd, input_files, NULL);
		if(jobid >= 0) {
			itable_insert(remote_job_table, jobid, NULL);
		} else {
			retry_count--;
			if(!retry_count) {
				fprintf(stderr, "Retry max reached. Stop submitting more workers..\n");
				break;
			}
			fprintf(stderr, "Failed to submit the %dth job: %s. Will retry it.\n", i + 1, cmd);
			i--;
		}
	}
	return i;
}

static void remove_workers(struct itable *jobs)
{
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

static void check_jobs_status_condor(struct itable **running_jobs, struct itable **idle_jobs, struct itable **bad_jobs)
{
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

	all_job_status = hash_table_create(0, 0);
	while(fgets(line, sizeof(line), fp)) {
		if(sscanf(line, "%d\t%c\n", &jobid, &status) == 2) {
			ws = (struct worker_status *) malloc(sizeof(struct worker_status));
			if(!ws) {
				fprintf(stderr, "Cannot record status for job %d (%s)", jobid, strerror(errno));
			}
			ws->batch_job_id = jobid;
			ws->status = status;

			sprintf(hash_key, "%d", jobid);
			hash_table_insert(all_job_status, hash_key, ws);
		} else {
			fprintf(stderr, "Unrecognized line in condor_q output: %s", line);
		}
	}
	pclose(fp);

	// Insert jobs to their corresponding job tables
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
		sprintf(hash_key, "%d", (int) ikey);
		ws = hash_table_lookup(all_job_status, hash_key);
		if(ws) {
			//printf("%d\t%c\n", ws->batch_job_id, ws->status);
			if(ws->status == 'R') {
				itable_insert(running_job_table, ikey, NULL);
			} else if(ws->status == 'I') {
				itable_insert(idle_job_table, ikey, NULL);
			} else {
				itable_insert(bad_job_table, ikey, NULL);
			}
		}
	}

	*running_jobs = running_job_table;
	*idle_jobs = idle_job_table;
	*bad_jobs = bad_job_table;

	// Remove hash table
	char *key;
	hash_table_firstkey(all_job_status);
	while(hash_table_nextkey(all_job_status, &key, (void **) &ws)) {
		free(ws);
	}
	hash_table_delete(all_job_status);
}

static int guarantee_x_running_workers_local(char *cmd, char *input_files, int goal)
{
	int goal_achieved = 0;
	int count;

	count = submit_workers(cmd, input_files, goal);
	if(count == goal)
		goal_achieved = 1;

	return goal_achieved;
}

static int guarantee_x_running_workers_condor(char *cmd, char *input_files, int goal)
{
	struct itable *running_jobs;
	struct itable *idle_jobs;
	struct itable *bad_jobs;
	void *x;
	UINT64_T key;

	int submitted = 0;
	int running, idle, bad;
	int goal_achieved = 0;
	int count, extra;

	extra = MIN(EXTRA_WORKERS_MAX, goal * EXTRA_WORKERS_PERCENTAGE);
	count = submit_workers(cmd, input_files, goal + extra);
	submitted += count;
	printf("Target number of running workers is %d and %d workers has been submitted successfully.\n", goal, count);

	while(!abort_flag) {
		check_jobs_status_condor(&running_jobs, &idle_jobs, &bad_jobs);
		running = itable_size(running_jobs);
		idle = itable_size(idle_jobs);
		bad = itable_size(bad_jobs);
		if(submitted > running + idle + bad) {
			sleep(3);
			continue;
		}
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

		count = goal + extra - running - idle;
		if(count > 0) {
			// submit more
			count = submit_workers(cmd, input_files, count);
			submitted += count;
			printf("%d more workers has been submitted successfully.\n", count);
		}
		// We can delete these hash tables directly because the data (e->value) of each entry is NULL
		itable_delete(running_jobs);
		itable_delete(idle_jobs);
		itable_delete(bad_jobs);

		sleep(3);
	}

	if(abort_flag) {
		check_jobs_status_condor(&running_jobs, &idle_jobs, &bad_jobs);
		remove_workers(running_jobs);
		remove_workers(idle_jobs);
		remove_workers(bad_jobs);
		debug(D_WQ, "All jobs aborted.\n");
	}

	return goal_achieved;
}

static void show_help(const char *cmd)
{
	printf("Use: %s [options] <count>\n", cmd);
	printf("where batch options are:\n");
	printf("  -d <subsystem> Enable debugging for this subsystem.\n");
	printf("  -S <scratch>   Scratch directory. (default is /tmp/${USER}-workers)\n");
	printf("  -T <type>      Batch system type: %s. (default is local)\n", batch_queue_type_string());
	printf("  -r <count>     Number of attemps to retry if failed to submit a worker.\n");
	printf("  -m <count>     Each batch job will start <count> local workers. (default is 1.)\n");
	printf("  -W <path>      Path to the work_queue_worker executable.\n");
	printf("  -A             Enable auto worker pool feature (experimental).\n");
	printf("  -q             Guarantee <count> running workers and quit. The workers would terminate after their idle timeouts unless the user explicitly shut them down. The user needs to manually delete the scratch directory, which is displayed on screen right before work_queue_pool exits. \n");
	printf("  -h             Show this screen.\n");
	printf("\n");
	printf("where worker options are:\n");
	printf("  -a             Enable auto mode. In this mode the workers would ask a catalog server for available masters.\n");
	printf("  -t <time>      Abort after this amount of idle time.\n");
	printf("  -C <catalog>   Set catalog server to <catalog>. Format: HOSTNAME:PORT \n");
	printf("  -N <project>   Name of a preferred project. A worker can have multiple preferred projects.\n");
	printf("  -s             Run as a shared worker. By default the workers would only work for preferred projects.\n");
	printf("  -o <file>      Send debugging to this file.\n");
}

int main(int argc, char *argv[])
{
	int c, count;
	int goal = 0;
	char scratch_dir[PATH_MAX] = "";

	char worker_cmd[PATH_MAX] = "";
	char worker_path[PATH_MAX] = "";
	char worker_args[PATH_MAX] = "";
	char worker_input_files[PATH_MAX] = "";
	char pool_path[PATH_MAX] = "";
	char new_worker_path[PATH_MAX];
	char new_pool_path[PATH_MAX];

	int batch_queue_type = BATCH_QUEUE_TYPE_LOCAL;
	batch_job_id_t jobid;
	struct batch_job_info info;

	int auto_worker = 0;
	int guarantee_x_running_workers_and_quit = 0;
	int auto_worker_pool = 0;
	int workers_per_job = 0;

	char *catalog_host;
	int catalog_port;

	struct list *regex_list;

	regex_list = list_create();
	if(!regex_list) {
		fprintf(stderr, "cannot allocate memory for regex list!\n");
		exit(1);
	}

	catalog_host = CATALOG_HOST;
	catalog_port = CATALOG_PORT;

	while((c = getopt(argc, argv, "aAC:d:hm:N:qr:sS:t:T:W:")) != (char) -1) {
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
			list_push_tail(regex_list, strdup(optarg));
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
		case 'm':
			count = atoi(optarg);
			if(count < 0 || count > WORKERS_PER_JOB_MAX) {
				workers_per_job = 0;
			} else {
				workers_per_job = count;
			}
			break;
		case 'q':
			guarantee_x_running_workers_and_quit = 1;
			break;
		case 'A':
			auto_worker_pool = 1;
			break;
		case 'T':
			batch_queue_type = batch_queue_type_from_string(optarg);
			if(batch_queue_type == BATCH_QUEUE_TYPE_UNKNOWN) {
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
		}
	}

	if(!auto_worker_pool) {
		if(!auto_worker) {
			if((argc - optind) != 3) {
				fprintf(stderr, "invalid number of arguments\n");
				show_help(argv[0]);
				return EXIT_FAILURE;
			}
			// Add host name
			strcat(worker_args, " ");
			strcat(worker_args, argv[optind]);
			// Add port
			strcat(worker_args, " ");
			strcat(worker_args, argv[optind + 1]);
			// Number of workers to submit
			goal = strtol(argv[optind + 2], NULL, 10);
		} else {
			if((argc - optind) != 1) {
				fprintf(stderr, "invalid number of arguments\n");
				show_help(argv[0]);
				return EXIT_FAILURE;
			}
			goal = strtol(argv[optind], NULL, 10);
		}
	}

	signal(SIGINT, handle_abort);
	signal(SIGQUIT, handle_abort);
	signal(SIGTERM, handle_abort);


	if(!locate_executable("work_queue_worker", worker_path))
		return EXIT_FAILURE;

	if(workers_per_job) {
		if(batch_queue_type == BATCH_QUEUE_TYPE_LOCAL) {
			fprintf(stderr, "Error: '-m' option is not intended for the \"local\" batch queue type (which is the default if the '-T' option is not specified).\n");
			return EXIT_FAILURE;
		}
		if(!locate_executable("work_queue_pool", pool_path))
			return EXIT_FAILURE;
	}
	// Create a tmp directory to hold all workers' runtime information
	if(strlen(scratch_dir) <= 0) {
		if(batch_queue_type == BATCH_QUEUE_TYPE_CONDOR) {
			snprintf(scratch_dir, PATH_MAX, "/tmp/%s-workers/%ld", getenv("USER"), (long) time(0));
		} else {
			snprintf(scratch_dir, PATH_MAX, "%s-workers/%ld", getenv("USER"), (long) time(0));
		}
	}
	create_dir(scratch_dir, 0755);
	debug(D_DEBUG, "scratch dir: %s", scratch_dir);

	// Copy the worker program to the tmp directory and we will enter that
	// directory afterwards. We have to copy the worker program to a local
	// filesystem (other than afs, etc.) because condor might not be able
	// to access your shared file system
	snprintf(new_worker_path, PATH_MAX, "%s/work_queue_worker", scratch_dir);
	if(!copy_executable(worker_path, new_worker_path))
		return EXIT_FAILURE;

	if(workers_per_job) {
		snprintf(new_pool_path, PATH_MAX, "%s/work_queue_pool", scratch_dir);
		if(!copy_executable(pool_path, new_pool_path))
			return EXIT_FAILURE;
	}
	// Switch to the scratch dir.
	if(chdir(scratch_dir) < 0) {
		fprintf(stderr, "Unable to cd into scratch directory %s: %s\n", scratch_dir, strerror(errno));
		return EXIT_FAILURE;
	}
	// Set start worker command and specify the required input files
	if(!workers_per_job) {
		snprintf(worker_cmd, PATH_MAX, "./work_queue_worker %s", worker_args);
		snprintf(worker_input_files, PATH_MAX, "work_queue_worker");
	} else {
		// Create multiple local workers
		snprintf(worker_cmd, PATH_MAX, "./work_queue_pool %s %d", worker_args, workers_per_job);
		snprintf(worker_input_files, PATH_MAX, "work_queue_worker,work_queue_pool");
	}

	q = batch_queue_create(batch_queue_type);
	if(!q) {
		fatal("Unable to create batch queue of type: %s", batch_queue_type_to_string(batch_queue_type));
	}
	batch_queue_set_options(q, getenv("BATCH_OPTIONS"));
	remote_job_table = itable_create(0);

	// option: start x running workers and quit
	if(guarantee_x_running_workers_and_quit) {
		if(batch_queue_type == BATCH_QUEUE_TYPE_CONDOR) {
			guarantee_x_running_workers_condor(worker_cmd, worker_input_files, goal);
		} else if(batch_queue_type == BATCH_QUEUE_TYPE_LOCAL) {
			guarantee_x_running_workers_local(worker_cmd, worker_input_files, goal);
		} else {
			fprintf(stderr, "Sorry! Batch queue type \"%s\" is not supported for \"-q\" option at this time.\n", batch_queue_type_to_string(batch_queue_type));
			fprintf(stderr, "Currently supported batch queue type(s) for \"-q\": \n");
			fprintf(stderr, "%s\n", batch_queue_type_to_string(BATCH_QUEUE_TYPE_CONDOR));
			fprintf(stderr, "%s\n", batch_queue_type_to_string(BATCH_QUEUE_TYPE_LOCAL));
			fprintf(stderr, "\n");
		}
		printf("scratch directory: %s\n", scratch_dir);
		return EXIT_SUCCESS;
	}
	// option: automatically allocate workers for new masters 
	if(auto_worker_pool) {
		processed_masters = hash_table_create(0, 0);
		while(!abort_flag) {
			start_serving_masters(catalog_host, catalog_port, regex_list);
		}
		hash_table_delete(processed_masters);
	}

	if(!abort_flag) {
		count = submit_workers(worker_cmd, worker_input_files, goal);
		printf("%d workers are submitted successfully.\n", count);
	}
	// option: maintain a fixed number of workers
	while(!abort_flag) {
		jobid = batch_job_wait_timeout(q, &info, time(0) + 5);
		if(jobid >= 0 && !abort_flag) {
			itable_remove(remote_job_table, jobid);
			jobid = batch_job_submit_simple(q, worker_cmd, worker_input_files, NULL);
			if(jobid >= 0) {
				itable_insert(remote_job_table, jobid, NULL);
			}
		}
	}

	// Abort all jobs
	remove_workers(remote_job_table);
	delete_dir(scratch_dir);
	debug(D_WQ, "All jobs aborted.\n");

	batch_queue_delete(q);

	return EXIT_SUCCESS;
}

/*
 * vim: sts=4 sw=4 ts=4 ft=c
 */
