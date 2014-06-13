/*
Copyright (C) 2014- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "work_queue_catalog.h"

#include "cctools.h"
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
#include "list.h"
#include "get_line.h"
#include "getopt.h"

#include <errno.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <signal.h>

static const char *catalog_host = 0;
static int catalog_port = 0;
static int workers_min = 5;
static int workers_max = 100;
static int consider_capacity = 0;
static const char *project_regex = 0;
static int worker_timeout = 300;
static const char *extra_worker_args=0;
static int abort_flag = 0;
static const char *scratch_dir = 0;
static const char *password_file = 0;

static void handle_abort( int sig )
{
	abort_flag = 1;
}

static void ignore_signal( int sig )
{
}

/*
Count up the workers needed in a given list of masters, IGNORING how many workers are actually connected.
*/

static int count_workers_needed( struct list *masters_list )
{
	int needed_workers=0;
	int masters=0;
	struct nvpair *nv;

	debug(D_WQ,"evaluating master list...");

	list_first_item(masters_list);
	while((nv=list_next_item(masters_list))) {

		const char *project =   nvpair_lookup_string(nv,"project");
		const char *host =   nvpair_lookup_string(nv,"name");
		const int  port =    nvpair_lookup_integer(nv,"port");
		const char *owner =  nvpair_lookup_string(nv,"owner");
		const int tr =       nvpair_lookup_integer(nv,"tasks_running");
		const int tw =       nvpair_lookup_integer(nv,"tasks_waiting");
		const int capacity = nvpair_lookup_integer(nv,"capacity");

		int tasks = tr+tw;
		int need = tasks;

		if(consider_capacity && capacity>0) {
			need = MIN(capacity,tasks);
		}

		debug(D_WQ,"%s %s:%d %s %d %d %d",project,host,port,owner,tasks,capacity,need);

		needed_workers += need;
		masters++;
	}

	debug(D_WQ,"%d total workers needed across %d masters",needed_workers,masters);

	return needed_workers;
}

static int submit_worker( struct batch_queue *queue )
{
	char cmd[1024];
	char extra_input_files[1024];

	sprintf(cmd,"./work_queue_worker -M %s -t %d -C %s:%d -d all -o worker.log ",project_regex,worker_timeout,catalog_host,catalog_port);
	strcpy(extra_input_files,"work_queue_worker");

	if(password_file) {
		strcat(cmd," -P pwfile");
		strcat(extra_input_files,",pwfile");
	}

	if(extra_worker_args) {
		strcat(cmd," ");
		strcat(cmd,extra_worker_args);
	}

	debug(D_WQ,"submitting worker: %s",cmd);

	return batch_job_submit_simple(queue,cmd,extra_input_files,"output.log");
}

static int submit_workers( struct batch_queue *queue, struct itable *job_table, int count )
{
	int i;
	for(i=0;i<count;i++) {
		int jobid = submit_worker(queue);
		if(jobid>0) {
			debug(D_WQ,"worker job %d submitted",jobid);
			itable_insert(job_table,jobid,(void*)1);
		} else {
			break;
		}
	}
	return i;
}

void remove_all_workers( struct batch_queue *queue, struct itable *job_table )
{
	uint64_t jobid;
	void *value;

	debug(D_WQ,"removing all remaining worker jobs...");
	int count = itable_size(job_table);
	itable_firstkey(job_table);
	while(itable_nextkey(job_table,&jobid,&value)) {
		debug(D_WQ,"removing job %"PRId64,jobid);
		batch_job_remove(queue,jobid);
	}
	debug(D_WQ,"%d workers removed.",count);

}

/*
Main loop of work queue pool.  Determine the number of workers needed by our
current list of masters, compare it to the number actually submitted, then
submit more until the desired state is reached.
*/

static void mainloop( struct batch_queue *queue, const char *project_regex )
{
	int workers_submitted = 0;
	struct itable *job_table = itable_create(0);

	while(!abort_flag) {
	        struct list *masters_list = work_queue_catalog_query_cached(catalog_host,catalog_port,project_regex);

		int workers_needed = count_workers_needed(masters_list);

		debug(D_WQ,"raw workers needed: %d", workers_needed);

		if(workers_needed > workers_max) {
			debug(D_WQ,"applying maximum of %d workers",workers_max);
			workers_needed = workers_max;
		}

		if(workers_needed < workers_min) {
			debug(D_WQ,"applying minimum of %d workers",workers_min);
			workers_needed = workers_min;
		}

		int new_workers_needed = workers_needed - workers_submitted;

		debug(D_WQ,"workers needed: %d",workers_needed);
		debug(D_WQ,"workers in queue: %d",workers_submitted);

		if(new_workers_needed>0) {
			debug(D_WQ,"submitting %d new workers to reach target",new_workers_needed);
			workers_submitted += submit_workers(queue,job_table,new_workers_needed);
		} else if(new_workers_needed<0) {
			debug(D_WQ,"too many workers, will wait for some to exit");
		} else {
			debug(D_WQ,"target number of workers is reached.");
		}

		debug(D_WQ,"checking for exited workers...");
		time_t stoptime = time(0)+5;

		while(1) {
			struct batch_job_info info;
			batch_job_id_t jobid;
			jobid = batch_job_wait_timeout(queue,&info,stoptime);
			if(jobid>0) {
				if(itable_lookup(job_table,jobid)) {
					itable_remove(job_table,jobid);
					debug(D_WQ,"worker job %"PRId64" exited",jobid);
					workers_submitted--;
				} else {
					// it may have been a job from a previous run.					
				}
			} else {
				break;
			}
		}

		sleep(5);
	}

	remove_all_workers(queue,job_table);
	itable_delete(job_table);
}

static void show_help(const char *cmd)
{
	printf("Use: work_queue_pool [options]\n");
	printf("where options are:\n");
	printf(" %-30s Project name of masters to serve, can be a regular expression.\n", "-M,--master-name=<project>");
	printf(" %-30s Batch system type. One of: %s (default is local)\n", "-T,--batch-type=<type>",batch_queue_type_string());
	printf(" %-30s Password file for workers to authenticate to master.\n","-P,--password");
	printf(" %-30s Minimum workers running.  (default=%d)\n", "-w,--min-workers", workers_min);
	printf(" %-30s Maximum workers running.  (default=%d)\n", "-W,--max-workers", workers_max);
	printf(" %-30s Workers abort after this amount of idle time. (default=%d)\n", "-t,--timeout=<time>",worker_timeout);
	printf(" %-30s Extra options that should be added to the worker.\n", "-E,--extra-options=<options>");
	printf(" %-30s Use this scratch dir for temporary files. (default is /tmp/wq-pool-$uid)","-S,--scratch-dir");
	printf(" %-30s Use worker capacity reported by masters.","-c,--capacity");
	printf(" %-30s Enable debugging for this subsystem.\n", "-d,--debug=<subsystem>");
	printf(" %-30s Send debugging to this file. (can also be :stderr, :stdout, :syslog, or :journal)\n", "-o,--debug-file=<file>");
	printf(" %-30s Show this screen.\n", "-h,--help");
}

int main(int argc, char *argv[])
{
	batch_queue_type_t batch_queue_type = BATCH_QUEUE_TYPE_LOCAL;

	catalog_host = CATALOG_HOST;
	catalog_port = CATALOG_PORT;

	debug_config(argv[0]);

	static struct option long_options[] = {
		{"master-name", required_argument, 0, 'M'},
		{"batch-type", required_argument, 0, 'T'},
		{"password", required_argument, 0, 'P'},
		{"min-workers", required_argument, 0, 'w'},
		{"max-workers", required_argument, 0, 'w'},
		{"timeout", required_argument, 0, 't'},
		{"extra-options", required_argument, 0, 'E'},
		{"scratch-dir", required_argument, 0, 'S' },
		{"capacity", no_argument, 0, 'c' },
		{"debug", required_argument, 0, 'd'},
		{"debug-file", required_argument, 0, 'o'},
		{"debug-file-size", required_argument, 0, 'O'},
		{"version", no_argument, 0, 'v'},
		{"help", no_argument, 0, 'h'},
		{0,0,0,0}
	};

	char c;

	while((c = getopt_long(argc, argv, "N:M:T:t:w:W:E:P:S:cd:o:O:vh", long_options, NULL)) > -1) {
		switch (c) {
		case 'N':
		case 'M':
			project_regex = optarg;
			break;
		case 'T':
			batch_queue_type = batch_queue_type_from_string(optarg);
			if(batch_queue_type == BATCH_QUEUE_TYPE_UNKNOWN) {
				fprintf(stderr, "unknown batch queue type: %s\n", optarg);
				return EXIT_FAILURE;
			}
			break;
		case 't':
			worker_timeout = atoi(optarg);
			break;
		case 'w':
			workers_min = atoi(optarg);
			break;
		case 'W':
			workers_max = atoi(optarg);
			break;
		case 'E':
			extra_worker_args = optarg;
			break;
		case 'P':
			password_file = optarg;
			break;
		case 'S':
			scratch_dir = optarg;
			break;
		case 'c':
			consider_capacity = 1;
			break;
		case 'd':
			debug_flags_set(optarg);
			break;
		case 'o':
			debug_config_file(optarg);
			break;
		case 'O':
			debug_config_file_size(string_metric_parse(optarg));
			break;
		case 'v':
			cctools_version_print(stdout, argv[0]);
			exit(EXIT_SUCCESS);
		case 'h':
			show_help(argv[0]);
 			exit(EXIT_SUCCESS);
		default:
			show_help(argv[0]);
			return EXIT_FAILURE;
		}
	}

	cctools_version_debug(D_DEBUG, argv[0]);

	if(!project_regex) {
		fprintf(stderr,"work_queue_pool: You must give a project name with the -M option.\n");
		return 1;
	}

	if(workers_min>workers_max) {
		fprintf(stderr,"work_queue_pool: --min-workers (%d) is greater than --max-workers (%d)\n",workers_min,workers_max);
		return 1;
	}

	if(!scratch_dir) {
		scratch_dir = string_format("/tmp/wq-pool-%d",getuid());
	}

	if(!create_dir(scratch_dir,0700)) {
		fprintf(stderr,"work_queue_pool: couldn't create %s: %s",scratch_dir,strerror(errno));
		return 1;
	}

	char cmd[1024];
	sprintf(cmd,"cp `which work_queue_worker` %s",scratch_dir);
	system(cmd);

	if(password_file) {
		sprintf(cmd,"cp %s %s/pwfile",password_file,scratch_dir);
		system(cmd);
	}

	if(chdir(scratch_dir)!=0) {
		fprintf(stderr,"work_queue_pool: couldn't chdir to %s: %s",scratch_dir,strerror(errno));
		return 1;
	}

	signal(SIGINT, handle_abort);
        signal(SIGQUIT, handle_abort);
        signal(SIGTERM, handle_abort);
        signal(SIGHUP, ignore_signal);

	struct batch_queue * queue = batch_queue_create(batch_queue_type);
	if(!queue) {
		fprintf(stderr,"work_queue_pool: couldn't establish queue type %s",batch_queue_type_to_string(batch_queue_type));
		return 1;
	}

	mainloop( queue, project_regex );

	batch_queue_delete(queue);

	return 0;
}
