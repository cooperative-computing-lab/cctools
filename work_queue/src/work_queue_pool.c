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
#include "path.h"
#include "buffer.h"

#include "json.h"
#include "json_aux.h"

#include <errno.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <math.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/time.h>
#include <unistd.h>
#include <signal.h>

static const char *catalog_host = 0;
static int catalog_port = 0;
static int workers_min = 5;
static int workers_max = 100;
static double tasks_per_worker = -1;
static int worker_timeout = 300;
static int consider_capacity = 0;
static char *project_regex = 0;
static char *foremen_regex = 0;
static char *extra_worker_args=0;
static const char *resource_args=0;
static int abort_flag = 0;
static const char *scratch_dir = 0;
static const char *password_file = 0;
static char *config_file = 0;

/* -1 means 'not specified' */
static int num_cores_option  = -1;
static int num_disk_option   = -1;
static int num_memory_option = -1;
static int num_gpus_option   = -1;

struct batch_queue *queue = 0;

static void handle_abort( int sig )
{
	abort_flag = 1;
}

static void ignore_signal( int sig )
{
}

/*
Count up the workers needed in a given list of masters, IGNORING how many
workers are actually connected.
*/

static int count_workers_needed( struct list *masters_list, int only_waiting )
{
	int needed_workers=0;
	int masters=0;
	struct nvpair *nv;

	if(!masters_list) {
		return needed_workers;
	}

	list_first_item(masters_list);
	while((nv=list_next_item(masters_list))) {

		const char *project =   nvpair_lookup_string(nv,"project");
		const char *host =   nvpair_lookup_string(nv,"name");
		const int  port =    nvpair_lookup_integer(nv,"port");
		const char *owner =  nvpair_lookup_string(nv,"owner");
		const int tr =       nvpair_lookup_integer(nv,"tasks_running");
		const int tw =       nvpair_lookup_integer(nv,"tasks_waiting");
		const int tl =       nvpair_lookup_integer(nv,"tasks_left");
		const int capacity = nvpair_lookup_integer(nv,"capacity");

		int tasks = tr+tw+tl;

		int need;

		if(only_waiting) {
			need = tw;
		} else {
			need = tasks;
		}

		if(consider_capacity && capacity>0) {
			need = MIN(capacity,tasks);
		}

		debug(D_WQ,"%s %s:%d %s %d %d %d",project,host,port,owner,tasks,capacity,need);

		needed_workers += need;
		masters++;
	}

	if(tasks_per_worker > 0) {
		needed_workers = (int) ceil(needed_workers / tasks_per_worker);
	}

	return needed_workers;
}

static void set_worker_resources( struct batch_queue *queue )
{

	buffer_t b;
	buffer_init(&b);

	if(num_cores_option > -1) {
		batch_queue_set_int_option(queue, "cores",  num_cores_option);
		buffer_printf(&b, " --cores=%d", num_cores_option);
	}

	if(num_memory_option > -1) {
		batch_queue_set_int_option(queue, "memory", num_memory_option);
		buffer_printf(&b, " --memory=%d", num_memory_option);
	}

	if(num_disk_option > -1) {
		batch_queue_set_int_option(queue, "disk",   num_disk_option);
		buffer_printf(&b, " --disk=%d", num_disk_option);
	}

	if(num_gpus_option > -1) {
		batch_queue_set_int_option(queue, "gpus",   num_gpus_option);
		buffer_printf(&b, " --gpus=%d", num_gpus_option);
	}

	resource_args = xxstrdup(buffer_tostring(&b));
	buffer_free(&b);
}

static int submit_worker( struct batch_queue *queue, const char *master_regex )
{
	char cmd[1024];
	char extra_input_files[1024];

	sprintf(cmd,"./work_queue_worker -M %s -t %d -C %s:%d -d all -o worker.log ",master_regex,worker_timeout,catalog_host,catalog_port);
	strcpy(extra_input_files,"work_queue_worker");

	if(password_file) {
		strcat(cmd," -P pwfile");
		strcat(extra_input_files,",pwfile");
	}

	if(resource_args) {
		strcat(cmd," ");
		strcat(cmd,resource_args);
	}

	if(extra_worker_args) {
		strcat(cmd," ");
		strcat(cmd,extra_worker_args);
	}


	debug(D_WQ,"submitting worker: %s",cmd);

	return batch_job_submit(queue,cmd,extra_input_files,"output.log",0);
}

static void update_blacklisted_workers( struct batch_queue *queue, struct list *masters_list ) {

	if(!masters_list || list_size(masters_list) < 1)
		return;

	buffer_t b;
	struct nvpair *nv;

	buffer_init(&b);

	char *sep = "";
	list_first_item(masters_list);
	while((nv=list_next_item(masters_list))) {
		const char *blacklisted = nvpair_lookup_string(nv,"workers-blacklisted");
		if(blacklisted) {
			buffer_printf(&b, "%s%s", sep, blacklisted);
			sep = " ";
		}
	}

	if(buffer_pos(&b) > 0) {
		batch_queue_set_option(queue, "workers-blacklisted", buffer_tostring(&b));
	} else {
		batch_queue_set_option(queue, "workers-blacklisted", NULL);
	}

	buffer_free(&b);
}

static int submit_workers( struct batch_queue *queue, struct itable *job_table, int count, const char *master_regex )
{
	int i;
	for(i=0;i<count;i++) {
		int jobid = submit_worker(queue, master_regex);
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


static struct nvpair_header queue_headers[] = {
	{"project",       "PROJECT", NVPAIR_MODE_STRING,  NVPAIR_ALIGN_LEFT, 18},
	{"name",          "HOST",    NVPAIR_MODE_STRING,  NVPAIR_ALIGN_LEFT, 21},
	{"port",          "PORT",    NVPAIR_MODE_INTEGER, NVPAIR_ALIGN_RIGHT, 5},
	{"tasks_waiting", "WAITING", NVPAIR_MODE_INTEGER, NVPAIR_ALIGN_RIGHT, 7},
	{"tasks_running", "RUNNING", NVPAIR_MODE_INTEGER, NVPAIR_ALIGN_RIGHT, 7},
	{"tasks_complete","COMPLETE",NVPAIR_MODE_INTEGER, NVPAIR_ALIGN_RIGHT, 8},
	{"workers",       "WORKERS", NVPAIR_MODE_INTEGER, NVPAIR_ALIGN_RIGHT, 7},
	{NULL,NULL,0,0,0}
};

void print_stats(struct list *masters, struct list *foremen, int submitted, int needed, int requested)
{
	struct timeval tv;
	struct tm *tm;
	gettimeofday(&tv, 0);
	tm = localtime(&tv.tv_sec);

	needed    = needed    > 0 ? needed    : 0;
	requested = requested > 0 ? requested : 0;

	fprintf(stdout, "%04d/%02d/%02d %02d:%02d:%02d: "
			"|submitted: %d |needed: %d |requested: %d \n",
			tm->tm_year + 1900, tm->tm_mon + 1, tm->tm_mday, tm->tm_hour, tm->tm_min, tm->tm_sec,
			submitted, needed, requested);

	int master_count = 0;
	master_count += masters ? list_size(masters) : 0;
	master_count += foremen ? list_size(foremen) : 0;

	if(master_count < 1)
	{
		fprintf(stdout, "No change this cycle.\n\n");
		return;
	}

	nvpair_print_table_header(stdout, queue_headers);

	struct nvpair *nv;
	if(masters && list_size(masters) > 0)
	{
		fprintf(stdout, "masters:\n");

		list_first_item(masters);
		while((nv = list_next_item(masters)))
		{
			nvpair_print_table(nv, stdout, queue_headers);

		}
	}

	if(foremen && list_size(foremen) > 0)
	{
		fprintf(stdout, "foremen:\n");

		list_first_item(foremen);
		while((nv = list_next_item(foremen)))
		{
			nvpair_print_table(nv, stdout, queue_headers);

		}
	}

	fprintf(stdout, "\n");
}

void delete_projects_list(struct list *l)
{
	if(l) {
		struct nvpair *nv;
		while((nv=list_pop_head(l))) {
			nvpair_delete(nv);
		}
		list_delete(l);
	}
}

#define assign_new_value(new_var, old_var, option, type_c, type_json, field) \
	type_c new_var = old_var;\
	{\
		json_value *jv = jsonA_getname_raw(J, #option);\
		if(jv) {\
			if(jistype(jv, type_json)) {\
				new_var = jv->u.field;\
			} else {\
				debug(D_NOTICE, #option " has not a valid value.");\
				error_found = 1;\
			}\
		}\
	}
int read_config_file(const char *config_file) {
	static time_t last_time_modified = 0;

	struct stat s;
	time_t new_time_modified;
	if(stat(config_file, &s) < 0) {
		debug(D_NOTICE, "Error reading file %s (%s)", config_file, strerror(errno));
		return 0;
	}

	new_time_modified = s.st_mtime;
	if(new_time_modified == last_time_modified) {
		return 1;
	}

	json_value *J = NULL;
	int error_found = 0;

	J = jsonA_parse_file(config_file);

	if(!J || !jistype(J, json_object)) {
		debug(D_NOTICE, "Configuration file is not a valid json object: %s\n", config_file);
		return 0;
	}

	assign_new_value(new_workers_max, workers_max, max-workers, int, json_integer, integer)
	assign_new_value(new_workers_min, workers_min, min-workers, int, json_integer, integer)
	assign_new_value(new_worker_timeout, worker_timeout, timeout, int, json_integer, integer)

	assign_new_value(new_num_cores_option, num_cores_option, cores,    int, json_integer, integer)
	assign_new_value(new_num_disk_option, num_disk_option, disk,       int, json_integer, integer)
	assign_new_value(new_num_memory_option, num_memory_option, memory, int, json_integer, integer)


	assign_new_value(new_tasks_per_worker, tasks_per_worker, tasks-per-worker, double, json_double, dbl)

	assign_new_value(new_project_regex, project_regex, master-name, const char *, json_string, string.ptr)
	assign_new_value(new_foremen_regex, foremen_regex, foremen-name, const char *, json_string, string.ptr)
	assign_new_value(new_extra_worker_args, extra_worker_args, worker-extra-options, const char *, json_string, string.ptr)


	if(!new_project_regex || strlen(new_project_regex) == 0) {
		debug(D_NOTICE, "%s: master name is missing.\n", config_file);
		error_found = 1;
	}

	if(new_workers_min > new_workers_max) {
		debug(D_NOTICE, "%s: min workers (%d) is greater than max workers (%d)\n", config_file, new_workers_min, new_workers_max);
		error_found = 1;
	}

	if(new_workers_min < 0) {
		debug(D_NOTICE, "%s: min workers (%d) is less than zero.\n", config_file, new_workers_min);
		error_found = 1;
	}

	if(new_workers_max < 0) {
		debug(D_NOTICE, "%s: max workers (%d) is less than zero.\n", config_file, new_workers_max);
		error_found = 1;
	}

	if(error_found) {
		goto end;
	}

	workers_max    = new_workers_max;
	workers_min    = new_workers_min;
	worker_timeout = new_worker_timeout;
	tasks_per_worker = new_tasks_per_worker;

	num_cores_option = new_num_cores_option;
	num_memory_option = new_num_memory_option;
	num_disk_option = new_num_disk_option;

	if(new_project_regex != project_regex) {
		if(project_regex) free(project_regex); project_regex = xxstrdup(new_project_regex);
	}

	if(new_foremen_regex != foremen_regex) {
		if(foremen_regex)
			free(foremen_regex);
		free(foremen_regex);
		foremen_regex = xxstrdup(new_foremen_regex);
	}

	if(extra_worker_args != new_extra_worker_args) {
		if(extra_worker_args)
			free(extra_worker_args);
		free(extra_worker_args);
		extra_worker_args = xxstrdup(new_extra_worker_args);
	}

	last_time_modified = new_time_modified;
	fprintf(stdout, "Configuration file '%s' has been loaded.", config_file);

	fprintf(stdout, "master-name: %s\n", project_regex);
	if(foremen_regex) {
		fprintf(stdout, "foremen-name: %s\n", foremen_regex);
	}
	fprintf(stdout, "max-workers: %d\n", workers_max);
	fprintf(stdout, "min-workers: %d\n", workers_min);

	fprintf(stdout, "tasks-per-worker: %3.3lf\n", tasks_per_worker > 0 ? tasks_per_worker : (num_cores_option > 0 ? num_cores_option : 1));
	fprintf(stdout, "timeout: %d s\n", worker_timeout);
	fprintf(stdout, "cores: %d\n", num_cores_option > 0 ? num_cores_option : 1);

	if(num_memory_option > -1) {
		fprintf(stdout, "memory: %d MB\n", num_memory_option);
	}

	if(num_disk_option > -1) {
		fprintf(stdout, "disk: %d MB\n", num_disk_option);
	}

	if(extra_worker_args) {
		fprintf(stdout, "worker-extra-options: %s", extra_worker_args);
	}

end:
	json_value_free(J);
	return !error_found;
}

/*
Main loop of work queue pool.  Determine the number of workers needed by our
current list of masters, compare it to the number actually submitted, then
submit more until the desired state is reached.
*/

static void mainloop( struct batch_queue *queue, const char *project_regex, const char *foremen_regex )
{
	int workers_submitted = 0;
	struct itable *job_table = itable_create(0);

	struct list *masters_list = NULL;
	struct list *foremen_list = NULL;

	while(!abort_flag) {

		if(config_file && !read_config_file(config_file)) {
			debug(D_NOTICE, "Error re-reading '%s'. Using previous values.", config_file);
		} else {
			set_worker_resources( queue );
		}

		const char *submission_regex = foremen_regex ? foremen_regex : project_regex;

		masters_list = work_queue_catalog_query(catalog_host,catalog_port,project_regex);

		debug(D_WQ,"evaluating master list...");
		int workers_needed = count_workers_needed(masters_list, 0);

		debug(D_WQ,"%d total workers needed across %d masters",
				workers_needed,
				masters_list ? list_size(masters_list) : 0);

		if(foremen_regex)
		{
			debug(D_WQ,"evaluating foremen list...");
			foremen_list    = work_queue_catalog_query(catalog_host,catalog_port,foremen_regex);
			workers_needed += count_workers_needed(foremen_list, 1);
			debug(D_WQ,"%d total workers needed across %d foremen",workers_needed,list_size(foremen_list));
		}

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

		print_stats(masters_list, foremen_list, workers_submitted, workers_needed, new_workers_needed);

		update_blacklisted_workers(queue, masters_list);

		if(new_workers_needed>0) {
			debug(D_WQ,"submitting %d new workers to reach target",new_workers_needed);
			workers_submitted += submit_workers(queue,job_table,new_workers_needed,submission_regex);
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

		delete_projects_list(masters_list);
		delete_projects_list(foremen_list);

		sleep(30);
	}

	remove_all_workers(queue,job_table);
	itable_delete(job_table);
}

static void show_help(const char *cmd)
{
	printf("Use: work_queue_pool [options]\n");
	printf("where options are:\n");
	printf(" %-30s Project name of masters to serve, can be a regular expression.\n", "-M,--master-name=<project>");
	printf(" %-30s Foremen to serve, can be a regular expression.\n", "-F,--foremen-name=<project>");
	printf(" %-30s Batch system type (required). One of: %s\n", "-T,--batch-type=<type>",batch_queue_type_string());
	printf(" %-30s Password file for workers to authenticate to master.\n","-P,--password");
	printf(" %-30s Use configuration file <file>.\n","-C,--config-file=<file>");
	printf(" %-30s Minimum workers running.  (default=%d)\n", "-w,--min-workers", workers_min);
	printf(" %-30s Maximum workers running.  (default=%d)\n", "-W,--max-workers", workers_max);
	printf(" %-30s Average tasks per worker. (default=one task per core)\n", "--tasks-per-worker");
	printf(" %-30s Workers abort after this amount of idle time. (default=%d)\n", "-t,--timeout=<time>",worker_timeout);
	printf(" %-30s Extra options that should be added to the worker.\n", "-E,--extra-options=<options>");
	printf(" %-30s Set the number of cores requested per worker.\n", "--cores=<n>");
	printf(" %-30s Set the number of GPUs requested per worker.\n", "--gpus=<n>");
	printf(" %-30s Set the amount of memory (in MB) requested per worker.\n", "--memory=<mb>           ");
	printf(" %-30s Set the amount of disk (in MB) requested per worker.\n", "--disk=<mb>");
	printf(" %-30s Use this scratch dir for temporary files. (default is /tmp/wq-pool-$uid)\n","-S,--scratch-dir");
	printf(" %-30s Use worker capacity reported by masters.","-c,--capacity");
	printf(" %-30s Enable debugging for this subsystem.\n", "-d,--debug=<subsystem>");
	printf(" %-30s Send debugging to this file. (can also be :stderr, :stdout, :syslog, or :journal)\n", "-o,--debug-file=<file>");
	printf(" %-30s Show this screen.\n", "-h,--help");
}

enum { LONG_OPT_CORES = 255, LONG_OPT_MEMORY, LONG_OPT_DISK, LONG_OPT_GPUS, LONG_OPT_TASKS_PER_WORKER, LONG_OPT_CONF_FILE };
static const struct option long_options[] = {
	{"master-name", required_argument, 0, 'M'},
	{"foremen-name", required_argument, 0, 'F'},
	{"batch-type", required_argument, 0, 'T'},
	{"password", required_argument, 0, 'P'},
	{"config-file", required_argument, 0, 'C'},
	{"min-workers", required_argument, 0, 'w'},
	{"max-workers", required_argument, 0, 'W'},
	{"tasks-per-worker", required_argument, 0, LONG_OPT_TASKS_PER_WORKER},
	{"timeout", required_argument, 0, 't'},
	{"extra-options", required_argument, 0, 'E'},
	{"cores",  required_argument,  0,  LONG_OPT_CORES},
	{"memory", required_argument,  0,  LONG_OPT_MEMORY},
	{"disk",   required_argument,  0,  LONG_OPT_DISK},
	{"gpus",   required_argument,  0,  LONG_OPT_GPUS},
	{"scratch-dir", required_argument, 0, 'S' },
	{"capacity", no_argument, 0, 'c' },
	{"debug", required_argument, 0, 'd'},
	{"debug-file", required_argument, 0, 'o'},
	{"debug-file-size", required_argument, 0, 'O'},
	{"version", no_argument, 0, 'v'},
	{"help", no_argument, 0, 'h'},
	{0,0,0,0}
};


int main(int argc, char *argv[])
{
	batch_queue_type_t batch_queue_type = BATCH_QUEUE_TYPE_UNKNOWN;

	catalog_host = CATALOG_HOST;
	catalog_port = CATALOG_PORT;

	debug_config(argv[0]);

	int c;

	while((c = getopt_long(argc, argv, "C:F:N:M:T:t:w:W:E:P:S:cd:o:O:vh", long_options, NULL)) > -1) {
		switch (c) {
			case 'C':
				config_file = xxstrdup(optarg);
				break;
			case 'F':
				foremen_regex = xxstrdup(optarg);
				break;
			case 'N':
			case 'M':
				project_regex = xxstrdup(optarg);
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
			case LONG_OPT_TASKS_PER_WORKER:
				tasks_per_worker = atof(optarg);
				break;
			case 'E':
				extra_worker_args = xxstrdup(optarg);
				break;
			case LONG_OPT_CORES:
				num_cores_option = atoi(optarg);
				break;
			case LONG_OPT_MEMORY:
				num_memory_option = atoi(optarg);
				break;
			case LONG_OPT_DISK:
				num_disk_option = atoi(optarg);
				break;
			case LONG_OPT_GPUS:
				num_gpus_option = atoi(optarg);
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

	if(batch_queue_type == BATCH_QUEUE_TYPE_UNKNOWN) {
		fprintf(stderr,"work_queue_pool: You must specify a batch type with the -T option.\n");
		fprintf(stderr, "valid options:\n");
		fprintf(stderr, "%s\n", batch_queue_type_string());
		return 1;
	}

	if(config_file) {
		char abs_path_name[PATH_MAX];

		if(!realpath(config_file, abs_path_name)) {
			fprintf(stderr, "work_queue_pool: could not resolve configuration file path: '%s'.\n", config_file);
			exit(EXIT_FAILURE);
		}

		free(config_file);

		/* From now on, read config_file from absolute path */
		config_file = xxstrdup(abs_path_name);

		if(!read_config_file(config_file)) {
			fprintf(stderr,"work_queue_pool: There were errors in the configuration file: %s\n", config_file);
			return 1;
		}
	}

	if(!project_regex) {
		fprintf(stderr,"work_queue_pool: You must give a project name with the -M option, or the master-name option with a configuration file.\n");
		return 1;
	}

	if(workers_min>workers_max) {
		fprintf(stderr,"work_queue_pool: min workers (%d) is greater than max workers (%d)\n",workers_min, workers_max);
		return 1;
	}

	if(!scratch_dir) {
		scratch_dir = string_format("/tmp/wq-pool-%d",getuid());
	}

	if(!create_dir(scratch_dir,0777)) {
		fprintf(stderr,"work_queue_pool: couldn't create %s: %s",scratch_dir,strerror(errno));
		return 1;
	}

	char cmd[1024];
	sprintf(cmd,"cp \"$(which work_queue_worker)\" '%s'",scratch_dir);
	if (system(cmd)) {
		fprintf(stderr, "work_queue_pool: please add work_queue_worker to your PATH.\n");
		exit(EXIT_FAILURE);
	}

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

	queue = batch_queue_create(batch_queue_type);
	if(!queue) {
		fprintf(stderr,"work_queue_pool: couldn't establish queue type %s",batch_queue_type_to_string(batch_queue_type));
		return 1;
	}

	set_worker_resources( queue );

	mainloop( queue, project_regex, foremen_regex );

	batch_queue_delete(queue);

	return 0;
}

/* vim: set noexpandtab tabstop=4: */
