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
#include "rmsummary.h"

#include "jx.h"
#include "jx_parse.h"
#include "jx_table.h"

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
static int autosize = 0;
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
static char *amazon_credentials = NULL;
static char *amazon_ami = NULL;
static char *condor_requirements = NULL;
static char *batch_submit_options = NULL;

/* -1 means 'not specified' */
static struct rmsummary *resources = NULL;

static int64_t factory_timeout = 0;

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
	struct jx *j;

	if(!masters_list) {
		return needed_workers;
	}

	list_first_item(masters_list);
	while((j=list_next_item(masters_list))) {

		const char *project =jx_lookup_string(j,"project");
		const char *host =   jx_lookup_string(j,"name");
		const int  port =    jx_lookup_integer(j,"port");
		const char *owner =  jx_lookup_string(j,"owner");
		const int tr =       jx_lookup_integer(j,"tasks_running");
		const int tw =       jx_lookup_integer(j,"tasks_waiting");
		const int tl =       jx_lookup_integer(j,"tasks_left");
		const int capacity = jx_lookup_integer(j,"capacity");

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

static void set_worker_resources_options( struct batch_queue *queue )
{
	buffer_t b;
	buffer_init(&b);

	if(batch_queue_supports_feature(queue, "autosize") && autosize) {
		buffer_printf(&b, " --cores=$$(TotalSlotCpus) --memory=$$(TotalSlotMemory) --disk=$$(TotalSlotDisk)");
	} else {
		if(resources->cores > -1) {
			buffer_printf(&b, " --cores=%" PRId64, resources->cores);
		}

		if(resources->memory > -1) {
			buffer_printf(&b, " --memory=%" PRId64, resources->memory);
		}

		if(resources->disk > -1) {
			buffer_printf(&b, " --disk=%" PRId64, resources->disk);
		}
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

	return batch_job_submit(queue,cmd,extra_input_files,"output.log",0,resources);
}

static void update_blacklisted_workers( struct batch_queue *queue, struct list *masters_list ) {

	if(!masters_list || list_size(masters_list) < 1)
		return;

	buffer_t b;
	struct jx *j;

	buffer_init(&b);

	char *sep = "";
	list_first_item(masters_list);
	while((j=list_next_item(masters_list))) {
		const char *blacklisted = jx_lookup_string(j,"workers-blacklisted");
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

static struct jx_table queue_headers[] = {
{"project",       "PROJECT", JX_TABLE_MODE_PLAIN, JX_TABLE_ALIGN_LEFT, 18},
{"name",          "HOST",    JX_TABLE_MODE_PLAIN, JX_TABLE_ALIGN_LEFT, 21},
{"port",          "PORT",    JX_TABLE_MODE_PLAIN, JX_TABLE_ALIGN_RIGHT, 5},
{"tasks_waiting", "WAITING", JX_TABLE_MODE_PLAIN, JX_TABLE_ALIGN_RIGHT, 7},
{"tasks_running", "RUNNING", JX_TABLE_MODE_PLAIN, JX_TABLE_ALIGN_RIGHT, 7},
{"tasks_complete","COMPLETE",JX_TABLE_MODE_PLAIN, JX_TABLE_ALIGN_RIGHT, 8},
{"workers",       "WORKERS", JX_TABLE_MODE_PLAIN, JX_TABLE_ALIGN_RIGHT, 7},
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

	jx_table_print_header(queue_headers,stdout);

	struct jx *j;
	if(masters && list_size(masters) > 0)
	{
		fprintf(stdout, "masters:\n");

		list_first_item(masters);
		while((j = list_next_item(masters)))
		{
			jx_table_print(queue_headers, j, stdout);
		}
	}

	if(foremen && list_size(foremen) > 0)
	{
		fprintf(stdout, "foremen:\n");

		list_first_item(foremen);
		while((j = list_next_item(foremen)))
		{
			jx_table_print(queue_headers, j, stdout);

		}
	}

	fprintf(stdout, "\n");
}

void delete_projects_list(struct list *l)
{
	if(l) {
		struct jx *j;
		while((j=list_pop_head(l))) {
			jx_delete(j);
		}
		list_delete(l);
	}
}

#define assign_new_value(new_var, old_var, option, type_c, type_json, field) \
	type_c new_var = old_var;\
	{\
		struct jx *jv = jx_lookup(J,#option); \
		if(jv) {\
			if(jv->type==type_json) {\
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

	int error_found = 0;

	struct jx *J = jx_parse_file(config_file);

	if(!J || J->type!=JX_OBJECT) {
		debug(D_NOTICE, "Configuration file is not a valid json object: %s\n", config_file);
		return 0;
	}

	assign_new_value(new_workers_max, workers_max, max-workers, int, JX_INTEGER, integer_value)
	assign_new_value(new_workers_min, workers_min, min-workers, int, JX_INTEGER, integer_value)
	assign_new_value(new_worker_timeout, worker_timeout, timeout, int, JX_INTEGER, integer_value)

	assign_new_value(new_num_cores_option, resources->cores, cores,    int, JX_INTEGER, integer_value)
	assign_new_value(new_num_disk_option,  resources->disk, disk,      int, JX_INTEGER, integer_value)
	assign_new_value(new_num_memory_option, resources->memory, memory, int, JX_INTEGER, integer_value)

	assign_new_value(new_autosize_option, autosize, autosize, int, JX_INTEGER, integer_value)

	assign_new_value(new_factory_timeout_option, factory_timeout, factory-timeout, int, JX_INTEGER, integer_value)

	assign_new_value(new_tasks_per_worker, tasks_per_worker, tasks-per-worker, double, JX_DOUBLE, double_value)

	assign_new_value(new_project_regex, project_regex, master-name, const char *, JX_STRING, string_value)
	assign_new_value(new_foremen_regex, foremen_regex, foremen-name, const char *, JX_STRING, string_value)
	assign_new_value(new_extra_worker_args, extra_worker_args, worker-extra-options, const char *, JX_STRING, string_value)

	assign_new_value(new_condor_requirements, condor_requirements, condor-requirements, const char *, JX_STRING, string_value)

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

	if(new_factory_timeout_option < 0) {
		debug(D_NOTICE, "%s: factory timeout (%d) is less than zero.\n", config_file, new_factory_timeout_option);
		error_found = 1;
	}

	if(error_found) {
		goto end;
	}

	workers_max    = new_workers_max;
	workers_min    = new_workers_min;
	worker_timeout = new_worker_timeout;
	tasks_per_worker = new_tasks_per_worker;
	autosize         = new_autosize_option;
	factory_timeout  = new_factory_timeout_option;

	resources->cores  = new_num_cores_option;
	resources->memory = new_num_memory_option;
	resources->disk   = new_num_disk_option;

	if(new_project_regex != project_regex) {
		if(project_regex) {
			free(project_regex);
		}
		project_regex = xxstrdup(new_project_regex);
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

	if(new_condor_requirements != condor_requirements) {
		if(condor_requirements) {
			free(condor_requirements);
		}
		condor_requirements = xxstrdup(new_condor_requirements);
	}

	last_time_modified = new_time_modified;
	fprintf(stdout, "Configuration file '%s' has been loaded.", config_file);

	fprintf(stdout, "master-name: %s\n", project_regex);
	if(foremen_regex) {
		fprintf(stdout, "foremen-name: %s\n", foremen_regex);
	}
	fprintf(stdout, "max-workers: %d\n", workers_max);
	fprintf(stdout, "min-workers: %d\n", workers_min);

	fprintf(stdout, "tasks-per-worker: %3.3lf\n", tasks_per_worker > 0 ? tasks_per_worker : (resources->cores > 0 ? resources->cores : 1));
	fprintf(stdout, "timeout: %d s\n", worker_timeout);
	fprintf(stdout, "cores: %" PRId64 "\n", resources->cores > 0 ? resources->cores : 1);

	fprintf(stdout, "condor-requirements: %s\n", condor_requirements);

	if(factory_timeout > 0) {
		fprintf(stdout, "factory-timeout: %" PRId64 " MB\n", factory_timeout);
	}

	if(resources->memory > -1) {
		fprintf(stdout, "memory: %" PRId64 " MB\n", resources->memory);
	}

	if(resources->disk > -1) {
		fprintf(stdout, "disk: %" PRId64 " MB\n", resources->disk);
	}

	if(extra_worker_args) {
		fprintf(stdout, "worker-extra-options: %s", extra_worker_args);
	}

end:
	jx_delete(J);
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

	int64_t factory_timeout_start = time(0);

	while(!abort_flag) {

		if(config_file && !read_config_file(config_file)) {
			debug(D_NOTICE, "Error re-reading '%s'. Using previous values.", config_file);
		} else {
			set_worker_resources_options( queue );
			batch_queue_set_option(queue, "autosize", autosize ? "yes" : NULL);
		}

		const char *submission_regex = foremen_regex ? foremen_regex : project_regex;

		masters_list = work_queue_catalog_query(catalog_host,catalog_port,project_regex);

		if(masters_list && list_size(masters_list) > 0)
		{
			factory_timeout_start = time(0);
		} else {
			// check to see if factory timeout is triggered, factory timeout will be 0 if flag isn't set
			if(factory_timeout > 0)
			{
				if(time(0) - factory_timeout_start > factory_timeout) {
					fprintf(stderr, "There have been no masters for longer then the factory timeout, exiting\n");
					abort_flag=1;
					break;
				}
			}
		}
	
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

		debug(D_WQ,"workers needed: %d",    workers_needed);
		debug(D_WQ,"workers submitted: %d", workers_submitted);
		debug(D_WQ,"workers requested: %d", new_workers_needed);

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
	printf("Use: work_queue_factory [options]\n");
	printf("where options are:\n");
	printf(" %-30s Project name of masters to serve, can be a regular expression.\n", "-M,--master-name=<project>");
	printf(" %-30s Foremen to serve, can be a regular expression.\n", "-F,--foremen-name=<project>");
	printf(" %-30s Batch system type (required). One of: %s\n", "-T,--batch-type=<type>",batch_queue_type_string());
	printf(" %-30s Add these options to all batch submit files.\n", "-B,--batch-options=<options>");
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
	printf(" %-30s Automatically size a worker to an available slot (Condor only).\n", "--autosize");
	printf(" %-30s Manually set requirements for the workers as condor jobs. May be specified several times, with the expresions and-ed together (Condor only).\n", "--condor-requirements");
	printf(" %-30s Exit after no master has been seen in <n> seconds.\n", "--factory-timeout");
	printf(" %-30s Use this scratch dir for temporary files. (default is /tmp/wq-pool-$uid)\n","-S,--scratch-dir");
	printf(" %-30s Use worker capacity reported by masters.","-c,--capacity");
	printf(" %-30s Enable debugging for this subsystem.\n", "-d,--debug=<subsystem>");
	printf(" %-30s Specify path to Amazon credentials (for use with -T amazon)\n", "--amazon-credentials");
	printf(" %-30s Specify amazon machine image (AMI). (for use with -T amazon)\n", "--amazon-ami");
	printf(" %-30s Send debugging to this file. (can also be :stderr, :stdout, :syslog, or :journal)\n", "-o,--debug-file=<file>");
	printf(" %-30s Show this screen.\n", "-h,--help");
}

enum { LONG_OPT_CORES = 255, LONG_OPT_MEMORY, LONG_OPT_DISK, LONG_OPT_GPUS, LONG_OPT_TASKS_PER_WORKER, LONG_OPT_CONF_FILE, LONG_OPT_AMAZON_CREDENTIALS, LONG_OPT_AMAZON_AMI, LONG_OPT_FACTORY_TIMEOUT, LONG_OPT_AUTOSIZE, LONG_OPT_CONDOR_REQUIREMENTS };
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
	{"amazon-credentials", required_argument, 0, LONG_OPT_AMAZON_CREDENTIALS},
	{"amazon-ami", required_argument, 0, LONG_OPT_AMAZON_AMI},
	{"autosize", no_argument, 0, LONG_OPT_AUTOSIZE},
	{"factory-timeout", required_argument, 0, LONG_OPT_FACTORY_TIMEOUT},
	{"condor-requirements", required_argument, 0, LONG_OPT_CONDOR_REQUIREMENTS},
	{0,0,0,0}
};


int main(int argc, char *argv[])
{
	batch_queue_type_t batch_queue_type = BATCH_QUEUE_TYPE_UNKNOWN;

	catalog_host = CATALOG_HOST;
	catalog_port = CATALOG_PORT;

	batch_submit_options = getenv("BATCH_OPTIONS");

	debug_config(argv[0]);

	resources = rmsummary_create(-1);

	int c;

	while((c = getopt_long(argc, argv, "B:C:F:N:M:T:t:w:W:E:P:S:cd:o:O:vh", long_options, NULL)) > -1) {
		switch (c) {
			case 'B':
				batch_submit_options = xxstrdup(optarg);
				break;
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
				resources->cores = atoi(optarg);
				break;
			case LONG_OPT_AMAZON_CREDENTIALS:
				amazon_credentials = xxstrdup(optarg);
				break;
			case LONG_OPT_AMAZON_AMI:
				amazon_ami = xxstrdup(optarg);
				break;
			case LONG_OPT_MEMORY:
				resources->memory = atoi(optarg);
				break;
			case LONG_OPT_DISK:
				resources->disk = atoi(optarg);
				break;
			case LONG_OPT_GPUS:
				resources->gpus = atoi(optarg);
				break;
			case LONG_OPT_AUTOSIZE:
				autosize = 1;
				break;
			case LONG_OPT_FACTORY_TIMEOUT:
				factory_timeout = MAX(0, atoi(optarg));
				break;
			case LONG_OPT_CONDOR_REQUIREMENTS:
				if(condor_requirements) {
					char *tmp = condor_requirements;
					condor_requirements = string_format("(%s && (%s))", tmp, optarg);
					free(tmp);
				} else {
					condor_requirements = string_format("(%s)", optarg);
				}
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
		fprintf(stderr,"work_queue_factory: You must specify a batch type with the -T option.\n");
		fprintf(stderr, "valid options:\n");
		fprintf(stderr, "%s\n", batch_queue_type_string());
		return 1;
	}

	if(config_file) {
		char abs_path_name[PATH_MAX];

		if(!realpath(config_file, abs_path_name)) {
			fprintf(stderr, "work_queue_factory: could not resolve configuration file path: '%s'.\n", config_file);
			exit(EXIT_FAILURE);
		}

		free(config_file);

		/* From now on, read config_file from absolute path */
		config_file = xxstrdup(abs_path_name);

		if(!read_config_file(config_file)) {
			fprintf(stderr,"work_queue_factory: There were errors in the configuration file: %s\n", config_file);
			return 1;
		}
	}

	if(!project_regex) {
		fprintf(stderr,"work_queue_factory: You must give a project name with the -M option, or the master-name option with a configuration file.\n");
		return 1;
	}

	if(workers_min>workers_max) {
		fprintf(stderr,"work_queue_factory: min workers (%d) is greater than max workers (%d)\n",workers_min, workers_max);
		return 1;
	}

	if(!scratch_dir) {
		scratch_dir = string_format("/tmp/wq-pool-%d",getuid());
	}

	if(!create_dir(scratch_dir,0777)) {
		fprintf(stderr,"work_queue_factory: couldn't create %s: %s",scratch_dir,strerror(errno));
		return 1;
	}

	char cmd[1024];
	sprintf(cmd,"cp \"$(which work_queue_worker)\" '%s'",scratch_dir);
	if (system(cmd)) {
		fprintf(stderr, "work_queue_factory: please add work_queue_worker to your PATH.\n");
		exit(EXIT_FAILURE);
	}

	if(password_file) {
		sprintf(cmd,"cp %s %s/pwfile",password_file,scratch_dir);
		system(cmd);
	}

	if(chdir(scratch_dir)!=0) {
		fprintf(stderr,"work_queue_factory: couldn't chdir to %s: %s",scratch_dir,strerror(errno));
		return 1;
	}

	signal(SIGINT, handle_abort);
	signal(SIGQUIT, handle_abort);
	signal(SIGTERM, handle_abort);
	signal(SIGHUP, ignore_signal);

	queue = batch_queue_create(batch_queue_type);
	if(!queue) {
		fprintf(stderr,"work_queue_factory: couldn't establish queue type %s",batch_queue_type_to_string(batch_queue_type));
		return 1;
	}

	batch_queue_set_option(queue, "batch-options", batch_submit_options);
	batch_queue_set_option(queue, "autosize", autosize ? "yes" : NULL);
	set_worker_resources_options( queue );

	if (amazon_credentials != NULL) {
		batch_queue_set_option(queue, "amazon-credentials", amazon_credentials);
	}
	if (amazon_ami != NULL) {
		batch_queue_set_option(queue, "amazon-ami", amazon_ami);
	}

	if(condor_requirements != NULL && batch_queue_type != BATCH_QUEUE_TYPE_CONDOR) {
		debug(D_NOTICE, "condor_requirements will be ignored as workers will not be running in condor.");
	} else {
		batch_queue_set_option(queue, "condor-requirements", condor_requirements);
	}

	mainloop( queue, project_regex, foremen_regex );

	batch_queue_delete(queue);

	return 0;
}

/* vim: set noexpandtab tabstop=4: */
