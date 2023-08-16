/*
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "vine_catalog.h"
#include "cctools.h"
#include "batch_job.h"
#include "hash_table.h"
#include "copy_stream.h"
#include "debug.h"
#include "domain_name_cache.h"
#include "envtools.h"
#include "stringtools.h"
#include "xxmalloc.h"
#include "itable.h"
#include "create_dir.h"
#include "unlink_recursive.h"
#include "macros.h"
#include "catalog_query.h"
#include "link.h"
#include "link_auth.h"
#include "list.h"
#include "get_line.h"
#include "getopt.h"
#include "path.h"
#include "buffer.h"
#include "rmsummary.h"

#include "jx.h"
#include "jx_parse.h"
#include "jx_eval.h"
#include "jx_print.h"
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

typedef enum {
	FORMAT_TABLE,
	FORMAT_LONG
} format_t;

static struct jx_table queue_headers[] = {
{"project",       "PROJECT", JX_TABLE_MODE_PLAIN, JX_TABLE_ALIGN_LEFT, -18},
{"name",          "HOST",    JX_TABLE_MODE_PLAIN, JX_TABLE_ALIGN_LEFT, -21},
{"port",          "PORT",    JX_TABLE_MODE_PLAIN, JX_TABLE_ALIGN_RIGHT, 5},
{"tasks_waiting", "WAITING", JX_TABLE_MODE_PLAIN, JX_TABLE_ALIGN_RIGHT, 7},
{"tasks_running", "RUNNING", JX_TABLE_MODE_PLAIN, JX_TABLE_ALIGN_RIGHT, 7},
{"tasks_complete","COMPLETE",JX_TABLE_MODE_PLAIN, JX_TABLE_ALIGN_RIGHT, 8},
{"workers",       "WORKERS", JX_TABLE_MODE_PLAIN, JX_TABLE_ALIGN_RIGHT, 7},
{NULL,NULL,0,0,0}
};

static int vine_status_timeout = 30;

static const char *catalog_host = 0;

static int factory_period = 30; // in seconds

static int workers_min = 5;
static int workers_max = 100;
static int workers_per_cycle = 5; // same as workers_min

static int tasks_per_worker = -1;
static int autosize = 0;
static int worker_timeout = 300;
static int consider_capacity = 0;
static int debug_workers = 0;

static char *project_regex = 0;
static char *submission_regex = 0;
static char *foremen_regex = 0;

char *manager_host = 0;
int manager_port = 0;
int using_catalog = 0;

static char *extra_worker_args=0;
static char *resource_args=0;
static int abort_flag = 0;
static pid_t initial_ppid = 0;
static const char *scratch_dir = 0;
static char *config_file = 0;
static char *amazon_config = NULL;
static char *condor_requirements = NULL;
static char *batch_submit_options = NULL;
static const char *password_file = 0;
char *password;

static char *wrapper_command = 0;

struct list *wrapper_inputs = 0;

static char *worker_command = 0;

/* Unique number assigned to each worker instance for troubleshooting. */
static int worker_instance = 0;

/* -1 means 'not specified' */
static struct rmsummary *resources = NULL;

static int64_t factory_timeout = 0;

static char *factory_name = NULL;

struct batch_queue *queue = 0;

// Whether workers should use ssl. If using the catalog server and the manager
// announces it is using SSL, then SSL is used regardless of manual_ssl_option.
int manual_ssl_option = 0;

//Environment variables to pass along in batch_job_submit
struct jx *batch_env = NULL;

/*
In a signal handler, only a limited number of functions are safe to
invoke, so we construct a string and emit it with a low-level write.
*/

static void handle_abort( int sig )
{
	const char *msg = "received abort signal, shutting down workers...\n";
	write(1,msg,strlen(msg));
	abort_flag = 1;
}

static void ignore_signal( int sig )
{
}

int manager_workers_capacity(struct jx *j) {
	int capacity_tasks   = jx_lookup_integer(j, "capacity_tasks");
	int capacity_cores   = jx_lookup_integer(j, "capacity_cores");
	int capacity_memory  = jx_lookup_integer(j, "capacity_memory");
	int capacity_disk    = jx_lookup_integer(j, "capacity_disk");
	int capacity_gpus    = jx_lookup_integer(j, "capacity_gpus");
	int capacity_weighted = jx_lookup_integer(j, "capacity_weighted");

	const int cores = resources->cores;
	const int memory = resources->memory;
	const int disk = resources->disk;
	const int gpus = resources->gpus;

	debug(D_VINE, "capacity_tasks: %d", capacity_tasks);
	debug(D_VINE, "capacity_cores: %d", capacity_cores);
	debug(D_VINE, "capacity_memory: %d", capacity_memory);
	debug(D_VINE, "capacity_disk: %d", capacity_disk);
	debug(D_VINE, "capacity_gpus: %d", capacity_gpus);

	// first, assume one task per worker
	int capacity = capacity_tasks;
	//use the capacity model if desired
	if(consider_capacity) {
		capacity = capacity_weighted;
	}

	// then, enforce tasks per worker
	if(tasks_per_worker > 0) {
		capacity = DIV_INT_ROUND_UP(capacity, tasks_per_worker);
	}

	// then, enforce capacity per resource
	if(cores > 0 && capacity_cores > 0) {
		capacity = MIN(capacity, DIV_INT_ROUND_UP(capacity_cores, cores));
	}

	if(memory > 0 && capacity_memory > 0) {
		capacity = MIN(capacity, DIV_INT_ROUND_UP(capacity_memory, memory));
	}

	if(disk > 0 && capacity_disk > 0) {
		capacity = MIN(capacity, DIV_INT_ROUND_UP(capacity_disk, disk));
	}

	if(gpus > 0 && capacity_gpus > 0) {
		capacity = MIN(capacity, DIV_INT_ROUND_UP(capacity_gpus, gpus));
	}

	return capacity;
}

int manager_workers_needed_by_resource(struct jx *j) {
	int tasks_total_cores  = jx_lookup_integer(j, "tasks_total_cores");
	int tasks_total_memory = jx_lookup_integer(j, "tasks_total_memory");
	int tasks_total_disk   = jx_lookup_integer(j, "tasks_total_disk");
	int tasks_total_gpus   = jx_lookup_integer(j, "tasks_total_gpus");

	const int cores = resources->cores;
	const int memory = resources->memory;
	const int disk = resources->disk;
	const int gpus = resources->gpus;

	int needed = 0;

	if(cores  > 0  && tasks_total_cores > 0) {
		needed = MAX(needed, DIV_INT_ROUND_UP(tasks_total_cores, cores));
	}

	if(memory > 0  && tasks_total_memory > 0) {
		needed = MAX(needed, DIV_INT_ROUND_UP(tasks_total_memory, memory));
	}

	if(disk > 0  && tasks_total_disk > 0) {
		needed = MAX(needed, DIV_INT_ROUND_UP(tasks_total_disk, disk));
	}

	if(gpus > 0 && tasks_total_gpus > 0) {
		needed = MAX(needed, DIV_INT_ROUND_UP(tasks_total_gpus, gpus));
	}

	return needed;
}

struct list* do_direct_query( const char *manager_host, int manager_port )
{
	const char *query_string = "manager";

	struct link *l;

	char manager_addr[LINK_ADDRESS_MAX];

	time_t stoptime = time(0) + vine_status_timeout;

	if(!domain_name_cache_lookup(manager_host,manager_addr)) {
		fprintf(stderr,"couldn't find address of %s\n",manager_host);
		return 0;
	}

	l = link_connect(manager_addr,manager_port,stoptime);
	if(!l) {
		fprintf(stderr,"couldn't connect to %s port %d: %s\n",manager_host,manager_port,strerror(errno));
		return 0;
	}

	if(manual_ssl_option) {
		if(link_ssl_wrap_connect(l) < 1) {
			fprintf(stderr,"vine_factory: could not setup ssl connection.\n");
			link_close(l);
			return 0;
		}
	}

	if(password) {
		debug(D_VINE,"authenticating to manager");
		if(!link_auth_password(l,password,stoptime)) {
			fprintf(stderr,"vine_factory: wrong password for manager.\n");
			link_close(l);
			return 0;
		}
	}

	link_printf(l,stoptime,"%s_status\n",query_string);

	struct jx *jarray = jx_parse_link(l,stoptime);
	link_close(l);

	if(!jarray || jarray->type!=JX_ARRAY || !jarray->u.items ) {
		fprintf(stderr,"couldn't read %s status from %s port %d\n",query_string,manager_host,manager_port);
		return 0;
	}

	struct jx *j = jarray->u.items->value;
	j->type = JX_OBJECT;
	
	struct list* manager_list = list_create();
	list_push_head(manager_list, j);
	return manager_list;
}

static int count_workers_connected( struct list *managers_list )
{
	int connected_workers=0;
	struct jx *j;

	if(!managers_list) {
		return connected_workers;
	}

	list_first_item(managers_list);
	while((j=list_next_item(managers_list))) {
		const int workers = jx_lookup_integer(j,"workers");
		connected_workers += workers;
	}

	return connected_workers;
}

/*
Count up the workers needed in a given list of managers, IGNORING how many
workers are actually connected.
*/

static int count_workers_needed( struct list *managers_list, int only_not_running )
{
	int needed_workers=0;
	struct jx *j;

	if(!managers_list) {
		return needed_workers;
	}

	list_first_item(managers_list);
	while((j=list_next_item(managers_list))) {

		const char *project =jx_lookup_string(j,"project");
		const char *host =   jx_lookup_string(j,"name");
		const int  port =    jx_lookup_integer(j,"port");
		const char *owner =  jx_lookup_string(j,"owner");
		const int tr =       jx_lookup_integer(j,"tasks_on_workers");
		const int tw =       jx_lookup_integer(j,"tasks_waiting");
		const int tl =       jx_lookup_integer(j,"tasks_left");

		int capacity = manager_workers_capacity(j);

		// first assume one task per worker
		int need;
		if(only_not_running) {
			need = tw + tl;
		} else {
			need = tw + tl + tr;
		}

		// enforce many tasks per worker
		if(tasks_per_worker > 0) {
			need = DIV_INT_ROUND_UP(need, tasks_per_worker);
			capacity = DIV_INT_ROUND_UP(capacity, tasks_per_worker);
		}

		// consider if tasks declared resources...
		need = MAX(need, manager_workers_needed_by_resource(j));

		if(consider_capacity && capacity > 0) {
			need = MIN(need, capacity);
		}

		debug(D_VINE,"%s %s:%d %s tasks: %d capacity: %d workers needed: %d tasks running: %d",project,host,port,owner,tw+tl+tr,capacity,need,tr);
		needed_workers += need;
	}

	return needed_workers;
}

static void set_worker_resources_options( struct batch_queue *queue )
{
	buffer_t b;
	buffer_init(&b);

	if(batch_queue_get_type(queue) == BATCH_QUEUE_TYPE_CONDOR) {
		// HTCondor has the ability to fill in, at placement time
		// Doing it this way enables the --autosize feature, making the worker fit the selected slot
		buffer_printf(&b, " --cores=$$([TARGET.Cpus]) --memory=$$([TARGET.Memory]) --disk=$$([TARGET.Disk/1024])");
		if(resources->gpus>0) {
			buffer_printf(&b," --gpus=$$([TARGET.GPUs])");
		}
	} else {
		if(resources->cores > -1) {
			buffer_printf(&b, " --cores=%s", rmsummary_resource_to_str("cores", resources->cores, 0));
		}

		if(resources->memory > -1) {
			buffer_printf(&b, " --memory=%s", rmsummary_resource_to_str("memory", resources->memory, 0));
		}

		if(resources->disk > -1) {
			buffer_printf(&b, " --disk=%s", rmsummary_resource_to_str("disk", resources->disk, 0));
		}

		if(resources->gpus > -1) {
			buffer_printf(&b, " --gpus=%s", rmsummary_resource_to_str("gpus", resources->gpus, 0));
		}
	}

	free(resource_args);
	resource_args = xxstrdup(buffer_tostring(&b));

	buffer_free(&b);
}

static int submit_worker( struct batch_queue *queue )
{
	char *cmd;

	char *worker_log_file = 0;
	char *debug_worker_options = 0;
	
	if(debug_workers) {
		worker_instance++;
		worker_log_file = string_format("worker.%d.log",worker_instance);
		debug_worker_options = string_format("-d all -o %s",worker_log_file);
	}
	
	char *worker = string_format("./%s", worker_command);
	if(using_catalog) {
		cmd = string_format(
		"%s --parent-death -M %s -t %d -C '%s' %s %s %s %s %s %s",
		worker,
		submission_regex,
		worker_timeout,
		catalog_host,
		debug_workers ? debug_worker_options : "",
		factory_name ? string_format("--from-factory \"%s\"", factory_name) : "",
		password_file ? "-P pwfile" : "",
		resource_args ? resource_args : "",
		manual_ssl_option ? "--ssl" : "",
		extra_worker_args ? extra_worker_args : ""
		);
	} else {
		cmd = string_format(
		"%s --parent-death %s %d -t %d -C '%s' %s %s %s %s %s",
		worker,
		manager_host,
		manager_port,
		worker_timeout,
		catalog_host,
		debug_workers ? debug_worker_options : "",
		password_file ? "-P pwfile" : "",
		resource_args ? resource_args : "",
		manual_ssl_option ? "--ssl" : "",
		extra_worker_args ? extra_worker_args : ""
		);
	}

	if(wrapper_command) {
		// Note that we don't use string_wrap_command here,
		// because the clever quoting interferes with the $$([Target.Memory]) substitution above.
		char *newcmd = string_format("%s %s",wrapper_command,cmd);
		free(cmd);
		cmd = newcmd;
	}

	char *files = NULL;
	files = xxstrdup(worker_command);

	if(password_file) {
		char *newfiles = string_format("%s,pwfile",files);
		free(files);
		files = newfiles;
	}

	const char *item = NULL;
	list_first_item(wrapper_inputs);
	while((item = list_next_item(wrapper_inputs))) {
		char *newfiles = string_format("%s,%s",files,path_basename(item));
		free(files);
		files = newfiles;
	}

	debug(D_VINE,"submitting worker: %s",cmd);

	int status = batch_job_submit(queue,cmd,files,worker_log_file,batch_env,resources);

	free(worker_log_file);
	free(debug_worker_options);
	free(cmd);
	free(files);
	free(worker);

	return status;
}

static void update_blocked_hosts( struct batch_queue *queue, struct list *managers_list ) {

	if(!managers_list || list_size(managers_list) < 1)
		return;

	buffer_t b;
	struct jx *j;

	buffer_init(&b);

	const char *sep = "";
	list_first_item(managers_list);
	while((j=list_next_item(managers_list))) {
		struct jx *blocked = jx_lookup(j,"workers_blocked");

		if(!blocked) {
			continue;
		}

		if(jx_istype(blocked, JX_STRING)) {
			buffer_printf(&b, "%s%s", sep, blocked->u.string_value);
			sep = " ";
		}

		if(jx_istype(blocked, JX_ARRAY)) {
			struct jx *item;
			for (void *i = NULL; (item = jx_iterate_array(blocked, &i));) {
				if(jx_istype(item, JX_STRING)) {
					buffer_printf(&b, "%s%s", sep, item->u.string_value);
					sep = " ";
				}
			}
		}
	}

	if(buffer_pos(&b) > 0) {
		batch_queue_set_option(queue, "workers-blocked", buffer_tostring(&b));
	} else {
		batch_queue_set_option(queue, "workers-blocked", NULL);
	}

	buffer_free(&b);
}

static int submit_workers( struct batch_queue *queue, struct itable *job_table, int count )
{
	int i;
	for(i=0;i<count;i++) {
		int jobid = submit_worker(queue);
		if(jobid>0) {
			debug(D_VINE,"worker job %d submitted",jobid);
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

	debug(D_VINE,"removing all remaining worker jobs...");
	int count = itable_size(job_table);
	itable_firstkey(job_table);
	while(itable_nextkey(job_table,&jobid,&value)) {
		debug(D_VINE,"removing job %"PRId64,jobid);
		batch_job_remove(queue,jobid);
	}
	debug(D_VINE,"%d workers removed.",count);

}

void print_stats(struct jx *j) {
	struct timeval tv;
	struct tm *tm;
	gettimeofday(&tv, 0);
	tm = localtime(&tv.tv_sec);

	fprintf(stdout, "%04d/%02d/%02d %02d:%02d:%02d: "
			"|submitted: %" PRId64 " |needed: %" PRId64 " |waiting connection: %" PRId64 " |requested: %" PRId64 " \n",
			tm->tm_year + 1900, tm->tm_mon + 1, tm->tm_mday, tm->tm_hour, tm->tm_min, tm->tm_sec,
			jx_lookup_integer(j, "workers_submitted"),
			jx_lookup_integer(j, "workers_needed"),
			jx_lookup_integer(j, "workers_to_connect"),
			jx_lookup_integer(j, "workers_requested"));


	int columns = 80;
	char *column_str = getenv("COLUMNS");
	if(column_str) {
		columns = atoi(column_str);
		columns = columns < 1 ? 80 : columns;
	}

	jx_table_print_header(queue_headers,stdout,columns);

	struct jx *a = jx_lookup(j, "managers");
	if(a) {
		struct jx *m;
		for (void *i = NULL; (m = jx_iterate_array(a, &i));) {
			jx_table_print(queue_headers, m, stdout, columns);
		}
	}

	a = jx_lookup(j, "foremen");
	if(a) {
		struct jx *m;
		for (void *i = NULL; (m = jx_iterate_array(a, &i));) {
			jx_table_print(queue_headers, m, stdout, columns);
		}
	}

	fprintf(stdout, "\n");
	fflush(stdout);
}

struct jx *manager_to_jx(struct jx *m) {

	struct jx *j = jx_object(NULL);

	const char *project_name = jx_lookup_string(m, "project");

	if(project_name) {
		jx_insert_string(j, "project", jx_lookup_string(m, "project"));
	} else {
		jx_insert_string(j, "project", manager_host);
	}

	if(using_catalog) {
		jx_insert_string(j, "name", jx_lookup_string(m, "name"));
	} else {
		jx_insert_string(j, "name", manager_host);
	}

	jx_insert_integer(j, "port",           jx_lookup_integer(m, "port"));
	jx_insert_integer(j, "tasks_waiting",  jx_lookup_integer(m, "tasks_waiting"));
	jx_insert_integer(j, "tasks_running",  jx_lookup_integer(m, "tasks_running"));
	jx_insert_integer(j, "tasks_complete", jx_lookup_integer(m, "tasks_complete"));
	jx_insert_integer(j, "workers",        jx_lookup_integer(m, "workers"));

	return j;
}

struct jx *factory_to_jx(struct list *managers, struct list *foremen, int submitted, int needed, int requested, int connected) {

	struct jx *j= jx_object(NULL);
	jx_insert_string(j, "type", "vine_factory");

	if(using_catalog) {
		jx_insert_string(j, "project_regex",    project_regex);
		jx_insert_string(j, "submission_regex", submission_regex);
		jx_insert_integer(j, "max_workers", workers_max);
		if (factory_name) jx_insert_string(j, "factory_name", factory_name);
	}

	int to_connect = submitted - connected;

	needed     = needed     > 0 ? needed    : 0;
	requested  = requested  > 0 ? requested : 0;
	to_connect = to_connect > 0 ? to_connect : 0;

	jx_insert_integer(j, "workers_submitted", submitted);
	jx_insert_integer(j, "workers_needed",     needed);
	jx_insert_integer(j, "workers_requested",  requested);
	jx_insert_integer(j, "workers_to_connect", to_connect);

	struct jx *ms = jx_array(NULL);
	if(managers && list_size(managers) > 0)
	{
		struct jx *m;
		list_first_item(managers);
		while((m = list_next_item(managers)))
		{
			jx_array_append(ms, manager_to_jx(m));
		}
	}
	jx_insert(j, jx_string("managers"), ms);


	struct jx *fs = jx_array(NULL);
	if(foremen && list_size(foremen) > 0)
	{
		struct jx *f;
		list_first_item(foremen);
		while((f = list_next_item(foremen)))
		{
			jx_array_append(fs, manager_to_jx(f));

		}
	}
	jx_insert(j, jx_string("foremen"), fs);

	return j;
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
		struct jx *jv = jx_eval(jx_lookup(J, #option), J); \
		if(jv) {\
			if (jv->type == JX_DOUBLE) {\
				int v = ceil(jv->u.double_value);\
				jx_delete(jv);\
				jv = jx_integer(v);\
			}\
			if (jv->type == type_json) {\
				new_var = jv->u.field;\
			} else if (jv->type == JX_ERROR) {\
				debug(D_NOTICE, "%s", jv->u.err->u.string_value);\
				error_found = 1;\
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
	assign_new_value(new_workers_per_cycle, workers_per_cycle, workers-per-cycle, int, JX_INTEGER, integer_value)
	assign_new_value(new_consider_capacity, consider_capacity, capacity, int, JX_INTEGER, integer_value)
	assign_new_value(new_worker_timeout, worker_timeout, timeout, int, JX_INTEGER, integer_value)

	assign_new_value(new_num_cores_option, resources->cores, cores,    int, JX_INTEGER, integer_value)
	assign_new_value(new_num_memory_option, resources->memory, memory, int, JX_INTEGER, integer_value)
	assign_new_value(new_num_disk_option,  resources->disk, disk,      int, JX_INTEGER, integer_value)
	assign_new_value(new_num_gpus_option, resources->gpus, gpus,       int, JX_INTEGER, integer_value)

	assign_new_value(new_autosize_option, autosize, autosize, int, JX_INTEGER, integer_value)

	assign_new_value(new_factory_timeout_option, factory_timeout, factory-timeout, int, JX_INTEGER, integer_value)

	assign_new_value(new_tasks_per_worker, tasks_per_worker, tasks-per-worker, int, JX_INTEGER, integer_value)

	assign_new_value(new_factory_name, factory_name, factory-name, const char *, JX_STRING, string_value)

	/* first try with old master option, then with manager */
	assign_new_value(new_project_regex_old_opt, project_regex, master-name, const char *, JX_STRING, string_value)
	assign_new_value(new_project_regex, project_regex, manager-name, const char *, JX_STRING, string_value)

	assign_new_value(new_foremen_regex, foremen_regex, foremen-name, const char *, JX_STRING, string_value)
	assign_new_value(new_extra_worker_args, extra_worker_args, worker-extra-options, const char *, JX_STRING, string_value)

	assign_new_value(new_condor_requirements, condor_requirements, condor-requirements, const char *, JX_STRING, string_value)

	if(!manager_host) {
		if(!new_project_regex) {
			// if manager-name not given, try with the old master-name
			new_project_regex = new_project_regex_old_opt;
		}
		if(!new_project_regex || strlen(new_project_regex) == 0) {
			debug(D_NOTICE, "%s: manager name is missing and no manager host was given.\n", config_file);
			error_found = 1;
		}
	}

	if(new_workers_min < 0) {
		debug(D_NOTICE, "%s: min workers (%d) is less than zero.\n", config_file, new_workers_min);
		error_found = 1;
	}

	if(new_workers_max < 1) {
		debug(D_NOTICE, "%s: max workers (%d) is less than one.\n", config_file, new_workers_max);
		error_found = 1;
	}

	if(new_workers_min > new_workers_max) {
		debug(D_NOTICE, "%s: min workers (%d) is greater than max workers (%d)\n", config_file, new_workers_min, new_workers_max);
		debug(D_NOTICE, "setting min workers and max workers to %d\n", new_workers_max);
		new_workers_min = new_workers_max;
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
	workers_per_cycle = new_workers_per_cycle;
	worker_timeout    = new_worker_timeout;
	tasks_per_worker = new_tasks_per_worker;
	autosize         = new_autosize_option;
	factory_timeout  = new_factory_timeout_option;
	consider_capacity = new_consider_capacity;

	resources->cores  = new_num_cores_option;
	resources->memory = new_num_memory_option;
	resources->disk   = new_num_disk_option;
	resources->gpus   = new_num_gpus_option;

	if(tasks_per_worker < 1) {
		tasks_per_worker = resources->cores > 0 ? resources->cores : 1;
	}

	if(new_project_regex != project_regex) {
		free(project_regex);
		project_regex = xxstrdup(new_project_regex);
	}

	if(new_foremen_regex != foremen_regex) {
		free(foremen_regex);
		foremen_regex = xxstrdup(new_foremen_regex);
	}

	if(extra_worker_args != new_extra_worker_args) {
		free(extra_worker_args);
		extra_worker_args = xxstrdup(new_extra_worker_args);
	}

	if(new_condor_requirements != condor_requirements) {
		free(condor_requirements);
		condor_requirements = xxstrdup(new_condor_requirements);
	}

	if(new_factory_name && new_factory_name != factory_name) {
		free(factory_name);
		factory_name = xxstrdup(new_factory_name);
	}

	last_time_modified = new_time_modified;
	fprintf(stdout, "Configuration file '%s' has been loaded.", config_file);

	fprintf(stdout, "manager-name: %s\n", project_regex);
	if(foremen_regex) {
		fprintf(stdout, "foremen-name: %s\n", foremen_regex);
	}
	fprintf(stdout, "max-workers: %d\n", workers_max);
	fprintf(stdout, "min-workers: %d\n", workers_min);
	fprintf(stdout, "workers-per-cycle: %d\n", workers_per_cycle);

	fprintf(stdout, "tasks-per-worker: %d\n", tasks_per_worker > 0 ? tasks_per_worker : (resources->cores > 0 ? (int) resources->cores : 1));
	fprintf(stdout, "timeout: %d s\n", worker_timeout);
	fprintf(stdout, "cores: %s\n", rmsummary_resource_to_str("cores", resources->cores > 0 ? resources->cores : 1, 0));

	if(factory_name) {
		fprintf(stdout, "factory_name: %s\n", factory_name);
	}

	if(condor_requirements) {
		fprintf(stdout, "condor-requirements: %s\n", condor_requirements);
	}

	if(factory_timeout > 0) {
		fprintf(stdout, "factory-timeout: %" PRId64 " MB\n", factory_timeout);
	}

	if(resources->memory > -1) {
		fprintf(stdout, "memory: %s\n", rmsummary_resource_to_str("memory", resources->memory, 1));
	}

	if(resources->disk > -1) {
		fprintf(stdout, "disk: %s\n", rmsummary_resource_to_str("disk", resources->disk, 1));
	}

	if(resources->gpus > -1) {
		fprintf(stdout, "gpus: %s\n", rmsummary_resource_to_str("gpus", resources->gpus, 0));
	}

	if(extra_worker_args) {
		fprintf(stdout, "worker-extra-options: %s", extra_worker_args);
	}

	fprintf(stdout, "\n");

end:
	jx_delete(J);
	return !error_found;
}

/*
Main loop of work queue pool.  Determine the number of workers needed by our
current list of managers, compare it to the number actually submitted, then
submit more until the desired state is reached.
*/

static void mainloop( struct batch_queue *queue )
{
	int workers_submitted = 0;
	struct itable *job_table = itable_create(0);

	struct list *managers_list = NULL;
	struct list *foremen_list = NULL;

	int64_t factory_timeout_start = time(0);

	while(!abort_flag) {
		if (initial_ppid != 0 && getppid() != initial_ppid) {
			printf("parent process exited, shutting down\n");
			abort_flag = 1;
			break;
		}

		if(config_file && !read_config_file(config_file)) {
			debug(D_NOTICE, "Error re-reading '%s'. Using previous values.", config_file);
		} else {
			set_worker_resources_options( queue );
			batch_queue_set_option(queue, "autosize", autosize ? "yes" : NULL);
		}

		submission_regex = foremen_regex ? foremen_regex : project_regex;

		if(using_catalog) {
			managers_list = vine_catalog_query(catalog_host,-1,project_regex);
		}
		else {
			managers_list = do_direct_query(manager_host,manager_port);
		}

		if(managers_list && list_size(managers_list) > 0)
		{
			factory_timeout_start = time(0);
		} else {
			// check to see if factory timeout is triggered, factory timeout will be 0 if flag isn't set
			if(factory_timeout > 0)
			{
				if(time(0) - factory_timeout_start > factory_timeout) {
					fprintf(stderr, "There have been no managers for longer then the factory timeout, exiting\n");
					abort_flag=1;
					break;
				}
			}
		}
	
		debug(D_VINE,"evaluating manager list...");
		int workers_connected = count_workers_connected(managers_list);
		int workers_needed = 0;

		if(foremen_regex)
		{
			/* If there are foremen, we only look at tasks not running in the
			 * managers' list. The rest of the tasks will be counted as waiting
			 * or running on the foremen. */
			workers_needed = count_workers_needed(managers_list, /* do not count running tasks */ 1);
			debug(D_VINE,"evaluating foremen list...");
			foremen_list    = vine_catalog_query(catalog_host,-1,foremen_regex);

			/* add workers on foremen. Also, subtract foremen from workers
			 * connected, as they were not deployed by the pool. */
			workers_needed    += count_workers_needed(foremen_list, 0);
			workers_connected += MAX(count_workers_connected(foremen_list) - list_size(foremen_list), 0);

			debug(D_VINE,"%d total workers needed across %d foremen",workers_needed,list_size(foremen_list));
		} else {
			/* If there are no foremen, workers needed are computed directly
			 * from the tasks running, waiting, and left from the managers'
			 * list. */
			workers_needed = count_workers_needed(managers_list, 0);
			debug(D_VINE,"%d total workers needed across %d managers",
					workers_needed,
					managers_list ? list_size(managers_list) : 0);
		}

		debug(D_VINE,"raw workers needed: %d", workers_needed);

		if(workers_needed > workers_max) {
			debug(D_VINE,"applying maximum of %d workers",workers_max);
			workers_needed = workers_max;
		}

		if(workers_needed < workers_min) {
			debug(D_VINE,"applying minimum of %d workers",workers_min);
			workers_needed = workers_min;
		}

		// if negative, this means we need less workers than the currently
		// running from this factory.
		int new_workers_needed = workers_needed - workers_submitted;

		// if negative, this means workers external from this factory have
		// connected.
		int workers_waiting_to_connect = workers_submitted - workers_connected;

		if(workers_waiting_to_connect < 0) {
			debug(D_VINE,"at least %d workers have already connected from other sources", -workers_waiting_to_connect);
			new_workers_needed -= abs(workers_waiting_to_connect);

			// this factory has no workers_waiting_to_connect
			workers_waiting_to_connect = 0;
		}

		if(workers_waiting_to_connect > 0) {
			debug(D_VINE,"waiting for %d previously submitted workers to connect", workers_waiting_to_connect);
		}

		// Apply workers_per_cycle. Never have more than workers_per_cycle waiting to connect.
		if(workers_per_cycle > 0 && (new_workers_needed + workers_waiting_to_connect) > workers_per_cycle) {
			debug(D_VINE,"applying maximum workers per cycle of %d",workers_per_cycle);
			new_workers_needed = MAX(0, workers_per_cycle - workers_waiting_to_connect);
		}

		debug(D_VINE,"workers needed: %d",    workers_needed);
		debug(D_VINE,"workers submitted: %d", workers_submitted);
		debug(D_VINE,"workers requested: %d", MAX(0, new_workers_needed));

		struct jx *j = factory_to_jx(managers_list, foremen_list, workers_submitted, workers_needed, new_workers_needed, workers_connected);

		char *update_str = jx_print_string(j);
		debug(D_VINE, "Sending status to the catalog server(s) at %s ...", catalog_host);
		catalog_query_send_update(catalog_host,update_str,0);
		print_stats(j);
		free(update_str);
		jx_delete(j);

		update_blocked_hosts(queue, managers_list);

		if(new_workers_needed > 0) {
			debug(D_VINE,"submitting %d new workers to reach target",new_workers_needed);
			workers_submitted += submit_workers(queue,job_table,new_workers_needed);
		} else if(new_workers_needed < 0) {
			debug(D_VINE,"too many workers, will wait for some to exit");
		} else {
			debug(D_VINE,"target number of workers is reached.");
		}

		debug(D_VINE,"checking for exited workers...");
		time_t stoptime = time(0)+5;

		while(1) {
			struct batch_job_info info;
			batch_job_id_t jobid;
			jobid = batch_job_wait_timeout(queue,&info,stoptime);
			if(jobid>0) {
				if(itable_lookup(job_table,jobid)) {
					itable_remove(job_table,jobid);
					debug(D_VINE,"worker job %"PRId64" exited",jobid);
					workers_submitted--;
				} else {
					// it may have been a job from a previous run.
				}
			} else {
				break;
			}
		}

		delete_projects_list(managers_list);
		delete_projects_list(foremen_list);

		int sleep_seconds = 0;
		while(sleep_seconds < factory_period) {
			if(abort_flag) {
				break;
			}
			sleep_seconds += 1;
			sleep(1);
		}
	}

	printf("removing %d workers...\n",itable_size(job_table));
	remove_all_workers(queue,job_table);
	printf("all workers removed.\n");
	itable_delete(job_table);
}

/* Add a wrapper command around the worker executable. */
/* Note that multiple wrappers can be nested. */

void add_wrapper_command( const char *cmd )
{
	if(!wrapper_command) {
		wrapper_command = strdup(cmd);
	} else {
		char *tmp = string_format("%s %s",cmd,wrapper_command);
		free(wrapper_command);
		wrapper_command = tmp;
	}
}

/* Add an additional input file to be consumed by the wrapper. */

void add_wrapper_input( const char *filename )
{
	list_push_tail(wrapper_inputs, xxstrdup(filename));
}

static void show_help(const char *cmd)
{
	printf("Use: vine_factory [options] <managerhost> <port>\n");
	printf("Or:  vine_factory [options] -M projectname\n");
	printf("\n");
	/*------------------------------------------------------------*/
	printf("General options:\n");
	printf(" %-30s Batch system type (required). One of: %s\n", "-T,--batch-type=<type>",batch_queue_type_string());
	printf(" %-30s Use configuration file <file>.\n","-C,--config-file=<file>");
	printf(" %-30s Project name of managers to server, can be regex\n","-M,-N,--manager-name=<project>");\
	printf(" %-30s Foremen to serve, can be a regular expression.\n", "-F,--foremen-name=<project>");
	printf(" %-30s Catalog server to query for managers.\n", "--catalog=<host:port>");
	printf(" %-30s Password file for workers to authenticate.\n","-P,--password");
	printf(" %-30s Use this scratch dir for factory.\n","-S,--scratch-dir");
	printf(" %-30s (default: /tmp/vine-factory-$uid).\n","");
	printf(" %-30s Exit if parent process dies.\n", "--parent-death");
	printf(" %-30s Enable debug log for each remote worker in scratch dir.\n","--debug-workers");
	printf(" %-30s Enable debugging for this subsystem.\n", "-d,--debug=<subsystem>");
	printf(" %-30s Send debugging to this file.\n", "-o,--debug-file=<file>");
	printf(" %-30s Specify the size of the debug file.\n", "-O,--debug-file-size=<mb>");
	printf(" %-30s Workers should use SSL to connect to managers. (Not needed if project names.)", "--ssl");
	printf(" %-30s Show the version string.\n", "-v,--version");
	printf(" %-30s Show this screen.\n", "-h,--help");

	/*------------------------------------------------------------*/
	printf("\nConcurrency control options:\n");
	printf(" %-30s Minimum workers running (default=%d).\n", "-w,--min-workers", workers_min);
	printf(" %-30s Maximum workers running (default=%d).\n", "-W,--max-workers", workers_max);
	printf(" %-30s Max number of new workers per %ds (default=%d)\n", "--workers-per-cycle", factory_period, workers_per_cycle);
	printf(" %-30s Workers abort after idle time (default=%d).\n", "-t,--timeout=<time>",worker_timeout);
	printf(" %-30s Exit after no manager seen in <n> seconds.\n", "--factory-timeout");
	printf(" %-30s Average tasks per worker (default=one per core).\n", "--tasks-per-worker");
	printf(" %-30s Use worker capacity reported by managers.\n","-c,--capacity");

	printf("\nResource management options:\n");
	printf(" %-30s Set the number of cores requested per worker.\n", "--cores=<n>");
	printf(" %-30s Set the number of GPUs requested per worker.\n", "--gpus=<n>");
	printf(" %-30s Set the amount of memory (in MB) per worker.\n", "--memory=<mb>           ");
	printf(" %-30s Set the amount of disk (in MB) per worker.\n", "--disk=<mb>");
	printf(" %-30s Autosize worker to slot (Condor, Mesos, K8S).\n", "--autosize");

	printf("\nWorker environment options:\n");
	printf(" %-30s Environment variable to add to worker.\n", "--env=<variable=value>");
	printf(" %-30s Extra options to give to worker.\n", "-E,--extra-options=<options>");
	printf(" %-30s Alternate binary instead of vine_worker.\n", "--worker-binary=<file>");
	printf(" %-30s Wrap factory with this command prefix.\n","--wrapper");
	printf(" %-30s Add this input file needed by the wrapper.\n","--wrapper-input");
	printf(" %-30s Run each worker inside this python environment.\n","--python-env=<file.tar.gz>");

	printf("\nOptions specific to batch systems:\n");
	printf(" %-30s Generic batch system options.\n", "-B,--batch-options=<options>");
	printf(" %-30s Specify Amazon config file.\n", "--amazon-config");
	printf(" %-30s Set requirements for the workers as Condor jobs.\n", "--condor-requirements");

}

enum{   LONG_OPT_CORES = 255,
		LONG_OPT_MEMORY, 
		LONG_OPT_DISK, 
		LONG_OPT_GPUS, 
		LONG_OPT_TASKS_PER_WORKER, 
		LONG_OPT_CONF_FILE, 
		LONG_OPT_AMAZON_CONFIG, 
		LONG_OPT_FACTORY_TIMEOUT, 
		LONG_OPT_AUTOSIZE, 
		LONG_OPT_CONDOR_REQUIREMENTS, 
		LONG_OPT_WORKERS_PER_CYCLE, 
		LONG_OPT_WRAPPER, 
		LONG_OPT_WRAPPER_INPUT,
		LONG_OPT_WORKER_BINARY,
		LONG_OPT_MESOS_MANAGER, 
		LONG_OPT_MESOS_PATH,
		LONG_OPT_MESOS_PRELOAD,
		LONG_OPT_K8S_IMAGE,
		LONG_OPT_K8S_WORKER_IMAGE,
		LONG_OPT_CATALOG,
		LONG_OPT_ENVIRONMENT_VARIABLE,
		LONG_OPT_RUN_AS_MANAGER,
		LONG_OPT_RUN_OS,
		LONG_OPT_PARENT_DEATH,
		LONG_OPT_PYTHON_PACKAGE,
		LONG_OPT_USE_SSL,
		LONG_OPT_FACTORY_NAME,
		LONG_OPT_DEBUG_WORKERS,
};

static const struct option long_options[] = {
	{"amazon-config", required_argument, 0, LONG_OPT_AMAZON_CONFIG},
	{"autosize", no_argument, 0, LONG_OPT_AUTOSIZE},
	{"batch-options", required_argument, 0, 'B'},
	{"batch-type", required_argument, 0, 'T'},
	{"capacity", no_argument, 0, 'c' },
	{"catalog", required_argument, 0, LONG_OPT_CATALOG},
	{"condor-requirements", required_argument, 0, LONG_OPT_CONDOR_REQUIREMENTS},
	{"config-file", required_argument, 0, 'C'},
	{"cores",  required_argument,  0,  LONG_OPT_CORES},
	{"debug", required_argument, 0, 'd'},
	{"debug-file", required_argument, 0, 'o'},
	{"debug-file-size", required_argument, 0, 'O'},
	{"debug-workers", no_argument, 0, LONG_OPT_DEBUG_WORKERS },
	{"disk",   required_argument,  0,  LONG_OPT_DISK},
	{"env", required_argument, 0, LONG_OPT_ENVIRONMENT_VARIABLE},
	{"extra-options", required_argument, 0, 'E'},
	{"factory-timeout", required_argument, 0, LONG_OPT_FACTORY_TIMEOUT},
	{"foremen-name", required_argument, 0, 'F'},
	{"gpus",   required_argument,  0,  LONG_OPT_GPUS},
	{"help", no_argument, 0, 'h'},
	{"k8s-image", required_argument, 0, LONG_OPT_K8S_IMAGE},
	{"k8s-worker-image", required_argument, 0, LONG_OPT_K8S_WORKER_IMAGE},
	{"manager-name", required_argument, 0, 'M'},
	{"master-name", required_argument, 0, 'M'},
	{"max-workers", required_argument, 0, 'W'},
	{"memory", required_argument,  0,  LONG_OPT_MEMORY},
	{"min-workers", required_argument, 0, 'w'},
	{"parent-death", no_argument, 0, LONG_OPT_PARENT_DEATH},
	{"password", required_argument, 0, 'P'},
	{"python-env", required_argument, 0, LONG_OPT_PYTHON_PACKAGE},
	{"python-package", required_argument, 0, LONG_OPT_PYTHON_PACKAGE}, //same as python-env, kept for compatibility
	{"scratch-dir", required_argument, 0, 'S' },
	{"tasks-per-worker", required_argument, 0, LONG_OPT_TASKS_PER_WORKER},
	{"timeout", required_argument, 0, 't'},
	{"version", no_argument, 0, 'v'},
	{"worker-binary", required_argument, 0, LONG_OPT_WORKER_BINARY},
	{"workers-per-cycle", required_argument, 0, LONG_OPT_WORKERS_PER_CYCLE},
	{"wrapper",required_argument, 0, LONG_OPT_WRAPPER},
	{"wrapper-input",required_argument, 0, LONG_OPT_WRAPPER_INPUT},
	{"ssl",no_argument, 0, LONG_OPT_USE_SSL},
	{"factory-name",required_argument, 0, LONG_OPT_FACTORY_NAME},
	{0,0,0,0}
};


int main(int argc, char *argv[])
{
	wrapper_inputs = list_create();

	//Environment variable handling
	char *ev = NULL;
	char *env = NULL;
	char *val = NULL;
	batch_env = jx_object(NULL);

	batch_queue_type_t batch_queue_type = BATCH_QUEUE_TYPE_UNKNOWN;

	catalog_host = CATALOG_HOST;

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
			case LONG_OPT_WORKERS_PER_CYCLE:
				workers_per_cycle = atoi(optarg);
				break;
			case LONG_OPT_TASKS_PER_WORKER:
				tasks_per_worker = atof(optarg);
				break;
			case 'E':
				extra_worker_args = xxstrdup(optarg);
				break;
			case LONG_OPT_ENVIRONMENT_VARIABLE:
				ev = xxstrdup(optarg);
				env = strtok(ev, "=");
				val = strtok(NULL, "=");
				if(env && val) {
					struct jx *jx_env = jx_string(env);
					struct jx *jx_val = jx_string(val);
					if(!jx_insert(batch_env, jx_env, jx_val)) {
						fprintf(stderr, "could not insert key:value pair into JX object: %s\n", ev);
						return EXIT_FAILURE;
					}
				}
				else {
					fprintf(stderr, "could not evaluate key:value pair: %s\n", ev);
					return EXIT_FAILURE;
				}
				break;
			case LONG_OPT_CORES:
				resources->cores = atoi(optarg);
				break;
			case LONG_OPT_AMAZON_CONFIG:
				amazon_config = xxstrdup(optarg);
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
			case LONG_OPT_PYTHON_PACKAGE:
				{
				// --package X is the equivalent of --wrapper "poncho_package_run X" --wrapper-input X
				char *fullpath = path_which("poncho_package_run");
				if(!fullpath) {
					fprintf(stderr,"vine_factory: could not find poncho_package_run in PATH");
					return 1;
				}
				add_wrapper_input(fullpath);
				add_wrapper_input(optarg);
				add_wrapper_command( string_format("./%s -e %s ",path_basename(fullpath), path_basename(optarg)));
				break;
				}
			case LONG_OPT_WRAPPER:
				add_wrapper_command(optarg);
				break;
			case LONG_OPT_WRAPPER_INPUT:
				add_wrapper_input(optarg);
				break;
			case LONG_OPT_WORKER_BINARY:
				worker_command = xxstrdup(optarg);
				break;
			case 'P':
				password_file = optarg;
				if(copy_file_to_buffer(optarg, &password, NULL) < 0) {
					fprintf(stderr,"vine_factory: couldn't load password from %s: %s\n",optarg,strerror(errno));
					exit(EXIT_FAILURE);
				}
				break;
			case 'S':
				scratch_dir = optarg;
				break;
			case 'c':
				consider_capacity = 1;
				break;
			case 'd':
				if (!debug_flags_set(optarg)) {
					fprintf(stderr, "Unknown debug flag: %s\n", optarg);
					exit(EXIT_FAILURE);
				}
				break;
			case 'o':
				debug_config_file(optarg);
				break;
			case 'O':
				debug_config_file_size(string_metric_parse(optarg));
				break;
			case LONG_OPT_DEBUG_WORKERS:
				debug_workers = 1;
				break;
			case 'v':
				cctools_version_print(stdout, argv[0]);
				exit(EXIT_SUCCESS);
			case 'h':
				show_help(argv[0]);
				exit(EXIT_SUCCESS);
			case LONG_OPT_CATALOG:
				catalog_host = xxstrdup(optarg);
				break;
			case LONG_OPT_PARENT_DEATH:
				initial_ppid = getppid();
				break;
			case LONG_OPT_USE_SSL:
				manual_ssl_option=1;
				break;
			case LONG_OPT_FACTORY_NAME:
				factory_name = xxstrdup(optarg);
				break;
			default:
				show_help(argv[0]);
				return EXIT_FAILURE;
		}
	}

	if(batch_queue_type == BATCH_QUEUE_TYPE_WORK_QUEUE) {
		fprintf(stderr, "vine_factory: batch system 'wq' specified, but you most likely want 'local'.\n");
		exit(EXIT_FAILURE);
	}

	if(config_file) {
		char abs_path_name[PATH_MAX];

		if(!realpath(config_file, abs_path_name)) {
			fprintf(stderr, "vine_factory: could not resolve configuration file path: '%s'.\n", config_file);
			exit(EXIT_FAILURE);
		}

		free(config_file);

		/* From now on, read config_file from absolute path */
		config_file = xxstrdup(abs_path_name);
	}

	if((argc - optind) == 2) {
		manager_host = argv[optind];
		manager_port = atoi(argv[optind+1]);
	}

	if(config_file) {
		if(!read_config_file(config_file)) {
			fprintf(stderr,"vine_factory: There were errors in the configuration file: %s\n", config_file);
			return 1;
		}
	}

	if(!(manager_host || config_file || project_regex)) {
		fprintf(stderr,"vine_factory: You must either give a project name with the -M option or manager-name option with a configuration file, or give the manager's host and port.\n");
		exit(1);
	}

	if(manager_host) {
		using_catalog = 0;
	} else {
		using_catalog = 1;
	}

	cctools_version_debug(D_DEBUG, argv[0]);

	if(batch_queue_type == BATCH_QUEUE_TYPE_UNKNOWN) {
		fprintf(stderr,"vine_factory: You must specify a batch type with the -T option.\n");
		fprintf(stderr, "valid options:\n");
		fprintf(stderr, "%s\n", batch_queue_type_string());
		return 1;
	}

	if(workers_min>workers_max) {
		fprintf(stderr,"vine_factory: min workers (%d) is greater than max workers (%d)\n",workers_min, workers_max);
		return 1;
	}

	if(amazon_config) {
		char abs_path_name[PATH_MAX];

		/* Store an absolute path b/c the factory will chdir later. */

		if(!realpath(amazon_config, abs_path_name)) {
			fprintf(stderr,"couldn't find full path of %s: %s\n",config_file,strerror(errno));
			return 1;
		}

		amazon_config = strdup(abs_path_name);
	}

	/*
	Careful here: most of the supported batch systems expect
	that jobs are submitting from a single shared filesystem.
	Changing to /tmp only works in the case of Condor.
	*/
	if(!scratch_dir) {
		const char *scratch_parent_dir = ".";
		if(batch_queue_type==BATCH_QUEUE_TYPE_CONDOR) {
			scratch_parent_dir = system_tmp_dir(NULL);
		}
		scratch_dir = string_format("%s/vine-factory-%d", scratch_parent_dir, getuid());
	}

	if(!create_dir(scratch_dir,0777)) {
		fprintf(stderr,"vine_factory: couldn't create %s: %s",scratch_dir,strerror(errno));
		return 1;
	}

	const char *item = NULL;
	list_first_item(wrapper_inputs);
	while((item = list_next_item(wrapper_inputs))) {
		char *file_at_scratch_dir = string_format("%s/%s", scratch_dir, path_basename(item));
		int result = copy_file_to_file(item, file_at_scratch_dir);
		if(result < 0) {
			fprintf(stderr,"vine_factory: Cannot copy wrapper input file %s to factory scratch directory %s:\n", item, file_at_scratch_dir);
			fprintf(stderr,"%s\n", strerror(errno));
			exit(EXIT_FAILURE);
		}
		free(file_at_scratch_dir);
	}

	char* cmd;
	if(worker_command != NULL){
		cmd = string_format("cp '%s' '%s'",worker_command,scratch_dir);
		if(system(cmd)){
			fprintf(stderr,"vine_factory: Could not Access specified worker binary.\n");
			exit(EXIT_FAILURE);
		}
		free(cmd);
		char *tmp = xxstrdup(path_basename(worker_command));
		free(worker_command);
		worker_command = tmp;
	}else{
		worker_command = xxstrdup("vine_worker");
		char *tmp = path_which(worker_command);
		if(!tmp) {
			fprintf(stderr, "vine_factory: please add vine_worker to your PATH, or use --worker-binary\n");
			exit(EXIT_FAILURE);
		}

		cmd = string_format("cp '%s' '%s'",tmp,scratch_dir);
		if (system(cmd)) {
			fprintf(stderr, "vine_factory: could not copy vine_worker to scratch directory.\n");
			exit(EXIT_FAILURE);
		}
		free(cmd);
	}

	if(password_file) {
		cmd = string_format("cp %s %s/pwfile",password_file,scratch_dir);
		system(cmd);
		free(cmd);
	}

	if(chdir(scratch_dir)!=0) {
		fprintf(stderr,"vine_factory: couldn't chdir to %s: %s",scratch_dir,strerror(errno));
		return 1;
	}

	signal(SIGINT, handle_abort);
	signal(SIGQUIT, handle_abort);
	signal(SIGTERM, handle_abort);
	signal(SIGHUP, ignore_signal);

	queue = batch_queue_create(batch_queue_type);
	if(!queue) {
		fprintf(stderr,"vine_factory: couldn't establish queue type %s",batch_queue_type_to_string(batch_queue_type));
		return 1;
	}

	batch_queue_set_option(queue, "batch-options", batch_submit_options);
	batch_queue_set_option(queue, "autosize", autosize ? "yes" : NULL);
	set_worker_resources_options( queue );

	if(amazon_config) {
		batch_queue_set_option(queue, "amazon-config", amazon_config );
	}

	if(condor_requirements != NULL && batch_queue_type != BATCH_QUEUE_TYPE_CONDOR) {
		debug(D_NOTICE, "condor_requirements will be ignored as workers will not be running in condor.");
	} else {
		batch_queue_set_option(queue, "condor-requirements", condor_requirements);
	}

	mainloop( queue );

	if(batch_queue_type == BATCH_QUEUE_TYPE_MESOS) {

		batch_queue_set_int_option(queue, "batch-queue-abort-flag", (int)abort_flag);
		batch_queue_set_int_option(queue, "batch-queue-failed-flag", 0);

	}

	batch_queue_delete(queue);

	return 0;
}

/* vim: set noexpandtab tabstop=8: */
