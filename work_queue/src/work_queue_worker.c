/*
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "work_queue.h"
#include "work_queue_protocol.h"
#include "work_queue_internal.h"
#include "work_queue_resources.h"
#include "work_queue_process.h"
#include "work_queue_catalog.h"
#include "work_queue_watcher.h"
#include "work_queue_gpus.h"
#include "work_queue_coprocess.h"
#include "work_queue_sandbox.h"

#include "cctools.h"
#include "envtools.h"
#include "macros.h"
#include "catalog_query.h"
#include "domain_name_cache.h"
#include "jx.h"
#include "jx_eval.h"
#include "jx_parse.h"
#include "jx_print.h"
#include "copy_stream.h"
#include "host_memory_info.h"
#include "host_disk_info.h"
#include "path_disk_size_info.h"
#include "hash_cache.h"
#include "link.h"
#include "link_auth.h"
#include "list.h"
#include "xxmalloc.h"
#include "debug.h"
#include "stringtools.h"
#include "path.h"
#include "load_average.h"
#include <getopt.h>
#include "getopt_aux.h"
#include "create_dir.h"
#include "unlink_recursive.h"
#include "itable.h"
#include "random.h"
#include "url_encode.h"
#include "md5.h"
#include "disk_alloc.h"
#include "hash_table.h"
#include "pattern.h"
#include "gpu_info.h"
#include "tlq_config.h"
#include "stringtools.h"
#include "trash.h"
#include "process.h"

#include <unistd.h>
#include <dirent.h>
#include <fcntl.h>
#include <assert.h>
#include <errno.h>
#include <limits.h>
#include <math.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include <poll.h>
#include <signal.h>

#include <sys/mman.h>
#include <sys/resource.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/utsname.h>
#include <sys/wait.h>

typedef enum {
	WORKER_MODE_WORKER,
	WORKER_MODE_FOREMAN
} worker_mode_t;

// In single shot mode, immediately quit when disconnected.
// Useful for accelerating the test suite.
static int single_shot_mode = 0;

// Maximum time to stay connected to a single manager without any work.
static int idle_timeout = 900;

// Current time at which we will give up if no work is received.
static time_t idle_stoptime = 0;

// Current time at which we will give up if no manager is found.
static time_t connect_stoptime = 0;

// Maximum time to attempt connecting to all available managers before giving up.
static int connect_timeout = 900;

// Maximum time to attempt sending/receiving any given file or message.
static const int active_timeout = 3600;

// Maximum time for the foreman to spend waiting in its internal loop
static const int foreman_internal_timeout = 5;

// Initial value for backoff interval (in seconds) when worker fails to connect to a manager.
static int init_backoff_interval = 1;

// Maximum value for backoff interval (in seconds) when worker fails to connect to a manager.
static int max_backoff_interval = 60;

// Absolute end time (in useconds) for worker, worker is killed after this point.
static timestamp_t end_time = 0;

// Chance that a worker will decide to shut down each minute without warning, to simulate failure.
static double worker_volatility = 0.0;

// If flag is set, then the worker proceeds to immediately cleanup and shut down.
// This can be set by Ctrl-C or by any condition that prevents further progress.
static int abort_flag = 0;

// Record the signal received, to inform the manager if appropiate.
static int abort_signal_received = 0;

// Flag used to indicate a child must be waited for.
static int sigchld_received_flag = 0;

// Password shared between manager and worker.
char *password = 0;

// Allow worker to use symlinks when link() fails.  Enabled by default.
int symlinks_enabled = 1;

// Worker id. A unique id for this worker instance.
static char *worker_id;

// If set to "by_ip", "by_hostname", or "by_apparent_ip", overrides manager's
// preferred connection mode.
char *preferred_connection = NULL;

// Whether to force a ssl connection. If using the catalog server and the
// manager announces it is using SSL, then SSL is used regardless of
// manual_ssl_option.
int manual_ssl_option = 0;

// pid of the worker's parent process. If different from zero, worker will be
// terminated when its parent process changes.
static pid_t initial_ppid = 0;

static worker_mode_t worker_mode = WORKER_MODE_WORKER;

struct manager_address {
	char host[DOMAIN_NAME_MAX];
	int port;
	char addr[DOMAIN_NAME_MAX];
};
struct list *manager_addresses;
struct manager_address *current_manager_address;

char *workspace;
static char *os_name = NULL;
static char *arch_name = NULL;
static char *user_specified_workdir = NULL;
static timestamp_t worker_start_time = 0;

static struct work_queue_watcher * watcher = 0;

static struct work_queue_resources * local_resources = 0;
struct work_queue_resources * total_resources = 0;
struct work_queue_resources * total_resources_last = 0;

static int64_t last_task_received  = 0;

/* 0 means not given as a command line option. */
static int64_t manual_cores_option = 0;
static int64_t manual_disk_option = 0;
static int64_t manual_memory_option = 0;
static time_t  manual_wall_time_option = 0;

/* -1 means not given as a command line option. */
static int64_t manual_gpus_option = -1;

static int64_t cores_allocated = 0;
static int64_t memory_allocated = 0;
static int64_t disk_allocated = 0;
static int64_t gpus_allocated = 0;

// Allow worker to use disk_alloc loop devices for task sandbox. Disabled by default.
static int disk_allocation = 0;

static int64_t files_counted = 0;

static int check_resources_interval = 5;
static int max_time_on_measurement  = 3;

static struct work_queue *foreman_q = NULL;

// Table of all processes in any state, indexed by taskid.
// Processes should be created/deleted when added/removed from this table.
static struct itable *procs_table = NULL;

// Table of all processes currently running, indexed by pid.
// These are additional pointers into procs_table.
static struct itable *procs_running = NULL;

// List of all procs that are waiting to be run.
// These are additional pointers into procs_table.
static struct list   *procs_waiting = NULL;

// Table of all processes with results to be sent back, indexed by taskid.
// These are additional pointers into procs_table.
static struct itable *procs_complete = NULL;

//User specified features this worker provides.
static struct hash_table *features = NULL;

static int results_to_be_sent_msg = 0;

static timestamp_t total_task_execution_time = 0;
static int total_tasks_executed = 0;

static const char *project_regex = 0;
static int released_by_manager = 0;

static char *tlq_url = NULL;
static char *debug_path = NULL;
static char *catalog_hosts = NULL;
static int tlq_port = 0;

static char *coprocess_command = NULL;
static char *coprocess_name = NULL;
static int number_of_coprocess_instances = 0;
struct work_queue_coprocess *coprocess_info = NULL;
struct work_queue_resources *coprocess_resources = NULL;

static int coprocess_cores = -1;
static int coprocess_memory = -1;
static int coprocess_disk = -1;
static int coprocess_gpus = -1;

static char *factory_name = NULL;

struct work_queue_cache *global_cache = 0;

extern int wq_hack_do_not_compute_cached_name;

__attribute__ (( format(printf,2,3) ))
static void send_manager_message( struct link *l, const char *fmt, ... )
{
	char debug_msg[2*WORK_QUEUE_LINE_MAX];
	va_list va;
	va_list debug_va;

	va_start(va,fmt);

	string_nformat(debug_msg, sizeof(debug_msg), "tx: %s", fmt);
	va_copy(debug_va, va);

	vdebug(D_WQ, debug_msg, debug_va);
	link_vprintf(l, time(0)+active_timeout, fmt, va);

	va_end(va);
}

static int recv_manager_message( struct link *l, char *line, int length, time_t stoptime )
{
	int result = link_readline(l,line,length,stoptime);
	if(result) debug(D_WQ,"rx: %s",line);
	return result;
}

/*
We track how much time has elapsed since the manager assigned a task.
If time(0) > idle_stoptime, then the worker will disconnect.
*/

static void reset_idle_timer()
{
	idle_stoptime = time(0) + idle_timeout;
}

/*
Measure the disk used by the worker. We only manually measure the cache directory, as processes measure themselves.
*/

static int64_t measure_worker_disk()
{
	static struct path_disk_size_info *state = NULL;

	path_disk_size_info_get_r("./cache", max_time_on_measurement, &state, NULL);

	int64_t disk_measured = 0;
	if(state->last_byte_size_complete >= 0) {
		disk_measured = (int64_t) ceil(state->last_byte_size_complete/(1.0*MEGA));
	}

	files_counted = state->last_file_count_complete;

	if(state->complete_measurement) {
		/* if a complete measurement has been done, then update
		 * for the found value, and add the known values of the processes. */

		struct work_queue_process *p;
		uint64_t taskid;

		itable_firstkey(procs_table);
		while(itable_nextkey(procs_table,&taskid,(void**)&p)) {
			if(p->sandbox_size > 0) {
				disk_measured += p->sandbox_size;
				files_counted += p->sandbox_file_count;
			}
		}
	}

	return disk_measured;
}

/*
Measure only the resources associated with this particular node
and apply any operations that override.
*/

static void measure_worker_resources()
{
	static time_t last_resources_measurement = 0;
	if(time(0) < last_resources_measurement + check_resources_interval) {
		return;
	}

	struct work_queue_resources *r = local_resources;

	work_queue_resources_measure_locally(r,workspace);

	if(worker_mode == WORKER_MODE_FOREMAN) {
		aggregate_workers_resources(foreman_q, total_resources, features);
	} else {
		if(manual_cores_option > 0)
			r->cores.total = manual_cores_option;
		if(manual_memory_option > 0)
			r->memory.total = manual_memory_option;
		if(manual_gpus_option > -1)
			r->gpus.total = manual_gpus_option;
	}

	if(manual_disk_option > 0) {
		r->disk.total = MIN(r->disk.total, manual_disk_option);
	}

	r->cores.smallest = r->cores.largest = r->cores.total;
	r->memory.smallest = r->memory.largest = r->memory.total;
	r->disk.smallest = r->disk.largest = r->disk.total;
	r->gpus.smallest = r->gpus.largest = r->gpus.total;

	r->disk.inuse = measure_worker_disk();
	r->tag = last_task_received;

	if(worker_mode == WORKER_MODE_FOREMAN) {
		total_resources->disk.total = r->disk.total;
		total_resources->disk.inuse = r->disk.inuse;
		total_resources->tag        = last_task_received;
	} else {
		/* in a regular worker, total and local resources are the same. */
		memcpy(total_resources, r, sizeof(struct work_queue_resources));
	}

	work_queue_gpus_init(r->gpus.total);

	if (coprocess_command != NULL && coprocess_info != NULL) {
		work_queue_coprocess_measure_resources(coprocess_info, number_of_coprocess_instances);
	}

	last_resources_measurement = time(0);
}

/*
Send a message to the manager with user defined features.
*/

static void send_features(struct link *manager)
{
	char *f;
	void *dummy;
	hash_table_firstkey(features);

	char fenc[WORK_QUEUE_LINE_MAX];
	while(hash_table_nextkey(features, &f, &dummy)) {
		url_encode(f, fenc, WORK_QUEUE_LINE_MAX);
		send_manager_message(manager, "feature %s\n", fenc);
	}
}


/*
Send a message to the manager with my current resources.
*/

static void send_resource_update(struct link *manager)
{
	time_t stoptime = time(0) + active_timeout;

	if(worker_mode == WORKER_MODE_FOREMAN) {
		total_resources->disk.total = local_resources->disk.total;
		total_resources->disk.inuse = local_resources->disk.inuse;
	} else {
		total_resources->memory.total    = MAX(0, local_resources->memory.total);
		total_resources->memory.largest  = MAX(0, local_resources->memory.largest);
		total_resources->memory.smallest = MAX(0, local_resources->memory.smallest);

		total_resources->disk.total    = MAX(0, local_resources->disk.total);
		total_resources->disk.largest  = MAX(0, local_resources->disk.largest);
		total_resources->disk.smallest = MAX(0, local_resources->disk.smallest);

		//if workers are set to expire in some time, send the expiration time to manager
		if(manual_wall_time_option > 0) {
			end_time = worker_start_time + (manual_wall_time_option * 1e6);
		}
	}

	if (coprocess_info != NULL) {
		work_queue_coprocess_resources_send(manager,coprocess_resources,stoptime);
	}

	work_queue_resources_send(manager,total_resources,stoptime);
	send_manager_message(manager, "info end_of_resource_update %d\n", 0);
}

/*
Send a message to the manager with my current statistics information.
*/

static void send_stats_update(struct link *manager)
{
	if(worker_mode == WORKER_MODE_FOREMAN) {
		struct work_queue_stats s;
		work_queue_get_stats_hierarchy(foreman_q, &s);

		send_manager_message(manager, "info workers_joined %lld\n", (long long) s.workers_joined);
		send_manager_message(manager, "info workers_removed %lld\n", (long long) s.workers_removed);
		send_manager_message(manager, "info workers_released %lld\n", (long long) s.workers_released);
		send_manager_message(manager, "info workers_idled_out %lld\n", (long long) s.workers_idled_out);
		send_manager_message(manager, "info workers_fast_aborted %lld\n", (long long) s.workers_fast_aborted);
		send_manager_message(manager, "info workers_blacklisted %lld\n", (long long) s.workers_blacklisted);
		send_manager_message(manager, "info workers_lost %lld\n", (long long) s.workers_lost);

		send_manager_message(manager, "info tasks_waiting %lld\n", (long long) s.tasks_waiting);
		send_manager_message(manager, "info tasks_on_workers %lld\n", (long long) s.tasks_on_workers);
		send_manager_message(manager, "info tasks_running %lld\n", (long long) s.tasks_running);
		send_manager_message(manager, "info tasks_waiting %lld\n", (long long) list_size(procs_waiting));
		send_manager_message(manager, "info tasks_with_results %lld\n", (long long) s.tasks_with_results);

		send_manager_message(manager, "info time_send %lld\n", (long long) s.time_send);
		send_manager_message(manager, "info time_receive %lld\n", (long long) s.time_receive);
		send_manager_message(manager, "info time_send_good %lld\n", (long long) s.time_send_good);
		send_manager_message(manager, "info time_receive_good %lld\n", (long long) s.time_receive_good);

		send_manager_message(manager, "info time_workers_execute %lld\n", (long long) s.time_workers_execute);
		send_manager_message(manager, "info time_workers_execute_good %lld\n", (long long) s.time_workers_execute_good);
		send_manager_message(manager, "info time_workers_execute_exhaustion %lld\n", (long long) s.time_workers_execute_exhaustion);

		send_manager_message(manager, "info bytes_sent %lld\n", (long long) s.bytes_sent);
		send_manager_message(manager, "info bytes_received %lld\n", (long long) s.bytes_received);
	}
	else {
		send_manager_message(manager, "info tasks_running %lld\n", (long long) itable_size(procs_running));
	}
}

/*
Send a periodic keepalive message to the manager, otherwise it will
think that the worker has crashed and gone away. 
*/

static int send_keepalive(struct link *manager, int force_resources)
{
	send_manager_message(manager, "alive\n");

	/* for regular workers we only send resources on special ocassions, thus
	 * the force_resources. */
	if(force_resources || worker_mode == WORKER_MODE_FOREMAN) {
		send_resource_update(manager);
	}

	send_stats_update(manager);

	return 1;
}

/*
Send an asynchronmous message to the manager indicating that an item was successfully loaded into the cache, along with its size in bytes and transfer time in usec.
*/

void send_cache_update( struct link *manager, const char *cachename, int64_t size, timestamp_t transfer_time )
{
	send_manager_message(manager,"cache-update %s %lld %lld\n",cachename,(long long)size,(long long)transfer_time);
}

/*
Send an asynchronous message to the manager indicating that an item previously queued in the cache is invalid because it could not be loaded.  Accompanied by a corresponding error message.
*/

void send_cache_invalid( struct link *manager, const char *cachename, const char *message )
{
	int length = strlen(message);
	send_manager_message(manager,"cache-invalid %s %d\n",cachename,length);
	link_write(manager,message,length,time(0)+active_timeout);
}

static int send_tlq_config( struct link *manager )
{
	//attempt to find local TLQ server to retrieve manager URL
	if(tlq_port && debug_path && !tlq_url) {
		debug(D_TLQ, "looking up worker TLQ URL");
		time_t config_stoptime = time(0) + 10;
		tlq_url = tlq_config_url(tlq_port, debug_path, config_stoptime);
		if(tlq_url) debug(D_TLQ, "set worker TLQ URL: %s", tlq_url);
		else debug(D_TLQ, "error setting worker TLQ URL");
	}
	else if(tlq_port && !debug_path && !tlq_url) debug(D_TLQ, "cannot get worker TLQ URL: no debug log path set");

	if(tlq_url) send_manager_message(manager, "tlq %s\n", tlq_url);
	return 1;
}

static int get_task_tlq_url( struct work_queue_task *task )
{
	if(tlq_port && debug_path) {
		char home_host[WORK_QUEUE_LINE_MAX];
		char tlq_workdir[WORK_QUEUE_LINE_MAX];
		char log_path[WORK_QUEUE_LINE_MAX];
		int home_port;
		debug(D_TLQ, "looking up task %d TLQ URL", task->taskid);
		//Command is assumed to be wrapped by log_define script from TLQ
		if(sscanf(task->command_line,"sh log_define %s %d %s %s", home_host, &home_port, tlq_workdir, log_path) == 4) {
			time_t config_stoptime = time(0) + 10;
			char *task_url = tlq_config_url(tlq_port, log_path, config_stoptime);
			if(!task_url) {
				debug(D_TLQ, "error setting task %d TLQ URL", task->taskid);
				return 0;
			}
			debug(D_TLQ, "set task %d TLQ URL: %s", task->taskid, task_url);
			return 1;
		}
		else {
			debug(D_TLQ, "could not find task %d debug log", task->taskid);
			return 0;
		}
		return 1;
	}
	else return 0;
}

/*
Send the initial "ready" message to the manager with the version and so forth.
The manager will not start sending tasks until this message is recevied.
*/

static void report_worker_ready( struct link *manager )
{
    /* 
    The hostname is useful for troubleshooting purposes, but not required.
    If there are naming problems, just use "unknown".
    */

    char hostname[DOMAIN_NAME_MAX];
    if(!domain_name_cache_guess(hostname)) {
        strcpy(hostname,"unknown");
    }

	send_manager_message(manager,"workqueue %d %s %s %s %d.%d.%d\n",WORK_QUEUE_PROTOCOL_VERSION,hostname,os_name,arch_name,CCTOOLS_VERSION_MAJOR,CCTOOLS_VERSION_MINOR,CCTOOLS_VERSION_MICRO);
	send_manager_message(manager, "info worker-id %s\n", worker_id);
	send_features(manager);
	send_tlq_config(manager);
	send_keepalive(manager, 1);
	send_manager_message(manager, "info worker-end-time %" PRId64 "\n", (int64_t) DIV_INT_ROUND_UP(end_time, USECOND));
	if (factory_name)
		send_manager_message(manager, "info from-factory %s\n", factory_name);
}

/*
Start executing the given process on the local host,
accounting for the resources as necessary.
Should maintain parallel structure to reap_process() above.
*/

static int start_process( struct work_queue_process *p, struct link *manager )
{
	pid_t pid;

	struct work_queue_task *t = p->task;

	if(!work_queue_sandbox_stagein(p,global_cache,manager)) {
		p->execution_start = p->execution_end = timestamp_get();
		p->task_status = WORK_QUEUE_RESULT_INPUT_MISSING;
		p->exit_status = 1;
		itable_insert(procs_complete,p->task->taskid,p);
		return 0;
	}
	
	cores_allocated += t->resources_requested->cores;
	memory_allocated += t->resources_requested->memory;
	disk_allocated += t->resources_requested->disk;
	gpus_allocated += t->resources_requested->gpus;

	if(t->resources_requested->gpus>0) {
		work_queue_gpus_allocate(t->resources_requested->gpus,t->taskid);
	}

	pid = work_queue_process_execute(p);
	if(pid<0) fatal("unable to fork process for taskid %d!",p->task->taskid);

	itable_insert(procs_running,p->pid,p);
	
	return 1;
}

/*
This process has ended so mark it complete and
account for the resources as necessary.
Should maintain parallel structure to start_process() above.
*/

static void reap_process( struct work_queue_process *p )
{
	p->execution_end = timestamp_get();

	cores_allocated  -= p->task->resources_requested->cores;
	memory_allocated -= p->task->resources_requested->memory;
	disk_allocated   -= p->task->resources_requested->disk;
	gpus_allocated   -= p->task->resources_requested->gpus;

	work_queue_gpus_free(p->task->taskid);

	if(!work_queue_sandbox_stageout(p,global_cache)) {
		p->task_status = WORK_QUEUE_RESULT_OUTPUT_MISSING;
		p->exit_status = 1;
	}

	itable_remove(procs_running, p->pid);
	itable_insert(procs_complete, p->task->taskid, p);
}

/*
Transmit the results of the given process to the manager.
If a local worker, stream the output from disk.
If a foreman, send the outputs contained in the task structure.
*/

static void report_task_complete( struct link *manager, struct work_queue_process *p )
{
	int64_t output_length;
	struct stat st;

	if(worker_mode==WORKER_MODE_WORKER) {
		fstat(p->output_fd, &st);
		output_length = st.st_size;
		lseek(p->output_fd, 0, SEEK_SET);
		send_manager_message(manager, "result %d %d %lld %llu %d\n", p->task_status, p->exit_status, (long long) output_length, (unsigned long long) p->execution_end-p->execution_start, p->task->taskid);
		link_stream_from_fd(manager, p->output_fd, output_length, time(0)+active_timeout);

		total_task_execution_time += (p->execution_end - p->execution_start);
		total_tasks_executed++;
	} else {
		struct work_queue_task *t = p->task;
		if(t->output) {
			output_length = strlen(t->output);
		} else {
			output_length = 0;
		}
		send_manager_message(manager, "result %d %d %lld %llu %d\n", t->result, t->return_status, (long long) output_length, (unsigned long long) t->time_workers_execute_last, t->taskid);
		if(output_length) {
			link_putlstring(manager, t->output, output_length, time(0)+active_timeout);
		}

		total_task_execution_time += t->time_workers_execute_last;
		total_tasks_executed++;
	}

	get_task_tlq_url(p->task);
	send_stats_update(manager);
}

/*
For every unreported complete task and watched file,
send the results to the manager.
*/

static void report_tasks_complete( struct link *manager )
{
	struct work_queue_process *p;

	while((p=itable_pop(procs_complete))) {
		report_task_complete(manager,p);
	}

	work_queue_watcher_send_changes(watcher,manager,time(0)+active_timeout);

	send_manager_message(manager, "end\n");

	results_to_be_sent_msg = 0;
}

/*
Find any processes that have overrun their declared absolute end time,
and send a kill signal.  The actual exit of the process will be detected at a later time.
*/

static void expire_procs_running()
{
	struct work_queue_process *p;
	uint64_t pid;

	double current_time = timestamp_get() / USECOND;

	itable_firstkey(procs_running);
	while(itable_nextkey(procs_running, (uint64_t*)&pid, (void**)&p)) {
		if(p->task->resources_requested->end > 0 && current_time > p->task->resources_requested->end)
		{
			p->task_status = WORK_QUEUE_RESULT_TASK_TIMEOUT;
			kill(pid, SIGKILL);
		}
	}
}

/*
Return true if task uses a disk allocation and it was overrun.
*/

static int is_disk_allocation_exhausted( struct work_queue_process *p )
{
	int result = 0;
	FILE *loop_full_check;
	char *buf = malloc(PATH_MAX);
	char *disk_alloc_filename = work_queue_generate_disk_alloc_full_filename(p->sandbox,p->task->taskid);
	
	if(p->loop_mount == 1 && (loop_full_check = fopen(disk_alloc_filename, "r"))) {
		fclose(loop_full_check);
		trash_file(disk_alloc_filename);
		result = 1;
	} else {
		result = 0;
	}

	free(buf);
	free(disk_alloc_filename);

	return result;
}

/*
Scan over all of the processes known by the worker,
and if they have exited, move them into the procs_complete table
for later processing.
*/

static int handle_completed_tasks(struct link *manager)
{
	struct work_queue_process *p;
	pid_t pid;
	int status;

	itable_firstkey(procs_running);
	while(itable_nextkey(procs_running, (uint64_t*)&pid, (void**)&p)) {
		int result = wait4(pid, &status, WNOHANG, &p->rusage);
		if(result==0) {
			// pid is still going
		} else if(result<0) {
			debug(D_WQ, "wait4 on pid %d returned an error: %s",pid,strerror(errno));
		} else if(result>0) {
			if (!WIFEXITED(status)){
				p->exit_status = WTERMSIG(status);
				debug(D_WQ, "task %d (pid %d) exited abnormally with signal %d",p->task->taskid,p->pid,p->exit_status);
			} else {
				p->exit_status = WEXITSTATUS(status);
				debug(D_WQ, "task %d (pid %d) exited normally with exit code %d",p->task->taskid,p->pid,p->exit_status);

				if(is_disk_allocation_exhausted(p)) {
					p->task_status = WORK_QUEUE_RESULT_DISK_ALLOC_FULL;
					p->task->disk_allocation_exhausted = 1;
				}
			}
			
			if (p->coprocess != NULL) {
				struct work_queue_coprocess *cop= (struct work_queue_coprocess *) p->coprocess;
				cop->state = WORK_QUEUE_COPROCESS_READY;
			}
			
			/* collect the resources associated with the process */
			reap_process(p);
			
			/* must reset the table iterator because an item was removed. */
			itable_firstkey(procs_running);


		}

	}
	return 1;
}

/**
 * Stream file/directory contents for the recursive get/put protocol.
 * Format:
 * 		for a directory: a new line in the format of "dir $DIR_NAME 0"
 * 		for a file: a new line in the format of "file $FILE_NAME $FILE_LENGTH"
 * 					then file contents.
 * 		string "end" at the end of the stream (on a new line).
 *
 * Example:
 * Assume we have the following directory structure:
 * mydir
 * 		-- 1.txt
 * 		-- 2.txt
 * 		-- mysubdir
 * 			-- a.txt
 * 			-- b.txt
 * 		-- z.jpg
 *
 * The stream contents would be:
 *
 * dir mydir 0
 * file 1.txt $file_len
 * $$ FILE 1.txt's CONTENTS $$
 * file 2.txt $file_len
 * $$ FILE 2.txt's CONTENTS $$
 * dir mysubdir 0
 * file mysubdir/a.txt $file_len
 * $$ FILE mysubdir/a.txt's CONTENTS $$
 * file mysubdir/b.txt $file_len
 * $$ FILE mysubdir/b.txt's CONTENTS $$
 * file z.jpg $file_len
 * $$ FILE z.jpg's CONTENTS $$
 * end
 *
 */

static int stream_output_item(struct link *manager, const char *filename, int recursive)
{
	DIR *dir;
	struct dirent *dent;
	struct stat info;
	int64_t actual, length;
	int fd;

	char *cached_path = work_queue_cache_full_path(global_cache,filename);
	
	if(stat(cached_path, &info) != 0) {
		goto access_failure;
	}

	if(S_ISDIR(info.st_mode)) {
		// stream a directory
		dir = opendir(cached_path);
		if(!dir) goto access_failure;

		send_manager_message(manager, "dir %s 0\n", filename);

		while(recursive && (dent = readdir(dir))) {
			if(!strcmp(dent->d_name, ".") || !strcmp(dent->d_name, ".."))
				continue;
			char *subfilename = string_format("%s/%s", filename, dent->d_name);
			stream_output_item(manager, subfilename, recursive);
			free(subfilename);
		}

		closedir(dir);
	} else {
		// stream a file
		fd = open(cached_path, O_RDONLY, 0);
		if(fd >= 0) {
			length = info.st_size;
			send_manager_message(manager, "file %s %"PRId64"\n", filename, length );
			actual = link_stream_from_fd(manager, fd, length, time(0) + active_timeout);
			close(fd);
			if(actual != length) goto send_failure;
		} else {
			goto access_failure;
		}
	}

	free(cached_path);
	return 1;

access_failure:
	free(cached_path);
	send_manager_message(manager, "missing %s %d\n", filename, errno);
	return 0;

send_failure:
	free(cached_path);
	debug(D_WQ, "Sending back output file - %s failed: bytes to send = %"PRId64" and bytes actually sent = %"PRId64".", filename, length, actual);
	return 0;
}

/*
For a task run locally, if the resources are all set to -1,
then assume that the task occupies all worker resources.
Otherwise, just make sure all values are non-zero.
*/

static void normalize_resources( struct work_queue_process *p )
{
	struct work_queue_task *t = p->task;

	if(t->resources_requested->cores < 0 && t->resources_requested->memory < 0 && t->resources_requested->disk < 0 && t->resources_requested->gpus < 0) {
		t->resources_requested->cores = local_resources->cores.total;
		t->resources_requested->memory = local_resources->memory.total;
		t->resources_requested->disk = local_resources->disk.total;
		t->resources_requested->gpus = local_resources->gpus.total;
	} else {
		t->resources_requested->cores = MAX(t->resources_requested->cores, 0);
		t->resources_requested->memory = MAX(t->resources_requested->memory, 0);
		t->resources_requested->disk = MAX(t->resources_requested->disk, 0);
		t->resources_requested->gpus = MAX(t->resources_requested->gpus, 0);
	}
}

/*
Handle an incoming task message from the manager.
Generate a work_queue_process wrapped around a work_queue_task,
and deposit it into the waiting list or the foreman_q as appropriate.
*/

static int do_task( struct link *manager, int taskid, time_t stoptime )
{
	char line[WORK_QUEUE_LINE_MAX];
	char filename[WORK_QUEUE_LINE_MAX];
	char localname[WORK_QUEUE_LINE_MAX];
	char taskname[WORK_QUEUE_LINE_MAX];
	char taskname_encoded[WORK_QUEUE_LINE_MAX];
	char category[WORK_QUEUE_LINE_MAX];
	int flags, length;
	int64_t n;

	timestamp_t nt;

	struct work_queue_task *task = work_queue_task_create(0);
	task->taskid = taskid;

	while(recv_manager_message(manager,line,sizeof(line),stoptime)) {
		if(!strcmp(line,"end")) {
			break;
		} else if(sscanf(line, "category %s",category)) {
			work_queue_task_specify_category(task, category);
		} else if(sscanf(line,"cmd %d",&length)==1) {
			char *cmd = malloc(length+1);
			link_read(manager,cmd,length,stoptime);
			cmd[length] = 0;
			work_queue_task_specify_command(task,cmd);
			debug(D_WQ,"rx: %s",cmd);
			free(cmd);
		} else if(sscanf(line,"coprocess %d",&length)==1) {
			char *cmd = malloc(length+1);
			link_read(manager,cmd,length,stoptime);
			cmd[length] = 0;
			work_queue_task_specify_coprocess(task,cmd);
			debug(D_WQ,"rx: %s",cmd);
			free(cmd);
		} else if(sscanf(line,"infile %s %s %d", localname, taskname_encoded, &flags)) {
			url_decode(taskname_encoded, taskname, WORK_QUEUE_LINE_MAX);
			wq_hack_do_not_compute_cached_name = 1;
			work_queue_task_specify_file(task, localname, taskname, WORK_QUEUE_INPUT, flags );
		} else if(sscanf(line,"outfile %s %s %d", localname, taskname_encoded, &flags)) {
			url_decode(taskname_encoded, taskname, WORK_QUEUE_LINE_MAX);
			wq_hack_do_not_compute_cached_name = 1;
			work_queue_task_specify_file(task, localname, taskname, WORK_QUEUE_OUTPUT, flags );
		} else if(sscanf(line, "dir %s", filename)) {
			work_queue_task_specify_directory(task, filename, filename, WORK_QUEUE_INPUT, 0700, 0);
		} else if(sscanf(line,"cores %" PRId64,&n)) {
			work_queue_task_specify_cores(task, n);
		} else if(sscanf(line,"memory %" PRId64,&n)) {
			work_queue_task_specify_memory(task, n);
		} else if(sscanf(line,"disk %" PRId64,&n)) {
			work_queue_task_specify_disk(task, n);
		} else if(sscanf(line,"gpus %" PRId64,&n)) {
			work_queue_task_specify_gpus(task, n);
		} else if(sscanf(line,"wall_time %" PRIu64,&nt)) {
			work_queue_task_specify_running_time_max(task, nt);
		} else if(sscanf(line,"end_time %" PRIu64,&nt)) {
			work_queue_task_specify_end_time(task, nt * USECOND); //end_time needs it usecs
		} else if(sscanf(line,"env %d",&length)==1) {
			char *env = malloc(length+2); /* +2 for \n and \0 */
			link_read(manager, env, length+1, stoptime);
			env[length] = 0;              /* replace \n with \0 */
			char *value = strchr(env,'=');
			if(value) {
				*value = 0;
				value++;
				work_queue_task_specify_environment_variable(task,env,value);
			}
			free(env);
		} else {
			debug(D_WQ|D_NOTICE,"invalid command from manager: %s",line);
			return 0;
		}
	}

	last_task_received = task->taskid;

	struct work_queue_process *p = work_queue_process_create(task, disk_allocation);
	if(!p) return 0;

	// Every received task goes into procs_table.
	itable_insert(procs_table,taskid,p);

	if(worker_mode==WORKER_MODE_FOREMAN) {
		work_queue_submit_internal(foreman_q,task);
	} else {
		normalize_resources(p);
		list_push_tail(procs_waiting,p);
	}

	work_queue_watcher_add_process(watcher,p);

	return 1;
}

/*
Return false if name is invalid as a simple filename.
For example, if it contains a slash, which would escape
the current working directory.
*/

static int is_valid_filename( const char *name )
{
	if(strchr(name,'/')) return 0;
	return 1;
}

/*
Handle an incoming symbolic link inside the rput protocol.
The filename of the symlink was already given in the message,
and the target of the symlink is given as the "body" which
must be read off of the wire.  The symlink target does not
need to be url_decoded because it is sent in the body.
*/

static int do_put_symlink_internal( struct link *manager, char *filename, int length )
{
	char *target = malloc(length);

	int actual = link_read(manager,target,length,time(0)+active_timeout);
	if(actual!=length) {
		free(target);
		return 0;
	}

	int result = symlink(target,filename);
	if(result<0) {
		debug(D_WQ,"could not create symlink %s: %s",filename,strerror(errno));
		free(target);
		return 0;
	}

	free(target);

	return 1;
}

/*
Handle an incoming file inside the rput protocol.
Notice that we trust the caller to have created
the necessary parent directories and checked the
name for validity.
*/

static int do_put_file_internal( struct link *manager, char *filename, int64_t length, int mode )
{
	if(!check_disk_space_for_filesize(".", length, 0)) {
		debug(D_WQ, "Could not put file %s, not enough disk space (%"PRId64" bytes needed)\n", filename, length);
		return 0;
	}

	/* Ensure that worker can access the file! */
	mode = mode | 0600;

	int fd = open(filename, O_WRONLY | O_CREAT | O_TRUNC, mode);
	if(fd<0) {
		debug(D_WQ, "Could not open %s for writing. (%s)\n", filename, strerror(errno));
		return 0;
	}

	int64_t actual = link_stream_to_fd(manager, fd, length, time(0) + active_timeout);
	close(fd);
	if(actual!=length) {
		debug(D_WQ, "Failed to put file - %s (%s)\n", filename, strerror(errno));
		return 0;
	}

	return 1;
}

/*
Handle an incoming directory inside the recursive dir protocol.
Notice that we have already checked the dirname for validity,
and now we process "put" and "dir" commands within the list
until "end" is reached.  Note that "put" is used instead of
"file" for historical reasons, to support recursive reuse
of existing code.
*/

static int do_put_dir_internal( struct link *manager, char *dirname, int *totalsize )
{
	char line[WORK_QUEUE_LINE_MAX];
	char name_encoded[WORK_QUEUE_LINE_MAX];
	char name[WORK_QUEUE_LINE_MAX];
	int64_t size;
	int mode;

	int result = mkdir(dirname,0777);
	if(result<0) {
		debug(D_WQ,"unable to create %s: %s",dirname,strerror(errno));
		return 0;
	}

	while(1) {
		if(!recv_manager_message(manager,line,sizeof(line),time(0)+active_timeout)) return 0;

		int r = 0;

		if(sscanf(line,"put %s %" SCNd64 " %o",name_encoded,&size,&mode)==3) {

			url_decode(name_encoded,name,sizeof(name));
			if(!is_valid_filename(name)) return 0;

			char *subname = string_format("%s/%s",dirname,name);
			r = do_put_file_internal(manager,subname,size,mode);
			free(subname);

			*totalsize += size;

		} else if(sscanf(line,"symlink %s %" SCNd64,name_encoded,&size)==2) {

			url_decode(name_encoded,name,sizeof(name));
			if(!is_valid_filename(name)) return 0;

			char *subname = string_format("%s/%s",dirname,name);
			r = do_put_symlink_internal(manager,subname,size);
			free(subname);

			*totalsize += size;

		} else if(sscanf(line,"dir %s",name_encoded)==1) {

			url_decode(name_encoded,name,sizeof(name));
			if(!is_valid_filename(name)) return 0;

			char *subname = string_format("%s/%s",dirname,name);
			r = do_put_dir_internal(manager,subname,totalsize);
			free(subname);

		} else if(!strcmp(line,"end")) {
			break;
		}

		if(!r) return 0;
	}

	return 1;
}

static int do_put_dir( struct link *manager, char *dirname )
{
	if(!is_valid_filename(dirname)) return 0;

	int totalsize = 0;

	char *cached_path = work_queue_cache_full_path(global_cache,dirname);
	int result = do_put_dir_internal(manager,cached_path,&totalsize);
	free(cached_path);

	if(result) work_queue_cache_addfile(global_cache,totalsize,dirname);

	return result;
}

/*
This is the old method for sending a single file.
It works, but it has the deficiency that the manager
expects the worker to create all parent directories
for the file, which is horrifically expensive when
sending a large directory tree.  The direction put
protocol (above) is preferred instead.
*/

static int do_put_single_file( struct link *manager, char *filename, int64_t length, int mode )
{
	if(!path_within_dir(filename, workspace)) {
		debug(D_WQ, "Path - %s is not within workspace %s.", filename, workspace);
		return 0;
	}

	char * cached_path = work_queue_cache_full_path(global_cache,filename);
	
	if(strchr(filename,'/')) {
		char dirname[WORK_QUEUE_LINE_MAX];
		path_dirname(filename,dirname);
		if(!create_dir(dirname,0777)) {
			debug(D_WQ, "could not create directory %s: %s",dirname,strerror(errno));
			free(cached_path);
			return 0;
		}
	}

	int result = do_put_file_internal(manager,cached_path,length,mode);

	free(cached_path);

	if(result) work_queue_cache_addfile(global_cache,length,filename);

	return result;
}

static int do_tlq_url(const char *manager_tlq_url)
{
	debug(D_TLQ, "set manager TLQ URL: %s", manager_tlq_url);
	return 1;
}

/*
Accept a url specification and queue it for later transfer.
*/

static int do_put_url( const char *cache_name, int64_t size, int mode, const char *source )
{
	return work_queue_cache_queue(global_cache,WORK_QUEUE_CACHE_TRANSFER,source,cache_name,size,mode);
}

/*
Accept a url specification and queue it for later transfer.
*/

static int do_put_cmd( const char *cache_name, int64_t size, int mode, const char *source )
{
	return work_queue_cache_queue(global_cache,WORK_QUEUE_CACHE_COMMAND,source,cache_name,size,mode);
}

/*
The manager has requested the deletion of a file in the cache
directory.  If the request is valid, then move the file to the
trash and deal with it there.
*/

static int do_unlink(const char *path)
{
	char *cached_path = work_queue_cache_full_path(global_cache,path);
  
	int result = 0;
	
	if(path_within_dir(cached_path, workspace)) {
		work_queue_cache_remove(global_cache,path);
		result = 1;
	} else {
		debug(D_WQ, "%s is not within workspace %s",cached_path,workspace);
		result = 0;
	}

	free(cached_path);
	return result;
}

static int do_get(struct link *manager, const char *filename, int recursive)
{
	stream_output_item(manager, filename, recursive);
	send_manager_message(manager, "end\n");
	return 1;
}

/*
do_kill removes a process currently known by the worker.
Note that a kill message from the manager is used for every case
where a task is to be removed, whether it is waiting, running,
of finished.  Regardless of the state, we kill the process and
remove all of the associated files and other state.
*/

static int do_kill(int taskid)
{
	struct work_queue_process *p;

	p = itable_remove(procs_table, taskid);
	if(!p) {
		debug(D_WQ,"manager requested kill of task %d which does not exist!",taskid);
		return 1;
	}

	if(worker_mode == WORKER_MODE_FOREMAN) {
		work_queue_cancel_by_taskid(foreman_q, taskid);
	} else {
		if(itable_remove(procs_running, p->pid)) {
			work_queue_process_kill(p);
			cores_allocated -= p->task->resources_requested->cores;
			memory_allocated -= p->task->resources_requested->memory;
			disk_allocated -= p->task->resources_requested->disk;
			gpus_allocated -= p->task->resources_requested->gpus;
			work_queue_gpus_free(taskid);
		}
	}

	itable_remove(procs_complete, p->task->taskid);
	list_remove(procs_waiting,p);

	work_queue_watcher_remove_process(watcher,p);

	work_queue_process_delete(p);

	return 1;
}

/*
Kill off all known tasks by iterating over the complete
procs_table and calling do_kill.  This should result in
all empty procs_* structures and zero resources allocated.
If this failed to bring the system back to a fresh state,
then we need to abort to clean things up.
*/

static void kill_all_tasks()
{
	struct work_queue_process *p;
	uint64_t taskid;

	itable_firstkey(procs_table);
	while(itable_nextkey(procs_table,&taskid,(void**)&p)) {
		do_kill(taskid);
		/* do_kill removes the task from the table, so we need to reset the iterator. */
		itable_firstkey(procs_table);
	}

	assert(itable_size(procs_table)==0);
	assert(itable_size(procs_running)==0);
	assert(itable_size(procs_complete)==0);
	assert(list_size(procs_waiting)==0);
	assert(cores_allocated==0);
	assert(memory_allocated==0);
	assert(disk_allocated==0);
	assert(gpus_allocated==0);

	debug(D_WQ,"all data structures are clean");
}

/*
Remove a file, even when mark as cached. Foreman broadcast this message to
foremen down its hierarchy. It is invalid for a worker to receice this message.
*/
static int do_invalidate_file(const char *filename)
{
	if(worker_mode == WORKER_MODE_FOREMAN) {
		work_queue_invalidate_cached_file_internal(foreman_q, filename);
		return 1;
	}

	return -1;
}

static void finish_running_task(struct work_queue_process *p, work_queue_result_t result)
{
	p->task_status |= result;
	kill(p->pid, SIGKILL);
}

static void finish_running_tasks(work_queue_result_t result)
{
	struct work_queue_process *p;
	pid_t pid;

	itable_firstkey(procs_running);
	while(itable_nextkey(procs_running, (uint64_t*) &pid, (void**)&p)) {
		finish_running_task(p, result);
	}
}

static int enforce_process_limits(struct work_queue_process *p)
{
	/* If the task did not specify disk usage, return right away. */
	if(p->disk < 1)
		return 1;

	work_queue_process_measure_disk(p, max_time_on_measurement);
	if(p->sandbox_size > p->task->resources_requested->disk) {
		debug(D_WQ,"Task %d went over its disk size limit: %s > %s\n",
				p->task->taskid,
				rmsummary_resource_to_str("disk", p->sandbox_size, /* with units */ 1),
				rmsummary_resource_to_str("disk", p->task->resources_requested->disk, 1));
		return 0;
	}

	return 1;
}

static int enforce_processes_limits()
{
	static time_t last_check_time = 0;

	struct work_queue_process *p;
	pid_t pid;

	int ok = 1;

	/* Do not check too often, as it is expensive (particularly disk) */
	if((time(0) - last_check_time) < check_resources_interval ) return 1;

	itable_firstkey(procs_table);
	while(itable_nextkey(procs_table,(uint64_t*)&pid,(void**)&p)) {
		if(!enforce_process_limits(p) || !work_queue_coprocess_enforce_limit(p->coprocess)) {
			finish_running_task(p, WORK_QUEUE_RESULT_RESOURCE_EXHAUSTION);

			/* we delete the sandbox, to free the exhausted resource. If a loop device is used, use remove loop device*/
			if(p->loop_mount == 1) {
				disk_alloc_delete(p->sandbox);
			} else {
				trash_file(p->sandbox);
			}

			ok = 0;
		}
	}

	last_check_time = time(0);

	return ok;
}

/*
We check maximum_running_time by itself (not in enforce_processes_limits),
as other running tasks should not be affected by a task timeout.
*/

static void enforce_processes_max_running_time()
{
	struct work_queue_process *p;
	pid_t pid;

	timestamp_t now = timestamp_get();

	itable_firstkey(procs_running);
	while(itable_nextkey(procs_running, (uint64_t*) &pid, (void**) &p)) {
		/* If the task did not specify wall_time, return right away. */
		if(p->task->resources_requested->wall_time < 1)
			continue;

		if(now > p->execution_start + (1e6 * p->task->resources_requested->wall_time)) {
			debug(D_WQ,"Task %d went over its running time limit: %s > %s\n",
					p->task->taskid,
					rmsummary_resource_to_str("wall_time", (now - p->execution_start)/1e6, 1),
					rmsummary_resource_to_str("wall_time", p->task->resources_requested->wall_time, 1));
			p->task_status = WORK_QUEUE_RESULT_TASK_MAX_RUN_TIME;
			kill(pid, SIGKILL);
		}
	}

	return;
}


static int do_release()
{
	debug(D_WQ, "released by manager %s:%d.\n", current_manager_address->addr, current_manager_address->port);
	released_by_manager = 1;
	return 0;
}

static void disconnect_manager(struct link *manager)
{
	debug(D_WQ, "disconnecting from manager %s:%d", current_manager_address->addr, current_manager_address->port);
	link_close(manager);

	debug(D_WQ, "killing all outstanding tasks");
	kill_all_tasks();

	//KNOWN HACK: We remove all workers on a manager disconnection to avoid
	//returning old tasks to a new manager.
	if(foreman_q) {
		debug(D_WQ, "Disconnecting all workers...\n");
		release_all_workers(foreman_q);

		if(project_regex) {
			update_catalog(foreman_q, manager, 1);
		}
	}

	if(released_by_manager) {
		released_by_manager = 0;
	} else if(abort_flag) {
		// Bail out quickly
	} else {
		sleep(5);
	}
}

static int handle_manager(struct link *manager)
{
	char line[WORK_QUEUE_LINE_MAX];
	char filename_encoded[WORK_QUEUE_LINE_MAX];
	char filename[WORK_QUEUE_LINE_MAX];
	char source_encoded[WORK_QUEUE_LINE_MAX];
	char source[WORK_QUEUE_LINE_MAX];
	char manager_tlq_url[WORK_QUEUE_LINE_MAX];
	int64_t length;
	int64_t taskid = 0;
	int mode, r, n;

	if(recv_manager_message(manager, line, sizeof(line), idle_stoptime )) {
		if(sscanf(line,"task %" SCNd64, &taskid)==1) {
			r = do_task(manager, taskid,time(0)+active_timeout);
		} else if(sscanf(line,"put %s %"SCNd64" %o",filename_encoded,&length,&mode)==3) {
			url_decode(filename_encoded,filename,sizeof(filename));
			r = do_put_single_file(manager, filename, length, mode);
			reset_idle_timer();
		} else if(sscanf(line, "dir %s", filename_encoded)==1) {
			url_decode(filename_encoded,filename,sizeof(filename));
			r = do_put_dir(manager,filename);
			reset_idle_timer();
		} else if(sscanf(line, "puturl %s %s %" SCNd64 " %o", source_encoded, filename_encoded, &length, &mode)==4) {
			url_decode(filename_encoded,filename,sizeof(filename));
			url_decode(source_encoded,source,sizeof(source));
			r = do_put_url(filename,length,mode,source);
			reset_idle_timer();
		} else if(sscanf(line, "putcmd %s %s %" SCNd64 " %o", source_encoded, filename_encoded, &length, &mode)==4) {
			url_decode(filename_encoded,filename,sizeof(filename));
			url_decode(source_encoded,source,sizeof(source));
			r = do_put_cmd(filename,length,mode,source);
			reset_idle_timer();
		} else if(sscanf(line, "tlq %s", manager_tlq_url) == 1) {
			r = do_tlq_url(manager_tlq_url);
			reset_idle_timer();
		} else if(sscanf(line, "unlink %s", filename_encoded) == 1) {
			url_decode(filename_encoded,filename,sizeof(filename));
			r = do_unlink(filename);
		} else if(sscanf(line, "get %s %d", filename_encoded, &mode) == 2) {
			url_decode(filename_encoded,filename,sizeof(filename));
			r = do_get(manager, filename, mode);
		} else if(sscanf(line, "kill %" SCNd64, &taskid) == 1) {
			if(taskid >= 0) {
				r = do_kill(taskid);
			} else {
				kill_all_tasks();
				r = 1;
			}
		} else if(sscanf(line, "invalidate-file %s", filename_encoded) == 1) {
			url_decode(filename_encoded,filename,sizeof(filename));
			r = do_invalidate_file(filename);
		} else if(!strncmp(line, "release", 8)) {
			r = do_release();
		} else if(!strncmp(line, "exit", 5)) {
			work_queue_broadcast_message(foreman_q, "exit\n");
			abort_flag = 1;
			r = 1;
		} else if(!strncmp(line, "check", 6)) {
			r = send_keepalive(manager, 0);
		} else if(!strncmp(line, "auth", 4)) {
			fprintf(stderr,"work_queue_worker: this manager requires a password. (use the -P option)\n");
			r = 0;
		} else if(sscanf(line, "send_results %d", &n) == 1) {
			report_tasks_complete(manager);
			r = 1;
		} else {
			debug(D_WQ, "Unrecognized manager message: %s.\n", line);
			r = 0;
		}
	} else {
		debug(D_WQ, "Failed to read from manager.\n");
		r = 0;
	}

	return r;
}

/*
Return true if this task can run with the resources currently available.
*/

static int task_resources_fit_now(struct work_queue_task *t)
{
	return
		(cores_allocated  + t->resources_requested->cores  <= local_resources->cores.total) &&
		(memory_allocated + t->resources_requested->memory <= local_resources->memory.total) &&
		(disk_allocated   + t->resources_requested->disk   <= local_resources->disk.total) &&
		(gpus_allocated   + t->resources_requested->gpus   <= local_resources->gpus.total);
}

/*
Return true if this task can eventually run with the resources available. For
example, this is needed for when the worker is launched without the --memory
option, and the free available memory of the system is consumed by some other
process.
*/

static int task_resources_fit_eventually(struct work_queue_task *t)
{
	struct work_queue_resources *r;

	if(worker_mode == WORKER_MODE_FOREMAN) {
		r = total_resources;
	}
	else {
		r = local_resources;
	}

	return
		(t->resources_requested->cores  <= r->cores.largest) &&
		(t->resources_requested->memory <= r->memory.largest) &&
		(t->resources_requested->disk   <= r->disk.largest) &&
		(t->resources_requested->gpus   <= r->gpus.largest);
}

void forsake_waiting_process(struct link *manager, struct work_queue_process *p)
{
	/* the task cannot run in this worker */
	p->task_status = WORK_QUEUE_RESULT_FORSAKEN;
	itable_insert(procs_complete, p->task->taskid, p);

	debug(D_WQ, "Waiting task %d has been forsaken.", p->task->taskid);

	/* we also send updated resources to the manager. */
	send_keepalive(manager, 1);
}

/*
If 0, the worker is using more resources than promised. 1 if resource usage holds that promise.
*/

static int enforce_worker_limits(struct link *manager)
{
	if( manual_disk_option > 0 && local_resources->disk.inuse > manual_disk_option ) {
		fprintf(stderr,"work_queue_worker: %s used more than declared disk space (--disk - < disk used) %"PRIu64" < %"PRIu64" MB\n", workspace, manual_disk_option, local_resources->disk.inuse);

		if(manager) {
			send_manager_message(manager, "info disk_exhausted %lld\n", (long long) local_resources->disk.inuse);
		}

		return 0;
	}

	if(manual_memory_option > 0 && local_resources->memory.inuse > manual_memory_option) {
		fprintf(stderr,"work_queue_worker: used more than declared memory (--memory < memory used) %"PRIu64" < %"PRIu64" MB\n", manual_memory_option, local_resources->memory.inuse);

		if(manager) {
			send_manager_message(manager, "info memory_exhausted %lld\n", (long long) local_resources->memory.inuse);
		}

		return 0;
	}

	return 1;
}

/*
If 0, the worker has less resources than promised. 1 otherwise.
*/

static int enforce_worker_promises(struct link *manager)
{
	if(end_time > 0 && timestamp_get() > ((uint64_t) end_time)) {
		warn(D_NOTICE, "work_queue_worker: reached the wall time limit %"PRIu64" s\n", (uint64_t) manual_wall_time_option);
		if(manager) {
			send_manager_message(manager, "info wall_time_exhausted %"PRIu64"\n", (uint64_t) manual_wall_time_option);
		}
		return 0;
	}

	if( manual_disk_option > 0 && local_resources->disk.total < manual_disk_option) {
		fprintf(stderr,"work_queue_worker: has less than the promised disk space (--disk > disk total) %"PRIu64" < %"PRIu64" MB\n", manual_disk_option, local_resources->disk.total);

		if(manager) {
			send_manager_message(manager, "info disk_error %lld\n", (long long) local_resources->disk.total);
		}

		return 0;
	}

	return 1;
}

static void work_for_manager(struct link *manager)
{
	sigset_t mask;

	debug(D_WQ, "working for manager at %s:%d.\n", current_manager_address->addr, current_manager_address->port);

	sigemptyset(&mask);
	sigaddset(&mask, SIGCHLD);
	sigaddset(&mask, SIGTERM);
	sigaddset(&mask, SIGQUIT);
	sigaddset(&mask, SIGINT);
	sigaddset(&mask, SIGUSR1);
	sigaddset(&mask, SIGUSR2);

	reset_idle_timer();

	time_t volatile_stoptime = time(0) + 60;
	// Start serving managers
	while(!abort_flag) {

		if(time(0) > idle_stoptime) {
			debug(D_NOTICE, "disconnecting from %s:%d because I did not receive any task in %d seconds (--idle-timeout).\n", current_manager_address->addr,current_manager_address->port,idle_timeout);
			send_manager_message(manager, "info idle-disconnecting %lld\n", (long long) idle_timeout);
			break;
		}

		if(worker_volatility && time(0) > volatile_stoptime) {
			if( (double)rand()/(double)RAND_MAX < worker_volatility) {
				debug(D_NOTICE, "work_queue_worker: disconnect from manager due to volatility check.\n");
				break;
			} else {
				volatile_stoptime = time(0) + 60;
			}
		}

		if (initial_ppid != 0 && getppid() != initial_ppid) {
			debug(D_NOTICE, "parent process exited, shutting down\n");
			break;
		}

		/*
		link_usleep will cause the worker to sleep for a time until
		interrupted by a SIGCHILD signal.  However, the signal could
		have been delivered while we were outside of the wait function,
		setting sigchld_received_flag.  In that case, do not block
		but proceed with the

		There is a still a (very small) race condition in that the
		signal could be received between the check and link_usleep,
		hence a maximum wait time of five seconds is enforced.
		*/

		int wait_msec = 5000;

		if(sigchld_received_flag) {
			wait_msec = 0;
			sigchld_received_flag = 0;
		}

		int manager_activity = link_usleep_mask(manager, wait_msec*1000, &mask, 1, 0);
		if(manager_activity < 0) break;

		int ok = 1;
		if(manager_activity) {
			ok &= handle_manager(manager);
		}

		expire_procs_running();

		ok &= handle_completed_tasks(manager);

		measure_worker_resources();

		if(!enforce_worker_promises(manager)) {
			finish_running_tasks(WORK_QUEUE_RESULT_FORSAKEN);
			abort_flag = 1;
			break;
		}

		enforce_processes_max_running_time();

		/* end a running processes if goes above its declared limits.
		 * Mark offending process as RESOURCE_EXHASTION. */
		enforce_processes_limits();

		/* end running processes if worker resources are exhasusted, and marked
		 * them as FORSAKEN, so they can be resubmitted somewhere else. */
		if(!enforce_worker_limits(manager)) {
			finish_running_tasks(WORK_QUEUE_RESULT_FORSAKEN);
			// finish all tasks, disconnect from manager, but don't kill the worker (no abort_flag = 1)
			break;
		}

		int task_event = 0;
		if(ok) {
			struct work_queue_process *p;
			int visited;
			int waiting = list_size(procs_waiting);
			for(visited = 0; visited < waiting; visited++) {
				p = list_pop_head(procs_waiting);
				if(!p) {
					break;
				} else if(task_resources_fit_now(p->task)) {
					if (p->task->coprocess) {
						struct work_queue_coprocess *ready_coprocess = work_queue_coprocess_find_state(coprocess_info, number_of_coprocess_instances, WORK_QUEUE_COPROCESS_READY);
						if (ready_coprocess == NULL) {
							list_push_tail(procs_waiting, p);
							continue;
						}
						p->coprocess = ready_coprocess;
						ready_coprocess->state = WORK_QUEUE_COPROCESS_RUNNING;
					}
					start_process(p,manager);
					task_event++;
				} else if(task_resources_fit_eventually(p->task)) {
					list_push_tail(procs_waiting, p);
				} else {
					forsake_waiting_process(manager, p);
					task_event++;
				}
			}
		}

		if(task_event > 0) {
			send_stats_update(manager);
		}

		if(ok && !results_to_be_sent_msg) {
			if(work_queue_watcher_check(watcher) || itable_size(procs_complete) > 0) {
				send_manager_message(manager, "available_results\n");
				results_to_be_sent_msg = 1;
			}
		}


		if(!ok) {
			break;
		}

		//Reset idle_stoptime if something interesting is happening at this worker.
		if(list_size(procs_waiting) > 0 || itable_size(procs_table) > 0 || itable_size(procs_complete) > 0) {
			reset_idle_timer();
		}
	}
}

static void foreman_for_manager(struct link *manager)
{
	int manager_active = 0;
	if(!manager) {
		return;
	}

	debug(D_WQ, "working for manager at %s:%d as foreman.\n", current_manager_address->addr, current_manager_address->port);

	reset_idle_timer();

	int prev_num_workers = 0;
	while(!abort_flag) {
		int result = 1;
		struct work_queue_task *task = NULL;

		if(time(0) > idle_stoptime && work_queue_empty(foreman_q)) {
			debug(D_NOTICE, "giving up because did not receive any task in %d seconds.\n", idle_timeout);
			send_manager_message(manager, "info idle-disconnecting %lld\n", (long long) idle_timeout);
			break;
		}

		measure_worker_resources();

		/* if the number of workers changed by more than %10, send an status update */
		int curr_num_workers = total_resources->workers.total;
		if(10*abs(curr_num_workers - prev_num_workers) > prev_num_workers) {
			send_keepalive(manager, 0);
		}
		prev_num_workers = curr_num_workers;

		task = work_queue_wait_internal(foreman_q, foreman_internal_timeout, manager, &manager_active, NULL);

		if(task) {
			struct work_queue_process *p;
			p = itable_lookup(procs_table,task->taskid);
			if(!p) fatal("no entry in procs table for taskid %d",task->taskid);
			itable_insert(procs_complete, task->taskid, p);
			result = 1;
		}

		if(!results_to_be_sent_msg && itable_size(procs_complete) > 0)
		{
			send_manager_message(manager, "available_results\n");
			results_to_be_sent_msg = 1;
		}

		if(manager_active) {
			result &= handle_manager(manager);
			reset_idle_timer();
		}

		if(!result) break;
	}
}

/*
workspace_create is done once when the worker starts.
*/

static int workspace_create()
{
	char absolute[WORK_QUEUE_LINE_MAX];

	// Setup working space(dir)
	if(!workspace) {
		const char *workdir = system_tmp_dir(user_specified_workdir);
		workspace = string_format("%s/worker-%d-%d", workdir, (int) getuid(), (int) getpid());
	}

	printf( "work_queue_worker: creating workspace %s\n", workspace);

	if(!create_dir(workspace,0777)) {
		return 0;
	}

	path_absolute(workspace, absolute, 1);
	free(workspace);
	workspace = xxstrdup(absolute);

	return 1;
}

/*
Create a test script and try to execute.
With this we check the scratch directory allows file execution.
*/
static int workspace_check()
{
	int error = 0; /* set 1 on error */
	char *fname = string_format("%s/test.sh", workspace);

	FILE *file = fopen(fname, "w");
	if(!file) {
		warn(D_NOTICE, "Could not write to %s", workspace);
		error = 1;
	} else {
		fprintf(file, "#!/bin/sh\nexit 0\n");
		fclose(file);
		chmod(fname, 0755);

		int exit_status = system(fname);

		if(WIFEXITED(exit_status) && WEXITSTATUS(exit_status) == 126) {
			/* Note that we do not set status=1 on 126, as the executables may live ouside workspace. */
			warn(D_NOTICE, "Could not execute a test script in the workspace directory '%s'.", workspace);
			warn(D_NOTICE, "Is the filesystem mounted as 'noexec'?\n");
			warn(D_NOTICE, "Unless the task command is an absolute path, the task will fail with exit status 126.\n");
		} else if(!WIFEXITED(exit_status) || WEXITSTATUS(exit_status) != 0) {
			error = 1;
		}
	}

	/* do not use trash here; workspace has not been set up yet */
	unlink(fname);
	free(fname);

	if(error) {
		warn(D_NOTICE, "The workspace %s could not be used.\n", workspace);
		warn(D_NOTICE, "Use the --workdir command line switch to change where the workspace is created.\n");
	}

	return !error;
}

/*
workspace_prepare is called every time we connect to a new manager,
*/

static int workspace_prepare()
{
	debug(D_WQ,"preparing workspace %s",workspace);

	char *cachedir = string_format("%s/cache",workspace);
	int result = create_dir(cachedir,0777);
	global_cache = work_queue_cache_create(cachedir);
	free(cachedir);

	char *tmp_name = string_format("%s/cache/tmp", workspace);
	result |= create_dir(tmp_name,0777);
	setenv("WORKER_TMPDIR", tmp_name, 1);	
	free(tmp_name);

	char *trash_dir = string_format("%s/trash", workspace);
	trash_setup(trash_dir);
	free(trash_dir);

	return result;
}

/*
workspace_cleanup is called every time we disconnect from a manager,
to remove any state left over from a previous run.  Remove all
directories (except trash) and move them to the trash directory.
*/

static void workspace_cleanup()
{
	debug(D_WQ,"cleaning workspace %s",workspace);
	DIR *dir = opendir(workspace);
	if(dir) {
		struct dirent *d;
		while((d=readdir(dir))) {
			if(!strcmp(d->d_name,".")) continue;
			if(!strcmp(d->d_name,"..")) continue;
			if(!strcmp(d->d_name,"trash")) continue;
			trash_file(d->d_name);
		}
		closedir(dir);
	}
	trash_empty();

	work_queue_cache_delete(global_cache);
}

/*
workspace_delete is called when the worker is about to exit,
so that all files are removed.
XXX the cleanup of internal data structures doesn't quite belong here.
*/

static void workspace_delete()
{
	if(user_specified_workdir) free(user_specified_workdir);
	if(os_name) free(os_name);
	if(arch_name) free(arch_name);

	if(foreman_q)          work_queue_delete(foreman_q);
	if(procs_running)      itable_delete(procs_running);
	if(procs_table)        itable_delete(procs_table);
	if(procs_complete)     itable_delete(procs_complete);
	if(procs_waiting)      list_delete(procs_waiting);

	if(watcher)            work_queue_watcher_delete(watcher);

	printf( "work_queue_worker: deleting workspace %s\n", workspace);

	/*
	Note that we cannot use trash_file here because the trash dir
	is inside the workspace.  Abort if we really cannot clean up.
	*/

	unlink_recursive(workspace);

	free(workspace);
}

static int serve_manager_by_hostport( const char *host, int port, const char *verify_project, int use_ssl )
{
	if(!domain_name_cache_lookup(host,current_manager_address->addr)) {
		fprintf(stderr,"couldn't resolve hostname %s",host);
		return 0;
	}

	/*
	For the preliminary steps of password and project verification, we use the
	idle timeout, because we have not yet been assigned any work and should
	leave if the manager is not responsive.

	It is tempting to use a short timeout here, but DON'T. The name and
	password messages are ayncronous; if the manager is busy handling other
	workers, a short window is not enough for a response to come back.
	*/

	reset_idle_timer();

	struct link *manager = link_connect(current_manager_address->addr,port,idle_stoptime);

	if(!manager) {
		fprintf(stderr,"couldn't connect to %s:%d: %s\n",current_manager_address->addr,port,strerror(errno));
		return 0;
	}

	if(manual_ssl_option && !use_ssl) {
		fprintf(stderr,"work_queue_worker: --ssl was given, but manager %s:%d is not using ssl.\n",host,port);
		link_close(manager);
		return 0;
	} else if(manual_ssl_option || use_ssl) {
		if(link_ssl_wrap_connect(manager, host) < 1) {
			fprintf(stderr,"work_queue_worker: could not setup ssl connection.\n");
			link_close(manager);
			return 0;
		}
	}

	link_tune(manager,LINK_TUNE_INTERACTIVE);

	char local_addr[LINK_ADDRESS_MAX];
	int  local_port;
	link_address_local(manager, local_addr, &local_port);

	printf("connected to manager %s:%d via local address %s:%d\n", host, port, local_addr, local_port);
	debug(D_WQ, "connected to manager %s:%d via local address %s:%d", host, port, local_addr, local_port);

	if(password) {
		debug(D_WQ,"authenticating to manager");
		if(!link_auth_password(manager,password,idle_stoptime)) {
			fprintf(stderr,"work_queue_worker: wrong password for manager %s:%d\n",host,port);
			link_close(manager);
			return 0;
		}
	}

	if(verify_project) {
		char line[WORK_QUEUE_LINE_MAX];
		debug(D_WQ, "verifying manager's project name");
		send_manager_message(manager, "name\n");
		if(!recv_manager_message(manager,line,sizeof(line),idle_stoptime)) {
			debug(D_WQ,"no response from manager while verifying name");
			link_close(manager);
			return 0;
		}

		if(strcmp(line,verify_project)) {
			fprintf(stderr, "work_queue_worker: manager has project %s instead of %s\n", line, verify_project);
			link_close(manager);
			return 0;
		}
	}

	workspace_prepare();

	measure_worker_resources();

	report_worker_ready(manager);

	if(worker_mode == WORKER_MODE_FOREMAN) {
		foreman_for_manager(manager);
	} else {
		work_for_manager(manager);
	}

	if(abort_signal_received) {
		send_manager_message(manager, "info vacating %d\n", abort_signal_received);
	}

	last_task_received     = 0;
	results_to_be_sent_msg = 0;

	disconnect_manager(manager);
	printf("disconnected from manager %s:%d\n", host, port );

	workspace_cleanup();

	return 1;
}

int serve_manager_by_hostport_list(struct list *manager_addresses, int use_ssl)
{
	int result = 0;

	/* keep trying managers in the list, until all manager addresses
	 * are tried, or a succesful connection was done */
	list_first_item(manager_addresses);
	while((current_manager_address = list_next_item(manager_addresses))) {
		result = serve_manager_by_hostport(current_manager_address->host,current_manager_address->port,/*verify name*/ 0, use_ssl);

		if(result) {
			break;
		}
	}

	return result;
}

static struct list *interfaces_to_list(const char *canonical_host_or_addr, int port, struct jx *host_aliases)
{
	struct list *l = list_create();
	struct jx *host_alias;

	int found_canonical = 0;

	if(host_aliases) {
		for (void *i = NULL; (host_alias = jx_iterate_array(host_aliases, &i));) {
			const char *address = jx_lookup_string(host_alias, "address");

			if(address && strcmp(canonical_host_or_addr, address) == 0) {
				found_canonical = 1;
			}

			// copy ip addr to hostname to work as if the user had entered a particular ip
			// for the manager.
			struct manager_address *m = calloc(1, sizeof(*m));
			strncpy(m->host, address, DOMAIN_NAME_MAX - 1);
			m->port = port;

			list_push_tail(l, m);
		}
	}

	if(host_aliases && !found_canonical) {
		warn(D_NOTICE, "Did not find the manager address '%s' in the list of interfaces.", canonical_host_or_addr);
	}

	if(!found_canonical) {
		/* We get here if no interfaces were defined, or if addr was not found in the interfaces. */

		struct manager_address *m = calloc(1, sizeof(*m));
		strncpy(m->host, canonical_host_or_addr, DOMAIN_NAME_MAX - 1);
		m->port = port;

		list_push_tail(l, m);
	}

	return l;
}

static int serve_manager_by_name( const char *catalog_hosts, const char *project_regex )
{
	struct list *managers_list = work_queue_catalog_query_cached(catalog_hosts,-1,project_regex);

	debug(D_WQ,"project name %s matches %d managers",project_regex,list_size(managers_list));

	if(list_size(managers_list)==0) return 0;

	// shuffle the list by r items to distribute the load across managers
	int r = rand() % list_size(managers_list);
	int i;
	for(i=0;i<r;i++) {
		list_push_tail(managers_list,list_pop_head(managers_list));
	}

	static struct manager_address *last_addr = NULL;

	while(1) {
		struct jx *jx = list_peek_head(managers_list);

		const char *project = jx_lookup_string(jx,"project");
		const char *name = jx_lookup_string(jx,"name");
		const char *addr = jx_lookup_string(jx,"address");
		const char *pref = jx_lookup_string(jx,"manager_preferred_connection");
		struct jx *host_aliases  = jx_lookup(jx,"network_interfaces");
		int port = jx_lookup_integer(jx,"port");
		int use_ssl = jx_lookup_boolean(jx,"ssl");

		// give priority to worker's preferred connection option
		if(preferred_connection) {
			pref = preferred_connection;
		}


		if(last_addr) {
			if(time(0) > idle_stoptime && strcmp(addr, last_addr->host) == 0 && port == last_addr->port) {
				if(list_size(managers_list) < 2) {
					free(last_addr);
					last_addr = NULL;

					/* convert idle_stoptime into connect_stoptime (e.g., time already served). */
					connect_stoptime = idle_stoptime;
					debug(D_WQ,"Previous idle disconnection from only manager available project=%s name=%s addr=%s port=%d",project,name,addr,port);

					return 0;
				} else {
					list_push_tail(managers_list,list_pop_head(managers_list));
					continue;
				}
			}
		}

		int result;

		if(pref && strcmp(pref, "by_hostname") == 0) {
			debug(D_WQ,"selected manager with project=%s hostname=%s addr=%s port=%d",project,name,addr,port);
			manager_addresses = interfaces_to_list(name, port, NULL);
		} else if(pref && strcmp(pref, "by_apparent_ip") == 0) {
			debug(D_WQ,"selected manager with project=%s apparent_addr=%s port=%d",project,addr,port);
			manager_addresses = interfaces_to_list(addr, port, NULL);
		} else {
			debug(D_WQ,"selected manager with project=%s addr=%s port=%d",project,addr,port);
			manager_addresses = interfaces_to_list(addr, port, host_aliases);
		}

		result = serve_manager_by_hostport_list(manager_addresses, use_ssl);

		struct manager_address *m;
		while((m = list_pop_head(manager_addresses))) {
			free(m);
		}
		list_delete(manager_addresses);
		manager_addresses = NULL;

		if(result) {
			free(last_addr);
			last_addr = calloc(1,sizeof(*last_addr));
			strncpy(last_addr->host, addr, DOMAIN_NAME_MAX - 1);
			last_addr->port = port;
		}

		return result;
	}
}

void set_worker_id()
{
	char *salt_and_pepper = string_format("%d%d%d", getpid(), getppid(), rand());
	unsigned char digest[MD5_DIGEST_LENGTH];

	md5_buffer(salt_and_pepper, strlen(salt_and_pepper), digest);
	worker_id = string_format("worker-%s", md5_to_string(digest));

	free(salt_and_pepper);
}

static void handle_abort(int sig)
{
	abort_flag = 1;
	abort_signal_received = sig;
}

static void handle_sigchld(int sig)
{
	sigchld_received_flag = 1;
}

static void read_resources_env_var(const char *name, int64_t *manual_option)
{
	char *value;
	value = getenv(name);
	if(value) {
		*manual_option = atoi(value);
		/* unset variable so that children task cannot read the global value */
		unsetenv(name);
	}
}

static void read_resources_env_vars()
{
	read_resources_env_var("CORES",  &manual_cores_option);
	read_resources_env_var("MEMORY", &manual_memory_option);
	read_resources_env_var("DISK",   &manual_disk_option);
	read_resources_env_var("GPUS",   &manual_gpus_option);
}

struct list *parse_manager_addresses(const char *specs, int default_port)
{
	struct list *managers = list_create();

	char *managers_args = xxstrdup(specs);

	char *next_manager = strtok(managers_args, ";");
	while(next_manager) {
		int port = default_port;

		char *port_str = strchr(next_manager, ':');
		if(port_str) {
			char *no_ipv4 = strchr(port_str+1, ':'); /* if another ':', then this is not ipv4. */
			if(!no_ipv4) {
				*port_str = '\0';
				port = atoi(port_str+1);
			}
		}

		if(port < 1) {
			fatal("Invalid port for manager '%s'", next_manager);
		}

		struct manager_address *m = calloc(1, sizeof(*m));
		strncpy(m->host, next_manager, DOMAIN_NAME_MAX - 1);
		m->port = port;

		if(port_str) {
			*port_str = ':';
		}

		list_push_tail(managers, m);
		next_manager = strtok(NULL, ";");
	}
	free(managers_args);

	return(managers);
}

static void show_help(const char *cmd)
{
	printf( "Use: %s [options] <managerhost> <port> \n"
			"or\n     %s [options] \"managerhost:port[;managerhost:port;managerhost:port;...]\"\n"
			"or\n     %s [options] -M projectname\n",
			cmd, cmd, cmd);
	printf( "where options are:\n");
	printf( " %-30s Show version string\n", "-v,--version");
	printf( " %-30s Show this help screen\n", "-h,--help");
	printf( " %-30s Name of manager (project) to contact.  May be a regular expression.\n", "-M,--manager-name=<name>");
	printf( " %-30s Catalog server to query for managers.  (default: %s:%d) \n", "-C,--catalog=<host:port>",CATALOG_HOST,CATALOG_PORT);
	printf( " %-30s Enable debugging for this subsystem.\n", "-d,--debug=<subsystem>");
	printf( " %-30s Send debugging to this file. (can also be :stderr, or :stdout)\n", "-o,--debug-file=<file>");
	printf( " %-30s Set the maximum size of the debug log (default 10M, 0 disables).\n", "--debug-rotate-max=<bytes>");
	printf( " %-30s Use SSL to connect to the manager. (Not needed if using -M)", "--ssl");
	printf( " %-30s Set worker to run as a foreman.\n", "--foreman");
	printf( " %-30s Run as a foreman, and advertise to the catalog server with <name>.\n", "-f,--foreman-name=<name>");
	printf( " %-30s\n", "--foreman-port=<port>[:<highport>]");
	printf( " %-30s Set the port for the foreman to listen on.  If <highport> is specified\n", "");
	printf( " %-30s the port is chosen from the range port:highport.  Implies --foreman.\n", "");
	printf( " %-30s Select port to listen to at random and write to this file.  Implies --foreman.\n", "-Z,--foreman-port-file=<file>");
	printf( " %-30s Set the fast abort multiplier for foreman (default=disabled).\n", "-F,--fast-abort=<mult>");
	printf( " %-30s Send statistics about foreman to this file.\n", "--specify-log=<logfile>");
	printf( " %-30s Password file for authenticating to the manager.\n", "-P,--password=<pwfile>");
	printf( " %-30s Set both --idle-timeout and --connect-timeout.\n", "-t,--timeout=<time>");
	printf( " %-30s Disconnect after this time if manager sends no work. (default=%ds)\n", "   --idle-timeout=<time>", idle_timeout);
	printf( " %-30s Abort after this time if no managers are available. (default=%ds)\n", "   --connect-timeout=<time>", idle_timeout);
	printf( " %-30s Exit if parent process dies.\n", "--parent-death");
	printf( " %-30s Set TCP window size.\n", "-w,--tcp-window-size=<size>");
	printf( " %-30s Set initial value for backoff interval when worker fails to connect\n", "-i,--min-backoff=<time>");
	printf( " %-30s to a manager. (default=%ds)\n", "", init_backoff_interval);
	printf( " %-30s Set maximum value for backoff interval when worker fails to connect\n", "-b,--max-backoff=<time>");
	printf( " %-30s to a manager. (default=%ds)\n", "", max_backoff_interval);
	printf( " %-30s Set architecture string for the worker to report to manager instead\n", "-A,--arch=<arch>");
	printf( " %-30s of the value in uname (%s).\n", "", arch_name);
	printf( " %-30s Set operating system string for the worker to report to manager instead\n", "-O,--os=<os>");
	printf( " %-30s of the value in uname (%s).\n", "", os_name);
	printf( " %-30s Set the location for creating the working directory of the worker.\n", "-s,--workdir=<path>");
	printf( " %-30s Set the maximum bandwidth the foreman will consume in bytes per second. Example: 100M for 100MBps. (default=unlimited)\n", "--bandwidth=<Bps>");

	printf( " %-30s Set the number of cores reported by this worker. If not given, or less than 1,\n", "--cores=<n>");
	printf( " %-30s then try to detect cores available.\n", "");

	printf( " %-30s Set the number of GPUs reported by this worker. If not given, or less than 0,\n", "--gpus=<n>");
	printf( " %-30s then try to detect gpus available.\n", "");

	printf( " %-30s Manually set the amount of memory (in MB) reported by this worker.\n", "--memory=<mb>");
	printf( " %-30s If not given, or less than 1, then try to detect memory available.\n", "");

	printf( " %-30s Manually set the amount of disk (in MB) reported by this worker.\n", "--disk=<mb>");
	printf( " %-30s If not given, or less than 1, then try to detect disk space available.\n", "");

	printf( " %-30s Use loop devices for task sandboxes (default=disabled, requires root access).\n", "--disk-allocation");
	printf( " %-30s Specifies a user-defined feature the worker provides. May be specified several times.\n", "--feature");
	printf( " %-30s Set the maximum number of seconds the worker may be active. (in s).\n", "--wall-time=<s>");

	printf( " %-30s When using -M, override manager preference to resolve its address.\n", "--connection-mode");
	printf( " %-30s One of by_ip, by_hostname, or by_apparent_ip. Default is set by manager.\n", "");

	printf( " %-30s Forbid the use of symlinks for cache management.\n", "--disable-symlinks");
	printf(" %-30s Single-shot mode -- quit immediately after disconnection.\n", "--single-shot");
	printf( " %-30s Set the percent chance per minute that the worker will shut down (simulates worker failures, for testing only).\n", "--volatility=<chance>");
	printf( " %-30s Set the port used to lookup the worker's TLQ URL (-d and -o options also required).\n", "--tlq=<port>");
	printf( " %-30s Start an arbitrary process when the worker starts up and kill the process when the worker shuts down.\n", "--coprocess <executable>");
	printf( " %-30s Specify the number of coprocesses for serverless functions that the worker should maintain. Default is consuming all worker resources to allocate 1 coprocess per core.\n", "--coprocesses-total=<number>");
}

enum {LONG_OPT_DEBUG_FILESIZE = 256, LONG_OPT_VOLATILITY, LONG_OPT_BANDWIDTH,
	  LONG_OPT_DEBUG_RELEASE, LONG_OPT_SPECIFY_LOG, LONG_OPT_CORES, LONG_OPT_MEMORY,
	  LONG_OPT_DISK, LONG_OPT_GPUS, LONG_OPT_FOREMAN, LONG_OPT_FOREMAN_PORT, LONG_OPT_DISABLE_SYMLINKS,
	  LONG_OPT_IDLE_TIMEOUT, LONG_OPT_CONNECT_TIMEOUT,
	  LONG_OPT_SINGLE_SHOT, LONG_OPT_WALL_TIME, LONG_OPT_DISK_ALLOCATION,
	  LONG_OPT_MEMORY_THRESHOLD, LONG_OPT_FEATURE, LONG_OPT_TLQ, LONG_OPT_PARENT_DEATH, LONG_OPT_CONN_MODE,
	  LONG_OPT_USE_SSL, LONG_OPT_PYTHON_FUNCTION, LONG_OPT_FROM_FACTORY, LONG_OPT_COPROCESS,
	  LONG_OPT_NUM_COPROCESS, LONG_OPT_COPROCESS_CORES,
	  LONG_OPT_COPROCESS_MEMORY, LONG_OPT_COPROCESS_DISK, LONG_OPT_COPROCESS_GPUS};

static const struct option long_options[] = {
	{"advertise",           no_argument,        0,  'a'},
	{"catalog",             required_argument,  0,  'C'},
	{"debug",               required_argument,  0,  'd'},
	{"debug-file",          required_argument,  0,  'o'},
	{"debug-rotate-max",    required_argument,  0,  LONG_OPT_DEBUG_FILESIZE},
	{"disk-allocation",     no_argument,  		0,  LONG_OPT_DISK_ALLOCATION},
	{"foreman",             no_argument,        0,  LONG_OPT_FOREMAN},
	{"foreman-port",        required_argument,  0,  LONG_OPT_FOREMAN_PORT},
	{"foreman-port-file",   required_argument,  0,  'Z'},
	{"foreman-name",        required_argument,  0,  'f'},
	{"measure-capacity",    no_argument,        0,  'c'},
	{"fast-abort",          required_argument,  0,  'F'},
	{"specify-log",         required_argument,  0,  LONG_OPT_SPECIFY_LOG},
	{"manager-name",        required_argument,  0,  'M'},
	{"master-name",         required_argument,  0,  'M'},
	{"password",            required_argument,  0,  'P'},
	{"timeout",             required_argument,  0,  't'},
	{"idle-timeout",        required_argument,  0,  LONG_OPT_IDLE_TIMEOUT},
	{"connect-timeout",     required_argument,  0,  LONG_OPT_CONNECT_TIMEOUT},
	{"tcp-window-size",     required_argument,  0,  'w'},
	{"min-backoff",         required_argument,  0,  'i'},
	{"max-backoff",         required_argument,  0,  'b'},
	{"single-shot",		    no_argument,        0,  LONG_OPT_SINGLE_SHOT },
	{"disable-symlinks",    no_argument,        0,  LONG_OPT_DISABLE_SYMLINKS},
	{"disk-threshold",      required_argument,  0,  'z'},
	{"memory-threshold",    required_argument,  0,  LONG_OPT_MEMORY_THRESHOLD},
	{"arch",                required_argument,  0,  'A'},
	{"os",                  required_argument,  0,  'O'},
	{"workdir",             required_argument,  0,  's'},
	{"volatility",          required_argument,  0,  LONG_OPT_VOLATILITY},
	{"bandwidth",           required_argument,  0,  LONG_OPT_BANDWIDTH},
	{"cores",               required_argument,  0,  LONG_OPT_CORES},
	{"memory",              required_argument,  0,  LONG_OPT_MEMORY},
	{"disk",                required_argument,  0,  LONG_OPT_DISK},
	{"gpus",                required_argument,  0,  LONG_OPT_GPUS},
	{"wall-time",           required_argument,  0,  LONG_OPT_WALL_TIME},
	{"help",                no_argument,        0,  'h'},
	{"version",             no_argument,        0,  'v'},
	{"feature",             required_argument,  0,  LONG_OPT_FEATURE},
	{"tlq",					required_argument,	0,  LONG_OPT_TLQ},
	{"parent-death",        no_argument,        0,  LONG_OPT_PARENT_DEATH},
	{"connection-mode",     required_argument,  0,  LONG_OPT_CONN_MODE},
	{"ssl",                 no_argument,        0,  LONG_OPT_USE_SSL},
	{"coprocess",           required_argument,  0,  LONG_OPT_COPROCESS},
	{"coprocesses-total",   required_argument,  0,  LONG_OPT_NUM_COPROCESS},
	{"coprocess-cores",     required_argument,  0,  LONG_OPT_COPROCESS_CORES},
	{"coprocess-memory",    required_argument,  0,  LONG_OPT_COPROCESS_MEMORY},
	{"coprocess-disk",      required_argument,  0,  LONG_OPT_COPROCESS_DISK},
	{"coprocess-gpus",      required_argument,  0,  LONG_OPT_COPROCESS_GPUS},
	{"from-factory",        required_argument,  0,  LONG_OPT_FROM_FACTORY},
	{0,0,0,0}
};

int main(int argc, char *argv[])
{
	int c;
	int w;
	int foreman_port = -1;
	char * foreman_name = NULL;
	char * port_file = NULL;
	struct utsname uname_data;
	int enable_capacity = 1; // enabled by default
	double fast_abort_multiplier = 0;
	char *foreman_stats_filename = NULL;

	catalog_hosts = CATALOG_HOST;

	features = hash_table_create(4, 0);

	random_init();

	worker_start_time = timestamp_get();

	set_worker_id();

	//obtain the architecture and os on which worker is running.
	uname(&uname_data);
	os_name = xxstrdup(uname_data.sysname);
	arch_name = xxstrdup(uname_data.machine);
	worker_mode = WORKER_MODE_WORKER;

	debug_config(argv[0]);
	read_resources_env_vars();

	while((c = getopt_long(argc, argv, "acC:d:f:F:t:o:p:M:N:P:w:i:b:z:A:O:s:vZ:h", long_options, 0)) != -1) {
		switch (c) {
		case 'a':
			//Left here for backwards compatibility
			break;
		case 'C':
			catalog_hosts = xxstrdup(optarg);
			break;
		case 'd':
			debug_flags_set(optarg);
			break;
		case LONG_OPT_DEBUG_FILESIZE:
			debug_config_file_size(MAX(0, string_metric_parse(optarg)));
			break;
		case 'f':
			worker_mode = WORKER_MODE_FOREMAN;
			foreman_name = xxstrdup(optarg);
			break;
		case LONG_OPT_FOREMAN_PORT:
		{	char *low_port = optarg;
			char *high_port= strchr(optarg, ':');

			worker_mode = WORKER_MODE_FOREMAN;

			if(high_port) {
				*high_port = '\0';
				high_port++;
			} else {
				foreman_port = atoi(low_port);
				break;
			}
			setenv("WORK_QUEUE_LOW_PORT", low_port, 0);
			setenv("WORK_QUEUE_HIGH_PORT", high_port, 0);
			foreman_port = -1;
			break;
		}
		case 'c':
			// This option is deprecated. Capacity estimation is now on by default for the foreman.
			enable_capacity = 1;
			break;
		case 'F':
			fast_abort_multiplier = atof(optarg);
			break;
		case LONG_OPT_SPECIFY_LOG:
			foreman_stats_filename = xxstrdup(optarg);
			break;
		case 't':
			connect_timeout = idle_timeout = string_time_parse(optarg);
			break;
		case LONG_OPT_IDLE_TIMEOUT:
			idle_timeout = string_time_parse(optarg);
			break;
		case LONG_OPT_CONNECT_TIMEOUT:
			connect_timeout = string_time_parse(optarg);
			break;
		case 'o':
			debug_path = xxstrdup(optarg);
			debug_config_file(optarg);
			break;
		case LONG_OPT_FOREMAN:
			worker_mode = WORKER_MODE_FOREMAN;
			break;
		case 'M':
		case 'N':
			project_regex = optarg;
			break;
		case 'p':
			// ignore for backwards compatibility
			break;
		case 'w':
			w = string_metric_parse(optarg);
			link_window_set(w, w);
			break;
		case 'i':
			init_backoff_interval = string_metric_parse(optarg);
			break;
		case 'b':
			max_backoff_interval = string_metric_parse(optarg);
			if (max_backoff_interval < init_backoff_interval) {
				fprintf(stderr, "Maximum backoff interval provided must be greater than the initial backoff interval of %ds.\n", init_backoff_interval);
				exit(1);
			}
			break;
		case 'z':
			/* deprecated */
			break;
		case LONG_OPT_MEMORY_THRESHOLD:
			/* deprecated */
			break;
		case 'A':
			free(arch_name); //free the arch string obtained from uname
			arch_name = xxstrdup(optarg);
			break;
		case 'O':
			free(os_name); //free the os string obtained from uname
			os_name = xxstrdup(optarg);
			break;
		case 's':
		{
			char temp_abs_path[PATH_MAX];
			path_absolute(optarg, temp_abs_path, 1);
			user_specified_workdir = xxstrdup(temp_abs_path);
			break;
		}
		case 'v':
			cctools_version_print(stdout, argv[0]);
			exit(EXIT_SUCCESS);
			break;
		case 'P':
			if(copy_file_to_buffer(optarg, &password, NULL) < 0) {
				fprintf(stderr,"work_queue_worker: couldn't load password from %s: %s\n",optarg,strerror(errno));
				exit(EXIT_FAILURE);
			}
			break;
		case 'Z':
			port_file = xxstrdup(optarg);
			worker_mode = WORKER_MODE_FOREMAN;
			break;
		case LONG_OPT_VOLATILITY:
			worker_volatility = atof(optarg);
			break;
		case LONG_OPT_BANDWIDTH:
			setenv("WORK_QUEUE_BANDWIDTH", optarg, 1);
			break;
		case LONG_OPT_DEBUG_RELEASE:
			setenv("WORK_QUEUE_RESET_DEBUG_FILE", "yes", 1);
			break;
		case LONG_OPT_CORES:
			if(!strncmp(optarg, "all", 3)) {
				manual_cores_option = 0;
			} else {
				manual_cores_option = atoi(optarg);
			}
			break;
		case LONG_OPT_MEMORY:
			if(!strncmp(optarg, "all", 3)) {
				manual_memory_option = 0;
			} else {
				manual_memory_option = atoll(optarg);
			}
			break;
		case LONG_OPT_DISK:
			if(!strncmp(optarg, "all", 3)) {
				manual_disk_option = 0;
			} else {
				manual_disk_option = atoll(optarg);
			}
			break;
		case LONG_OPT_GPUS:
			if(!strncmp(optarg, "all", 3)) {
				manual_gpus_option = -1;
			} else {
				manual_gpus_option = atoi(optarg);
			}
			break;
		case LONG_OPT_WALL_TIME:
			manual_wall_time_option = atoi(optarg);
			if(manual_wall_time_option < 1) {
				manual_wall_time_option = 0;
				warn(D_NOTICE, "Ignoring --wall-time, a positive integer is expected.");
			}
			break;
		case LONG_OPT_DISABLE_SYMLINKS:
			symlinks_enabled = 0;
			break;
		case LONG_OPT_SINGLE_SHOT:
			single_shot_mode = 1;
			break;
		case 'h':
			show_help(argv[0]);
			return 0;
		case LONG_OPT_DISK_ALLOCATION:
		{
			char *abs_path_preloader = string_format("%s/lib/libforce_halt_enospc.so", INSTALL_PATH);
			int preload_result;
			char *curr_ld_preload = getenv("LD_PRELOAD");
			if(curr_ld_preload && abs_path_preloader) {
				char *new_ld_preload = string_format("%s:%s", curr_ld_preload, abs_path_preloader);
				preload_result = setenv("LD_PRELOAD", new_ld_preload, 1);
				free(new_ld_preload);
			}
			else if(abs_path_preloader) {
				preload_result = setenv("LD_PRELOAD", abs_path_preloader, 1);
			}
			else {
				preload_result = 1;
			}
			free(abs_path_preloader);
			if(preload_result) {
				timestamp_t preload_fail_time = timestamp_get();
				debug(D_WQ|D_NOTICE, "i/o dynamic library linking via LD_PRELOAD for loop device failed at: %"PRId64"", preload_fail_time);
			}
			disk_allocation = 1;
			break;
		}
		case LONG_OPT_FEATURE:
			hash_table_insert(features, optarg, (void **) 1);
			break;
		case LONG_OPT_TLQ:
			tlq_port = atoi(optarg);
			break;
		case LONG_OPT_PARENT_DEATH:
			initial_ppid = getppid();
			break;
		case LONG_OPT_CONN_MODE:
			free(preferred_connection);
			preferred_connection = xxstrdup(optarg);
			if(strcmp(preferred_connection, "by_ip") && strcmp(preferred_connection, "by_hostname") && strcmp(preferred_connection, "by_apparent_ip")) {
				fatal("connection-mode should be one of: by_ip, by_hostname, by_apparent_ip");
			}
			break;
		case LONG_OPT_USE_SSL:
			manual_ssl_option=1;
			break;
		case LONG_OPT_COPROCESS:
			// if no / in filepath, call which on the executable name to find its path
			// if we can't find it, we call path_absolute to check if its in local directory
			if (strchr(optarg, '/') == NULL) {
				coprocess_command = path_which(optarg);
				// found
				if (coprocess_command != NULL) {
					break;
				}
			}
			coprocess_command = calloc(PATH_MAX, sizeof(char));
			path_absolute(optarg, coprocess_command, 1);
			realloc(coprocess_command, strlen(coprocess_command)+1);
			break;
		case LONG_OPT_NUM_COPROCESS:
			number_of_coprocess_instances = atoi(optarg);
			break;
		case LONG_OPT_COPROCESS_CORES:
			coprocess_cores = atoi(optarg);
			break;
		case LONG_OPT_COPROCESS_MEMORY:
			coprocess_memory = atoi(optarg);
			break;
		case LONG_OPT_COPROCESS_DISK:
			coprocess_disk = atoi(optarg);
			break;
		case LONG_OPT_COPROCESS_GPUS:
			coprocess_gpus = atoi(optarg);
			break;
		case LONG_OPT_FROM_FACTORY:
			if (factory_name) free(factory_name);
			factory_name = xxstrdup(optarg);
			break;
		default:
			show_help(argv[0]);
			return 1;
		}
	}

	cctools_version_debug(D_DEBUG, argv[0]);

	// for backwards compatibility with the old syntax for specifying a worker's project name
	if(worker_mode != WORKER_MODE_FOREMAN && foreman_name) {
		if(foreman_name) {
			project_regex = foreman_name;
		}
	}

	//checks that the foreman has a unique name from the manager
	if(worker_mode == WORKER_MODE_FOREMAN && foreman_name){
		if(project_regex && strcmp(foreman_name,project_regex) == 0) {
			fatal("Foreman (%s) and Master (%s) share a name. Ensure that these are unique.\n",foreman_name,project_regex);
		}
	}

	if(!project_regex) {
		if((argc - optind) < 1 || (argc - optind) > 2) {
			show_help(argv[0]);
			exit(1);
		}

		int default_manager_port = (argc - optind) == 2 ? atoi(argv[optind+1]) : 0;
		manager_addresses = parse_manager_addresses(argv[optind], default_manager_port);

		if(list_size(manager_addresses) < 1) {
			show_help(argv[0]);
			fatal("No manager has been specified");
		}
	}

	char *gpu_name = gpu_name_get();
	if(gpu_name) {
		hash_table_insert(features, gpu_name, (void **) 1);
		free(gpu_name);
	}

	signal(SIGTERM, handle_abort);
	signal(SIGQUIT, handle_abort);
	signal(SIGINT, handle_abort);
	//Also do cleanup on SIGUSR1 & SIGUSR2 to allow using -notify and -l s_rt= options if submitting 
	//this worker process with UGE qsub. Otherwise task processes are left running when UGE
	//terminates this process with SIGKILL.
	signal(SIGUSR1, handle_abort);
	signal(SIGUSR2, handle_abort);
	signal(SIGCHLD, handle_sigchld);

	if(!workspace_create()) {
		fprintf(stderr, "work_queue_worker: failed to setup workspace at %s.\n", workspace);
		exit(1);
	}

	if(!workspace_check()) {
		return 1;
	}

	// set $WORK_QUEUE_SANDBOX to workspace.
	debug(D_WQ, "WORK_QUEUE_SANDBOX set to %s.\n", workspace);
	setenv("WORK_QUEUE_SANDBOX", workspace, 0);

	//get absolute pathnames of port and log file.
	char temp_abs_path[PATH_MAX];
	if(port_file)
	{
		path_absolute(port_file, temp_abs_path, 0);
		free(port_file);
		port_file = xxstrdup(temp_abs_path);
	}
	if(foreman_stats_filename)
	{
		path_absolute(foreman_stats_filename, temp_abs_path, 0);
		free(foreman_stats_filename);
		foreman_stats_filename = xxstrdup(temp_abs_path);
	}

	// change to workspace
	chdir(workspace);

	if(worker_mode == WORKER_MODE_FOREMAN) {
		char foreman_string[WORK_QUEUE_LINE_MAX];

		free(os_name); //free the os string obtained from uname
		os_name = xxstrdup("foreman");

		string_nformat(foreman_string, sizeof(foreman_string), "%s-foreman", argv[0]);
		debug_config(foreman_string);
		foreman_q = work_queue_create(foreman_port);

		if(!foreman_q) {
			fprintf(stderr, "work_queue_worker-foreman: failed to create foreman queue.  Terminating.\n");
			exit(1);
		}

		printf( "work_queue_worker-foreman: listening on port %d\n", work_queue_port(foreman_q));

		if(port_file)
		{	opts_write_port_file(port_file, work_queue_port(foreman_q));	}

		if(foreman_name) {
			work_queue_specify_name(foreman_q, foreman_name);
			work_queue_specify_manager_mode(foreman_q, WORK_QUEUE_MANAGER_MODE_CATALOG);
		}

		if(password) {
			work_queue_specify_password(foreman_q,password);
		}

		work_queue_specify_estimate_capacity_on(foreman_q, enable_capacity);
		work_queue_activate_fast_abort(foreman_q, fast_abort_multiplier);
		work_queue_specify_category_mode(foreman_q, NULL, WORK_QUEUE_ALLOCATION_MODE_FIXED);

		if(foreman_stats_filename) {
			work_queue_specify_log(foreman_q, foreman_stats_filename);
		}
	}

	procs_running  = itable_create(0);
	procs_table    = itable_create(0);
	procs_waiting  = list_create();
	procs_complete = itable_create(0);

	watcher = work_queue_watcher_create();

	local_resources = work_queue_resources_create();
	total_resources = work_queue_resources_create();
	total_resources_last = work_queue_resources_create();

	if(manual_cores_option < 1) {
		manual_cores_option = load_average_get_cpus();
	}

	int backoff_interval = init_backoff_interval;
	connect_stoptime = time(0) + connect_timeout;

	measure_worker_resources();
	printf("work_queue_worker: using %"PRId64 " cores, %"PRId64 " MB memory, %"PRId64 " MB disk, %"PRId64 " gpus\n",
		total_resources->cores.total,
		total_resources->memory.total,
		total_resources->disk.total,
		total_resources->gpus.total);

	if(coprocess_command) {
		// if the user did not specify the number of instances, or they specified 0, automatically allocate 1 coprocess per core.
		if (number_of_coprocess_instances == 0) {
			number_of_coprocess_instances = total_resources->cores.total;
		}
		else {
			// if manual resource allocation, issue warning messages if the user overallocates worker resources
			if ( (coprocess_cores * number_of_coprocess_instances) > total_resources->cores.total ) {
				debug(D_WQ|D_NOTICE, "Warning: cores allocated to coprocesses is greater than cores allocated to worker\n");
			}
			else if  ((coprocess_memory * number_of_coprocess_instances) > total_resources->memory.total ) {
				debug(D_WQ|D_NOTICE, "Warning: memory allocated to coprocesses is greater than cores allocated to worker\n");
			}
			else if  ((coprocess_disk * number_of_coprocess_instances) > total_resources->disk.total ) {
				debug(D_WQ|D_NOTICE, "Warning: disk allocated to coprocesses is greater than cores allocated to worker\n");
			}
			else if  ((coprocess_gpus * number_of_coprocess_instances) > total_resources->gpus.total ) {
				debug(D_WQ|D_NOTICE, "Warning: gpus allocated to coprocesses is greater than cores allocated to worker\n");
			}
		}
		coprocess_resources = work_queue_resources_create();
		coprocess_info = work_queue_coprocess_initialize_all_coprocesses(coprocess_cores, coprocess_memory, coprocess_disk, coprocess_gpus, total_resources, coprocess_resources, coprocess_command, number_of_coprocess_instances);
		coprocess_name = xxstrdup(coprocess_info[0].name);
		hash_table_insert(features, coprocess_name, (void **) 1);
	}

	while(1) {
		int result = 0;

		if (initial_ppid != 0 && getppid() != initial_ppid) {
			debug(D_NOTICE, "parent process exited, shutting down\n");
			break;
		}

		measure_worker_resources();
		if(!enforce_worker_promises(NULL)) {
			abort_flag = 1;
			break;
		}

		if(project_regex) {
			result = serve_manager_by_name(catalog_hosts, project_regex);
		} else {
			result = serve_manager_by_hostport_list(manager_addresses, /* use ssl only if --ssl */ manual_ssl_option);
		}

		/*
		If the last attempt was a succesful connection, then reset the backoff_interval,
		and the connect timeout, then try again if a project name was given.
		If the connect attempt failed, then slow down the retries.
		*/

		if(result) {
			if(single_shot_mode) {
				debug(D_DEBUG,"stopping: single shot mode");
				break;
			}
			backoff_interval = init_backoff_interval;
			connect_stoptime = time(0) + connect_timeout;

			if(!project_regex && (time(0)>idle_stoptime)) {
				debug(D_NOTICE,"stopping: no other managers available");
				break;
			}
		} else {
			backoff_interval = MIN(backoff_interval*2,max_backoff_interval);
		}

		if(abort_flag) {
			debug(D_NOTICE,"stopping: abort signal received");
			break;
		}

		if(time(0)>connect_stoptime) {
			debug(D_NOTICE,"stopping: could not connect after %d seconds.",connect_timeout);
			break;
		}

		sleep(backoff_interval);
	}

	if (coprocess_command && number_of_coprocess_instances > 0) {
		work_queue_coprocess_shutdown_all_coprocesses(coprocess_info, coprocess_resources, number_of_coprocess_instances);
		free(coprocess_command);
		free(coprocess_name);
	}

	workspace_delete();

	return 0;
}

/* vim: set noexpandtab tabstop=4: */
