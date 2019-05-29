/*
Copyright (C) 2008- The University of Notre Dame
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

#include "cctools.h"
#include "macros.h"
#include "catalog_query.h"
#include "domain_name_cache.h"
#include "jx.h"
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
#include "getopt.h"
#include "getopt_aux.h"
#include "create_dir.h"
#include "delete_dir.h"
#include "itable.h"
#include "random.h"
#include "url_encode.h"
#include "md5.h"
#include "disk_alloc.h"
#include "hash_table.h"
#include "pattern.h"
#include "gpu_info.h"

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

#ifdef CCTOOLS_WITH_MPI
#include <mpi.h>
#include "hash_table.h"
#include "jx_parse.h"
#endif

typedef enum {
	WORKER_MODE_WORKER,
	WORKER_MODE_FOREMAN
} worker_mode_t;

typedef enum {
	CONTAINER_MODE_NONE,
	CONTAINER_MODE_DOCKER,
	CONTAINER_MODE_DOCKER_PRESERVE,
	CONTAINER_MODE_UMBRELLA
} container_mode_t;

#define DOCKER_WORK_DIR "/home/worker"

// In single shot mode, immediately quit when disconnected.
// Useful for accelerating the test suite.
static int single_shot_mode = 0;

// Maximum time to stay connected to a single master without any work.
static int idle_timeout = 900;

// Current time at which we will give up if no work is received.
static time_t idle_stoptime = 0;

// Current time at which we will give up if no master is found.
static time_t connect_stoptime = 0;

// Maximum time to attempt connecting to all available masters before giving up.
static int connect_timeout = 900;

// Maximum time to attempt sending/receiving any given file or message.
static const int active_timeout = 3600;

// Maximum time for the foreman to spend waiting in its internal loop
static const int foreman_internal_timeout = 5;

// Initial value for backoff interval (in seconds) when worker fails to connect to a master.
static int init_backoff_interval = 1;

// Maximum value for backoff interval (in seconds) when worker fails to connect to a master.
static int max_backoff_interval = 60;

// Chance that a worker will decide to shut down each minute without warning, to simulate failure.
static double worker_volatility = 0.0;

// If flag is set, then the worker proceeds to immediately cleanup and shut down.
// This can be set by Ctrl-C or by any condition that prevents further progress.
static int abort_flag = 0;

// Record the signal received, to inform the master if appropiate.
static int abort_signal_received = 0;

// Flag used to indicate a child must be waited for.
static int sigchld_received_flag = 0;

// Threshold for available memory, and disk space (MB) beyond which clean up and quit.
static int64_t disk_avail_threshold = 100;
static int64_t memory_avail_threshold = 100;

// Password shared between master and worker.
char *password = 0;

// Allow worker to use symlinks when link() fails.  Enabled by default.
static int symlinks_enabled = 1;

// Worker id. A unique id for this worker instance.
static char *worker_id;

static worker_mode_t worker_mode = WORKER_MODE_WORKER;

static container_mode_t container_mode = CONTAINER_MODE_NONE;
static int load_from_tar = 0;

struct master_address {
	char host[DOMAIN_NAME_MAX];
	int port;
	char addr[DOMAIN_NAME_MAX];
};
struct list *master_addresses;
struct master_address *current_master_address;

static char *workspace;
static char *os_name = NULL;
static char *arch_name = NULL;
static char *user_specified_workdir = NULL;
static time_t worker_start_time = 0;

static struct work_queue_watcher * watcher = 0;

static struct work_queue_resources * local_resources = 0;
static struct work_queue_resources * total_resources = 0;
static struct work_queue_resources * total_resources_last = 0;

static int64_t last_task_received  = 0;
static int64_t manual_cores_option = 0;
static int64_t manual_disk_option = 0;
static int64_t manual_memory_option = 0;
static int64_t manual_gpus_option = 0;
static time_t  manual_wall_time_option = 0;

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

// docker image name
static char *img_name       = NULL;
static char *container_name = NULL;
static char *tar_fn         = NULL;

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
static int released_by_master = 0;

__attribute__ (( format(printf,2,3) ))
static void send_master_message( struct link *master, const char *fmt, ... )
{
	char debug_msg[2*WORK_QUEUE_LINE_MAX];
	va_list va;
	va_list debug_va;

	va_start(va,fmt);

	string_nformat(debug_msg, sizeof(debug_msg), "tx to master: %s", fmt);
	va_copy(debug_va, va);

	vdebug(D_WQ, debug_msg, debug_va);
	link_putvfstring(master, fmt, time(0)+active_timeout, va);

	va_end(va);
}

static int recv_master_message( struct link *master, char *line, int length, time_t stoptime )
{
	int result = link_readline(master,line,length,stoptime);
	if(result) debug(D_WQ,"rx from master: %s",line);
	return result;
}

/*
We track how much time has elapsed since the master assigned a task.
If time(0) > idle_stoptime, then the worker will disconnect.
*/

void reset_idle_timer()
{
	idle_stoptime = time(0) + idle_timeout;
}

/*
   Measure the disk used by the worker. We only manually measure the cache directory, as processes measure themselves.
   */

int64_t measure_worker_disk() {
	static struct path_disk_size_info *state = NULL;

	path_disk_size_info_get_r("./cache", max_time_on_measurement, &state);

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

void measure_worker_resources()
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
		if(manual_memory_option)
			r->memory.total = manual_memory_option;
		if(manual_gpus_option)
			r->gpus.total = manual_gpus_option;
	}

	if(manual_disk_option)
		r->disk.total = MIN(r->disk.total, manual_disk_option);

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

	last_resources_measurement = time(0);
}

/*
Send a message to the master with user defined features.
*/
static void send_features(struct link *master) {
	char *f;
	void *dummy;
	hash_table_firstkey(features);

	char fenc[WORK_QUEUE_LINE_MAX];
	while(hash_table_nextkey(features, &f, &dummy)) {
		url_encode(f, fenc, WORK_QUEUE_LINE_MAX);
		send_master_message(master, "feature %s\n", fenc);
	}
}


/*
Send a message to the master with my current resources.
*/

static void send_resource_update(struct link *master)
{
	time_t stoptime = time(0) + active_timeout;

	if(worker_mode == WORKER_MODE_FOREMAN) {
		total_resources->disk.total = local_resources->disk.total - disk_avail_threshold;
		total_resources->disk.inuse = local_resources->disk.inuse;
	} else {
		total_resources->memory.total    = MAX(0, local_resources->memory.total    - memory_avail_threshold);
		total_resources->memory.largest  = MAX(0, local_resources->memory.largest  - memory_avail_threshold);
		total_resources->memory.smallest = MAX(0, local_resources->memory.smallest - memory_avail_threshold);

		total_resources->disk.total    = MAX(0, local_resources->disk.total    - disk_avail_threshold);
		total_resources->disk.largest  = MAX(0, local_resources->disk.largest  - disk_avail_threshold);
		total_resources->disk.smallest = MAX(0, local_resources->disk.smallest - disk_avail_threshold);
	}

	work_queue_resources_send(master,total_resources,stoptime);
	send_master_message(master, "info end_of_resource_update %d\n", 0);
}

/*
Send a message to the master with my current statistics information.
*/

static void send_stats_update(struct link *master)
{
	if(worker_mode == WORKER_MODE_FOREMAN) {
		struct work_queue_stats s;
		work_queue_get_stats_hierarchy(foreman_q, &s);

		send_master_message(master, "info workers_joined %lld\n", (long long) s.workers_joined);
		send_master_message(master, "info workers_removed %lld\n", (long long) s.workers_removed);
		send_master_message(master, "info workers_released %lld\n", (long long) s.workers_released);
		send_master_message(master, "info workers_idled_out %lld\n", (long long) s.workers_idled_out);
		send_master_message(master, "info workers_fast_aborted %lld\n", (long long) s.workers_fast_aborted);
		send_master_message(master, "info workers_blacklisted %lld\n", (long long) s.workers_blacklisted);
		send_master_message(master, "info workers_lost %lld\n", (long long) s.workers_lost);

		send_master_message(master, "info tasks_waiting %lld\n", (long long) s.tasks_waiting);
		send_master_message(master, "info tasks_on_workers %lld\n", (long long) s.tasks_on_workers);
		send_master_message(master, "info tasks_running %lld\n", (long long) s.tasks_running);
		send_master_message(master, "info tasks_waiting %lld\n", (long long) list_size(procs_waiting));
		send_master_message(master, "info tasks_with_results %lld\n", (long long) s.tasks_with_results);

		send_master_message(master, "info time_send %lld\n", (long long) s.time_send);
		send_master_message(master, "info time_receive %lld\n", (long long) s.time_receive);
		send_master_message(master, "info time_send_good %lld\n", (long long) s.time_send_good);
		send_master_message(master, "info time_receive_good %lld\n", (long long) s.time_receive_good);

		send_master_message(master, "info time_workers_execute %lld\n", (long long) s.time_workers_execute);
		send_master_message(master, "info time_workers_execute_good %lld\n", (long long) s.time_workers_execute_good);
		send_master_message(master, "info time_workers_execute_exhaustion %lld\n", (long long) s.time_workers_execute_exhaustion);

		send_master_message(master, "info bytes_sent %lld\n", (long long) s.bytes_sent);
		send_master_message(master, "info bytes_received %lld\n", (long long) s.bytes_received);
	}
	else {
		send_master_message(master, "info tasks_running %lld\n", (long long) itable_size(procs_running));
	}
}

static int send_keepalive(struct link *master, int force_resources){

	send_master_message(master, "alive\n");

	/* for regular workers we only send resources on special ocassions, thus
	 * the force_resources. */
	if(force_resources || worker_mode == WORKER_MODE_FOREMAN) {
		send_resource_update(master);
	}

	send_stats_update(master);

	return 1;
}

/*
Send the initial "ready" message to the master with the version and so forth.
The master will not start sending tasks until this message is recevied.
*/

static void report_worker_ready( struct link *master )
{
	char hostname[DOMAIN_NAME_MAX];
	domain_name_cache_guess(hostname);
	send_master_message(master,"workqueue %d %s %s %s %d.%d.%d\n",WORK_QUEUE_PROTOCOL_VERSION,hostname,os_name,arch_name,CCTOOLS_VERSION_MAJOR,CCTOOLS_VERSION_MINOR,CCTOOLS_VERSION_MICRO);
	send_master_message(master, "info worker-id %s\n", worker_id);
	send_features(master);
	send_keepalive(master, 1);
}


const char *skip_dotslash( const char *s )
{
	while(!strncmp(s,"./",2)) s+=2;
	return s;
}

/*
Link a file from one place to another.
If a hard link doesn't work, use a symbolic link.
If it is a directory, do it recursively.
*/

int link_recursive( const char *source, const char *target )
{
	struct stat info;

	if(stat(source,&info)<0) return 0;

	if(S_ISDIR(info.st_mode)) {
		DIR *dir = opendir(source);
		if(!dir) return 0;

		mkdir(target, 0777);

		struct dirent *d;
		int result = 1;

		while((d = readdir(dir))) {
			if(!strcmp(d->d_name,".")) continue;
			if(!strcmp(d->d_name,"..")) continue;

			char *subsource = string_format("%s/%s",source,d->d_name);
			char *subtarget = string_format("%s/%s",target,d->d_name);

			result = link_recursive(subsource,subtarget);

			free(subsource);
			free(subtarget);

			if(!result) break;
		}
		closedir(dir);

		return result;
	} else {
		if(link(source, target)==0) return 1;

		/*
		If the hard link failed, perhaps because the source
		was a directory, or if hard links are not supported
		in that file system, fall back to a symlink.
		*/

		if(symlinks_enabled) {

			/*
			Use an absolute path when symlinking, otherwise the link will
			be accidentally relative to the current directory.
			*/

			char *cwd = path_getcwd();
			char *absolute_source = string_format("%s/%s", cwd, source);

			int result = symlink(absolute_source, target);

			free(absolute_source);
			free(cwd);

			if(result==0) return 1;
		}

		return 0;
	}
}

/*
Start executing the given process on the local host,
accounting for the resources as necessary.
*/

static int start_process( struct work_queue_process *p )
{

	pid_t pid;

	if (container_mode == CONTAINER_MODE_DOCKER)
		pid = work_queue_process_execute(p, container_mode, img_name);
	else if (container_mode == CONTAINER_MODE_DOCKER_PRESERVE)
		pid = work_queue_process_execute(p, container_mode, container_name);
	else
		pid = work_queue_process_execute(p, container_mode);

	if(pid<0) fatal("unable to fork process for taskid %d!",p->task->taskid);

	itable_insert(procs_running,pid,p);

	struct work_queue_task *t = p->task;

	cores_allocated += t->resources_requested->cores;
	memory_allocated += t->resources_requested->memory;
	disk_allocated += t->resources_requested->disk;
	gpus_allocated += t->resources_requested->gpus;

	return 1;
}

/*
Transmit the results of the given process to the master.
If a local worker, stream the output from disk.
If a foreman, send the outputs contained in the task structure.
*/

static void report_task_complete( struct link *master, struct work_queue_process *p )
{
	int64_t output_length;
	struct stat st;

	if(worker_mode==WORKER_MODE_WORKER) {
		fstat(p->output_fd, &st);
		output_length = st.st_size;
		lseek(p->output_fd, 0, SEEK_SET);
		send_master_message(master, "result %d %d %lld %llu %d\n", p->task_status, p->exit_status, (long long) output_length, (unsigned long long) p->execution_end-p->execution_start, p->task->taskid);
		link_stream_from_fd(master, p->output_fd, output_length, time(0)+active_timeout);

		total_task_execution_time += (p->execution_end - p->execution_start);
		total_tasks_executed++;
	} else {
		struct work_queue_task *t = p->task;
		if(t->output) {
			output_length = strlen(t->output);
		} else {
			output_length = 0;
		}
		send_master_message(master, "result %d %d %lld %llu %d\n", t->result, t->return_status, (long long) output_length, (unsigned long long) t->time_workers_execute_last, t->taskid);
		if(output_length) {
			link_putlstring(master, t->output, output_length, time(0)+active_timeout);
		}

		total_task_execution_time += t->time_workers_execute_last;
		total_tasks_executed++;
	}

	send_stats_update(master);
}

/*
Remove one item from an itable, ignoring the key
*/

static void * itable_pop(struct itable *t )
{
	uint64_t key;
	void *value;

	itable_firstkey(t);
	if(itable_nextkey(t, &key, (void*)&value)) {
		return itable_remove(t,key);
	} else {
		return 0;
	}
}

/*
For every unreported complete task and watched file,
send the results to the master.
*/

static void report_tasks_complete( struct link *master )
{
	struct work_queue_process *p;

	while((p=itable_pop(procs_complete))) {
		report_task_complete(master,p);
	}

	work_queue_watcher_send_changes(watcher,master,time(0)+active_timeout);

	send_master_message(master, "end\n");

	results_to_be_sent_msg = 0;
}

static void expire_procs_running() {
	struct work_queue_process *p;
	uint64_t pid;

	timestamp_t current_time = timestamp_get();

	itable_firstkey(procs_running);
	while(itable_nextkey(procs_running, (uint64_t*)&pid, (void**)&p)) {
		if(p->task->resources_requested->end > 0 && current_time > (uint64_t) p->task->resources_requested->end)
		{
			p->task_status = WORK_QUEUE_RESULT_TASK_TIMEOUT;
			kill(pid, SIGKILL);
		}
	}
}

/*
Scan over all of the processes known by the worker,
and if they have exited, move them into the procs_complete table
for later processing.
*/

static int handle_tasks(struct link *master)
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
				FILE *loop_full_check;
				char *buf = malloc(PATH_MAX);
				char *pwd = getcwd(buf, PATH_MAX);
				char *disk_alloc_filename = work_queue_generate_disk_alloc_full_filename(pwd, p->task->taskid);
				if(p->loop_mount == 1 && (loop_full_check = fopen(disk_alloc_filename, "r"))) {
					p->task_status = WORK_QUEUE_RESULT_DISK_ALLOC_FULL;
					p->task->disk_allocation_exhausted = 1;
					fclose(loop_full_check);
					unlink(disk_alloc_filename);
				}

				free(buf);
				free(disk_alloc_filename);

				debug(D_WQ, "task %d (pid %d) exited normally with exit code %d",p->task->taskid,p->pid,p->exit_status);
			}

			p->execution_end = timestamp_get();

			cores_allocated  -= p->task->resources_requested->cores;
			memory_allocated -= p->task->resources_requested->memory;
			disk_allocated   -= p->task->resources_requested->disk;
			gpus_allocated   -= p->task->resources_requested->gpus;

			itable_remove(procs_running, p->pid);
			itable_firstkey(procs_running);

			// Output files must be moved back into the cache directory.

			struct work_queue_file *f;
			list_first_item(p->task->output_files);
			while((f = list_next_item(p->task->output_files))) {

				char *sandbox_name = string_format("%s/%s",p->sandbox,f->remote_name);

				debug(D_WQ,"moving output file from %s to %s",sandbox_name,f->payload);

				/* First we try a cheap rename. It that does not work, we try to copy the file. */
				if(rename(sandbox_name,f->payload) == -1) {
					debug(D_WQ, "could not rename output file %s to %s: %s",sandbox_name,f->payload,strerror(errno));
					if(copy_file_to_file(sandbox_name, f->payload)  == -1) {
						debug(D_WQ, "could not copy output file %s to %s: %s",sandbox_name,f->payload,strerror(errno));
					}
				}

				free(sandbox_name);
			}

			itable_insert(procs_complete, p->task->taskid, p);

		}

	}
	return 1;
}

/**
 * Stream file/directory contents for the rget protocol.
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
static int stream_output_item(struct link *master, const char *filename, int recursive)
{
	DIR *dir;
	struct dirent *dent;
	char dentline[WORK_QUEUE_LINE_MAX];
	char cached_filename[WORK_QUEUE_LINE_MAX];
	struct stat info;
	int64_t actual, length;
	int fd;

	string_nformat(cached_filename, sizeof(cached_filename), "cache/%s", filename);

	if(stat(cached_filename, &info) != 0) {
		goto failure;
	}

	if(S_ISDIR(info.st_mode)) {
		// stream a directory
		dir = opendir(cached_filename);
		if(!dir) {
			goto failure;
		}
		send_master_message(master, "dir %s 0\n", filename);

		while(recursive && (dent = readdir(dir))) {
			if(!strcmp(dent->d_name, ".") || !strcmp(dent->d_name, ".."))
				continue;
			string_nformat(dentline, sizeof(dentline), "%s/%s", filename, dent->d_name);
			stream_output_item(master, dentline, recursive);
		}

		closedir(dir);
	} else {
		// stream a file
		fd = open(cached_filename, O_RDONLY, 0);
		if(fd >= 0) {
			length = info.st_size;
			send_master_message(master, "file %s %"PRId64"\n", filename, length);
			actual = link_stream_from_fd(master, fd, length, time(0) + active_timeout);
			close(fd);
			if(actual != length) {
				debug(D_WQ, "Sending back output file - %s failed: bytes to send = %"PRId64" and bytes actually sent = %"PRId64".", filename, length, actual);
				return 0;
			}
		} else {
			goto failure;
		}
	}

	return 1;

failure:
	send_master_message(master, "missing %s %d\n", filename, errno);
	return 0;
}

/*
For each of the files and directories needed by a task, link
them into the sandbox.  Return true if successful.
*/

int setup_sandbox( struct work_queue_process *p )
{
	struct work_queue_file *f;

	list_first_item(p->task->input_files);
	while((f = list_next_item(p->task->input_files))) {

		char *sandbox_name = string_format("%s/%s",skip_dotslash(p->sandbox),f->remote_name);
		int result = 0;

		// remote name may contain relative path components, so create them in advance
		create_dir_parents(sandbox_name,0777);

		if(f->type == WORK_QUEUE_DIRECTORY) {
			debug(D_WQ,"creating directory %s",sandbox_name);
			result = create_dir(sandbox_name, 0700);
			if(!result) debug(D_WQ,"couldn't create directory %s: %s", sandbox_name, strerror(errno));
		} else {
			debug(D_WQ,"linking %s to %s",f->payload,sandbox_name);
			result = link_recursive(skip_dotslash(f->payload),skip_dotslash(sandbox_name));
			if(!result) {
				if(errno==EEXIST) {
					// XXX silently ignore the case where the target file exists.
					// This happens when masters apps map the same input file twice, or to the same name.
					// Would be better to reject this at the master instead.
					result = 1;
				} else {
					debug(D_WQ,"couldn't link %s into sandbox as %s: %s",f->payload,sandbox_name,strerror(errno));
				}
			}
		}

		free(sandbox_name);
		if(!result) return 0;
	}

	return 1;
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
Handle an incoming task message from the master.
Generate a work_queue_process wrapped around a work_queue_task,
and deposit it into the waiting list or the foreman_q as appropriate.
*/

static int do_task( struct link *master, int taskid, time_t stoptime )
{
	char line[WORK_QUEUE_LINE_MAX];
	char filename[WORK_QUEUE_LINE_MAX];
	char localname[WORK_QUEUE_LINE_MAX];
	char taskname[WORK_QUEUE_LINE_MAX];
	char taskname_encoded[WORK_QUEUE_LINE_MAX];
	char category[WORK_QUEUE_LINE_MAX];
	int flags, length;
	int64_t n;
	int disk_alloc = disk_allocation;

	timestamp_t nt;

	struct work_queue_task *task = work_queue_task_create(0);
	task->taskid = taskid;

	while(recv_master_message(master,line,sizeof(line),stoptime)) {
		if(!strcmp(line,"end")) {
			break;
		} else if(sscanf(line, "category %s",category)) {
			work_queue_task_specify_category(task, category);
		} else if(sscanf(line,"cmd %d",&length)==1) {
			char *cmd = malloc(length+1);
			link_read(master,cmd,length,stoptime);
			cmd[length] = 0;
			work_queue_task_specify_command(task,cmd);
			debug(D_WQ,"rx from master: %s",cmd);
			free(cmd);
		} else if(sscanf(line,"infile %s %s %d", filename, taskname_encoded, &flags)) {
			string_nformat(localname, sizeof(localname), "cache/%s", filename);
			url_decode(taskname_encoded, taskname, WORK_QUEUE_LINE_MAX);
			work_queue_task_specify_file(task, localname, taskname, WORK_QUEUE_INPUT, flags);
		} else if(sscanf(line,"outfile %s %s %d", filename, taskname_encoded, &flags)) {
			string_nformat(localname, sizeof(localname), "cache/%s", filename);
			url_decode(taskname_encoded, taskname, WORK_QUEUE_LINE_MAX);
			work_queue_task_specify_file(task, localname, taskname, WORK_QUEUE_OUTPUT, flags);
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
			work_queue_task_specify_running_time(task, nt);
		} else if(sscanf(line,"end_time %" PRIu64,&nt)) {
			work_queue_task_specify_end_time(task, nt);
		} else if(sscanf(line,"env %d",&length)==1) {
			char *env = malloc(length+2); /* +2 for \n and \0 */
			link_read(master, env, length+1, stoptime);
			env[length] = 0;              /* replace \n with \0 */
			char *value = strchr(env,'=');
			if(value) {
				*value = 0;
				value++;
				work_queue_task_specify_enviroment_variable(task,env,value);
			}
			free(env);
		} else {
			debug(D_WQ|D_NOTICE,"invalid command from master: %s",line);
			return 0;
		}
	}

	last_task_received = task->taskid;

	struct work_queue_process *p = work_queue_process_create(task, disk_alloc);

	if(!p) {
		return 0;
	}

	// Every received task goes into procs_table.
	itable_insert(procs_table,taskid,p);

	if(worker_mode==WORKER_MODE_FOREMAN) {
		work_queue_submit_internal(foreman_q,task);
	} else {
		// XXX sandbox setup should be done in task execution,
		// so that it can be returned cleanly as a failure to execute.
		if(!setup_sandbox(p)) {
			itable_remove(procs_table,taskid);
			work_queue_process_delete(p);
			return 0;
		}
		normalize_resources(p);
		list_push_tail(procs_waiting,p);
	}

	work_queue_watcher_add_process(watcher,p);

	return 1;
}

/*
Handle an incoming "put" message from the master,
which places a file into the cache directory.
*/

static int do_put( struct link *master, char *filename, int64_t length, int mode )
{
	char cached_filename[WORK_QUEUE_LINE_MAX];
	char *cur_pos;

	debug(D_WQ, "Putting file %s into workspace\n", filename);
	if(!check_disk_space_for_filesize(".", length, disk_avail_threshold)) {
		debug(D_WQ, "Could not put file %s, not enough disk space (%"PRId64" bytes needed)\n", filename, length);
		return 0;
	}


	mode = mode | 0600;

	cur_pos = filename;

	while(!strncmp(cur_pos, "./", 2)) {
		cur_pos += 2;
	}

	string_nformat(cached_filename, sizeof(cached_filename), "cache/%s", cur_pos);

	cur_pos = strrchr(cached_filename, '/');
	if(cur_pos) {
		*cur_pos = '\0';
		if(!create_dir(cached_filename, mode | 0700)) {
			debug(D_WQ, "Could not create directory - %s (%s)\n", cached_filename, strerror(errno));
			return 0;
		}
		*cur_pos = '/';
	}

	int fd = open(cached_filename, O_WRONLY | O_CREAT | O_TRUNC, mode);
	if(fd < 0) {
		debug(D_WQ, "Could not open %s for writing. (%s)\n", filename, strerror(errno));
		return 0;
	}

	int64_t actual = link_stream_to_fd(master, fd, length, time(0) + active_timeout);
	close(fd);
	if(actual != length) {
		debug(D_WQ, "Failed to put file - %s (%s)\n", filename, strerror(errno));
		return 0;
	}

	return 1;
}

static int file_from_url(const char *url, const char *filename) {

		debug(D_WQ, "Retrieving %s from (%s)\n", filename, url);
		char command[WORK_QUEUE_LINE_MAX];
		string_nformat(command, sizeof(command), "curl -f -o \"%s\" \"%s\"", filename, url);

	if (system(command) == 0) {
				debug(D_WQ, "Success, file retrieved from %s\n", url);
		} else {
				debug(D_WQ, "Failed to retrieve file from %s\n", url);
				return 0;
		}

		return 1;
}

static int do_url(struct link* master, const char *filename, int length, int mode) {

		char url[WORK_QUEUE_LINE_MAX];
		link_read(master, url, length, time(0) + active_timeout);

		char cache_name[WORK_QUEUE_LINE_MAX];
		string_nformat(cache_name, sizeof(cache_name), "cache/%s", filename);

		return file_from_url(url, cache_name);
}

static int do_unlink(const char *path) {
	char cached_path[WORK_QUEUE_LINE_MAX];
	string_nformat(cached_path, sizeof(cached_path), "cache/%s", path);
	//Use delete_dir() since it calls unlink() if path is a file.
	if(delete_dir(cached_path) != 0) {
		struct stat buf;
		if(stat(cached_path, &buf) != 0) {
			if(errno == ENOENT) {
				// If the path does not exist, return success
				return 1;
			}
		}
		// Failed to do unlink
		return 0;
	}
	return 1;
}

static int do_get(struct link *master, const char *filename, int recursive) {
	stream_output_item(master, filename, recursive);
	send_master_message(master, "end\n");
	return 1;
}

static int do_thirdget(int mode, char *filename, const char *path) {
	char cmd[WORK_QUEUE_LINE_MAX];
	char cached_filename[WORK_QUEUE_LINE_MAX];
	char *cur_pos;

	if(mode != WORK_QUEUE_FS_CMD) {
		struct stat info;
		if(stat(path, &info) != 0) {
			debug(D_WQ, "Path %s not accessible. (%s)\n", path, strerror(errno));
			return 0;
		}
		if(!strcmp(filename, path)) {
			debug(D_WQ, "thirdget aborted: filename (%s) and path (%s) are the same\n", filename, path);
			return 1;
		}
	}

	cur_pos = filename;

	while(!strncmp(cur_pos, "./", 2)) {
		cur_pos += 2;
	}

	string_nformat(cached_filename, sizeof(cached_filename), "cache/%s", cur_pos);

	cur_pos = strrchr(cached_filename, '/');
	if(cur_pos) {
		*cur_pos = '\0';
		if(!create_dir(cached_filename, mode | 0700)) {
			debug(D_WQ, "Could not create directory - %s (%s)\n", cached_filename, strerror(errno));
			return 0;
		}
		*cur_pos = '/';
	}

	switch (mode) {
	case WORK_QUEUE_FS_SYMLINK:
		if(symlink(path, cached_filename) != 0) {
			debug(D_WQ, "Could not thirdget %s, symlink (%s) failed. (%s)\n", filename, path, strerror(errno));
			return 0;
		}
		/* falls through */
	case WORK_QUEUE_FS_PATH:
		string_nformat(cmd, sizeof(cmd), "/bin/cp %s %s", path, cached_filename);
		if(system(cmd) != 0) {
			debug(D_WQ, "Could not thirdget %s, copy (%s) failed. (%s)\n", filename, path, strerror(errno));
			return 0;
		}
		break;
	case WORK_QUEUE_FS_CMD:
		string_nformat(cmd, sizeof(cmd), "%s > %s", path, cached_filename);
		if(system(cmd) != 0) {
			debug(D_WQ, "Could not thirdget %s, command (%s) failed. (%s)\n", filename, cmd, strerror(errno));
			return 0;
		}
		break;
	}
	return 1;
}

static int do_thirdput(struct link *master, int mode, char *filename, const char *path) {
	struct stat info;
	char cmd[WORK_QUEUE_LINE_MAX];
	char cached_filename[WORK_QUEUE_LINE_MAX];
	char *cur_pos;
	int result = 1;

	cur_pos = filename;

	while(!strncmp(cur_pos, "./", 2)) {
		cur_pos += 2;
	}

	string_nformat(cached_filename, sizeof(cached_filename), "cache/%s", cur_pos);


	if(stat(cached_filename, &info) != 0) {
		debug(D_WQ, "File %s not accessible. (%s)\n", cached_filename, strerror(errno));
		result = 0;
	}


	switch (mode) {
	case WORK_QUEUE_FS_SYMLINK:
	case WORK_QUEUE_FS_PATH:
		if(!strcmp(filename, path)) {
			debug(D_WQ, "thirdput aborted: filename (%s) and path (%s) are the same\n", filename, path);
			result = 1;
		}
		cur_pos = strrchr(path, '/');
		if(cur_pos) {
			*cur_pos = '\0';
			if(!create_dir(path, mode | 0700)) {
				debug(D_WQ, "Could not create directory - %s (%s)\n", path, strerror(errno));
				result = 0;
				*cur_pos = '/';
				break;
			}
			*cur_pos = '/';
		}
		string_nformat(cmd, sizeof(cmd), "/bin/cp -r %s %s", cached_filename, path);
		if(system(cmd) != 0) {
			debug(D_WQ, "Could not thirdput %s, copy (%s) failed. (%s)\n", cached_filename, path, strerror(errno));
			result = 0;
		}
		break;
	case WORK_QUEUE_FS_CMD:
		string_nformat(cmd, sizeof(cmd), "%s < %s", path, cached_filename);
		if(system(cmd) != 0) {
			debug(D_WQ, "Could not thirdput %s, command (%s) failed. (%s)\n", filename, cmd, strerror(errno));
			result = 0;
		}
		break;
	}

	send_master_message(master, "thirdput-complete %d\n", result);

	return result;

}

/*
do_kill removes a process currently known by the worker.
Note that a kill message from the master is used for every case
where a task is to be removed, whether it is waiting, running,
of finished.  Regardless of the state, we kill the process and
remove all of the associated files and other state.
*/

static int do_kill(int taskid)
{
	struct work_queue_process *p;

	p = itable_remove(procs_table, taskid);
	if(!p) {
		debug(D_WQ,"master requested kill of task %d which does not exist!",taskid);
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

static void kill_all_tasks() {
	struct work_queue_process *p;
	uint64_t taskid;

	itable_firstkey(procs_table);
	while(itable_nextkey(procs_table,&taskid,(void**)&p)) {
		do_kill(taskid);
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

/* Remove a file, even when mark as cached. Foreman broadcast this message to
 * foremen down its hierarchy. It is invalid for a worker to receice this message. */
static int do_invalidate_file(const char *filename) {

	if(worker_mode == WORKER_MODE_FOREMAN) {
		work_queue_invalidate_cached_file_internal(foreman_q, filename);
		return 1;
	}

	return -1;
}

static void finish_running_task(struct work_queue_process *p, work_queue_result_t result) {
	p->task_status |= result;
	kill(p->pid, SIGKILL);
}

static void finish_running_tasks(work_queue_result_t result) {
	struct work_queue_process *p;
	pid_t pid;

	itable_firstkey(procs_running);
	while(itable_nextkey(procs_running, (uint64_t*) &pid, (void**)&p)) {
		finish_running_task(p, result);
	}
}

static int enforce_process_limits(struct work_queue_process *p) {
	/* If the task did not specify disk usage, return right away. */
	if(p->disk < 1)
		return 1;

	work_queue_process_measure_disk(p, max_time_on_measurement);
	if(p->sandbox_size > p->task->resources_requested->disk) {
		debug(D_WQ,"Task %d went over its disk size limit: %" PRId64 " MB > %" PRIu64 " MB\n", p->task->taskid, p->sandbox_size, p->task->resources_requested->disk);
		return 0;
	}

	return 1;
}

static int enforce_processes_limits() {
	static time_t last_check_time = 0;

	struct work_queue_process *p;
	pid_t pid;

	int ok = 1;

	/* Do not check too often, as it is expensive (particularly disk) */
	if((time(0) - last_check_time) < check_resources_interval ) return 1;

	itable_firstkey(procs_table);
	while(itable_nextkey(procs_table,(uint64_t*)&pid,(void**)&p)) {
		if(!enforce_process_limits(p)) {
			finish_running_task(p, WORK_QUEUE_RESULT_RESOURCE_EXHAUSTION);

			/* we delete the sandbox, to free the exhausted resource. If a loop device is used, use remove loop device*/
			if(p->loop_mount == 1) {
				disk_alloc_delete(p->sandbox);
			}
			else {
				delete_dir(p->sandbox);
			}

			ok = 0;
		}
	}

	last_check_time = time(0);

	return ok;
}

/* We check maximum_running_time by itself (not in enforce_processes_limits),
 * as other running tasks should not be affected by a task timeout. */
static void enforce_processes_max_running_time() {
	struct work_queue_process *p;
	pid_t pid;

	timestamp_t now = timestamp_get();

	itable_firstkey(procs_running);
	while(itable_nextkey(procs_running, (uint64_t*) &pid, (void**) &p)) {
		/* If the task did not specify wall_time, return right away. */
		if(p->task->resources_requested->wall_time < 1)
			continue;

		if(now < p->execution_start + p->task->resources_requested->wall_time) {
			debug(D_WQ,"Task %d went over its running time limit: %" PRId64 " us > %" PRIu64 " us\n", p->task->taskid, now - p->execution_start, p->task->resources_requested->wall_time);
			p->task_status = WORK_QUEUE_RESULT_TASK_MAX_RUN_TIME;
			kill(pid, SIGKILL);
		}
	}

	return;
}


static int do_release() {
	debug(D_WQ, "released by master %s:%d.\n", current_master_address->addr, current_master_address->port);
	released_by_master = 1;
	return 0;
}

static void disconnect_master(struct link *master) {

	debug(D_WQ, "disconnecting from master %s:%d", current_master_address->addr, current_master_address->port);
	link_close(master);

	debug(D_WQ, "killing all outstanding tasks");
	kill_all_tasks();

	//KNOWN HACK: We remove all workers on a master disconnection to avoid
	//returning old tasks to a new master.
	if(foreman_q) {
		debug(D_WQ, "Disconnecting all workers...\n");
		release_all_workers(foreman_q);

		if(project_regex) {
			update_catalog(foreman_q, master, 1);
		}
	}

	if(released_by_master) {
		released_by_master = 0;
	} else if(abort_flag) {
		// Bail out quickly
	} else {
		sleep(5);
	}
}

static int handle_master(struct link *master) {
	char line[WORK_QUEUE_LINE_MAX];
	char filename[WORK_QUEUE_LINE_MAX];
	char path[WORK_QUEUE_LINE_MAX];
	int64_t length;
	int64_t taskid = 0;
	int mode, r, n;

	if(recv_master_message(master, line, sizeof(line), idle_stoptime )) {
		if(sscanf(line,"task %" SCNd64, &taskid)==1) {
			r = do_task(master, taskid,time(0)+active_timeout);
		} else if(string_prefix_is(line, "put ")) {
			char *f = NULL, *l = NULL, *m = NULL, *g = NULL;
			if(pattern_match(line, "^put (.+) (%d+) ([0-7]+) (%d+)$", &f, &l, &m, &g) >= 0) {
				strncpy(filename, f, WORK_QUEUE_LINE_MAX); free(f);
				length = strtoll(l, 0, 10); free(l);
				mode   = strtol(m, NULL, 8); free(m);
				free(g); //flags are not used anymore.
				
				if(path_within_dir(filename, workspace)) {
					r = do_put(master, filename, length, mode);
					reset_idle_timer();
				} else {
					debug(D_WQ, "Path - %s is not within workspace %s.", filename, workspace);
					r = 0;
				}
			} else {
				debug(D_WQ, "Malformed put message.");
				r = 0;
			}
		} else if(sscanf(line, "url %s %" SCNd64 " %o", filename, &length, &mode) == 3) {
			r = do_url(master, filename, length, mode);
			reset_idle_timer();
		} else if(sscanf(line, "unlink %s", filename) == 1) {
			if(path_within_dir(filename, workspace)) {
				r = do_unlink(filename);
			} else {
				debug(D_WQ, "Path - %s is not within workspace %s.", filename, workspace);
				r= 0;
			}
		} else if(sscanf(line, "get %s %d", filename, &mode) == 2) {
			r = do_get(master, filename, mode);
		} else if(sscanf(line, "thirdget %o %s %[^\n]", &mode, filename, path) == 3) {
			r = do_thirdget(mode, filename, path);
		} else if(sscanf(line, "thirdput %o %s %[^\n]", &mode, filename, path) == 3) {
			r = do_thirdput(master, mode, filename, path);
			reset_idle_timer();
		} else if(sscanf(line, "kill %" SCNd64, &taskid) == 1) {
			if(taskid >= 0) {
				r = do_kill(taskid);
			} else {
				kill_all_tasks();
				r = 1;
			}
		} else if(sscanf(line, "invalidate-file %s", filename) == 1) {
			r = do_invalidate_file(filename);
		} else if(!strncmp(line, "release", 8)) {
			r = do_release();
		} else if(!strncmp(line, "exit", 5)) {
			work_queue_broadcast_message(foreman_q, "exit\n");
			abort_flag = 1;
			r = 1;
		} else if(!strncmp(line, "check", 6)) {
			r = send_keepalive(master, 0);
		} else if(!strncmp(line, "auth", 4)) {
			fprintf(stderr,"work_queue_worker: this master requires a password. (use the -P option)\n");
			r = 0;
		} else if(sscanf(line, "send_results %d", &n) == 1) {
			report_tasks_complete(master);
			r = 1;
		} else {
			debug(D_WQ, "Unrecognized master message: %s.\n", line);
			r = 0;
		}
	} else {
		debug(D_WQ, "Failed to read from master.\n");
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

void forsake_waiting_process(struct link *master, struct work_queue_process *p) {

	/* the task cannot run in this worker */
	p->task_status = WORK_QUEUE_RESULT_FORSAKEN;
	itable_insert(procs_complete, p->task->taskid, p);

	debug(D_WQ, "Waiting task %d has been forsaken.", p->task->taskid);

	/* we also send updated resources to the master. */
	send_keepalive(master, 1);
}

/*
If 0, the worker is using more resources than promised. 1 if resource usage holds that promise.
*/
static int enforce_worker_limits(struct link *master) {
	if( manual_wall_time_option > 0 && (time(0) - worker_start_time) > manual_wall_time_option) {
		fprintf(stderr,"work_queue_worker: reached the wall time limit %"PRIu64" s\n", (uint64_t)manual_wall_time_option);
		if(master) {
			send_master_message(master, "info wall_time_exhausted %"PRIu64"\n", (uint64_t)manual_wall_time_option);
		}
		return 0;
	}

	if( manual_disk_option > 0 && local_resources->disk.inuse > (manual_disk_option - disk_avail_threshold/2) ) {
		fprintf(stderr,"work_queue_worker: %s used more than declared disk space (--disk - --disk-threshold < disk used) %"PRIu64" - %"PRIu64 " < %"PRIu64" MB\n", workspace, manual_disk_option, disk_avail_threshold, local_resources->disk.inuse);

		if(master) {
			send_master_message(master, "info disk_exhausted %lld\n", (long long) local_resources->disk.inuse);
		}

		return 0;
	}

	if( manual_memory_option > 0 && local_resources->memory.inuse > (manual_memory_option - memory_avail_threshold/2) ) {
		fprintf(stderr,"work_queue_worker: used more than declared memory (--memory - --memory-threshold < memory used) %"PRIu64" - %"PRIu64 " < %"PRIu64" MB\n", manual_memory_option, memory_avail_threshold, local_resources->memory.inuse);

		if(master) {
			send_master_message(master, "info memory_exhausted %lld\n", (long long) local_resources->memory.inuse);
		}

		return 0;
	}

	return 1;
}

/*
If 0, the worker has less resources than promised. 1 otherwise.
*/
static int enforce_worker_promises(struct link *master) {

	if( manual_disk_option > 0 && local_resources->disk.total < manual_disk_option) {
		fprintf(stderr,"work_queue_worker: has less than the promised disk space (--disk > disk total) %"PRIu64" < %"PRIu64" MB\n", manual_disk_option, local_resources->disk.total);

		if(master) {
			send_master_message(master, "info disk_error %lld\n", (long long) local_resources->disk.total);
		}

		return 0;
	}

	return 1;
}

static void work_for_master(struct link *master) {
	sigset_t mask;

	debug(D_WQ, "working for master at %s:%d.\n", current_master_address->addr, current_master_address->port);

	sigemptyset(&mask);
	sigaddset(&mask, SIGCHLD);
	sigaddset(&mask, SIGTERM);
	sigaddset(&mask, SIGQUIT);
	sigaddset(&mask, SIGINT);
	sigaddset(&mask, SIGUSR1);
	sigaddset(&mask, SIGUSR2);

	reset_idle_timer();

	time_t volatile_stoptime = time(0) + 60;
	// Start serving masters
	while(!abort_flag) {

		if(time(0) > idle_stoptime) {
			debug(D_NOTICE, "disconnecting from %s:%d because I did not receive any task in %d seconds (--idle-timeout).\n", current_master_address->addr,current_master_address->port,idle_timeout);
			send_master_message(master, "info idle-disconnecting %lld\n", (long long) idle_timeout);
			break;
		}

		if(worker_volatility && time(0) > volatile_stoptime) {
			if( (double)rand()/(double)RAND_MAX < worker_volatility) {
				debug(D_NOTICE, "work_queue_worker: disconnect from master due to volatility check.\n");
				break;
			} else {
				volatile_stoptime = time(0) + 60;
			}
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

		int master_activity = link_usleep_mask(master, wait_msec*1000, &mask, 1, 0);
		if(master_activity < 0) break;

		int ok = 1;
		if(master_activity) {
			ok &= handle_master(master);
		}

		expire_procs_running();

		ok &= handle_tasks(master);

		measure_worker_resources();

		if(!enforce_worker_promises(master)) {
			abort_flag = 1;
			break;
		}

		enforce_processes_max_running_time();

		/* end a running processes if goes above its declared limits.
		 * Mark offending process as RESOURCE_EXHASTION. */
		enforce_processes_limits();

		/* end running processes if worker resources are exhasusted, and marked
		 * them as FORSAKEN, so they can be resubmitted somewhere else. */
		if(!enforce_worker_limits(master)) {
			finish_running_tasks(WORK_QUEUE_RESULT_FORSAKEN);
			// finish all tasks, disconnect from master, but don't kill the worker (no abort_flag = 1)
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
					start_process(p);
					task_event++;
				} else if(task_resources_fit_eventually(p->task)) {
					list_push_tail(procs_waiting, p);
				} else {
					forsake_waiting_process(master, p);
					task_event++;
				}
			}
		}

		if(task_event > 0) {
			send_stats_update(master);
		}

		if(ok && !results_to_be_sent_msg) {
			if(work_queue_watcher_check(watcher) || itable_size(procs_complete) > 0) {
				send_master_message(master, "available_results\n");
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

static void foreman_for_master(struct link *master) {
	int master_active = 0;
	if(!master) {
		return;
	}

	debug(D_WQ, "working for master at %s:%d as foreman.\n", current_master_address->addr, current_master_address->port);

	reset_idle_timer();

	int prev_num_workers = 0;
	while(!abort_flag) {
		int result = 1;
		struct work_queue_task *task = NULL;

		if(time(0) > idle_stoptime && work_queue_empty(foreman_q)) {
			debug(D_NOTICE, "giving up because did not receive any task in %d seconds.\n", idle_timeout);
			send_master_message(master, "info idle-disconnecting %lld\n", (long long) idle_timeout);
			break;
		}

		measure_worker_resources();

		/* if the number of workers changed by more than %10, send an status update */
		int curr_num_workers = total_resources->workers.total;
		if(10*abs(curr_num_workers - prev_num_workers) > prev_num_workers) {
			send_keepalive(master, 0);
		}
		prev_num_workers = curr_num_workers;

		task = work_queue_wait_internal(foreman_q, foreman_internal_timeout, master, &master_active);

		if(task) {
			struct work_queue_process *p;
			p = itable_lookup(procs_table,task->taskid);
			if(!p) fatal("no entry in procs table for taskid %d",task->taskid);
			itable_insert(procs_complete, task->taskid, p);
			result = 1;
		}

		if(!results_to_be_sent_msg && itable_size(procs_complete) > 0)
		{
			send_master_message(master, "available_results\n");
			results_to_be_sent_msg = 1;
		}

		if(master_active) {
			result &= handle_master(master);
			reset_idle_timer();
		}

		if(!result) break;
	}
}

/*
workspace_create is done once when the worker starts.
*/

static int workspace_create() {
	char absolute[WORK_QUEUE_LINE_MAX];

	// Setup working space(dir)
	const char *workdir;
	const char *workdir_tmp;
	if (user_specified_workdir) {
		workdir = user_specified_workdir;
	} else if((workdir_tmp = getenv("_CONDOR_SCRATCH_DIR")) && access(workdir_tmp, R_OK|W_OK|X_OK) == 0) {
		workdir = workdir_tmp;
	} else if((workdir_tmp = getenv("TMPDIR")) && access(workdir_tmp, R_OK|W_OK|X_OK) == 0) {
		workdir = workdir_tmp;
	} else if((workdir_tmp = getenv("TEMP")) && access(workdir_tmp, R_OK|W_OK|X_OK) == 0) {
		workdir = workdir_tmp;
	} else if((workdir_tmp = getenv("TMP")) && access(workdir_tmp, R_OK|W_OK|X_OK) == 0) {
		workdir = workdir_tmp;
	} else {
		workdir = "/tmp";
	}

	if(!workspace) {
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
workspace_prepare is called every time we connect to a new master,
*/

static int workspace_prepare()
{
	debug(D_WQ,"preparing workspace %s",workspace);
	char *cachedir = string_format("%s/cache",workspace);
	int result = create_dir(cachedir,0777);
	free(cachedir);

	char *tmp_name = string_format("%s/cache/tmp", workspace);
	result |= create_dir(tmp_name,0777);

	setenv("WORKER_TMPDIR", tmp_name, 1);
	free(tmp_name);

	return result;
}

/*
workspace_cleanup is called every time we disconnect from a master,
to remove any state left over from a previous run.
*/

static void workspace_cleanup()
{
	debug(D_WQ,"cleaning workspace %s",workspace);
	delete_dir_contents(workspace);
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

	delete_dir(workspace);
	free(workspace);
}

static int serve_master_by_hostport( const char *host, int port, const char *verify_project )
{
	if(!domain_name_cache_lookup(host,current_master_address->addr)) {
		fprintf(stderr,"couldn't resolve hostname %s",host);
		return 0;
	}

	/*
	For the preliminary steps of password and project verification, we use the
	idle timeout, because we have not yet been assigned any work and should
	leave if the master is not responsive.

	It is tempting to use a short timeout here, but DON'T. The name and
	password messages are ayncronous; if the master is busy handling other
	workers, a short window is not enough for a response to come back.
	*/

	reset_idle_timer();

	struct link *master = link_connect(current_master_address->addr,port,idle_stoptime);
	if(!master) {
		fprintf(stderr,"couldn't connect to %s:%d: %s\n",current_master_address->addr,port,strerror(errno));
		return 0;
	}
	link_tune(master,LINK_TUNE_INTERACTIVE);

	char local_addr[LINK_ADDRESS_MAX];
	int  local_port;
	link_address_local(master, local_addr, &local_port);

	printf("connected to master %s:%d via local address %s:%d\n", host, port, local_addr, local_port);
	debug(D_WQ, "connected to master %s:%d via local address %s:%d", host, port, local_addr, local_port);

	if(password) {
		debug(D_WQ,"authenticating to master");
		if(!link_auth_password(master,password,idle_stoptime)) {
			fprintf(stderr,"work_queue_worker: wrong password for master %s:%d\n",host,port);
			link_close(master);
			return 0;
		}
	}

	if(verify_project) {
		char line[WORK_QUEUE_LINE_MAX];
		debug(D_WQ, "verifying master's project name");
		send_master_message(master, "name\n");
		if(!recv_master_message(master,line,sizeof(line),idle_stoptime)) {
			debug(D_WQ,"no response from master while verifying name");
			link_close(master);
			return 0;
		}

		if(strcmp(line,verify_project)) {
			fprintf(stderr, "work_queue_worker: master has project %s instead of %s\n", line, verify_project);
			link_close(master);
			return 0;
		}
	}

	workspace_prepare();

	measure_worker_resources();

	report_worker_ready(master);

	if(worker_mode == WORKER_MODE_FOREMAN) {
		foreman_for_master(master);
	} else {
		work_for_master(master);
	}

	if(abort_signal_received) {
		send_master_message(master, "info vacating %d\n", abort_signal_received);
	}

	last_task_received     = 0;
	results_to_be_sent_msg = 0;

	workspace_cleanup();
	disconnect_master(master);
	printf("disconnected from master %s:%d\n", host, port );

	return 1;
}

int serve_master_by_hostport_list(struct list *master_addresses) {
	int result = 0;

	/* keep trying masters in the list, until all master addresses
	 * are tried, or a succesful connection was done */
	list_first_item(master_addresses);
	while((current_master_address = list_next_item(master_addresses))) {
		result = serve_master_by_hostport(current_master_address->host,current_master_address->port,0);

		if(result) {
			break;
		}
	}

	return result;
}

static struct list *interfaces_to_list(const char *addr, int port, struct jx *ifas) {

	struct list *l = list_create();
	struct jx *ifa;

	int found_canonical = 0;

	for (void *i = NULL; (ifa = jx_iterate_array(ifas, &i));) {
		const char *ifa_addr = jx_lookup_string(ifa, "host");

		if(ifa_addr && strcmp(addr, ifa_addr) == 0) {
			found_canonical = 1;
		}

		struct master_address *m = calloc(1, sizeof(*m));
		strncpy(m->host, ifa_addr, LINK_ADDRESS_MAX);
		m->port = port;

		list_push_tail(l, m);
	}

	if(ifas && !found_canonical) {
		warn(D_NOTICE, "Did not find the master address '%s' in the list of interfaces.", addr);
	}

	if(!found_canonical) {
		/* We get here if no interfaces were defined, or if addr was not found in the interfaces. */

		struct master_address *m = calloc(1, sizeof(*m));
		strncpy(m->host, addr, LINK_ADDRESS_MAX);
		m->port = port;

		list_push_tail(l, m);
	}

	return l;
}

static int serve_master_by_name( const char *catalog_hosts, const char *project_regex )
{
	struct list *masters_list = work_queue_catalog_query_cached(catalog_hosts,-1,project_regex);

	debug(D_WQ,"project name %s matches %d masters",project_regex,list_size(masters_list));

	if(list_size(masters_list)==0) return 0;

	// shuffle the list by r items to distribute the load across masters
	int r = rand() % list_size(masters_list);
	int i;
	for(i=0;i<r;i++) {
		list_push_tail(masters_list,list_pop_head(masters_list));
	}

	static struct master_address *last_addr = NULL;

	while(1) {
		struct jx *jx = list_peek_head(masters_list);

		const char *project = jx_lookup_string(jx,"project");
		const char *name = jx_lookup_string(jx,"name");
		const char *addr = jx_lookup_string(jx,"address");
		const char *pref = jx_lookup_string(jx,"master_preferred_connection");
		struct jx *ifas  = jx_lookup(jx,"network_interfaces");
		int port = jx_lookup_integer(jx,"port");


		if(last_addr) {
			if(time(0) > idle_stoptime && strcmp(addr, last_addr->host) == 0 && port == last_addr->port) {
				if(list_size(masters_list) < 2) {
					free(last_addr);
					last_addr = NULL;

					/* convert idle_stoptime into connect_stoptime (e.g., time already served). */
					connect_stoptime = idle_stoptime;
					debug(D_WQ,"Previous idle disconnection from only master available project=%s name=%s addr=%s port=%d",project,name,addr,port);

					return 0;
				} else {
					list_push_tail(masters_list,list_pop_head(masters_list));
					continue;
				}
			}
		}

		int result;

		if(pref && strcmp(pref, "by_hostname") == 0) {
			debug(D_WQ,"selected master with project=%s name=%s addr=%s port=%d",project,name,addr,port);
			result = serve_master_by_hostport(name,port,project);
		} else {
			master_addresses = interfaces_to_list(addr, port, ifas);

			result = serve_master_by_hostport_list(master_addresses);

			struct master_address *m;
			while((m = list_pop_head(master_addresses))) {
				free(m);
			}
			list_delete(master_addresses);
			master_addresses = NULL;
		}

		if(result) {
			free(last_addr);
			last_addr = calloc(1,sizeof(*last_addr));
			strncpy(last_addr->host, addr, DOMAIN_NAME_MAX);
			last_addr->port = port;
		}

		return result;
	}
}

void set_worker_id() {
	srand(time(NULL));

	char *salt_and_pepper = string_format("%d%d%d", getpid(), getppid(), rand());
	unsigned char digest[MD5_DIGEST_LENGTH];

	md5_buffer(salt_and_pepper, strlen(salt_and_pepper), digest);
	worker_id = string_format("worker-%s", md5_string(digest));

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

static void read_resources_env_var(const char *name, int64_t *manual_option) {
	char *value;
	value = getenv(name);
	if(value) {
		*manual_option = atoi(value);
		/* unset variable so that children task cannot read the global value */
		unsetenv(name);
	}
}

static void read_resources_env_vars() {
	read_resources_env_var("CORES",  &manual_cores_option);
	read_resources_env_var("MEMORY", &manual_memory_option);
	read_resources_env_var("DISK",   &manual_disk_option);
	read_resources_env_var("GPUS",   &manual_gpus_option);
}

struct list *parse_master_addresses(const char *specs, int default_port) {
	struct list *masters = list_create();

	char *masters_args = xxstrdup(specs);

	char *next_master = strtok(masters_args, ";");
	while(next_master) {
		int port = default_port;

		char *port_str = strchr(next_master, ':');
		if(port_str) {
			char *no_ipv4 = strchr(port_str+1, ':'); /* if another ':', then this is not ipv4. */
			if(!no_ipv4) {
				*port_str = '\0';
				port = atoi(port_str+1);
			}
		}

		if(port < 1) {
			fatal("Invalid port for master '%s'", next_master);
		}

		struct master_address *m = calloc(1, sizeof(*m));
		strncpy(m->host, next_master, LINK_ADDRESS_MAX);
		m->port = port;

		if(port_str) {
			*port_str = ':';
		}

		list_push_tail(masters, m);
		next_master = strtok(NULL, ";");
	}
	free(masters_args);

	return(masters);
}

static void show_help(const char *cmd)
{
	printf( "Use: %s [options] <masterhost> <port> \n"
			"or\n     %s [options] \"masterhost:port[;masterhost:port;masterhost:port;...]\"\n"
			"or\n     %s [options] -M projectname\n",
			cmd, cmd, cmd);
	printf( "where options are:\n");
	printf( " %-30s Show version string\n", "-v,--version");
	printf( " %-30s Show this help screen\n", "-h,--help");
	printf( " %-30s Name of master (project) to contact.  May be a regular expression.\n", "-N,-M,--master-name=<name>");
	printf( " %-30s Catalog server to query for masters.  (default: %s:%d) \n", "-C,--catalog=<host:port>",CATALOG_HOST,CATALOG_PORT);
	printf( " %-30s Enable debugging for this subsystem.\n", "-d,--debug=<subsystem>");
	printf( " %-30s Send debugging to this file. (can also be :stderr, :stdout, :syslog, or :journal)\n", "-o,--debug-file=<file>");
	printf( " %-30s Set the maximum size of the debug log (default 10M, 0 disables).\n", "--debug-rotate-max=<bytes>");
	printf( " %-30s Set worker to run as a foreman.\n", "--foreman");
	printf( " %-30s Run as a foreman, and advertise to the catalog server with <name>.\n", "-f,--foreman-name=<name>");
	printf( " %-30s\n", "--foreman-port=<port>[:<highport>]");
	printf( " %-30s Set the port for the foreman to listen on.  If <highport> is specified\n", "");
	printf( " %-30s the port is chosen from the range port:highport.  Implies --foreman.\n", "");
	printf( " %-30s Select port to listen to at random and write to this file.  Implies --foreman.\n", "-Z,--foreman-port-file=<file>");
	printf( " %-30s Set the fast abort multiplier for foreman (default=disabled).\n", "-F,--fast-abort=<mult>");
	printf( " %-30s Send statistics about foreman to this file.\n", "--specify-log=<logfile>");
	printf( " %-30s Password file for authenticating to the master.\n", "-P,--password=<pwfile>");
	printf( " %-30s Set both --idle-timeout and --connect-timeout.\n", "-t,--timeout=<time>");
	printf( " %-30s Disconnect after this time if master sends no work. (default=%ds)\n", "   --idle-timeout=<time>", idle_timeout);
	printf( " %-30s Abort after this time if no masters are available. (default=%ds)\n", "   --connect-timeout=<time>", idle_timeout);
	printf( " %-30s Set TCP window size.\n", "-w,--tcp-window-size=<size>");
	printf( " %-30s Set initial value for backoff interval when worker fails to connect\n", "-i,--min-backoff=<time>");
	printf( " %-30s to a master. (default=%ds)\n", "", init_backoff_interval);
	printf( " %-30s Set maximum value for backoff interval when worker fails to connect\n", "-b,--max-backoff=<time>");
	printf( " %-30s to a master. (default=%ds)\n", "", max_backoff_interval);
	printf( " %-30s Minimum free disk space in MB. When free disk space is less than this value, the\n", "-z,--disk-threshold=<size>");
	printf( " %-30s worker will clean up and try to reconnect. (default=%" PRIu64 "MB)\n", "", disk_avail_threshold);
	printf( " %-30s Set available memory size threshold (in MB). When exceeded worker will\n", "--memory-threshold=<size>");
	printf( " %-30s clean up and reconnect. (default=%" PRIu64 "MB)\n", "", memory_avail_threshold);
	printf( " %-30s Set architecture string for the worker to report to master instead\n", "-A,--arch=<arch>");
	printf( " %-30s of the value in uname (%s).\n", "", arch_name);
	printf( " %-30s Set operating system string for the worker to report to master instead\n", "-O,--os=<os>");
	printf( " %-30s of the value in uname (%s).\n", "", os_name);
	printf( " %-30s Set the location for creating the working directory of the worker.\n", "-s,--workdir=<path>");
	printf( " %-30s Set the maximum bandwidth the foreman will consume in bytes per second. Example: 100M for 100MBps. (default=unlimited)\n", "--bandwidth=<Bps>");
	printf( " %-30s Set the number of cores reported by this worker.  Set to 0 to have the\n", "--cores=<n>");
	printf( " %-30s worker automatically measure. (default=%"PRId64")\n", "", manual_cores_option);
	printf( " %-30s Set the number of GPUs reported by this worker. (default=0)\n", "--gpus=<n>");
	printf( " %-30s Manually set the amount of memory (in MB) reported by this worker.\n", "--memory=<mb>           ");
	printf( " %-30s Manually set the amount of disk (in MB) reported by this worker.\n", "--disk=<mb>");
	printf( " %-30s Use loop devices for task sandboxes (default=disabled, requires root access).\n", "--disk-allocation");
	printf( " %-30s Specifies a user-defined feature the worker provides. May be specified several times.\n", "--feature");
	printf( " %-30s Set the maximum number of seconds the worker may be active. (in s).\n", "--wall-time=<s>");
	printf( " %-30s Forbid the use of symlinks for cache management.\n", "--disable-symlinks");
	printf(" %-30s Single-shot mode -- quit immediately after disconnection.\n", "--single-shot");
	printf(" %-30s docker mode -- run each task with a container based on this docker image.\n", "--docker=<image>");
	printf(" %-30s docker-preserve mode -- tasks execute by a worker share a container based on this docker image.\n", "--docker-preserve=<image>");
	printf(" %-30s docker-tar mode -- build docker image from tarball, this mode must be used with --docker or --docker-preserve.\n", "--docker-tar=<tarball>");
	printf( " %-30s Set the percent chance per minute that the worker will shut down (simulates worker failures, for testing only).\n", "--volatility=<chance>");
	printf("%-30s Initialize as MPI programs (requires being built with --with-mpicc-path in cctools configuration).\n", "--mpi");
}

enum {LONG_OPT_DEBUG_FILESIZE = 256, LONG_OPT_VOLATILITY, LONG_OPT_BANDWIDTH,
	  LONG_OPT_DEBUG_RELEASE, LONG_OPT_SPECIFY_LOG, LONG_OPT_CORES, LONG_OPT_MEMORY,
	  LONG_OPT_DISK, LONG_OPT_GPUS, LONG_OPT_FOREMAN, LONG_OPT_FOREMAN_PORT, LONG_OPT_DISABLE_SYMLINKS,
	  LONG_OPT_IDLE_TIMEOUT, LONG_OPT_CONNECT_TIMEOUT, LONG_OPT_RUN_DOCKER, LONG_OPT_RUN_DOCKER_PRESERVE,
	  LONG_OPT_BUILD_FROM_TAR, LONG_OPT_SINGLE_SHOT, LONG_OPT_WALL_TIME, LONG_OPT_DISK_ALLOCATION,
	  LONG_OPT_MEMORY_THRESHOLD, LONG_OPT_FEATURE, LONG_OPT_MPI};

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
	{"disable-symlinks",    no_argument,        0,  LONG_OPT_DISABLE_SYMLINKS},
	{"docker",              required_argument,  0,  LONG_OPT_RUN_DOCKER},
	{"docker-preserve",     required_argument,  0,  LONG_OPT_RUN_DOCKER_PRESERVE},
	{"docker-tar",          required_argument,  0,  LONG_OPT_BUILD_FROM_TAR},
	{"feature",             required_argument,  0,  LONG_OPT_FEATURE},
	{"mpi",                 no_argument,        0, LONG_OPT_MPI},
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
	char * catalog_hosts = CATALOG_HOST;

#ifdef CCTOOLS_WITH_MPI
	int using_mpi = 0;
#endif

	features = hash_table_create(4, 0);

	worker_start_time = time(0);

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
			disk_avail_threshold = atoll(optarg) * MEGA;
			break;
		case LONG_OPT_MEMORY_THRESHOLD:
			memory_avail_threshold = atoll(optarg);
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
				manual_gpus_option = 0;
			} else {
				manual_gpus_option = atoi(optarg);
			}
			break;
		case LONG_OPT_WALL_TIME:
			manual_wall_time_option = atoi(optarg);
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
		case LONG_OPT_RUN_DOCKER:
			container_mode = CONTAINER_MODE_DOCKER;
			img_name = xxstrdup(optarg);
			break;
		case LONG_OPT_RUN_DOCKER_PRESERVE:
			container_mode = CONTAINER_MODE_DOCKER_PRESERVE;
			img_name = xxstrdup(optarg);
			break;
		case LONG_OPT_BUILD_FROM_TAR:
			load_from_tar = 1;
			tar_fn = xxstrdup(optarg);
			break;
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
#ifdef CCTOOLS_WITH_MPI
			case LONG_OPT_MPI:
				using_mpi = 1;
				break;
#endif
		default:
			show_help(argv[0]);
			return 1;
		}
	}

	cctools_version_debug(D_DEBUG, argv[0]);

#ifdef CCTOOLS_WITH_MPI
	//to move into an init function
	
	if (using_mpi == 1) {
		//mpi boilerplate code modified from tutorial at www.mpitutorial.com
		MPI_Init(NULL, NULL);
		int mpi_world_size;
		MPI_Comm_size(MPI_COMM_WORLD, &mpi_world_size);
		int mpi_rank;
		MPI_Comm_rank(MPI_COMM_WORLD, &mpi_rank);
		char procname[MPI_MAX_PROCESSOR_NAME];
		int procnamelen;
		MPI_Get_processor_name(procname, &procnamelen);

		if (mpi_rank == 0) { //Master to decide who stays and who doesn't
			int i;
			struct hash_table* comps = hash_table_create(0, 0);

			for (i = 1; i < mpi_world_size; i++) {
				unsigned len = 0;
				MPI_Recv(&len, 1, MPI_UNSIGNED, i, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
				char* str = malloc(sizeof (char*)*len + 1);
				memset(str, '\0', sizeof (char)*len + 1);
				MPI_Recv(str, len, MPI_CHAR, i, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

				struct jx* recobj = jx_parse_string(str);
				char* name = (char*)jx_lookup_string(recobj, "name");
				uint64_t* rank = malloc(sizeof(uint64_t)*1); 
				*rank = (uint64_t) jx_lookup_integer(recobj, "rank");

				if (strstr(procname, name)) { //0 will always be the master on its own comp
					continue;
				}

				if (hash_table_lookup(comps, name) == NULL) {
					hash_table_insert(comps, name, (void*)rank);
				}

				jx_delete(recobj);

			}
			for (i = 1; i < mpi_world_size; i++) {
				hash_table_firstkey(comps);
				char* key;
				int value;
				int sent = 0;
				while (hash_table_nextkey(comps, &key, (void**) &value)) {
					if (value == i) {
						MPI_Send("LIVE", 4, MPI_CHAR, i, 0, MPI_COMM_WORLD);
						sent = 1;
					}
				}
				if (sent == 0) {
					MPI_Send("DIE ", 4, MPI_CHAR, i, 0, MPI_COMM_WORLD);
				}
			}

			hash_table_delete(comps);

		} else { //send proc name and num
			char* sendstr = string_format("{\"name\":\"%s\",\"rank\":%i}", procname, mpi_rank);
			unsigned len = strlen(sendstr);
			MPI_Send(&len, 1, MPI_UNSIGNED, 0, 0, MPI_COMM_WORLD);
			MPI_Send(sendstr, len, MPI_CHAR, 0, 0, MPI_COMM_WORLD);

			free(sendstr);
			//get if live or die
			char livedie[10];
			MPI_Recv(livedie, 4, MPI_CHAR, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
			if (strstr(livedie, "DIE")) {
				MPI_Finalize();
				return 0;
			} else if (strstr(livedie, "LIVE")) {
				//do nothing, continue
			} else {
				fprintf(stderr, "livedie string got corrupted, wrong command sent.... %s\n", livedie);
				MPI_Finalize();
				return 1;
			}//corrupted string or something
		}
	}

#endif


	// for backwards compatibility with the old syntax for specifying a worker's project name
	if(worker_mode != WORKER_MODE_FOREMAN && foreman_name) {
		if(foreman_name) {
			project_regex = foreman_name;
		}
	}

	//checks that the foreman has a unique name from the master
	if(worker_mode == WORKER_MODE_FOREMAN && foreman_name){
		if(project_regex && strcmp(foreman_name,project_regex) == 0) {
			fatal("Foreman (%s) and Master (%s) share a name. Ensure that these are unique.\n",foreman_name,project_regex);
		}
	}

	//checks disk options make sense
	if(manual_disk_option > 0 &&  manual_disk_option <= disk_avail_threshold) {
		fatal("Disk space specified (%" PRId64 " MB) is less than minimum threshold (%"PRId64 " MB).\n See --disk and --disk-threshold options.", manual_disk_option, disk_avail_threshold);
	}

	//checks memory options make sense
	if(manual_memory_option > 0 &&  manual_memory_option <= memory_avail_threshold) {
		fatal("Memory specified (%" PRId64 " MB) is less than minimum threshold (%"PRId64 " MB).\n See --memory and --memory-threshold options.", manual_memory_option, memory_avail_threshold);
	}

	if(!project_regex) {
		if((argc - optind) < 1 || (argc - optind) > 2) {
			show_help(argv[0]);
			exit(1);
		}

		int default_master_port = (argc - optind) == 2 ? atoi(argv[optind+1]) : 0;
		master_addresses = parse_master_addresses(argv[optind], default_master_port);

		if(list_size(master_addresses) < 1) {
			show_help(argv[0]);
			fatal("No master has been specified");
		}
	}

	//Check GPU name
	char *gpu_name = gpu_name_get();
	if(gpu_name) {
		hash_table_insert(features, gpu_name, (void **) 1);
	}

	signal(SIGTERM, handle_abort);
	signal(SIGQUIT, handle_abort);
	signal(SIGINT, handle_abort);
	//Also do cleanup on SIGUSR1 & SIGUSR2 to allow using -notify and -l s_rt= options if submitting 
	//this worker process with SGE qsub. Otherwise task processes are left running when SGE
	//terminates this process with SIGKILL.
	signal(SIGUSR1, handle_abort);
	signal(SIGUSR2, handle_abort);
	signal(SIGCHLD, handle_sigchld);

	random_init();

	if(!workspace_create()) {
		fprintf(stderr, "work_queue_worker: failed to setup workspace at %s.\n", workspace);
		exit(1);
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
			work_queue_specify_master_mode(foreman_q, WORK_QUEUE_MASTER_MODE_CATALOG);
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

	if(container_mode == CONTAINER_MODE_DOCKER && load_from_tar == 1) {
		char load_cmd[1024];
		string_nformat(load_cmd, sizeof(load_cmd), "docker load < %s", tar_fn);
		system(load_cmd);
	}

	if(container_mode == CONTAINER_MODE_DOCKER_PRESERVE) {
		if (load_from_tar == 1) {
			char load_cmd[1024];
			string_nformat(load_cmd, sizeof(load_cmd), "docker load < %s", tar_fn);
			system(load_cmd);
		}

		string_nformat(container_name, sizeof(container_name), "worker-%d-%d", (int) getuid(), (int) getpid());
		char container_mnt_point[1024];
		char start_container_cmd[1024];
		string_nformat(container_mnt_point, sizeof(container_mnt_point), "%s:%s", workspace, DOCKER_WORK_DIR);
		string_nformat(start_container_cmd, sizeof(start_container_cmd), "docker run -i -d --name=\"%s\" -v %s -w %s %s", container_name, container_mnt_point, DOCKER_WORK_DIR, img_name);
		system(start_container_cmd);
	}

	procs_running  = itable_create(0);
	procs_table    = itable_create(0);
	procs_waiting  = list_create();
	procs_complete = itable_create(0);

	watcher = work_queue_watcher_create();

	if(!check_disk_space_for_filesize(".", 0, disk_avail_threshold)) {
		fprintf(stderr,"work_queue_worker: %s has less than minimum disk space %"PRIu64" MB\n",workspace,disk_avail_threshold);
		return 1;
	}

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

	while(1) {
		int result = 0;

		measure_worker_resources();
		if(!enforce_worker_promises(NULL)) {
			abort_flag = 1;
			break;
		}

		if(project_regex) {
			result = serve_master_by_name(catalog_hosts, project_regex);
		} else {
			result = serve_master_by_hostport_list(master_addresses);
		}

		/*
		If the last attempt was a succesful connection, then reset the backoff_interval,
		and the connect timeout, then try again if a project name was given.
		If the connect attempt failed, then slow down the retries.
		*/

		if(result) {
			if(single_shot_mode) {
				debug(D_NOTICE,"stopping: single shot mode");
				break;
			}
			backoff_interval = init_backoff_interval;
			connect_stoptime = time(0) + connect_timeout;

			if(!project_regex && (time(0)>idle_stoptime)) {
				debug(D_NOTICE,"stopping: no other masters available");
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

	if(container_mode == CONTAINER_MODE_DOCKER_PRESERVE || container_mode == CONTAINER_MODE_DOCKER) {
		char stop_container_cmd[WORK_QUEUE_LINE_MAX];
		char rm_container_cmd[WORK_QUEUE_LINE_MAX];

		string_nformat(stop_container_cmd, sizeof(stop_container_cmd), "docker stop %s", container_name);
		string_nformat(rm_container_cmd, sizeof(rm_container_cmd), "docker rm %s", container_name);

		if(container_mode == CONTAINER_MODE_DOCKER_PRESERVE) {
			//1. stop the container
			system(stop_container_cmd);
			//2. remove the container
			system(rm_container_cmd);
		}

	}

	workspace_delete();

#ifdef CCTOOLS_WITH_MPI
	if (using_mpi == 1) {
		MPI_Finalize();
	}

#endif

	return 0;
}

/* vim: set noexpandtab tabstop=4: */
