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
#include "nvpair.h"
#include "copy_stream.h"
#include "memory_info.h"
#include "disk_info.h"
#include "cwd_disk_info.h"
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

#include <sys/mman.h>
#include <sys/poll.h>
#include <sys/resource.h>
#include <sys/signal.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/utsname.h>
#include <sys/wait.h>

#ifdef CCTOOLS_OPSYS_SUNOS
extern int setenv(const char *name, const char *value, int overwrite);
#endif

#define WORKER_MODE_WORKER  1
#define WORKER_MODE_FOREMAN 2

#define NONE 0 
#define DOCKER 1
#define DOCKER_PRESERVE 2
#define UMBRELLA 3 

#define DEFAULT_WORK_DIR "/home/worker"

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

// Maximum time to attempt a single link_connect before trying other options.
static int single_connect_timeout = 15;

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

// Flag used to indicate a child must be waited for.
static int sigchld_received_flag = 0;

// Threshold for available disk space (MB) beyond which clean up and restart.
static uint64_t disk_avail_threshold = 100;

// Password shared between master and worker.
char *password = 0;

// Allow worker to use symlinks when link() fails.  Enabled by default.
static int symlinks_enabled = 1;

// Worker id. A unique id for this worker instance.
static char *worker_id;

static int worker_mode = WORKER_MODE_WORKER;

// Do not run any container by default
static int container_mode = NONE;
static int load_from_tar = 0;

static const char *master_host = 0;
static char master_addr[LINK_ADDRESS_MAX];
static int master_port;
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
static int64_t manual_cores_option = 1;
static int64_t manual_disk_option = 0;
static int64_t manual_memory_option = 0;
static int64_t manual_gpus_option = 0;

static time_t  last_cwd_measure_time = 0;
static int64_t last_workspace_usage  = 0;

static int64_t cores_allocated = 0;
static int64_t memory_allocated = 0;
static int64_t disk_allocated = 0;
static int64_t gpus_allocated = 0;

static int send_resources_interval = 30;
static int send_stats_interval     = 60;
static int measure_wd_interval     = 180;

static struct work_queue *foreman_q = NULL;
// docker image name
static char *img_name = NULL;
static char container_name[1024]; 
static char *tar_fn = NULL;
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

	sprintf(debug_msg, "tx to master: %s", fmt);
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
Measure only the resources associated with this particular node
and apply any operations that override.
*/

void resources_measure_locally(struct work_queue_resources *r)
{
	work_queue_resources_measure_locally(r,workspace);

	if(worker_mode == WORKER_MODE_FOREMAN) {
		r->cores.total = 0;
		r->memory.total = 0;
		r->gpus.total = 0;
	} else {
		if(manual_cores_option)
			r->cores.total = manual_cores_option;
		if(manual_memory_option)
			r->memory.total = MIN(r->memory.total, manual_memory_option);
		if(manual_gpus_option)
			r->gpus.total = manual_gpus_option;
	}

	if(manual_disk_option)
		r->disk.total = MIN(r->disk.total, manual_disk_option);

	r->cores.smallest = r->cores.largest = r->cores.total;
	r->memory.smallest = r->memory.largest = r->memory.total;
	r->disk.smallest = r->disk.largest = r->disk.total;
	r->gpus.smallest = r->gpus.largest = r->gpus.total;
}

/*
Send a message to the master with my current resources, if they have changed.
Don't send a message more than every send_resources_interval seconds.
Only a foreman needs to send regular updates after the initial ready message.
*/

static void send_resource_update( struct link *master, int force_update )
{
	static time_t last_send_time = 0;

	time_t stoptime = time(0) + active_timeout;

	if(!force_update) {
		if( results_to_be_sent_msg ) return;
		if( (time(0)-last_send_time) < send_resources_interval ) return;
	}

	if(worker_mode == WORKER_MODE_FOREMAN) {
		aggregate_workers_resources(foreman_q, total_resources);
		total_resources->disk.total = local_resources->disk.total;
		total_resources->disk.inuse = local_resources->disk.inuse;
		// do not send resource update until we get at least one capable worker.
		if(total_resources->cores.total < 1) return;

	} else {
		memcpy(total_resources, local_resources, sizeof(struct work_queue_resources));
	}

	if(!force_update) {
		if( !memcmp(total_resources_last,total_resources,sizeof(struct work_queue_resources))) return;
	}

	total_resources->tag = last_task_received;

	work_queue_resources_send(master,total_resources,stoptime);
	send_master_message(master, "info end_of_resource_update %d\n", 0);

	memcpy(total_resources_last,total_resources,sizeof(*total_resources));
	last_send_time = time(0);
}

/*
Send a message to the master with my current statistics information.
*/

static void send_stats_update( struct link *master, int force_update)
{
	static time_t last_send_time = 0;

	if(!force_update) {
		if( results_to_be_sent_msg ) return;
		if((time(0) - last_send_time) < send_stats_interval ) return;
	}


	if(worker_mode == WORKER_MODE_FOREMAN) {
		struct work_queue_stats s;
		work_queue_get_stats_hierarchy(foreman_q, &s);

		send_master_message(master, "info total_workers_joined %lld\n", (long long) s.total_workers_joined);
		send_master_message(master, "info total_workers_removed %lld\n", (long long) s.total_workers_removed);
		send_master_message(master, "info total_send_time %lld\n", (long long) s.total_send_time);
		send_master_message(master, "info total_receive_time %lld\n", (long long) s.total_receive_time);
		send_master_message(master, "info total_execute_time %lld\n", (long long) s.total_execute_time);
		send_master_message(master, "info total_bytes_sent %lld\n", (long long) s.total_bytes_sent);
		send_master_message(master, "info total_bytes_received %lld\n", (long long) s.total_bytes_received);
	}

	last_send_time = time(0);
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
	send_resource_update(master,1);
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

		if( (errno == EXDEV || errno == EPERM) && symlinks_enabled) {

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

    if (container_mode == DOCKER) 
	    pid = work_queue_process_execute(p, container_mode, img_name);
    else if (container_mode == DOCKER_PRESERVE)
	    pid = work_queue_process_execute(p, container_mode, container_name);
    else
	    pid = work_queue_process_execute(p, container_mode);
    
	if(pid<0) fatal("unable to fork process for taskid %d!",p->task->taskid);

	itable_insert(procs_running,pid,p);

	struct work_queue_task *t = p->task;

	cores_allocated += t->cores;
	memory_allocated += t->memory;
	disk_allocated += t->disk;
	gpus_allocated += t->gpus;

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
		send_master_message(master, "result %d %d %lld %llu %d\n", t->result, t->return_status, (long long) output_length, (unsigned long long) t->cmd_execution_time, t->taskid);
		if(output_length) {
			link_putlstring(master, t->output, output_length, time(0)+active_timeout);
		}

		total_task_execution_time += t->cmd_execution_time;
		total_tasks_executed++;
	}

	send_stats_update(master, 1);
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

	int64_t current_time = time(0);

	itable_firstkey(procs_running);
	while(itable_nextkey(procs_running, (uint64_t*)&pid, (void**)&p)) {
		if(p->task->maximum_end_time > 0 && current_time - p->task->maximum_end_time > 0)
		{
			kill(-1 * pid, SIGKILL);
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
				debug(D_WQ, "task %d (pid %d) exited normally with exit code %d",p->task->taskid,p->pid,p->exit_status);
			}

			p->execution_end = timestamp_get();

			if(p->task->maximum_end_time > 0 && p->execution_end - p->task->maximum_end_time)
			{
				p->task->result |= WORK_QUEUE_RESULT_TASK_TIMEOUT;
			}

			cores_allocated  -= p->task->cores;
			memory_allocated -= p->task->memory;
			disk_allocated   -= p->task->disk;
			gpus_allocated   -= p->task->gpus;

			itable_remove(procs_running, p->pid);
			itable_firstkey(procs_running);

			// Output files must be moved back into the cache directory.

			struct work_queue_file *f;
			list_first_item(p->task->output_files);
			while((f = list_next_item(p->task->output_files))) {

				char *sandbox_name = string_format("%s/%s",p->sandbox,f->remote_name);

				debug(D_WQ,"moving output file from %s to %s",sandbox_name,f->payload);
				if(rename(sandbox_name,f->payload)!=0) {
					debug(D_WQ, "could not rename output file %s to %s: %s",sandbox_name,f->payload,strerror(errno));
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

	sprintf(cached_filename, "cache/%s", filename);

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
			sprintf(dentline, "%s/%s", filename, dent->d_name);
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

	if(t->cores < 0 && t->memory < 0 && t->disk < 0 && t->gpus < 0) {
		t->cores = local_resources->cores.total;
		t->memory = local_resources->memory.total;
		t->disk = local_resources->disk.total;
		t->gpus = local_resources->gpus.total;
	} else {
		t->cores = MAX(t->cores, 0);
		t->memory = MAX(t->memory, 0);
		t->disk = MAX(t->disk, 0);
		t->gpus = MAX(t->gpus, 0);
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
	int n, flags, length;

	struct work_queue_process *p = work_queue_process_create(taskid);
	struct work_queue_task *task = p->task;

	while(recv_master_message(master,line,sizeof(line),stoptime)) {
	  	if(sscanf(line,"cmd %d",&length)==1) {
			char *cmd = malloc(length+1);
			link_read(master,cmd,length,stoptime);
			cmd[length] = 0;
			work_queue_task_specify_command(task,cmd);
			debug(D_WQ,"rx from master: %s",cmd);
			free(cmd);
		} else if(sscanf(line,"infile %s %s %d", filename, taskname_encoded, &flags)) {
			sprintf(localname, "cache/%s", filename);
			url_decode(taskname_encoded, taskname, WORK_QUEUE_LINE_MAX);
			work_queue_task_specify_file(task, localname, taskname, WORK_QUEUE_INPUT, flags);
		} else if(sscanf(line,"outfile %s %s %d", filename, taskname_encoded, &flags)) {
			sprintf(localname, "cache/%s", filename);
			url_decode(taskname_encoded, taskname, WORK_QUEUE_LINE_MAX);
			work_queue_task_specify_file(task, localname, taskname, WORK_QUEUE_OUTPUT, flags);
		} else if(sscanf(line, "dir %s", filename)) {
			work_queue_task_specify_directory(task, filename, filename, WORK_QUEUE_INPUT, 0700, 0);
		} else if(sscanf(line,"cores %d",&n)) {
		       	work_queue_task_specify_cores(task, n);
		} else if(sscanf(line,"memory %d",&n)) {
		       	work_queue_task_specify_memory(task, n);
		} else if(sscanf(line,"disk %d",&n)) {
		       	work_queue_task_specify_disk(task, n);
		} else if(sscanf(line,"gpus %d",&n)) {
			work_queue_task_specify_gpus(task, n);
	  	} else if(sscanf(line,"env %d",&length)==1) {
			char *env = malloc(length+1);
			link_read(master,env,length,stoptime);
			env[length] = 0;
			char *value = strchr(env,'=');
			if(value) {
				*value = 0;
				value++;
				work_queue_task_specify_env(task,env,value);
			}
			free(env);
		} else if(!strcmp(line,"end")) {
		       	break;
		} else {
			debug(D_WQ|D_NOTICE,"invalid command from master: %s",line);
			work_queue_process_delete(p);
			return 0;
		}
	}

	last_task_received = task->taskid;

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

	sprintf(cached_filename, "cache/%s", cur_pos);

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
		debug(D_WQ, "Could not open %s for writing\n", filename);
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
        snprintf(command, WORK_QUEUE_LINE_MAX, "curl -f -o \"%s\" \"%s\"", filename, url);

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
        snprintf(cache_name,WORK_QUEUE_LINE_MAX, "cache/%s", filename);

        return file_from_url(url, cache_name);
}

static int do_unlink(const char *path) {
	char cached_path[WORK_QUEUE_LINE_MAX];
	sprintf(cached_path, "cache/%s", path);
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

	sprintf(cached_filename, "cache/%s", cur_pos);

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
	case WORK_QUEUE_FS_PATH:
		sprintf(cmd, "/bin/cp %s %s", path, cached_filename);
		if(system(cmd) != 0) {
			debug(D_WQ, "Could not thirdget %s, copy (%s) failed. (%s)\n", filename, path, strerror(errno));
			return 0;
		}
		break;
	case WORK_QUEUE_FS_CMD:
		sprintf(cmd, "%s > %s", path, cached_filename);
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

	sprintf(cached_filename, "cache/%s", cur_pos);


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
		sprintf(cmd, "/bin/cp -r %s %s", cached_filename, path);
		if(system(cmd) != 0) {
			debug(D_WQ, "Could not thirdput %s, copy (%s) failed. (%s)\n", cached_filename, path, strerror(errno));
			result = 0;
		}
		break;
	case WORK_QUEUE_FS_CMD:
		sprintf(cmd, "%s < %s", path, cached_filename);
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
			cores_allocated -= p->task->cores;
			memory_allocated -= p->task->memory;
			disk_allocated -= p->task->disk;
			gpus_allocated -= p->task->gpus;
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

static int do_release() {
	debug(D_WQ, "released by master %s:%d.\n", master_addr, master_port);
	released_by_master = 1;
	return 0;
}

static int send_keepalive(struct link *master){
	send_master_message(master, "alive\n");
	return 1;
}

static void disconnect_master(struct link *master) {

	debug(D_WQ, "disconnecting from master %s:%d",master_addr,master_port);
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
	int flags = WORK_QUEUE_NOCACHE;
	int mode, r, n;

	if(recv_master_message(master, line, sizeof(line), idle_stoptime )) {
		if(sscanf(line,"task %" SCNd64, &taskid)==1) {
			r = do_task(master, taskid,time(0)+active_timeout);
		} else if((n = sscanf(line, "put %s %" SCNd64 " %o %d", filename, &length, &mode, &flags)) >= 3) {
			if(path_within_dir(filename, workspace)) {
				r = do_put(master, filename, length, mode);
			} else {
				debug(D_WQ, "Path - %s is not within workspace %s.", filename, workspace);
				r= 0;
			}
                } else if(sscanf(line, "url %s %" SCNd64 " %o", filename, &length, &mode) == 3) {
                        r = do_url(master, filename, length, mode);
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
		} else if(sscanf(line, "kill %" SCNd64, &taskid) == 1) {
			if(taskid >= 0) {
				r = do_kill(taskid);
			} else {
				kill_all_tasks();
				r = 1;
			}
		} else if(!strncmp(line, "release", 8)) {
			r = do_release();
		} else if(!strncmp(line, "exit", 5)) {
			r = 0;
		} else if(!strncmp(line, "check", 6)) {
			r = send_keepalive(master);
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

static int check_for_resources(struct work_queue_task *t)
{
	return
		(cores_allocated  + t->cores  <= local_resources->cores.total) &&
		(memory_allocated + t->memory <= local_resources->memory.total) &&
		(disk_allocated   + t->disk   <= local_resources->disk.total) &&
		(gpus_allocated   + t->gpus   <= local_resources->gpus.total);
}

static void work_for_master(struct link *master) {
	sigset_t mask;

	debug(D_WQ, "working for master at %s:%d.\n", master_addr, master_port);

	sigemptyset(&mask);
	sigaddset(&mask, SIGCHLD);

	reset_idle_timer();

	time_t volatile_stoptime = time(0) + 60;
	// Start serving masters
	while(!abort_flag) {

		if(time(0) > idle_stoptime) {
			debug(D_NOTICE, "disconnecting from %s:%d because I did not receive any task in %d seconds (--idle-timeout).\n", master_addr,master_port,idle_timeout);
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

		if(!results_to_be_sent_msg) {
			if(work_queue_watcher_check(watcher) || itable_size(procs_complete) > 0) {
				send_master_message(master, "available_results\n");
				results_to_be_sent_msg = 1;
			}
		}

		ok &= check_disk_space_for_filesize(".", 0, disk_avail_threshold);

		int64_t disk_usage;
		if(!check_disk_workspace(workspace, &disk_usage, 0, manual_disk_option, measure_wd_interval, last_cwd_measure_time, last_workspace_usage, disk_avail_threshold)) {
			fprintf(stderr,"work_queue_worker: %s has less than the promised disk space %"PRIu64" < %"PRIu64" MB\n",workspace, manual_cores_option, disk_usage);
			send_master_message(master, "info disk_space_exhausted %lld\n", (long long) disk_usage);
			ok = 0;
		}


		if(ok) {
            
            
             
			int visited = 0;
			while(list_size(procs_waiting) > visited && cores_allocated < local_resources->cores.total) {
				struct work_queue_process *p;

				p = list_pop_head(procs_waiting);
				if(p && check_for_resources(p->task)) {
					start_process(p);
				} else {
					list_push_tail(procs_waiting, p);
					visited++;
				}
			}

			// If all resources are free, but we cannot execute any of the
			// waiting tasks, then disconnect so that the master gets the tasks
			// back. (Imagine for example that some other process in the host
			// running the worker used so much memory or disk, that now no task
			// cannot be scheduled.) Note we check against procs_table, and
			// not procs_running, so that we do not disconnect if there are
			// results waiting to be sent back (which in turn may free some
			// disk).  Note also this is a short-term solution. In the long
			// term we want the worker to report to the master something like
			// 'task not done'.
			if(list_size(procs_waiting) > 0 && itable_size(procs_table) == 0)
			{
				debug(D_WQ, "No task can be executed with the available resources.\n");
				ok = 0;
			}

            
             
		}

		if(!ok) break;

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

	debug(D_WQ, "working for master at %s:%d as foreman.\n", master_addr, master_port);

	reset_idle_timer();

	while(!abort_flag) {
		int result = 1;
		struct work_queue_task *task = NULL;

		if(time(0) > idle_stoptime && work_queue_empty(foreman_q)) {
			debug(D_NOTICE, "work_queue_worker: giving up because did not receive any task in %d seconds.\n", idle_timeout);
			break;
		}

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

		send_stats_update(master,0);
		send_resource_update(master,0);

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
	// Setup working space(dir)
	const char *workdir;
	if (user_specified_workdir){
		workdir = user_specified_workdir;
	} else if(getenv("_CONDOR_SCRATCH_DIR")) {
		workdir = getenv("_CONDOR_SCRATCH_DIR");
	} else if(getenv("TEMP")) {
		workdir = getenv("TEMP");
	} else {
		workdir = "/tmp";
	}
	//}
	
	if(!workspace) {
		workspace = string_format("%s/worker-%d-%d", workdir, (int) getuid(), (int) getpid());
	}

	printf( "work_queue_worker: creating workspace %s\n", workspace);
	if(!create_dir(workspace,0777)) return 0;

	return 1;
}

/*
workspace_prepare is called every time we connect to a new master,
*/

static int workspace_prepare()
{
	debug(D_WQ,"preparing workspace %s",workspace);
	char *cachedir = string_format("%s/cache",workspace);
	int result = create_dir (cachedir,0777);
	free(cachedir);
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
	if(!domain_name_cache_lookup(host,master_addr)) {
		fprintf(stderr,"couldn't resolve hostname %s",host);
		return 0;
	}

	/*
	For a single connection attempt, we use the short single_connect_timeout.
	If this fails, the outer loop will try again up to connect_timeout.
	*/

	struct link *master = link_connect(master_addr,port,time(0)+single_connect_timeout);
	if(!master) {
		fprintf(stderr,"couldn't connect to %s:%d: %s\n",master_addr,port,strerror(errno));
		return 0;
	}

	printf("connected to master %s:%d\n", host, port );
	debug(D_WQ, "connected to master %s:%d", host, port );

	link_tune(master,LINK_TUNE_INTERACTIVE);

	/*
	For the preliminary steps of password and project verification, we use the idle timeout,
	because we have not yet been assigned any work and should leave if the master is not responsive.
	*/


	reset_idle_timer();

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
		if(!recv_master_message(master, line, sizeof(line),time(0) + single_connect_timeout)) {
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

	check_disk_workspace(workspace, NULL, 1, manual_disk_option, measure_wd_interval, last_cwd_measure_time, last_workspace_usage, disk_avail_threshold);
	report_worker_ready(master);

	send_master_message(master, "info worker-id %s\n", worker_id);

	if(worker_mode == WORKER_MODE_FOREMAN) {
		foreman_for_master(master);
	} else {
		work_for_master(master);
	}

	last_task_received  = -1;
	results_to_be_sent_msg = 0;

	workspace_cleanup();
	disconnect_master(master);
	printf("disconnected from master %s:%d\n", host, port );

	return 1;
}

static int serve_master_by_name( const char *catalog_host, int catalog_port, const char *project_regex )
{
	static char *last_addr = NULL; 
	static int   last_port = -1; 

	struct list *masters_list = work_queue_catalog_query_cached(catalog_host,catalog_port,project_regex);

	debug(D_WQ,"project name %s matches %d masters",project_regex,list_size(masters_list));

	if(list_size(masters_list)==0) return 0;

	// shuffle the list by r items to distribute the load across masters
	int r = rand() % list_size(masters_list);
	int i;
	for(i=0;i<r;i++) {
		list_push_tail(masters_list,list_pop_head(masters_list));
	}

	while(1) {
		struct nvpair *nv = list_peek_head(masters_list);
		const char *project = nvpair_lookup_string(nv,"project");
		const char *name = nvpair_lookup_string(nv,"name");
		const char *addr = nvpair_lookup_string(nv,"address");
		int port = nvpair_lookup_integer(nv,"port");

		/* Do not connect to the same master after idle disconnection. */
		if(last_addr) {
			if( time(0) > idle_stoptime && strcmp(addr, last_addr) == 0 && port == last_port) {
				if(list_size(masters_list) < 2) {
					free(last_addr);
					/* convert idle_stoptime into connect_stoptime (e.g., time already served). */
					connect_stoptime = idle_stoptime;
					debug(D_WQ,"Previous idle disconnection from only master available project=%s name=%s addr=%s port=%d",project,name,addr,port);
					return 0;
				} else {
					list_push_tail(masters_list,list_pop_head(masters_list));
					continue;
				}
			}

			free(last_addr);
		}

		last_addr = xxstrdup(addr);
		last_port = port;

		debug(D_WQ,"selected master with project=%s name=%s addr=%s port=%d",project,name,addr,port);

		return serve_master_by_hostport(addr,port,project);
	}
}

void set_worker_id() {
	srand(time(NULL));

	char *salt_and_pepper = string_format("%d%d%d", getpid(), getppid(), rand());
	unsigned char digest[MD5_DIGEST_LENGTH];

	md5_buffer(salt_and_pepper, strlen(salt_and_pepper), digest);
	worker_id = string_format("worker-%s", md5_string(digest));
}

static void handle_abort(int sig)
{
	abort_flag = 1;
}

static void handle_sigchld(int sig)
{
	sigchld_received_flag = 1;
}

static void show_help(const char *cmd)
{
	printf( "Use: %s [options] <masterhost> <port>\n", cmd);
	printf( "where options are:\n");
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
	printf( " %-30s When in Foreman mode, this foreman will advertise to the catalog server\n", "-N,--foreman-name=<name>");
	printf( " %-30s as <name>.\n", "");
	printf( " %-30s Password file for authenticating to the master.\n", "-P,--password=<pwfile>");
	printf( " %-30s Set both --idle-timeout and --connect-timeout.\n", "-t,--timeout=<time>");
	printf( " %-30s Disconnect after this time if master sends no work. (default=%ds)\n", "   --idle-timeout=<time>", idle_timeout);
	printf( " %-30s Abort after this time if no masters are available. (default=%ds)\n", "   --connect-timeout=<time>", idle_timeout);
	printf( " %-30s Set TCP window size.\n", "-w,--tcp-window-size=<size>");
	printf( " %-30s Set initial value for backoff interval when worker fails to connect\n", "-i,--min-backoff=<time>");
	printf( " %-30s to a master. (default=%ds)\n", "", init_backoff_interval);
	printf( " %-30s Set maximum value for backoff interval when worker fails to connect\n", "-b,--max-backoff=<time>");
	printf( " %-30s to a master. (default=%ds)\n", "", max_backoff_interval);
	printf( " %-30s Set available disk space threshold (in MB). When exceeded worker will\n", "-z,--disk-threshold=<size>");
	printf( " %-30s clean up and reconnect. (default=%" PRIu64 "MB)\n", "", disk_avail_threshold);
	printf( " %-30s Set architecture string for the worker to report to master instead\n", "-A,--arch=<arch>");
	printf( " %-30s of the value in uname (%s).\n", "", arch_name);
	printf( " %-30s Set operating system string for the worker to report to master instead\n", "-O,--os=<os>");
	printf( " %-30s of the value in uname (%s).\n", "", os_name);
	printf( " %-30s Set the location for creating the working directory of the worker.\n", "-s,--workdir=<path>");
	printf( " %-30s Show version string\n", "-v,--version");
	printf( " %-30s Set the percent chance a worker will decide to shut down every minute.\n", "--volatility=<chance>");
	printf( " %-30s Set the maximum bandwidth the foreman will consume in bytes per second. Example: 100M for 100MBps. (default=unlimited)\n", "--bandwidth=<Bps>");
	printf( " %-30s Set the number of cores reported by this worker.  Set to 0 to have the\n", "--cores=<n>");
	printf( " %-30s worker automatically measure. (default=%"PRId64")\n", "", manual_cores_option);
	printf( " %-30s Set the number of GPUs reported by this worker. (default=0)\n", "--gpus=<n>");
	printf( " %-30s Manually set the amount of memory (in MB) reported by this worker.\n", "--memory=<mb>           ");
	printf( " %-30s Manually set the amount of disk (in MB) reported by this worker.\n", "--disk=<mb>");
	printf( " %-30s Forbid the use of symlinks for cache management.\n", "--disable-symlinks");
	printf(" %-30s Single-shot mode -- quit immediately after disconnection.\n", "--single-shot");
	printf(" %-30s docker mode -- run each task with a container based on this docker image.\n", "--doker=<image>");
	printf(" %-30s docker-preserve mode -- tasks execute by a worker share a container based on this docker image.\n", "--docker-preserve=<image>");
	printf(" %-30s docker-tar mode -- build docker image from tarball, this mode must be used with --docker or --docker-preserve.\n", "--docker-tar=<tarball>");
	printf( " %-30s Show this help screen\n", "-h,--help");
}

enum {LONG_OPT_DEBUG_FILESIZE = 256, LONG_OPT_VOLATILITY, LONG_OPT_BANDWIDTH,
      LONG_OPT_DEBUG_RELEASE, LONG_OPT_SPECIFY_LOG, LONG_OPT_CORES, LONG_OPT_MEMORY,
      LONG_OPT_DISK, LONG_OPT_GPUS, LONG_OPT_FOREMAN, LONG_OPT_FOREMAN_PORT, LONG_OPT_DISABLE_SYMLINKS,
      LONG_OPT_IDLE_TIMEOUT, LONG_OPT_CONNECT_TIMEOUT, LONG_OPT_RUN_DOCKER, LONG_OPT_RUN_DOCKER_PRESERVE, 
      LONG_OPT_BUILD_FROM_TAR, LONG_OPT_SINGLE_SHOT};

struct option long_options[] = {
	{"advertise",           no_argument,        0,  'a'},
	{"catalog",             required_argument,  0,  'C'},
	{"debug",               required_argument,  0,  'd'},
	{"debug-file",          required_argument,  0,  'o'},
	{"debug-rotate-max",    required_argument,  0,  LONG_OPT_DEBUG_FILESIZE},
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
	{"arch",                required_argument,  0,  'A'},
	{"os",                  required_argument,  0,  'O'},
	{"workdir",             required_argument,  0,  's'},
	{"volatility",          required_argument,  0,  LONG_OPT_VOLATILITY},
	{"bandwidth",           required_argument,  0,  LONG_OPT_BANDWIDTH},
	{"cores",               required_argument,  0,  LONG_OPT_CORES},
	{"memory",              required_argument,  0,  LONG_OPT_MEMORY},
	{"disk",                required_argument,  0,  LONG_OPT_DISK},
	{"gpus",                required_argument,  0,  LONG_OPT_GPUS},
	{"help",                no_argument,        0,  'h'},
	{"version",             no_argument,        0,  'v'},
	{"disable-symlinks",    no_argument,        0,  LONG_OPT_DISABLE_SYMLINKS},
	{"docker",              required_argument,  0,  LONG_OPT_RUN_DOCKER},
	{"docker-preserve",     required_argument,  0,  LONG_OPT_RUN_DOCKER_PRESERVE},
	{"docker-tar",          required_argument,  0,  LONG_OPT_BUILD_FROM_TAR},
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
	char * catalog_host = CATALOG_HOST;
    int catalog_port = CATALOG_PORT;

	worker_start_time = time(0);

	set_worker_id();

	//obtain the architecture and os on which worker is running.
	uname(&uname_data);
	os_name = xxstrdup(uname_data.sysname);
	arch_name = xxstrdup(uname_data.machine);
	worker_mode = WORKER_MODE_WORKER;

	debug_config(argv[0]);

	while((c = getopt_long(argc, argv, "acC:d:f:F:t:j:o:p:M:N:P:w:i:b:z:A:O:s:vZ:h", long_options, 0)) != (char) -1) {
		switch (c) {
		case 'a':
			//Left here for backwards compatibility
			break;
		case 'C':
			if(!work_queue_catalog_parse(optarg, &catalog_host, &catalog_port)) {
				fprintf(stderr, "The provided catalog server is invalid. The format of the '-C' option is '-C HOSTNAME:PORT'.\n");
				exit(1);
			}
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
		case 'j':
			manual_cores_option = atoi(optarg);
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
			container_mode = DOCKER;
            img_name = xxstrdup(optarg); 
			break;
        case LONG_OPT_RUN_DOCKER_PRESERVE:
            container_mode = DOCKER_PRESERVE;
            img_name = xxstrdup(optarg);
            break;
        case LONG_OPT_BUILD_FROM_TAR:
            load_from_tar = 1;
            tar_fn = xxstrdup(optarg);
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

	//checks that the foreman has a unique name from the master
	if(worker_mode == WORKER_MODE_FOREMAN && foreman_name){
		if(project_regex && strcmp(foreman_name,project_regex) == 0) {
			fatal("Foreman (%s) and Master (%s) share a name. Ensure that these are unique.\n",foreman_name,project_regex);
		}
	}

	if(!project_regex) {
		if((argc - optind) != 2) {
			show_help(argv[0]);
			exit(1);
		}

		master_host = argv[optind];
		master_port = atoi(argv[optind+1]);

	}

	signal(SIGTERM, handle_abort);
	signal(SIGQUIT, handle_abort);
	signal(SIGINT, handle_abort);
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

		sprintf(foreman_string, "%s-foreman", argv[0]);
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
		work_queue_specify_log(foreman_q, foreman_stats_filename);

	}

    if(container_mode == DOCKER && load_from_tar == 1) {
 		char load_cmd[1024];
        sprintf(load_cmd, "docker load < %s", tar_fn);
        system(load_cmd);
    }

    if(container_mode == DOCKER_PRESERVE) {
        if (load_from_tar == 1) {
            char load_cmd[1024];
            sprintf(load_cmd, "docker load < %s", tar_fn);
            system(load_cmd);
        }

        sprintf(container_name, "worker-%d-%d", (int) getuid(), (int) getpid());
        char container_mnt_point[1024];
        char start_container_cmd[1024];
        sprintf(container_mnt_point, "%s:%s", workspace, DEFAULT_WORK_DIR);
        sprintf(start_container_cmd, "docker run -i -d --name=\"%s\" -v %s -w %s %s", container_name, container_mnt_point, DEFAULT_WORK_DIR, img_name);
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

	resources_measure_locally(local_resources);

	int backoff_interval = init_backoff_interval;
	connect_stoptime = time(0) + connect_timeout;

	while(1) {
		int result;

		if(project_regex) {
			result = serve_master_by_name(catalog_host,catalog_port,project_regex);
		} else {
			result = serve_master_by_hostport(master_host,master_port,0);
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

    if(container_mode == DOCKER_PRESERVE || container_mode == DOCKER) {
        char stop_container_cmd[1024];
        char rm_container_cmd[1024];
        
        sprintf(stop_container_cmd, "docker stop %s", container_name);
        sprintf(rm_container_cmd, "docker rm %s", container_name);

        if(container_mode == DOCKER_PRESERVE) {
            //1. stop the container
            system(stop_container_cmd); 
            //2. remove the container
            system(rm_container_cmd);
        }

    } 

	workspace_delete();

	return 0;
}

/* vim: set noexpandtab tabstop=4: */
