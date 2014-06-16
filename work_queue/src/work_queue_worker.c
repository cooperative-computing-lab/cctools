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

#include "cctools.h"
#include "macros.h"
#include "catalog_query.h"
#include "domain_name_cache.h"
#include "nvpair.h"
#include "copy_stream.h"
#include "memory_info.h"
#include "disk_info.h"
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
#include "random_init.h"

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

/*
Timeouts explained:

main loop:
	if project name given:
		query catalog using single_connect_timeout
		pick master at random from matches

	connect to master with single_connect_timeout

	worker service loop:
		disconnect if no work received within idle_timeout
		disconnect if ctrl-c sets abort_flag
		disconnect if send/recv messages takes longer than active_timeout

	stop main loop if single-master and idle_timeout has expired
	stop main loop if connect timeout has expired.
	stop if ctrl-c sets abort_flag

	sleep before trying again, with exp backoff
*/


// Maximum time to stay connected to a single master without any work.
static int idle_timeout = 900;

// Current time at which we will give up if no work is received.
static time_t idle_stoptime = 0;

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

// Threshold for available disk space (MB) beyond which clean up and restart.
static uint64_t disk_avail_threshold = 100;

// Password shared between master and worker.
char *password = 0;

// Allow worker to use symlinks when link() fails.  Enabled by default.
static int symlinks_enabled = 1;

// Basic worker global variables
static int worker_mode = WORKER_MODE_WORKER;
static const char *master_host = 0;
static char master_addr[LINK_ADDRESS_MAX];
static int master_port;
static char workspace[WORKER_WORKSPACE_NAME_MAX];
static char *os_name = NULL; 
static char *arch_name = NULL;
static char *user_specified_workdir = NULL;
static time_t worker_start_time = 0;

// Local resource controls
static struct work_queue_resources * local_resources = 0;
static struct work_queue_resources * aggregated_resources = 0;
static struct work_queue_resources * aggregated_resources_last = 0;
static int64_t last_task_received  = -1;
static int64_t manual_cores_option = 1;
static int64_t manual_disk_option = 0;
static int64_t manual_memory_option = 0;
static int64_t manual_gpus_option = 0;

static int64_t cores_allocated = 0;
static int64_t memory_allocated = 0;
static int64_t disk_allocated = 0;
static int64_t gpus_allocated = 0;
// do not send consecutive resource updates in less than this many seconds
static int send_resources_interval = 120;

// Foreman mode global variables
static struct work_queue *foreman_q = NULL;

// Forked task related
static struct itable *active_tasks = NULL;
static struct itable *stored_tasks = NULL;
static struct list *waiting_tasks = NULL;
static struct itable *results_to_be_sent = NULL;

static int results_to_be_sent_msg = 0;         //Flag to indicate we have sent an available_results message.

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

void reset_idle_timer()
{
	idle_stoptime = time(0) + idle_timeout;
}

/* Resources related tasks */

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

void resources_measure_all(struct work_queue_resources *local, struct work_queue_resources *aggr)
{
	resources_measure_locally(local);

	if(worker_mode == WORKER_MODE_FOREMAN)
	{
		aggregate_workers_resources(foreman_q, aggregated_resources);
		aggregated_resources->disk.total = local->disk.total;
		aggregated_resources->disk.inuse = local->disk.inuse; 

	}
	else
	{
		memcpy(aggr, local, sizeof(struct work_queue_resources));
	}
}

static void send_resource_update( struct link *master, int force_update )
{
	static time_t last_stop_time = 0;

	time_t stoptime = time(0) + active_timeout;

	if(!force_update && (stoptime - last_stop_time < send_resources_interval))
		return;

	resources_measure_all(local_resources, aggregated_resources);

	/* send updates only if resources changed, and if the master may not ask
	 * about results (to avoid deadlocks) */
	if(!force_update && (stoptime - last_stop_time < send_resources_interval))
	{
		return;
	}

	int normal_update = 0;
	if(!results_to_be_sent_msg && memcmp(aggregated_resources_last,aggregated_resources,sizeof(struct work_queue_resources)))
	{
		normal_update = 1;
	}

	aggregated_resources->tag = last_task_received;

	if(force_update || normal_update) {
		work_queue_resources_send(master,aggregated_resources,stoptime);
		memcpy(aggregated_resources_last,aggregated_resources,sizeof(*aggregated_resources));
		last_stop_time = stoptime;
	}
}

/* End of resources related tasks */

static void report_worker_ready( struct link *master )
{
	char hostname[DOMAIN_NAME_MAX];
	domain_name_cache_guess(hostname);
	send_master_message(master,"workqueue %d %s %s %s %s\n",WORK_QUEUE_PROTOCOL_VERSION,hostname,os_name,arch_name,CCTOOLS_VERSION);
	send_resource_update(master,1);
}

// XXX This function modifies the taskname, which is not friendly to the caller.

int link_file_in_workspace( const char *localname, char *taskname, const char *workspace, int into) {
	int result = 1;
	struct stat st;
	
	const char *cache_name;
	char workspace_name[WORK_QUEUE_LINE_MAX];
	
	cache_name = localname;
	while(!strncmp(cache_name, "./", 2)) {
		cache_name += 2;
	}
	
	char *cur_pos = taskname;
	while(!strncmp(cur_pos, "./", 2)) {
		cur_pos += 2;
	}
	sprintf(workspace_name, "%s/%s", workspace, cur_pos);
	
	const char *targetname, *sourcename;

	if(into) {
		sourcename = cache_name;
		targetname = workspace_name;
	} else {
		sourcename = workspace_name;
		targetname = cache_name;
	}

	if(stat((char*)sourcename, &st)) {
		debug(D_WQ, "Could not link %s %s workspace (does not exist)", sourcename, into?"into":"from");
		return 0;
	}
	
	if(S_ISDIR(st.st_mode)) {
		DIR *sourcedir = opendir(sourcename);
		struct dirent *d;
		char dir_localname[WORK_QUEUE_LINE_MAX];
		char dir_taskname[WORK_QUEUE_LINE_MAX];
		if(!sourcedir) {
			debug(D_WQ, "Could not open directory %s for reading (%s)\n", targetname, strerror(errno));
			return 1;
		}
		
		mkdir(targetname, 0700);
		
		while((d = readdir(sourcedir))) {
			if(!strcmp(d->d_name, ".") || !strcmp(d->d_name, ".."))
			{	continue;	}
			
			sprintf(dir_localname, "%s/%s", localname, d->d_name);
			sprintf(dir_taskname, "%s/%s", taskname, d->d_name);
			result &= link_file_in_workspace(dir_localname, dir_taskname, workspace, into);
		}
		closedir(sourcedir);
	} else {
		debug(D_WQ, "linking file %s %s workspace %s as %s\n", cache_name, into?"into":"from", workspace, workspace_name);

		cur_pos = strrchr(targetname, '/');
		if(cur_pos) {
			*cur_pos = '\0';
			if(!create_dir(targetname, 0700)) {
				debug(D_WQ, "Could not create directory - %s (%s)\n", targetname, strerror(errno));
				return 1;
			}
			*cur_pos = '/';
		}
		
		if(link(sourcename, targetname)) {
			debug(D_WQ, "Could not link file %s -> %s (%s)\n", sourcename, targetname, strerror(errno));
			if(errno == EEXIST)	{ 
				//if the destination already exists, it isn't WQ's fault. So don't treat as failure.
				return 1; 			
			} 
			
			if((errno == EXDEV || errno == EPERM) && symlinks_enabled) {
				//use absolute path when symlinking. Else link will point to a 
				//file relative to current directory.	
				char *cwd = path_getcwd();	
				char *absolute_sourcename = string_format("%s/%s", cwd, sourcename);	
				free(cwd);	
				debug(D_WQ, "symlinking file %s -> %s\n", absolute_sourcename, targetname);
				if(symlink(absolute_sourcename, targetname)) {
					debug(D_WQ, "Could not symlink file %s -> %s (%s)\n", absolute_sourcename, targetname, strerror(errno));
					free(absolute_sourcename);	
					return 0;
				}
				free(absolute_sourcename);	
			} else {
				return 0;
			}
		}
	}
	
	return result;
}

static int start_task(struct work_queue_task *t)
{
	struct work_queue_process *ti = work_queue_process_create(t);
	work_queue_process_execute(t->command_line,ti);
	
	if(ti->pid < 0) {
		fprintf(stderr, "work_queue_worker: failed to fork task. Shutting down worker...\n");
		work_queue_process_delete(ti);
		abort_flag = 1;
		return 0;
	}

	ti->status = 0;
		
	if(t->cores < 0 && t->memory < 0 && t->disk < 0 && t->gpus < 0) {
		t->cores = MAX((double)local_resources->cores.total/(double)local_resources->workers.total, 1);
		t->memory = MAX((double)local_resources->memory.total/(double)local_resources->workers.total, 0);
		t->disk = MAX((double)local_resources->disk.total/(double)local_resources->workers.total, 0);
		t->gpus = MAX((double)local_resources->gpus.total/(double)local_resources->workers.total, 0);
	} else {
		// Otherwise use any values given, and assume the task will take "whatever it can get" for unlabeled resources
		t->cores = MAX(t->cores, 0);
		t->memory = MAX(t->memory, 0);
		t->disk = MAX(t->disk, 0);
		t->gpus = MAX(t->gpus, 0);
	}

	cores_allocated += t->cores;
	memory_allocated += t->memory;
	disk_allocated += t->disk;
	gpus_allocated += t->gpus;

	itable_insert(active_tasks, ti->pid, ti);

	return 1;
}

static void report_task_complete(struct link *master, struct work_queue_process *ti)
{
	int64_t output_length;
	struct stat st;

	if(ti->pid) {
		fstat(ti->output_fd, &st);
		output_length = st.st_size;
		lseek(ti->output_fd, 0, SEEK_SET);
		send_master_message(master, "result %d %lld %llu %d\n", ti->status, (long long) output_length, (unsigned long long) ti->execution_end-ti->execution_start, ti->task->taskid);
		link_stream_from_fd(master, ti->output_fd, output_length, time(0)+active_timeout);
		
		cores_allocated -= ti->task->cores;
		memory_allocated -= ti->task->memory;
		disk_allocated -= ti->task->disk;
		gpus_allocated -= ti->task->gpus;

		total_task_execution_time += (ti->execution_end - ti->execution_start);
		total_tasks_executed++;
	} else {
		struct work_queue_task *t = ti->task;
		if(t->output) {
			output_length = strlen(t->output);
		} else {
			output_length = 0;
		}
		send_master_message(master, "result %d %lld %llu %d\n",t->return_status, (long long) output_length, (unsigned long long) t->cmd_execution_time, t->taskid);
		if(output_length) {
			link_putlstring(master, t->output, output_length, time(0)+active_timeout);
		}

		total_task_execution_time += t->cmd_execution_time;
		total_tasks_executed++;
	}
	
}

static int itable_pop(struct itable *t, uint64_t *key, void **value)
{
	itable_firstkey(t);

	int result = itable_nextkey(t, key, value);

	if(result)
	{
		itable_remove(t, *key);
	}

	return result;
}

static int report_tasks(struct link *master, struct itable *tasks_infos, int max_count)
{
	if(max_count < 0)
	{
		max_count = itable_size(tasks_infos);
	}

	struct work_queue_process *ti;
	uint64_t          taskid;

	int count = 0;
	while((count < max_count) && itable_pop(tasks_infos, &taskid, (void **) &ti))
	{
		report_task_complete(master, ti);
		work_queue_task_delete(ti->task);
		work_queue_process_delete(ti);
		count++;
	}

	send_master_message(master, "end\n");

	if(itable_size(tasks_infos) == 0)
	{
		results_to_be_sent_msg = 0;
	}

	send_resource_update(master, 1);

	return count;
}

static int handle_tasks(struct link *master) {
	struct work_queue_process *ti;
	pid_t pid;
	int result = 0;
	struct work_queue_file *tf;
	char dirname[WORK_QUEUE_LINE_MAX];
	int status;
	
	itable_firstkey(active_tasks);
	while(itable_nextkey(active_tasks, (uint64_t*)&pid, (void**)&ti)) {
		result = wait4(pid, &status, WNOHANG, &ti->rusage);
		if(result) {
			if(result < 0) {
				debug(D_WQ, "Error checking on child process (%d).", ti->pid);
				abort_flag = 1;
				return 0;
			}
			if (!WIFEXITED(status)){
				debug(D_WQ, "Task (process %d) did not exit normally.\n", ti->pid);
				ti->status = WTERMSIG(status);
			} else {
				ti->status = WEXITSTATUS(status);
			}
			
			ti->execution_end = timestamp_get();
			
			itable_remove(active_tasks, ti->pid);
			itable_firstkey(active_tasks);
			
			if(WIFEXITED(status)) {
				sprintf(dirname, "t.%d", ti->task->taskid);
				list_first_item(ti->task->output_files);
				while((tf = (struct work_queue_file *)list_next_item(ti->task->output_files))) {
					if(!link_file_in_workspace(tf->payload, tf->remote_name, dirname, 0)) {
						debug(D_NOTICE, "File %s does not exist and is output of task %d.", (char*)tf->remote_name, ti->task->taskid);
					}
				}
			}

			itable_insert(results_to_be_sent, ti->task->taskid, ti);
		}
		
	}
	return 1;
}

static int check_disk_space_for_filesize(int64_t file_size) {
	uint64_t disk_avail, disk_total;

	// Check available disk space
	if(disk_avail_threshold > 0) {
		disk_info_get(".", &disk_avail, &disk_total);
		if(file_size > 0) {	
			if((uint64_t)file_size > disk_avail || (disk_avail - file_size) < disk_avail_threshold) {
			debug(D_WQ, "Incoming file of size %"PRId64" MB will lower available disk space (%"PRIu64" MB) below threshold (%"PRIu64" MB).\n", file_size/MEGA, disk_avail/MEGA, disk_avail_threshold/MEGA);
			return 0;
			}
		} else {
			if(disk_avail < disk_avail_threshold) {
			debug(D_WQ, "Available disk space (%"PRIu64" MB) lower than threshold (%"PRIu64" MB).\n", disk_avail/MEGA, disk_avail_threshold/MEGA);
			return 0;
			}	
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

int setup_sandbox( struct work_queue_process *p, const char *dirname )
{
	struct work_queue_file *f;
	char taskname[WORK_QUEUE_LINE_MAX];

	list_first_item(p->task->input_files);
	while((f = list_next_item(p->task->input_files))) {
		if(f->type == WORK_QUEUE_DIRECTORY) {
			sprintf(taskname, "t.%d/%s", p->task->taskid, f->remote_name);
			if(!create_dir(taskname, 0700)) {
				debug(D_NOTICE, "Directory %s could not be created and is needed by task %d.", taskname, p->task->taskid);
				return 0;
			}
		} else if(!link_file_in_workspace(f->payload, f->remote_name, dirname, 1)) {
			debug(D_NOTICE, "File %s does not exist and is needed by task %d.", (char*)f->payload, p->task->taskid);
			return 0;
		}
	}

	return 1;
}

static int do_task( struct link *master, int taskid, time_t stoptime )
{
	char line[WORK_QUEUE_LINE_MAX];
	char filename[WORK_QUEUE_LINE_MAX];
	char localname[WORK_QUEUE_LINE_MAX];
	char taskname[WORK_QUEUE_LINE_MAX];
	char dirname[WORK_QUEUE_LINE_MAX];
	int n, flags, length;

	struct work_queue_task *task = work_queue_task_create(0);

	task->taskid = taskid;

	sprintf(dirname, "t.%d" , taskid);
	mkdir(dirname, 0700);

	while(recv_master_message(master,line,sizeof(line),stoptime)) {
	  	if(sscanf(line,"cmd %d",&length)==1) {
			char *cmd = malloc(length+1);
			link_read(master,cmd,length,stoptime);
			cmd[length] = 0;
			work_queue_task_specify_command(task,cmd);
			debug(D_WQ,"rx from master: %s",cmd);
			free(cmd);
		} else if(sscanf(line,"infile %s %s %d", filename, taskname, &flags)) {
			sprintf(localname, "cache/%s", filename);
			work_queue_task_specify_file(task, localname, taskname, WORK_QUEUE_INPUT, flags);
		} else if(sscanf(line,"outfile %s %s %d", filename, taskname, &flags)) {
			sprintf(localname, "cache/%s", filename);
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
		} else if(!strcmp(line,"end")) {
		       	break;
		} else {
			debug(D_WQ|D_NOTICE,"invalid command from master: %s",line);
			work_queue_task_delete(task);
			delete_dir(dirname);
			return 0;
		}
	}

	last_task_received = task->taskid;

	// Measure and report resources, given that disk space decreased given the
	// task input files.
	send_resource_update(master, 1);

	// Every received task goes into stored_tasks.
	struct work_queue_process *p = work_queue_process_create(task);
	itable_insert(stored_tasks,taskid,p);

	if(worker_mode==WORKER_MODE_FOREMAN) {
		work_queue_submit_internal(foreman_q,task);
	} else {
		// XXX sandbox setup should be done in task execution,
		// so that it can be returned cleanly as a failure to execute.
		if(!setup_sandbox(p,dirname)) {
			work_queue_process_delete(p);
			work_queue_task_delete(task);
			delete_dir(dirname);
			return 0;
		}
		list_push_tail(waiting_tasks, task);
	}
	return 1;
}

static int do_put(struct link *master, char *filename, int64_t length, int mode) {
	char cached_filename[WORK_QUEUE_LINE_MAX];
	char *cur_pos;
	
	debug(D_WQ, "Putting file %s into workspace\n", filename);
	if(!check_disk_space_for_filesize(length)) {
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
Note that a kill message from the master is used for every case
where a task is to be removed, whether it is waiting, running,
of finished.  Regardless of the state, we kill the process and
remove all of the associated files and other state.
*/

static int do_kill(int taskid)
{
	struct work_queue_process *p;

	p = itable_remove(stored_tasks, taskid);
	if(!p) {
		debug(D_WQ,"master requested kill of task %d which does not exist!",taskid);
		return 1;
	}
	
	if(worker_mode == WORKER_MODE_FOREMAN) {
		work_queue_cancel_by_taskid(foreman_q, taskid);
	} else {
		work_queue_process_kill(p);

	}

	char *dirname = string_format("t.%d",taskid);
	delete_dir(dirname);
	free(dirname);

	// XXX also remove from the waiting list!
	itable_remove(results_to_be_sent, p->task->taskid);
	itable_remove(active_tasks, p->pid);

	// XXX double-check when allocations are made and removed.	
	cores_allocated -= p->task->cores;
	memory_allocated -= p->task->memory;
	disk_allocated -= p->task->disk;
	gpus_allocated -= p->task->gpus;

	// XXX process_delete and task_delete should be coupled.
	work_queue_process_delete(p);
	work_queue_task_delete(p->task);

	return 1;
}

static void kill_all_tasks() {
	struct work_queue_process *p;
	uint64_t taskid;

	itable_firstkey(stored_tasks);
	while(itable_nextkey(stored_tasks,&taskid,(void**)&p)) {
		do_kill(taskid);
	}

	// These should have already gone to zero,
	// but let's be extra careful.

	cores_allocated = 0;
	memory_allocated = 0;
	disk_allocated = 0;
	gpus_allocated = 0;
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
	}

	debug(D_WQ,"cleaning up workspace %s",workspace);
	delete_dir_contents(workspace);

	if(released_by_master) {
		released_by_master = 0;
	} else {
		sleep(5);
	}
}

static int path_within_workspace(const char *path, const char *workspace) {
	if(!path) return 0;

	char absolute_workspace[PATH_MAX+1];
	if(!realpath(workspace, absolute_workspace)) {
		debug(D_WQ, "Failed to resolve the absolute path of workspace - %s: %s", workspace, strerror(errno));
		return 0;
	}	

	char *p;
	if(path[0] == '/') {
		p = strstr(path, absolute_workspace);
		if(p != path) {
			return 0;
		}
	}

	char absolute_path[PATH_MAX+1];
	char *tmp_path = xxstrdup(path);

	int rv = 1;
	while((p = strrchr(tmp_path, '/')) != NULL) {
		//debug(D_WQ, "Check if %s is within workspace - %s", tmp_path, absolute_workspace);
		*p = '\0';
		if(realpath(tmp_path, absolute_path)) {
			p = strstr(absolute_path, absolute_workspace);
			if(p != absolute_path) {
				rv = 0;
			}
			break;
		} else {
			if(errno != ENOENT) {
				debug(D_WQ, "Failed to resolve the absolute path of %s: %s", tmp_path, strerror(errno));
				rv = 0;
				break;
			}
		}
	}

	free(tmp_path);
	return rv;
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
			if(path_within_workspace(filename, workspace)) {
				r = do_put(master, filename, length, mode);
			} else {
				debug(D_WQ, "Path - %s is not within workspace %s.", filename, workspace);
				r= 0;
			}
                } else if(sscanf(line, "url %s %" SCNd64 " %o", filename, &length, &mode) == 3) {
                        r = do_url(master, filename, length, mode);
		} else if(sscanf(line, "unlink %s", filename) == 1) {
			if(path_within_workspace(filename, workspace)) {
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
			report_tasks(master, results_to_be_sent, n);
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

static int check_for_resources(struct work_queue_task *t) {
	int64_t cores_used, disk_used, mem_used, gpus_used;
	int ok = 1;
	
	// If resources used have not been specified, treat the task as consuming the entire real worker
	if(t->cores < 0 && t->memory < 0 && t->disk < 0 && t->gpus < 0) {
		cores_used = MAX((double)local_resources->cores.total/(double)local_resources->workers.total, 1);
		mem_used = MAX((double)local_resources->memory.total/(double)local_resources->workers.total, 0);
		disk_used = MAX((double)local_resources->disk.total/(double)local_resources->workers.total, 0);
		gpus_used = MAX((double)local_resources->gpus.total/(double)local_resources->workers.total, 0);
	} else {
		// Otherwise use any values given, and assume the task will take "whatever it can get" for unlabled resources
		cores_used = MAX(t->cores, 0);
		mem_used = MAX(t->memory, 0);
		disk_used = MAX(t->disk, 0);
		gpus_used = MAX(t->gpus, 0);
	}
	
	if(cores_allocated + cores_used > local_resources->cores.total) {
		ok = 0;
	}
	
	if(memory_allocated + mem_used > local_resources->memory.total) {
		ok = 0;
	}
	
	if(disk_allocated + disk_used > local_resources->disk.total) {
		ok = 0;
	}

	if(gpus_allocated + gpus_used > local_resources->gpus.total) {
		ok = 0;
	}
	
	return ok;
}

static void work_for_master(struct link *master) {
	timestamp_t msec;
	sigset_t mask;

	if(!master) {
		return;
	}

	debug(D_WQ, "working for master at %s:%d.\n", master_addr, master_port);

	sigemptyset(&mask);
	sigaddset(&mask, SIGCHLD);
	
	reset_idle_timer();

	time_t volatile_stoptime = time(0) + 60;
	// Start serving masters
	while(!abort_flag) {

		if(time(0) > idle_stoptime) {
			debug(D_NOTICE, "disconnecting from %s:%d because I did not receive any task in %d seconds (--idle-timeout).\n", master_addr,master_port,idle_timeout);
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
		
		// There is a race condition where if a child finishes while the worker is handling tasks, SIGCHLD is lost and does not interrupt
		// the poll in link_usleep_mask().  For short-running tasks this can cause a drastic slowdown.  This adapts the amount of time
		// spent in the link to be close to the average runtime for short tasks, so short-running tasks aren't unduly impacted.
		if(total_tasks_executed > 0) {
			msec = MAX(10, total_task_execution_time/total_tasks_executed);  // minimum check time of 10 milliseconds
			msec = MIN(msec, 5000);  // maximum wait time of 5 seconds in between checks.
		} else {
			msec = 1000;
		}

		int master_activity = link_usleep_mask(master, msec*1000, &mask, 1, 0);

		if(master_activity < 0) break;
		
		send_resource_update(master,0);
		
		int ok = 1;
		if(master_activity) {
			ok &= handle_master(master);
		}

		ok &= handle_tasks(master);

		if(!results_to_be_sent_msg && itable_size(results_to_be_sent) > 0)
		{
			send_master_message(master, "available_results\n");
			results_to_be_sent_msg = 1;
		}

		ok &= check_disk_space_for_filesize(0);

		if(ok) {
			int visited = 0;
			while(list_size(waiting_tasks) > visited && cores_allocated < local_resources->cores.total) {
				struct work_queue_task *t;
				
				t = list_pop_head(waiting_tasks);
				if(t && check_for_resources(t)) {
					start_task(t);
					//Update the master with the resources now in use
					send_resource_update(master, 1);
				} else {
					list_push_tail(waiting_tasks, t);
					visited++;
				}
			}

			// If all resources are free, but we cannot execute any of the
			// waiting tasks, then disconnect so that the master gets the tasks
			// back. (Imagine for example that some other process in the host
			// running the worker used so much memory or disk, that now no task
			// cannot be scheduled.) Note we check against stored_tasks, and
			// not active_tasks, so that we do not disconnect if there are
			// results waiting to be sent back (which in turn may free some
			// disk).  Note also this is a short-term solution. In the long
			// term we want the worker to report to the master something like
			// 'task not done'.
			if(list_size(waiting_tasks) > 0 && itable_size(stored_tasks) == 0)
			{
				debug(D_WQ, "No task can be executed with the available resources.\n");
				ok = 0;
			}
		}

		if(!ok) break;

		//Reset idle_stoptime if something interesting is happening at this worker.
		if(list_size(waiting_tasks) > 0 || itable_size(stored_tasks) > 0 || itable_size(results_to_be_sent) > 0) {
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
			struct work_queue_process *ti = work_queue_process_create(task);
			itable_insert(results_to_be_sent, task->taskid, ti);
			result = 1;
		}

		if(!results_to_be_sent_msg && itable_size(results_to_be_sent) > 0)
		{
			send_master_message(master, "available_results\n");
			results_to_be_sent_msg = 1;
		}

		send_resource_update(master,0);
		
		if(master_active) {
			result &= handle_master(master);
			reset_idle_timer();
		}

		if(!result) break;
	}
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
		if(!recv_master_message(master, line, sizeof(line),idle_stoptime)) {
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


	report_worker_ready(master);

	if(worker_mode == WORKER_MODE_FOREMAN) {
		foreman_for_master(master);
	} else {
		work_for_master(master);
	}

	last_task_received  = -1;               //Reset last task received flag.

	disconnect_master(master);
	printf("disconnected from master %s:%d\n", host, port );

	return 1;
}

static int serve_master_by_name( const char *catalog_host, int catalog_port, const char *project_regex )
{
	struct list *masters_list = work_queue_catalog_query_cached(catalog_host,catalog_port,project_regex);
	
	debug(D_WQ,"project name %s matches %d masters",project_regex,list_size(masters_list));

	if(list_size(masters_list)==0) return 0;

	// shuffle the list by r items to distribute the load across masters
	int r = rand() % list_size(masters_list);
	int i;
	for(i=0;i<r;i++) {
		list_push_tail(masters_list,list_pop_head(masters_list));
	}

	struct nvpair *nv = list_peek_head(masters_list);
	const char *project = nvpair_lookup_string(nv,"project");
	const char *name = nvpair_lookup_string(nv,"name");
	const char *addr = nvpair_lookup_string(nv,"address");
	int port = nvpair_lookup_integer(nv,"port");

	debug(D_WQ,"selected master with project=%s name=%s addr=%s port=%d",project,name,addr,port);

	return serve_master_by_hostport(addr,port,project);
}

static void handle_abort(int sig)
{
	abort_flag = 1;
}

static void handle_sigchld(int sig)
{
	return;
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
	printf( " %-30s Show this help screen\n", "-h,--help");
}

static int workspace_setup() {
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

	sprintf(workspace, "%s/worker-%d-%d", workdir, (int) getuid(), (int) getpid());
	if(mkdir(workspace, 0700) == -1) {
		return 0;
	} 

	printf( "work_queue_worker: working in %s\n", workspace);
	return 1;
}

static void workspace_cleanup()
{
	if(user_specified_workdir) free(user_specified_workdir);
	if(os_name) free(os_name);
	if(arch_name) free(arch_name);

	if(foreman_q)          work_queue_delete(foreman_q);
	if(active_tasks)       itable_delete(active_tasks);
	if(stored_tasks)       itable_delete(stored_tasks);
	if(results_to_be_sent) itable_delete(results_to_be_sent);

	fprintf(stdout, "work_queue_worker: cleaning up %s\n", workspace);
	delete_dir(workspace);
}

enum {LONG_OPT_DEBUG_FILESIZE = 256, LONG_OPT_VOLATILITY, LONG_OPT_BANDWIDTH,
      LONG_OPT_DEBUG_RELEASE, LONG_OPT_SPECIFY_LOG, LONG_OPT_CORES, LONG_OPT_MEMORY,
      LONG_OPT_DISK, LONG_OPT_GPUS, LONG_OPT_FOREMAN, LONG_OPT_FOREMAN_PORT, LONG_OPT_DISABLE_SYMLINKS,
      LONG_OPT_IDLE_TIMEOUT, LONG_OPT_CONNECT_TIMEOUT};

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
	{"max-mackoff",         required_argument,  0,  'b'},
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
		  	if(copy_file_to_buffer(optarg, &password) < 0) {
			fprintf(stderr,"work_queue_worker: couldn't load password from %s: %s\n",optarg,strerror(errno));
				return 1;
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
		case 'h':
			show_help(argv[0]);
			return 0;
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
		if(!strcmp(foreman_name,project_regex) == 0) {
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

	if(!workspace_setup()) {
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

	active_tasks = itable_create(0);
	stored_tasks = itable_create(0);
	waiting_tasks = list_create();
	results_to_be_sent = itable_create(0);

	if(!check_disk_space_for_filesize(0)) {
		fprintf(stderr,"work_queue_worker: %s has less than minimum disk space %"PRIu64" MB\n",workspace,disk_avail_threshold);
		return 1;
	}

	local_resources = work_queue_resources_create();
	aggregated_resources = work_queue_resources_create();
	aggregated_resources_last = work_queue_resources_create();

	resources_measure_locally(local_resources);

	int backoff_interval = init_backoff_interval;
	time_t connect_stoptime = time(0) + connect_timeout;

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
			debug(D_NOTICE,"stopping: could not connect after %d seconds (--connect-timeout)",connect_timeout);
			break;
		}

		sleep(backoff_interval);
	}

	workspace_cleanup();

	return 0;
}

/* vim: set noexpandtab tabstop=4: */
