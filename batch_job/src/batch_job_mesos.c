/*
  Copyright (c) 2016- The University of Notre Dame.
  This software is distributed under the GNU General Public License.
  See the file COPYING for details. 
*/

/*
 * Principle of Operation:
 * 
 * Each time when makeflow submit a task, the information of the task is written into 
 * mesos_task_info file. Meanwhile, the makeflow mesos scheduler (makeflow/src/mf_mesos_scheduler) 
 * keep polling mesos_task_info, try to find if there are new tasks ready to launch on mesos. 
 * 
 * After the task is complete, the makeflow mesos scheduler writes the final state
 * of the task to mesos_task_state. And the makeflow is keep monitoring this file and 
 * collect all finished tasks.   
 *
 */

#include "batch_job.h"
#include "batch_job_internal.h"
#include "debug.h"
#include "process.h"
#include "macros.h"
#include "stringtools.h"
#include "path.h"
#include "xxmalloc.h"
#include "jx.h"
#include "jx_print.h"
#include "itable.h"
#include "mesos_task.h"
#include "text_list.h"

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <signal.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <time.h>

#define MAX_BUF_SIZE 4096
#define FILE_TASK_INFO "mesos_task_info"
#define FILE_TASK_STATE "mesos_task_state"
#define MESOS_DONE_FILE "mesos_done"

static int counter = 0;
static struct itable *finished_tasks = NULL;
static int is_mesos_py_path_known = 0;
static int is_mesos_master_known = 0;
static int is_scheduler_running = 0;
static const char *mesos_py_path = NULL;
static const char *mesos_master = NULL;
static const char *mesos_preload = NULL;

static void start_mesos_scheduler(struct batch_queue *q)
{

	pid_t mesos_pid;
	mesos_pid = fork();			

	if (mesos_pid > 0) {

		debug(D_INFO, "Start makeflow mesos scheduler.");

	} else if (mesos_pid == 0) {

		char *mesos_cwd;
		mesos_cwd = path_getcwd();
	
		char exe_path[MAX_BUF_SIZE];
	
		if(readlink("/proc/self/exe", exe_path, MAX_BUF_SIZE) == -1) {
			fatal("read \"proc/self/exe\" fail\n");
		}
	
		char exe_dir_path[MAX_BUF_SIZE];
		path_dirname(exe_path, exe_dir_path);
	
	    char *exe_py_path = string_format("%s/mf_mesos_scheduler", exe_dir_path);
		char *ld_preload_str = NULL;
		char *python_path = NULL;
	
		if(mesos_preload) {
			ld_preload_str = string_format("LD_PRELOAD=%s", mesos_preload);
		}

		if(mesos_py_path) {
			char *mesos_python_path = xxstrdup(mesos_py_path);
			python_path = string_format("PYTHONPATH=%s", mesos_python_path);
		}	
		
		char *envs[3];
		if(ld_preload_str && python_path) {
			envs[0] = ld_preload_str;
			envs[1] = python_path;
		} else if(!ld_preload_str && python_path) {
			envs[0] = python_path;
		} else if(ld_preload_str && !python_path) {
			envs[0] = ld_preload_str;
		} else {
			envs[0] = NULL;
		}

		const char *batch_log_name = q->logfile;  

		int mesos_fd = open(batch_log_name, O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);
		if (mesos_fd == -1) {
			fatal("Failed to open %s \n", batch_log_name);
	    }

	    if (dup2(mesos_fd, 1) == -1) {
			fatal("Failed to duplicate file descriptor: %s\n", strerror(errno));
	   	}

		if (dup2(mesos_fd, 2) == -1) {
			fatal("Failed to duplicate file descriptor: %s\n", strerror(errno));
	   	}

	    close(mesos_fd);
		

		execle("/usr/bin/python", "python", exe_py_path, mesos_cwd, 
			mesos_master, (char *) 0, envs);

		exit(errno);

	} else {

		fatal("mesos batch system couldn't create new process: %s\n", strerror(errno));

	}

}


static batch_job_id_t batch_job_mesos_submit (struct batch_queue *q, const char *cmd, 
	const char *extra_input_files, const char *extra_output_files, 
	struct jx *envlist, const struct rmsummary *resources )
{

	// Get the path to mesos python site-packages
	if (!is_mesos_py_path_known) {
		mesos_py_path = batch_queue_get_option(q, "mesos-path");
		if (mesos_py_path != NULL) {
			debug(D_INFO, "Get mesos_path %s from command line\n", mesos_py_path);
		}
		is_mesos_py_path_known = 1;
	}

	// Get the mesos master address
	if (!is_mesos_master_known) {
		mesos_master = batch_queue_get_option(q, "mesos-master");
		if (mesos_master == NULL) {
			fatal("Please specify the hostname of mesos master by using --mesos-master");
		} else {
			debug(D_INFO, "Get mesos_path %s from command line\n", mesos_py_path);
			is_mesos_master_known = 1;
		}
	}

	mesos_preload = batch_queue_get_option(q, "mesos-preload");

	if (is_mesos_py_path_known && 
		is_mesos_master_known && 
		!is_scheduler_running ) {
		// start mesos scheduler if it is not running
		start_mesos_scheduler(q);
		is_scheduler_running = 1;
	}

	int task_id = ++counter;

	debug(D_BATCH, "task %d is ready", task_id);
	struct batch_job_info *info = calloc(1, sizeof(*info));
	info->started = time(0);
	info->submitted = time(0);
	itable_insert(q->job_table, task_id, info);

	// write the ready task information as  
	// "task_id, task_cmd, inputs, outputs" to 
	// mesos_task_info, which will be scanned by 
	// mf_mesos_scheduler later. 

	FILE *task_info_fp;

	if(access(FILE_TASK_INFO, F_OK) != -1) {
		task_info_fp = fopen(FILE_TASK_INFO, "a+");
	} else {
		task_info_fp = fopen(FILE_TASK_INFO, "w+");
	}
	
	struct mesos_task *mt = mesos_task_create(task_id, cmd, extra_input_files, extra_output_files);

	fprintf(task_info_fp, "%d,%s,", mt->task_id, mt->task_cmd);

	if (extra_input_files != NULL && strlen(extra_input_files) != 0) {

		int j = 0;
		int num_input_files = text_list_size(mt->task_input_files);
		for (j = 0; j < (num_input_files-1); j++) {
			fprintf(task_info_fp, "%s ", text_list_get(mt->task_input_files, j));
		}
		fprintf(task_info_fp, "%s,", text_list_get(mt->task_input_files, num_input_files-1));

	} else {
		fprintf(task_info_fp, ",");
	}
    
	if (extra_output_files != NULL && strlen(extra_output_files) != 0) {
		int j = 0;
		int num_output_files = text_list_size(mt->task_output_files);
		for (j = 0; j < (num_output_files-1); j++) {
			fprintf(task_info_fp, "%s ", text_list_get(mt->task_output_files, j));
		}
		fprintf(task_info_fp, "%s,",text_list_get(mt->task_output_files, num_output_files-1));
	} else {
		fprintf(task_info_fp, ",");	
	}

	// The default resource requirements for each task
	int64_t cores = -1;
	int64_t memory = -1;
	int64_t disk = -1;

	if (resources) {
		cores  = resources->cores  > -1 ? resources->cores  : cores;
		memory = resources->memory > -1 ? resources->memory : memory;
		disk   = resources->disk   > -1 ? resources->disk   : disk;
	}

	fprintf(task_info_fp, "%" PRId64 ",%" PRId64 ",%" PRId64 ",", cores, memory, disk);

	fputs("submitted\n", task_info_fp);

	mesos_task_delete(mt);
	fclose(task_info_fp);

	return task_id;
}

static batch_job_id_t batch_job_mesos_wait (struct batch_queue * q, struct batch_job_info * info_out, time_t stoptime)
{
		
	char line[MAX_BUF_SIZE];
	FILE *task_state_fp;
	int last_pos = 0;
	int curr_pos = 0;
	int read_len = 0;

	if(!finished_tasks) {
		finished_tasks = itable_create(0);
	}

	while(access(FILE_TASK_STATE, F_OK) == -1) {}

	task_state_fp = fopen(FILE_TASK_STATE, "r");

	while(1) {

		char *task_id_str;
		char *task_stat_str;
		const char *task_exit_code;
		int task_id;
				
		while(fgets(line, MAX_BUF_SIZE, task_state_fp) != NULL) {
			
			curr_pos = ftell(task_state_fp);
			read_len = curr_pos - last_pos;
			last_pos = curr_pos;

			// trim the newline character
			if (line[read_len-1] == '\n') {
				line[read_len-1] = '\0';
				--read_len;
			}

			task_id_str = strtok(line, ",");
			task_id = atoi(task_id_str);

			// There is a new task finished
			if(itable_lookup(finished_tasks, task_id) == NULL) {

				struct batch_job_info *info = itable_remove(q->job_table, task_id);
			    	
				info->finished = time(0);
				task_stat_str = strtok(NULL, ",");

				if (strcmp(task_stat_str, "finished") == 0) {
					info->exited_normally = 1;
				} else if (strcmp(task_stat_str, "failed") == 0) {
					info->exited_normally = 0;
					task_exit_code = strtok(NULL, ",");

					// 444 is an arbitrary exit code set in mf_mesos_scheduler, 
					// which means the task failed to retrieve the outpus 
					if(atoi(task_exit_code) == 444) {
						info->exit_code = 444;
						debug(D_BATCH, "Task %s failed to retrieve the output.", task_id_str);
					}
					info->exit_code = atoi(task_exit_code);
				} else {
					info->exited_normally = 0;
				}

				memcpy(info_out, info, sizeof(*info));
				free(info);
				fclose(task_state_fp);

				int itable_val = 1;
				itable_insert(finished_tasks, task_id, &itable_val);

				return task_id;
			}
		}
		sleep(1);

		if(stoptime != 0 && time(0) >= stoptime) {
			fclose(task_state_fp);
			return -1;
		}
	}

}

/*
 * To remove a batch job, we mark the task state as "aborting", and return. Then 
 * the mesos scheduler will try to terminate the corresponding executors. This 
 * method does not guarantee the termination of executors, but all executors would be
 * terminated before the mesos scheduler stop.
 */
static int batch_job_mesos_remove (struct batch_queue *q, batch_job_id_t jobid)
{
	struct batch_job_info *info = itable_lookup(q->job_table, jobid);
	info->finished = time(0);
	info->exited_normally = 0;
	info->exit_signal = 0;
	// append the new task state to the "mesos_task_info" file
	FILE *task_info_fp;	
	task_info_fp = fopen(FILE_TASK_INFO, "a+");
	if(task_info_fp == NULL) {
		fatal("can not open \"mesos_task_info\n ");
	}
	fprintf(task_info_fp, "%" PRIbjid ",,,,,,,aborting\n", jobid);
	fclose(task_info_fp);
	return 0;
}

static int batch_queue_mesos_create (struct batch_queue *q)
{
	batch_queue_set_feature(q, "mesos_job_queue", NULL);
	batch_queue_set_feature(q, "batch_log_name", "%s.mesoslog");
	batch_queue_set_feature(q, "batch_log_transactions", "%s.tr");
	batch_queue_set_feature(q, "autosize", "yes");

	return 0;
}

static int batch_queue_mesos_free(struct batch_queue *q)
{
	FILE *fp;
	fp = fopen(MESOS_DONE_FILE, "w");

	if(fp == NULL) {
		fatal("Fail to clean up batch queue. %s\n", strerror(errno));
	}

	int batch_queue_abort_flag = atoi(batch_queue_get_option(q, "batch-queue-abort-flag"));
	int batch_queue_failed_flag = atoi(batch_queue_get_option(q, "batch-queue-failed-flag"));

	if(batch_queue_abort_flag) {
		fprintf(fp, "aborted");
	} else if(batch_queue_failed_flag) {
		fprintf(fp, "failed");
	} else {
		fprintf(fp, "finished");
	}

	fclose(fp);
	return 0;
}

batch_queue_stub_port(mesos);
batch_queue_stub_option_update(mesos);

batch_fs_stub_chdir(mesos);
batch_fs_stub_getcwd(mesos);
batch_fs_stub_mkdir(mesos);
batch_fs_stub_putfile(mesos);
batch_fs_stub_rename(mesos);
batch_fs_stub_stat(mesos);
batch_fs_stub_unlink(mesos);

const struct batch_queue_module batch_queue_mesos = {
	BATCH_QUEUE_TYPE_MESOS,
	"mesos",

	batch_queue_mesos_create,
	batch_queue_mesos_free,
	batch_queue_mesos_port,
	batch_queue_mesos_option_update,

	{
		batch_job_mesos_submit,
		batch_job_mesos_wait,
		batch_job_mesos_remove,
	},

	{
		batch_fs_mesos_chdir,
		batch_fs_mesos_getcwd,
		batch_fs_mesos_mkdir,
		batch_fs_mesos_putfile,
		batch_fs_mesos_rename,
		batch_fs_mesos_stat,
		batch_fs_mesos_unlink,
	},
};

/* vim: set noexpandtab tabstop=8: */
