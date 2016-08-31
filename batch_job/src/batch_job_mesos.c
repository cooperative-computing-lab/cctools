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
#include "text_list.h"
#include "mesos_task.h"

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <signal.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <time.h>

#define FILE_TASK_INFO "mesos_task_info"
#define FILE_TASK_STATE "mesos_task_state"

/*
 * Principle of Operation:
 * 
 * Each time when makeflow submit a task, the information of the task is written into 
 * mesos_task_info file. Meanwhile, the makeflow mesos scheduler keep polling 
 * mesos_task_info, try to find if there is new task is ready to launch on mesos. 
 * 
 * After the task is complete on, the makeflow mesos scheduler writes the final state
 * of the task to mesos_task_state. And the makeflow is keep monitoring this file and 
 * collect all finished tasks.   
 *
 */

static int counter = 0;
static struct itable *finished_tasks = NULL;

static batch_job_id_t batch_job_mesos_submit (struct batch_queue *q, const char *cmd, \
		const char *extra_input_files, const char *extra_output_files, \
		struct jx *envlist, const struct rmsummary *resources )
{
	int task_id = ++counter;

	debug(D_BATCH, "task %d is ready", task_id);
	struct batch_job_info *info = malloc(sizeof(*info));
	memset(info, 0, sizeof(*info));
	info->submitted = time(0);
	info->started = time(0);
	itable_insert(q->job_table, task_id, info);

	FILE *task_info_fp;

	if(access(FILE_TASK_INFO, F_OK) != -1) {
		task_info_fp = fopen(FILE_TASK_INFO, "a+");
	} else {
		task_info_fp = fopen(FILE_TASK_INFO, "w+");
	}
	
	struct mesos_task *mt = mesos_task_create(task_id, cmd, extra_input_files, extra_output_files);

	fprintf(task_info_fp, "%d,%s,", mt->task_id, mt->task_cmd);

	// Get the absolut path o Meanwhile, the makeflow mesos scheduler keep polling 
	// mesos_task_info, try to find if there is new task is ready to launch on mesos,. 
	// 
	// After the task is complete on, the makeflow mesos scheduler writes the final state
	// of the task to mesos_task_state. And the makeflow is keep monitoring this file and 
	// collect all finished task. f each input file

	if (extra_input_files != NULL && strlen(extra_input_files) != 0) {

		int j = 0;
		int num_input_files = mt->task_input_files->used_length;
		for (j = 0; j < (num_input_files-1); j++) {
			fprintf(task_info_fp, "%s ", (mt->task_input_files->items)[j]);
		}
		fprintf(task_info_fp, "%s,", (mt->task_input_files->items)[num_input_files-1]);

	} else {
		fputs(",", task_info_fp);
	}
    
	if (extra_output_files != NULL && strlen(extra_output_files) != 0) {
		int j = 0;
		int num_output_files = mt->task_output_files->used_length;
		for (j = 0; j < (num_output_files-1); j++) {
			fprintf(task_info_fp, "%s ", (mt->task_output_files->items)[j]);
		}
		fprintf(task_info_fp, "%s,", (mt->task_output_files->items)[num_output_files-1]);
	} else {
		fputs(",", task_info_fp);
	}
	fputs("submitted\n", task_info_fp);

	mesos_task_delete(mt);
	fclose(task_info_fp);

	return task_id;
}

static batch_job_id_t batch_job_mesos_wait (struct batch_queue * q, struct batch_job_info * info_out, time_t stoptime)
{

		
	char *line = NULL;
	size_t len = 0;
	ssize_t read_len;
	FILE *task_state_fp;

	if(finished_tasks == NULL) {
		finished_tasks = itable_create(0);
	}

	while(access(FILE_TASK_STATE, F_OK) == -1) {}

	while(1) {

		char *task_id_ch;
		char *task_stat_str;
		int task_id;
				
		task_state_fp = fopen(FILE_TASK_STATE, "r");
		while((read_len = getline(&line, &len, task_state_fp)) != -1) {

			// trim the newline character
			if (line[read_len-1] == '\n') {
				line[read_len-1] = '\0';
				--read_len;
			}

			task_id_ch = strtok(line, ",");
			task_id = atoi(task_id_ch);

			// There is a new task finished
			if(itable_lookup(finished_tasks, task_id) == NULL) {
				struct batch_job_info *info = itable_remove(q->job_table, task_id);
			    	
				info->finished = time(0);
				task_stat_str = strtok(NULL, ",");

				if (strcmp(task_stat_str, "finished") == 0) {
					info->exited_normally = 1;
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
			return -1;
		}
	}

}

static int batch_job_mesos_remove (struct batch_queue *q, batch_job_id_t jobid)
{
	struct batch_job_info *info = itable_lookup(q->job_table, jobid);
	info->finished = time(0);
	info->exited_normally = 0;
	info->exit_signal = 0;
	// append the new task state to the "mesos_task_info" file
	char *cmd = string_format("awk -F \',\' \'{if($1==\"%" PRIbjid "\"){gsub(\"submitted\",\"aborting\",$5);print $1\",\"$2\",\"$3\",\"$4\",\"$5}}\' %s >> %s", \
			jobid, FILE_TASK_INFO, FILE_TASK_INFO);
	system(cmd);
	free(cmd);

	char *line = NULL;
	size_t len = 0;
	ssize_t read_len;
	FILE *task_state_fp;
	char *task_id_ch;
	char *task_stat_str;
	int task_id;
	// TODO what is the proper timeout?
	int timeout = 40;

	task_state_fp = fopen(FILE_TASK_STATE, "r");
	while(1) {
		while((read_len = getline(&line, &len, task_state_fp)) != -1) {
			// trim the newline character
			if (line[read_len-1] == '\n') {
				line[read_len-1] = '\0';
				--read_len;
			}

			task_id_ch = strtok(line, ",");
			task_id = atoi(task_id_ch);
			task_stat_str = strtok(NULL, ",");

			if (task_id == (int)jobid && \
					(strcmp(task_stat_str, "finished") == 0 || \
					 strcmp(task_stat_str, "failed") == 0 || \
					 strcmp(task_stat_str, "aborted") == 0)) {

				fclose(task_state_fp);
				return 0;

			}
		}
		sleep(1);

		if(timeout != 0 && time(0) >= timeout) {
			fclose(task_state_fp);
			return 1;
		}
	}
}

static int batch_queue_mesos_create (struct batch_queue *q)
{
	batch_queue_set_feature(q, "mesos_job_queue", NULL);
	batch_queue_set_feature(q, "batch_log_name", "%s.mesoslog");
	return 0;
}

batch_queue_stub_free(mesos);
batch_queue_stub_port(mesos);
batch_queue_stub_option_update(mesos);

batch_fs_stub_chdir(mesos);
batch_fs_stub_getcwd(mesos);
batch_fs_stub_mkdir(mesos);
batch_fs_stub_putfile(mesos);
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
		batch_fs_mesos_stat,
		batch_fs_mesos_unlink,
	},
};

/* vim: set noexpandtab tabstop=4: */
