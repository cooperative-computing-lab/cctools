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

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <signal.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

#define FILE_RUN_TASKS "task_to_run"
#define FILE_FINISH_TASKS "finished_tasks"

#define NUM_OF_TASKS 4096

static int counter = 0;
static int finished_tasks[NUM_OF_TASKS];
static int num_finished_tasks = 0;

static int is_in_array(int a, int *int_array, int size) 
{
	int i = 0;
	for(i = 0; i < size; i++) {
		if(int_array[i] == a) {
			return 1;
		}
	}
	return 0;
}

static batch_job_id_t batch_job_mesos_submit (struct batch_queue *q, const char *cmd, const char *extra_input_files, const char *extra_output_files, struct jx *envlist, const struct rmsummary *resources )
{
	int task_id = ++counter;

	debug(D_BATCH, "task %d is ready", task_id);
	struct batch_job_info *info = malloc(sizeof(*info));
	memset(info, 0, sizeof(*info));
	info->submitted = time(0);
	info->started = time(0);
	itable_insert(q->job_table, task_id, info);

	FILE *fp;

	if(access(FILE_RUN_TASKS, F_OK) != -1) {
		fp = fopen(FILE_RUN_TASKS, "a");
	} else {
		fp = fopen(FILE_RUN_TASKS, "w+");
	}

	fprintf(fp, "task_id: %d\n", task_id);
	fprintf(fp, "cmd: %s\n", cmd);

	// Get the absolut path of each input file
	
	if (extra_input_files != NULL) {
		fputs("input files: ", fp);
		char *path_buf = path_getcwd();
		char *pch;
		char *input_files_cpy = xxstrdup(extra_input_files); 
		pch = strtok(input_files_cpy, " ,");
		char *tmp_fn_path;
		char *tmp_fn_abs_path;
		while(pch != NULL) {
			if(pch[0] != '/') {
				tmp_fn_path = string_combine(path_buf, "/");
				tmp_fn_abs_path = string_combine(tmp_fn_path, pch);
				fprintf(fp, "%s,", tmp_fn_abs_path);
			} else {
				fprintf(fp, "%s,", pch);
			}	
			pch = strtok (NULL, " ,");
		}
		free(path_buf);

		fputs("\n", fp);
	} else {
		fputs("input files: \n", fp);
	}
		
	fprintf(fp, "output files: %s\n", extra_output_files);

	fclose(fp);

	return task_id;
}

static batch_job_id_t batch_job_mesos_wait (struct batch_queue * q, struct batch_job_info * info_out, time_t stoptime)
{

	// read FILE_FINISH_TASKS and check if there is job finished
	// remove the job from batch_queue->job_table 

	struct stat oup_fn_stat;
	stat(FILE_FINISH_TASKS, &oup_fn_stat);
	off_t oup_fn_size = oup_fn_stat.st_size;

	// polling the FILE_FINISH_TASKS to check if there is
	// new task finished
	
	char *line = NULL;
	size_t len = 0;
	ssize_t read_len;
	FILE *fp;

	while(1) {

		stat(FILE_FINISH_TASKS, &oup_fn_stat);

		// if the file size has changed
		if (oup_fn_stat.st_size - oup_fn_size > 0) {
			char *task_id_ch;
			char *task_stat_str;
			int task_id;
					
			fp = fopen(FILE_FINISH_TASKS, "r");
	    	while((read_len = getline(&line, &len, fp)) != -1) {

				// trim the newline character
				if (line[read_len-1] == '\n') {
					line[read_len-1] = '\0';
					--read_len;
				}

				task_id_ch = strtok(line, " ");
				task_id = atoi(task_id_ch);

				// There is a new task finished
				if(!is_in_array(task_id, finished_tasks, num_finished_tasks)) {
					struct batch_job_info *info = itable_remove(q->job_table, task_id);
				    	
					info->finished = time(0);
					task_stat_str = strtok(NULL, " ");

					if (strcmp(task_stat_str, "finished") == 0) {
						info->exited_normally = 1;
					} else {
						info->exited_normally = 0;
					}

					memcpy(info_out, info, sizeof(*info));
					free(info);
					fclose(fp);

					finished_tasks[num_finished_tasks] = task_id;	
					num_finished_tasks++;

					return task_id;
				}
			}

		} else {
			sleep(1);
		}

		oup_fn_size = oup_fn_stat.st_size;	
	}

}

static int batch_job_mesos_remove (struct batch_queue *q, batch_job_id_t jobid)
{
	return 0;
}

static int batch_queue_mesos_create (struct batch_queue *q)
{
	batch_queue_set_feature(q, "mesos_job_queue", NULL);
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
