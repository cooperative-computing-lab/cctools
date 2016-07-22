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
#include <time.h>

#define FILE_TASK_INFO "task_info"
#define FILE_TASK_STATE "task_state"

#define NUM_OF_TASKS 4096

static int counter = 0;
static int finished_tasks[NUM_OF_TASKS];
static int num_finished_tasks = 0;
static time_t old_time;
static int is_first_time = 1;

// mesos task struct
typedef struct mesos_task{
	int task_id;
	char *task_cmd;
	int num_input_files;
	char **task_input_files;
	int num_output_files;
	char **task_output_files;
} mesos_task;

// get list of input files 
char **build_str_lst_from_str(int *num_str, const char *str) 
{
	char **str_lst = NULL;

	*num_str = 0;
	char *pch = NULL;
	char *str_cpy_1 = strdup(str);
	pch = strtok(str_cpy_1, ",");
	while(pch != NULL) {
		(*num_str)++;
		pch = strtok(NULL, ",");
	}
	free(str_cpy_1);

	str_lst = malloc(sizeof(char *) * (*num_str));

	char *str_cpy_2 = strdup(str);
    char *tmp_str = NULL;
	int i = 0;	
	pch = strtok(str_cpy_2, ",");
	while(pch != NULL) {
		tmp_str = xxstrdup(pch);
		str_lst[i++] = tmp_str;
		pch = strtok(NULL, ",");
	} 
	free(str_cpy_2);	

	return str_lst;
}

struct mesos_task *create_mesos_task(int task_id, const char *cmd, const char *extra_input_files, const char *extra_output_files)
{
	mesos_task *mt = malloc(sizeof(*mt));
	mt->task_id = task_id;
	mt->task_cmd = xxstrdup(cmd);

	if (extra_input_files != NULL) {
	    mt->task_input_files = build_str_lst_from_str(&(mt->num_input_files), extra_input_files);
		int i = 0;
		for(i = 0; i < mt->num_input_files; i++) {
			if (mt->task_input_files[i][0] != '/') {
				char *path_buf = path_getcwd();
				string_combine(path_buf, "/");
				string_combine(path_buf, mt->task_input_files[i]);
				mt->task_input_files[i] = path_buf;
			}
		}	
	} else {
		mt->task_input_files = NULL;
		mt->num_input_files = 0;
	}

	if (extra_output_files != NULL) {
		mt->task_output_files = build_str_lst_from_str(&(mt->num_output_files), extra_output_files);
	} else {
		mt->task_output_files = NULL;
		mt->num_output_files = 0;
	}

	return mt;
}

void destroy_mesos_task(mesos_task *mt) 
{
	free(mt->task_cmd);
	int i = 0;
	for (i = 0; i < mt->num_input_files; i++) {
		free(mt->task_input_files[i]);
	}
	free(mt->task_input_files);	
	int j = 0;
	for (j = 0; j < mt->num_output_files; j++) {
		free(mt->task_output_files[j]);
	}
	free(mt->task_output_files);
	free(mt);
}

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

// check is file is exist
int is_file_exist(const char *path) 
{
	return access(path, F_OK) != -1;
}

// check the is file is modified since @old_time
int is_file_modified(const char *path) {
	struct stat file_stat;
	int err = stat(path, &file_stat);
	if (err != 0) {
		exit(errno);
	}
	if (file_stat.st_mtime > old_time) {
		old_time = file_stat.st_mtime;
		return 1;
	} else {
		return 0;
	}
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

	FILE *fp_1;

	if(access(FILE_TASK_INFO, F_OK) != -1) {
		fp_1 = fopen(FILE_TASK_INFO, "a+");
	} else {
		fp_1 = fopen(FILE_TASK_INFO, "w+");
	}
	
	mesos_task *mt = create_mesos_task(task_id, cmd, extra_input_files, extra_output_files);

	fprintf(fp_1, "%d,%s,", mt->task_id, mt->task_cmd);

	// Get the absolut path of each input file

	if (extra_input_files != NULL) {

		int j = 0;
		for (j = 0; j < (mt->num_input_files-1); j++) {
			fprintf(fp_1, "%s ", mt->task_input_files[j]);
		}
		fprintf(fp_1, "%s,", mt->task_input_files[mt->num_input_files-1]);

	} else {
		fputs(",", fp_1);
	}
    
	if (extra_output_files != NULL) {
		int j = 0;
		for (j = 0; j < (mt->num_output_files-1); j++) {
			fprintf(fp_1, "%s ", mt->task_output_files[j]);
		}
		fprintf(fp_1, "%s,", mt->task_output_files[mt->num_output_files-1]);
	} else {
		fputs(",", fp_1);
	}
	fputs("submitted\n", fp_1);

	destroy_mesos_task(mt);
	fclose(fp_1);

	return task_id;
}

static batch_job_id_t batch_job_mesos_wait (struct batch_queue * q, struct batch_job_info * info_out, time_t stoptime)
{

	// read FILE_TASK_STATE and check if there is job finished
	// remove the job from batch_queue->job_table 
	//
	// polling the FILE_TASK_STATE to check if there is
	// new task finished
	
	char *line = NULL;
	size_t len = 0;
	ssize_t read_len;
	FILE *fp;

	while(!is_file_exist(FILE_TASK_STATE)) {}

	if (is_first_time) {
		old_time = time(0);
		is_first_time = 0;
	}

	while(1) {

		// if the file size has changed
		//if (is_file_modified(FILE_TASK_STATE)) {
			char *task_id_ch;
			char *task_stat_str;
			int task_id;
					
			fp = fopen(FILE_TASK_STATE, "r");
	    	while((read_len = getline(&line, &len, fp)) != -1) {

				// trim the newline character
				if (line[read_len-1] == '\n') {
					line[read_len-1] = '\0';
					--read_len;
				}

				task_id_ch = strtok(line, ",");
				task_id = atoi(task_id_ch);

				// There is a new task finished
				if(!is_in_array(task_id, finished_tasks, num_finished_tasks)) {
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
					fclose(fp);

					finished_tasks[num_finished_tasks] = task_id;	
					num_finished_tasks++;

					return task_id;
				}
			}
		//} else {
			sleep(1);
		//}

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
	// append the new task state to the "task_info" file
	char *cmd = string_format("awk -F \',\' \'{if($1==\"%" PRIbjid "\"){gsub(\"submitted\",\"aborting\",$5);print $1\",\"$2\",\"$3\",\"$4\",\"$5}}\' %s >> %s", \
			jobid, FILE_TASK_INFO, FILE_TASK_INFO);
	system(cmd);
	free(cmd);

	char *line = NULL;
	size_t len = 0;
	ssize_t read_len;
	FILE *fp;
	char *task_id_ch;
	char *task_stat_str;
	int task_id;
	// TODO what is the proper timeout?
	int timeout = 40;

	fp = fopen(FILE_TASK_STATE, "r");
	while(1) {
		while((read_len = getline(&line, &len, fp)) != -1) {
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

				fclose(fp);
				return 0;

			}
		}
		sleep(1);

		if(timeout != 0 && time(0) >= timeout) {
			fclose(fp);
			return 1;
		}
	}
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
