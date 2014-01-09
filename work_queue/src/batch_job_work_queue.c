#include "batch_job.h"
#include "batch_job_internal.h"
#include "work_queue.h"
#include "debug.h"
#include "path.h"
#include "stringtools.h"
#include "macros.h"
#include "rmsummary.h"

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>


void specify_work_queue_task_files(struct work_queue_task *t, const char *input_files, const char *output_files, int caching_directive)
{
	char *f, *p, *files;
	int caching;

	if(input_files) {
		files = strdup(input_files);
		f = strtok(files, " \t,");
		while(f) {
			p = strchr(f, '=');
			if(p) {
				*p = 0;

				if(strcmp(f, p+1) || !caching_directive)
					caching = WORK_QUEUE_NOCACHE;
				else
					caching = WORK_QUEUE_CACHE;
			
				work_queue_task_specify_file(t, f, p + 1, WORK_QUEUE_INPUT, caching);
				debug(D_BATCH, "local file %s is %s on remote system:", f, p + 1);
				*p = '=';
			} else {

				if(caching_directive == WORK_QUEUE_NOCACHE) 
					caching = WORK_QUEUE_NOCACHE;
				else 
					caching = WORK_QUEUE_CACHE;

				work_queue_task_specify_file(t, f, f, WORK_QUEUE_INPUT, caching);
			}
			f = strtok(0, " \t,");
		}
		free(files);
	}

	if(output_files) {
		files = strdup(output_files);
		f = strtok(files, " \t,");
		while(f) {
			p = strchr(f, '=');
			if(p) {
				*p = 0;

				if(strcmp(f, p+1) || !caching_directive)
					caching = WORK_QUEUE_NOCACHE;
				else
					caching = WORK_QUEUE_CACHE;

				work_queue_task_specify_file(t, f, p + 1, WORK_QUEUE_OUTPUT, caching);
				debug(D_BATCH, "remote file %s is %s on local system:", p + 1, f);
				*p = '=';
			} else {

				if(caching_directive == WORK_QUEUE_NOCACHE) 
					caching = WORK_QUEUE_NOCACHE;
				else 
					caching = WORK_QUEUE_CACHE;

				work_queue_task_specify_file(t, f, f, WORK_QUEUE_OUTPUT, caching);
			}
			f = strtok(0, " \t,");
		}
		free(files);
	}
}

void specify_work_queue_task_shared_files(struct work_queue_task *t, const char *input_files, const char *output_files)
{
	if(input_files) {
		char *files = strdup(input_files);
		char *file = strtok(files, " \t,");
		while(file) {
			file = strdup(file);
			char *p = strchr(file, '=');
			if(p) {
				*p = 0;
			}

			if(file[0] != '/') {
				char *cwd = path_getcwd();
				char *new = string_format("%s/%s", cwd, file);
				free(file);
				free(cwd);
				file = new;
			}

			if(p) {
				work_queue_task_specify_file(t, file, p + 1, WORK_QUEUE_INPUT, WORK_QUEUE_CACHE | WORK_QUEUE_THIRDGET);
				debug(D_BATCH, "shared file %s is %s on remote system:", file, p + 1);
				*p = '=';
			} else {
				work_queue_task_specify_file(t, file, file, WORK_QUEUE_INPUT, WORK_QUEUE_CACHE | WORK_QUEUE_THIRDGET);
			}
			free(file);
			file = strtok(0, " \t,");
		}
		free(files);
	}

	if(output_files) {
		char *files = strdup(output_files);
		char *file = strtok(files, " \t,");
		while(file) {
			file = strdup(file);
			char *p = strchr(file, '=');
			if(p) {
				*p = 0;
			}

			if(file[0] != '/') {
				char *cwd = path_getcwd();
				char *new = string_format("%s/%s", cwd, file);
				free(file);
				free(cwd);
				file = new;
			}

			if(p) {
				work_queue_task_specify_file(t, file, p + 1, WORK_QUEUE_OUTPUT, WORK_QUEUE_THIRDPUT);
				debug(D_BATCH, "shared file %s is %s on remote system:", file, p + 1);
				*p = '=';
			} else {
				work_queue_task_specify_file(t, file, file, WORK_QUEUE_OUTPUT, WORK_QUEUE_THIRDPUT);
			}
			free(file);
			file = strtok(0, " \t,");
		}
		free(files);
	}
}

struct rmsummary *parse_batch_options_resources(char *options_text)
{
	if(!options_text)
		return NULL;
	
	char *resources = strstr(options_text, "resources:");

	if(!resources)
		return NULL;

	resources = strchr(resources, ':') + 1;

	return rmsummary_parse_single(resources, ',');
}

void work_queue_task_specify_resources(struct work_queue_task *t, struct rmsummary *resources)
{
		if(resources->cores > -1)
			work_queue_task_specify_cores(t, resources->cores);

		if(resources->resident_memory > -1)
			work_queue_task_specify_memory(t, resources->resident_memory);

		if(resources->workdir_footprint > -1)
			work_queue_task_specify_disk(t, resources->workdir_footprint);
}

batch_job_id_t batch_job_submit_work_queue(struct batch_queue *q, const char *cmd, const char *args, const char *infile, const char *outfile, const char *errfile, const char *extra_input_files, const char *extra_output_files)
{
	char *command = string_format("%s %s", cmd, args);
	if(infile) {
		char *new = string_format("%s <%s", command, infile);
		free(command);
		command = new;
	}

	struct work_queue_task *t = work_queue_task_create(command);

	free(command);

	if(q->type == BATCH_QUEUE_TYPE_WORK_QUEUE_SHAREDFS) {
		if(infile)
			work_queue_task_specify_file(t, infile, infile, WORK_QUEUE_INPUT, WORK_QUEUE_CACHE | WORK_QUEUE_THIRDGET);
		if(cmd)
			work_queue_task_specify_file(t, cmd, cmd, WORK_QUEUE_INPUT, WORK_QUEUE_CACHE | WORK_QUEUE_THIRDGET);

		specify_work_queue_task_shared_files(t, extra_input_files, extra_output_files);
	} else {
		if(infile)
			work_queue_task_specify_input_file(t, infile, infile);
		if(cmd)
			work_queue_task_specify_input_file(t, cmd, cmd);

		specify_work_queue_task_files(t, extra_input_files, extra_output_files, q->caching);
	}

	struct rmsummary *resources = parse_batch_options_resources(q->options_text);
	if(resources)
	{
		work_queue_task_specify_resources(t, resources);
		free(resources);
	}

	work_queue_submit(q->work_queue, t);

	if(outfile) {
		itable_insert(q->output_table, t->taskid, strdup(outfile));
	}

	return t->taskid;
}

batch_job_id_t batch_job_submit_simple_work_queue(struct batch_queue * q, const char *cmd, const char *extra_input_files, const char *extra_output_files)
{
	struct work_queue_task *t;

	t = work_queue_task_create(cmd);

	if(q->type == BATCH_QUEUE_TYPE_WORK_QUEUE_SHAREDFS) {
		specify_work_queue_task_shared_files(t, extra_input_files, extra_output_files);
	} else {
		specify_work_queue_task_files(t, extra_input_files, extra_output_files, q->caching);
	}

	struct rmsummary *resources = parse_batch_options_resources(q->options_text);
	if(resources)
	{
		work_queue_task_specify_resources(t, resources);
		free(resources);
	}

	work_queue_submit(q->work_queue, t);

	return t->taskid;
}

batch_job_id_t batch_job_wait_work_queue(struct batch_queue * q, struct batch_job_info * info, time_t stoptime)
{
	static int try_open_log = 0;
	int timeout, taskid = -1;

	if(!try_open_log)
	{
		try_open_log = 1;
		if(!work_queue_specify_log(q->work_queue, q->logfile))
		{
			return -1;
		}
	}

	if(stoptime == 0) {
		timeout = WORK_QUEUE_WAITFORTASK;
	} else {
		timeout = MAX(0, stoptime - time(0));
	}

	struct work_queue_task *t = work_queue_wait(q->work_queue, timeout);
	if(t) {
		info->submitted = t->time_task_submit / 1000000;
		info->started = t->time_send_input_start / 1000000;
		info->finished = t->time_receive_output_finish / 1000000;
		info->exited_normally = 1;
		info->exit_code = t->return_status;
		info->exit_signal = 0;

		/*
		   If the standard ouput of the job is not empty,
		   then print it, because this is analogous to a Unix
		   job, and would otherwise be lost.  Important for
		   capturing errors from the program.
		 */

		if(t->output && t->output[0]) {
			if(t->output[1] || t->output[0] != '\n') {
				string_chomp(t->output);
				printf("%s\n", t->output);
			}
		}

		char *outfile = itable_remove(q->output_table, t->taskid);
		if(outfile) {
			FILE *file = fopen(outfile, "w");
			if(file) {
				fwrite(t->output, strlen(t->output), 1, file);
				fclose(file);
			}
			free(outfile);
		}

		taskid = t->taskid;
		work_queue_task_delete(t);
	}

	if(taskid >= 0) {
		return taskid;
	}

	if(work_queue_empty(q->work_queue)) {
		return 0;
	} else {
		return -1;
	}
}

int batch_job_remove_work_queue(struct batch_queue *q, batch_job_id_t jobid)
{
	return 0;
}

/* vim: set noexpandtab tabstop=4: */
