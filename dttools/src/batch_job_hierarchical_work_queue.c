#include "batch_job.h"
#include "batch_job_internal.h"
#include "hierarchical_work_queue.h"
#include "debug.h"
#include "stringtools.h"
#include "macros.h"

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>


void specify_worker_job_files(struct worker_job *t, const char *input_files, const char *output_files)
{
	char *f, *p, *files;

	if(input_files) {
		files = strdup(input_files);
		f = strtok(files, " \t,");
		while(f) {
			p = strchr(f, '=');
			if(p) {
				*p = 0;
				hierarchical_work_queue_job_specify_file(t, f, p + 1, WORKER_FILES_INPUT, 0);
				debug(D_DEBUG, "local file %s is %s on remote system:", f, p + 1);
				*p = '=';
			} else {
				hierarchical_work_queue_job_specify_file(t, f, f, WORKER_FILES_INPUT, 0);
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
				hierarchical_work_queue_job_specify_file(t, f, p + 1, WORKER_FILES_OUTPUT, 0);
				debug(D_DEBUG, "remote file %s is %s on local system:", f, p + 1);
				*p = '=';
			} else {
				hierarchical_work_queue_job_specify_file(t, f, f, WORKER_FILES_OUTPUT, 0);
			}
			f = strtok(0, " \t,");
		}
		free(files);
	}
}


batch_job_id_t batch_job_submit_hierarchical_work_queue(struct batch_queue *q, const char *cmd, const char *args, const char *infile, const char *outfile, const char *errfile, const char *extra_input_files, const char *extra_output_files)
{
	struct worker_job *t;
	char *full_command;

	if(infile)
		full_command = (char *) malloc((strlen(cmd) + strlen(args) + strlen(infile) + 5) * sizeof(char));
	else
		full_command = (char *) malloc((strlen(cmd) + strlen(args) + 2) * sizeof(char));

	if(!full_command) {
		debug(D_DEBUG, "couldn't create new hierarchical_work_queue task: out of memory\n");
		return -1;
	}

	if(infile)
		sprintf(full_command, "%s %s < %s", cmd, args, infile);
	else
		sprintf(full_command, "%s %s", cmd, args);

	t = hierarchical_work_queue_job_create(full_command);

	free(full_command);

	if(infile)
		hierarchical_work_queue_job_specify_file(t, infile, infile, WORKER_FILES_INPUT, 0);
	if(cmd)
		hierarchical_work_queue_job_specify_file(t, cmd, cmd, WORKER_FILES_INPUT, 0);

	specify_worker_job_files(t, extra_input_files, extra_output_files);

	
	hierarchical_work_queue_submit(q->hierarchical_work_queue, t);

	if(outfile) {
		itable_insert(q->output_table, t->id, strdup(outfile));
	}

	return t->id;
}

batch_job_id_t batch_job_submit_simple_hierarchical_work_queue(struct batch_queue *q, const char *cmd, const char *extra_input_files, const char *extra_output_files)
{
	struct worker_job *j;

	j = hierarchical_work_queue_job_create(cmd);

	specify_worker_job_files(j, extra_input_files, extra_output_files);

	hierarchical_work_queue_submit(q->hierarchical_work_queue, j);

	return j->id;
}

batch_job_id_t batch_job_wait_hierarchical_work_queue(struct batch_queue * q, struct batch_job_info * info, time_t stoptime)
{
	static FILE *logfile = 0;

	int taskid = -1;

	if(!logfile) {
		logfile = fopen(q->logfile, "a");
		if(!logfile) {
			debug(D_NOTICE, "couldn't open logfile %s: %s\n", q->logfile, strerror(errno));
			return -1;
		}
	}

/*	if(stoptime == 0) {
		timeout = WORK_QUEUE_WAITFORTASK;
	} else {
		timeout = MAX(0, stoptime - time(0));
	}
*/
	struct worker_job *t = hierarchical_work_queue_wait(q->hierarchical_work_queue);
	if(t) {
		info->submitted = t->submit_time / 1000000;
		info->started = t->start_time / 1000000;
		info->finished = t->finish_time / 1000000;
		info->exited_normally = 1;
		info->exit_code = t->exit_code;
		info->exit_signal = 0;

		/*
		   If the standard ouput of the job is not empty,
		   then print it, because this is analogous to a Unix
		   job, and would otherwise be lost.  Important for
		   capturing errors from the program.
		 */

		if(t->stdout_buffer && t->stdout_buffer[0]) {
			if(t->stdout_buffer[1] || t->stdout_buffer[0] != '\n') {
				string_chomp(t->stdout_buffer);
				printf("%s\n", t->stdout_buffer);
			}
		}
		if(t->stderr_buffer && t->stderr_buffer[0]) {
			if(t->stderr_buffer[1] || t->stderr_buffer[0] != '\n') {
				string_chomp(t->stderr_buffer);
				printf("%s\n", t->stderr_buffer);
			}
		}

		char *outfile = itable_remove(q->output_table, t->id);
		if(outfile) {
			FILE *file = fopen(outfile, "w");
			if(file) {
				fwrite(t->stdout_buffer, t->stdout_buffersize, 1, file);
				fclose(file);
			}
			free(outfile);
		}
		fprintf(logfile, "TASK %llu %d %d %d %llu %llu %llu \"%s\" \"%s\"\n", timestamp_get(), t->id, t->status, t->exit_code, t->submit_time, t->start_time, t->finish_time, t->tag ? t->tag : "", t->command);

		taskid = t->id;
		hierarchical_work_queue_job_delete(t);
	}
	// Print to work queue log since status has been changed.
//	hierarchical_work_queue_get_stats(q->hierarchical_work_queue, &s);
//	fprintf(logfile, "QUEUE %llu %d %d %d %d %d %d %d %d %d %d %lld %lld\n", timestamp_get(), s.workers_init, s.workers_ready, s.workers_busy, s.tasks_running, s.tasks_waiting, s.tasks_complete, s.total_tasks_dispatched, s.total_tasks_complete,
//		s.total_workers_joined, s.total_workers_removed, s.total_bytes_sent, s.total_bytes_received);
	fflush(logfile);
	fsync(fileno(logfile));

	if(taskid >= 0) {
		return taskid;
	}

	if(hierarchical_work_queue_empty(q->hierarchical_work_queue)) {
		return 0;
	} else {
		return -1;
	}
}

int batch_job_remove_hierarchical_work_queue(struct batch_queue *q, batch_job_id_t jobid)
{
	return 0;
}

