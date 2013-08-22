#include "batch_job.h"
#include "batch_job_internal.h"
#include "mpi_queue.h"
#include "debug.h"
#include "stringtools.h"
#include "macros.h"

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>

void specify_mpi_queue_task_files(struct mpi_queue_task *t, const char *input_files, const char *output_files)
{
	char *f, *p, *files;

	if(input_files) {
		files = strdup(input_files);
		f = strtok(files, " \t,");
		while(f) {
			p = strchr(f, '=');
			if(p) {
				*p = 0;
				mpi_queue_task_specify_file(t, f, MPI_QUEUE_INPUT);
				*p = '=';
			} else {
				mpi_queue_task_specify_file(t, f, MPI_QUEUE_INPUT);
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
				mpi_queue_task_specify_file(t, f, MPI_QUEUE_OUTPUT);
				*p = '=';
			} else {
				mpi_queue_task_specify_file(t, f, MPI_QUEUE_OUTPUT);
			}
			f = strtok(0, " \t,");
		}
		free(files);
	}
}

batch_job_id_t batch_job_submit_simple_mpi_queue( struct batch_queue *q, const char *cmd, const char *extra_input_files, const char *extra_output_files )
{
	struct mpi_queue_task *t;

	t = mpi_queue_task_create(cmd);

	specify_mpi_queue_task_files(t, extra_input_files, extra_output_files);

	mpi_queue_submit(q->mpi_queue, t);

	return t->taskid;
}

batch_job_id_t batch_job_submit_mpi_queue( struct batch_queue *q, const char *cmd, const char *args, const char *infile, const char *outfile, const char *errfile, const char *extra_input_files, const char *extra_output_files )
{
	char *command = string_format("%s %s", cmd, args);
	if(infile) {
		char *new = string_format("%s <%s", cmd, infile);
		free(command);
		command = new;
	}

	struct mpi_queue_task *t = mpi_queue_task_create(command);
	free(command);

	if(infile)
		mpi_queue_task_specify_file(t, infile, MPI_QUEUE_INPUT);
	if(cmd)
		mpi_queue_task_specify_file(t, cmd, MPI_QUEUE_INPUT);

	specify_mpi_queue_task_files(t, extra_input_files, extra_output_files);

	mpi_queue_submit(q->mpi_queue, t);

	if(outfile) {
		itable_insert(q->output_table, t->taskid, strdup(outfile));
	}

	return t->taskid;
}

batch_job_id_t batch_job_wait_mpi_queue( struct batch_queue *q, struct batch_job_info *info, time_t stoptime )
{
	static FILE *logfile = 0;
//	struct work_queue_stats s;

	int timeout, taskid = -1;

	if(!logfile) {
		logfile = fopen(q->logfile, "a");
		if(!logfile) {
			debug(D_NOTICE, "couldn't open logfile %s: %s\n", q->logfile, strerror(errno));
			return -1;
		}
	}

	if(stoptime == 0) {
		timeout = MPI_QUEUE_WAITFORTASK;
	} else {
		timeout = MAX(0, stoptime - time(0));
	}

	struct mpi_queue_task *t = mpi_queue_wait(q->mpi_queue, timeout);
	if(t) {
		info->submitted = t->submit_time / 1000000;
		info->started = t->start_time / 1000000;
		info->finished = t->finish_time / 1000000;
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
		fprintf(logfile, "TASK %" PRIu64 "%d %d %d %" PRIu64 " %" PRIu64 " \"%s\" \"%s\"\n", timestamp_get(), t->taskid, t->result, t->return_status, t->submit_time, t->finish_time, t->tag ? t->tag : "", t->command_line);

		taskid = t->taskid;
		mpi_queue_task_delete(t);
	}
	// Print to work queue log since status has been changed.
//	mpi_queue_get_stats(q->mpi_queue, &s);
//	fprintf(logfile, "QUEUE %llu %d %d %d %d %d\n", timestamp_get(), s.tasks_running, s.tasks_waiting, s.tasks_complete, s.total_tasks_dispatched, s.total_tasks_complete);
//	fflush(logfile);
//	fsync(fileno(logfile));

	if(taskid >= 0) {
		return taskid;
	}

	if(mpi_queue_empty(q->mpi_queue)) {
		return 0;
	} else {
		return -1;
	}
}

int batch_job_remove_mpi_queue(struct batch_queue *q, batch_job_id_t jobid)
{
	return 0;
}

