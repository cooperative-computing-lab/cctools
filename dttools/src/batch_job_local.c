#include "batch_job.h"
#include "batch_job_internal.h"
#include "debug.h"
#include "process.h"
#include "macros.h"
#include "stringtools.h"

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <signal.h>

batch_job_id_t batch_job_submit_simple_local(struct batch_queue *q, const char *cmd, const char *extra_input_files, const char *extra_output_files)
{
	batch_job_id_t jobid;

	fflush(NULL);
	jobid = fork();
	if(jobid > 0) {
		debug(D_DEBUG, "started process %d: %s", jobid, cmd);
		struct batch_job_info *info = malloc(sizeof(*info));
		memset(info, 0, sizeof(*info));
		info->submitted = time(0);
		info->started = time(0);
		itable_insert(q->job_table, jobid, info);
		return jobid;
	} else if(jobid < 0) {
		debug(D_DEBUG, "couldn't create new process: %s\n", strerror(errno));
		return -1;
	} else {
		int result = system(cmd);
		if(WIFEXITED(result)) {
			_exit(WEXITSTATUS(result));
		} else {
			_exit(1);
		}
	}

}

batch_job_id_t batch_job_submit_local(struct batch_queue *q, const char *cmd, const char *args, const char *infile, const char *outfile, const char *errfile, const char *extra_input_files, const char *extra_output_files)
{
	if(cmd == NULL)
		cmd = "/bin/false";
	if(args == NULL)
		args = "";
	if(infile == NULL)
		infile = "/dev/null";
	if(outfile == NULL)
		outfile = "/dev/null";
	if(errfile == NULL)
		errfile = "/dev/null";

	char *command = string_format("%s %s <%s >%s 2>%s", cmd, args, infile, outfile, errfile);

	batch_job_id_t status = batch_job_submit_simple_local(q, command, extra_input_files, extra_output_files);
	free(command);
	return status;
}

batch_job_id_t batch_job_wait_local(struct batch_queue * q, struct batch_job_info * info_out, time_t stoptime)
{
	while(1) {
		int timeout;

		if(stoptime > 0) {
			timeout = MAX(0, stoptime - time(0));
		} else {
			timeout = 5;
		}

		struct process_info *p = process_wait(timeout);
		if(p) {
			struct batch_job_info *info = itable_remove(q->job_table, p->pid);
			if(!info) {
				process_putback(p);
				return -1;
			}

			info->finished = time(0);
			if(WIFEXITED(p->status)) {
				info->exited_normally = 1;
				info->exit_code = WEXITSTATUS(p->status);
			} else {
				info->exited_normally = 0;
				info->exit_signal = WTERMSIG(p->status);
			}

			memcpy(info_out, info, sizeof(*info));

			int jobid = p->pid;
			free(p);
			free(info);
			return jobid;

		} else if(errno == ESRCH || errno == ECHILD) {
			return 0;
		}

		if(stoptime != 0 && time(0) >= stoptime)
			return -1;
	}
}

int batch_job_remove_local(struct batch_queue *q, batch_job_id_t jobid)
{
	if(itable_lookup(q->job_table, jobid)) {
		if(kill(jobid, SIGTERM) == 0) {
			debug(D_DEBUG, "signalled process %d", jobid);
			return 1;
		} else {
			debug(D_DEBUG, "could not signal process %d: %s\n", jobid, strerror(errno));
			return 0;
		}
	} else {
		debug(D_DEBUG, "process %d is not under my control.\n", jobid);
		return 0;
	}
}

