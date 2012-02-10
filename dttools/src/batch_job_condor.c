#include "batch_job.h"
#include "batch_job_internal.h"
#include "debug.h"
#include "itable.h"
#include "process.h"
#include "stringtools.h"

#include <stdlib.h>
#include <stdio.h>
#include <errno.h>
#include <string.h>
#include <signal.h>
#include <sys/stat.h>

batch_job_id_t batch_job_submit_condor(struct batch_queue *q, const char *cmd, const char *args, const char *infile, const char *outfile, const char *errfile, const char *extra_input_files, const char *extra_output_files)
{
	FILE *file;
	int njobs;
	int jobid;

	file = fopen("condor.submit", "w");
	if(!file) {
		debug(D_DEBUG, "could not create condor.submit: %s", strerror(errno));
		return -1;
	}

	fprintf(file, "universe = vanilla\n");
	fprintf(file, "executable = %s\n", cmd);
	fprintf(file, "getenv = true\n");
	if(args)
		fprintf(file, "arguments = %s\n", args);
	if(infile)
		fprintf(file, "input = %s\n", infile);
	if(outfile)
		fprintf(file, "output = %s\n", outfile);
	if(errfile)
		fprintf(file, "error = %s\n", errfile);
	if(extra_input_files)
		fprintf(file, "transfer_input_files = %s\n", extra_input_files);
	// Note that we do not use transfer_output_files, because that causes the job
	// to get stuck in a system hold if the files are not created.
	fprintf(file, "should_transfer_files = yes\n");
	fprintf(file, "when_to_transfer_output = on_exit\n");
	fprintf(file, "notification = never\n");
	fprintf(file, "copy_to_spool = true\n");
	fprintf(file, "transfer_executable = true\n");
	fprintf(file, "log = %s\n", q->logfile);
	if(q->options_text)
		fprintf(file, "%s\n", q->options_text);
	fprintf(file, "queue\n");
	fclose(file);

	file = popen("condor_submit condor.submit", "r");
	if(!file)
		return -1;

	char line[BATCH_JOB_LINE_MAX];
	while(fgets(line, sizeof(line), file)) {
		if(sscanf(line, "%d job(s) submitted to cluster %d", &njobs, &jobid) == 2) {
			pclose(file);
			debug(D_DEBUG, "job %d submitted to condor", jobid);
			struct batch_job_info *info;
			info = malloc(sizeof(*info));
			memset(info, 0, sizeof(*info));
			info->submitted = time(0);
			itable_insert(q->job_table, jobid, info);
			return jobid;
		}
	}

	pclose(file);
	debug(D_DEBUG, "failed to submit job to condor!");
	return -1;
}

int setup_condor_wrapper(const char *wrapperfile)
{
	FILE *file;

	if(access(wrapperfile, R_OK | X_OK) == 0)
		return 0;

	file = fopen(wrapperfile, "w");
	if(!file)
		return -1;

	fprintf(file, "#!/bin/sh\n");
	fprintf(file, "eval \"$@\"\n");
	fprintf(file, "exit $?\n");
	fclose(file);

	chmod(wrapperfile, 0755);

	return 0;
}


batch_job_id_t batch_job_submit_simple_condor(struct batch_queue *q, const char *cmd, const char *extra_input_files, const char *extra_output_files)
{
	if(setup_condor_wrapper("condor.sh") < 0) {
		debug(D_DEBUG, "could not create condor.sh: %s", strerror(errno));
		return -1;
	}

	return batch_job_submit_condor(q, "condor.sh", cmd, 0, 0, 0, extra_input_files, extra_output_files);
}

batch_job_id_t batch_job_wait_condor(struct batch_queue * q, struct batch_job_info * info_out, time_t stoptime)
{
	static FILE *logfile = 0;

	if(!logfile) {
		logfile = fopen(q->logfile, "r");
		if(!logfile) {
			debug(D_NOTICE, "couldn't open logfile %s: %s\n", q->logfile, strerror(errno));
			return -1;
		}
	}

	while(1) {
		/*
		   Note: clearerr is necessary to clear any cached end-of-file condition,
		   otherwise some implementations of fgets (i.e. darwin) will read to end
		   of file once and then never look for any more data.
		 */

		clearerr(logfile);

		char line[BATCH_JOB_LINE_MAX];
		while(fgets(line, sizeof(line), logfile)) {
			int type, proc, subproc;
			batch_job_id_t jobid;
			time_t current;
			struct tm tm;

			struct batch_job_info *info;
			int logcode, exitcode;

			if(sscanf(line, "%d (%d.%d.%d) %d/%d %d:%d:%d", &type, &jobid, &proc, &subproc, &tm.tm_mon, &tm.tm_mday, &tm.tm_hour, &tm.tm_min, &tm.tm_sec) == 9) {
				tm.tm_year = 2008 - 1900;
				tm.tm_isdst = 0;

				current = mktime(&tm);

				info = itable_lookup(q->job_table, jobid);
				if(!info) {
					info = malloc(sizeof(*info));
					memset(info, 0, sizeof(*info));
					itable_insert(q->job_table, jobid, info);
				}


				debug(D_DEBUG, "line: %s", line);

				if(type == 0) {
					info->submitted = current;
				} else if(type == 1) {
					info->started = current;
					debug(D_DEBUG, "job %d running now", jobid);
				} else if(type == 9) {
					itable_remove(q->job_table, jobid);

					info->finished = current;
					info->exited_normally = 0;
					info->exit_signal = SIGKILL;

					debug(D_DEBUG, "job %d was removed", jobid);

					memcpy(info_out, info, sizeof(*info));

					return jobid;
				} else if(type == 5) {
					itable_remove(q->job_table, jobid);

					info->finished = current;

					fgets(line, sizeof(line), logfile);
					if(sscanf(line, " (%d) Normal termination (return value %d)", &logcode, &exitcode) == 2) {
						debug(D_DEBUG, "job %d completed normally with status %d.", jobid, exitcode);
						info->exited_normally = 1;
						info->exit_code = exitcode;
					} else if(sscanf(line, " (%d) Abnormal termination (signal %d)", &logcode, &exitcode) == 2) {
						debug(D_DEBUG, "job %d completed abnormally with signal %d.", jobid, exitcode);
						info->exited_normally = 0;
						info->exit_signal = exitcode;
					} else {
						debug(D_DEBUG, "job %d completed with unknown status.", jobid);
						info->exited_normally = 0;
						info->exit_signal = 0;
					}

					memcpy(info_out, info, sizeof(*info));
					return jobid;
				}
			}
		}


		if(itable_size(q->job_table) <= 0)
			return 0;

		if(stoptime != 0 && time(0) >= stoptime)
			return -1;

		if(process_pending())
			return -1;

		sleep(1);
	}

	return -1;
}

int batch_job_remove_condor(struct batch_queue *q, batch_job_id_t jobid)
{
	char *command = string_format("condor_rm %d", jobid);

	debug(D_DEBUG, "%s", command);
	FILE *file = popen(command, "r");
	free(command);
	if(!file) {
		debug(D_DEBUG, "condor_rm failed");
		return 0;
	} else {
		char buffer[1024];
		while (fread(buffer, sizeof(char), sizeof(buffer)/sizeof(char), file) > 0)
		  ;
		pclose(file);
		return 1;
	}
}
