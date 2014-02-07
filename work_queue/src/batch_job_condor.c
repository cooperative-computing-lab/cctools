#include "batch_job.h"
#include "batch_job_internal.h"
#include "debug.h"
#include "itable.h"
#include "path.h"
#include "process.h"
#include "stringtools.h"

#include <stdlib.h>
#include <stdio.h>
#include <errno.h>
#include <string.h>
#include <signal.h>
#include <sys/stat.h>

static batch_job_id_t batch_job_condor_submit (struct batch_queue *q, const char *cmd, const char *args, const char *infile, const char *outfile, const char *errfile, const char *extra_input_files, const char *extra_output_files)
{
	FILE *file;
	int njobs;
	int jobid;
	const char *options = hash_table_lookup(q->options, "batch-options");

	if(!string_istrue(hash_table_lookup(q->options, "skip-afs-check"))) {
		char *cwd = path_getcwd();
		if(!strncmp(cwd, "/afs", 4)) {
			debug(D_NOTICE|D_BATCH, "makeflow: This won't work because Condor is not able to write to files in AFS.\n");
			debug(D_NOTICE|D_BATCH, "makeflow: Instead, run makeflow from a local disk like /tmp.\n");
			debug(D_NOTICE|D_BATCH, "makeflow: Or, use the Work Queue with -T wq and condor_submit_workers.\n");
			return -1;
		}
		free(cwd);
	}

	file = fopen("condor.submit", "w");
	if(!file) {
		debug(D_BATCH, "could not create condor.submit: %s", strerror(errno));
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
	if(options)
		fprintf(file, "%s\n", options);
	fprintf(file, "queue\n");
	fclose(file);

	file = popen("condor_submit condor.submit", "r");
	if(!file)
		return -1;

	char line[BATCH_JOB_LINE_MAX];
	while(fgets(line, sizeof(line), file)) {
		if(sscanf(line, "%d job(s) submitted to cluster %d", &njobs, &jobid) == 2) {
			pclose(file);
			debug(D_BATCH, "job %d submitted to condor", jobid);
			struct batch_job_info *info;
			info = malloc(sizeof(*info));
			memset(info, 0, sizeof(*info));
			info->submitted = time(0);
			itable_insert(q->job_table, jobid, info);
			return jobid;
		}
	}

	pclose(file);
	debug(D_BATCH, "failed to submit job to condor!");
	return -1;
}

static int setup_condor_wrapper(const char *wrapperfile)
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

static batch_job_id_t batch_job_condor_submit_simple (struct batch_queue *q, const char *cmd, const char *extra_input_files, const char *extra_output_files)
{
	if(setup_condor_wrapper("condor.sh") < 0) {
		debug(D_BATCH, "could not create condor.sh: %s", strerror(errno));
		return -1;
	}

	return batch_job_condor_submit(q, "condor.sh", cmd, 0, 0, 0, extra_input_files, extra_output_files);
}

static batch_job_id_t batch_job_condor_wait (struct batch_queue * q, struct batch_job_info * info_out, time_t stoptime)
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

			if(sscanf(line, "%d (%" SCNbjid ".%d.%d) %d/%d %d:%d:%d", &type, &jobid, &proc, &subproc, &tm.tm_mon, &tm.tm_mday, &tm.tm_hour, &tm.tm_min, &tm.tm_sec) == 9) {
				tm.tm_year = 2008 - 1900;
				tm.tm_isdst = 0;

				current = mktime(&tm);

				info = itable_lookup(q->job_table, jobid);
				if(!info) {
					info = malloc(sizeof(*info));
					memset(info, 0, sizeof(*info));
					itable_insert(q->job_table, jobid, info);
				}

				debug(D_BATCH, "line: %s", line);

				if(type == 0) {
					info->submitted = current;
				} else if(type == 1) {
					info->started = current;
					debug(D_BATCH, "job %" PRIbjid " running now", jobid);
				} else if(type == 9) {
					itable_remove(q->job_table, jobid);

					info->finished = current;
					info->exited_normally = 0;
					info->exit_signal = SIGKILL;

					debug(D_BATCH, "job %" PRIbjid " was removed", jobid);

					memcpy(info_out, info, sizeof(*info));
					free(info);
					return jobid;
				} else if(type == 5) {
					itable_remove(q->job_table, jobid);

					info->finished = current;

					fgets(line, sizeof(line), logfile);
					if(sscanf(line, " (%d) Normal termination (return value %d)", &logcode, &exitcode) == 2) {
						debug(D_BATCH, "job %" PRIbjid " completed normally with status %d.", jobid, exitcode);
						info->exited_normally = 1;
						info->exit_code = exitcode;
					} else if(sscanf(line, " (%d) Abnormal termination (signal %d)", &logcode, &exitcode) == 2) {
						debug(D_BATCH, "job %" PRIbjid " completed abnormally with signal %d.", jobid, exitcode);
						info->exited_normally = 0;
						info->exit_signal = exitcode;
					} else {
						debug(D_BATCH, "job %" PRIbjid " completed with unknown status.", jobid);
						info->exited_normally = 0;
						info->exit_signal = 0;
					}

					memcpy(info_out, info, sizeof(*info));
					free(info);
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

static int batch_job_condor_remove (struct batch_queue *q, batch_job_id_t jobid)
{
	char *command = string_format("condor_rm %" PRIbjid, jobid);

	debug(D_BATCH, "%s", command);
	FILE *file = popen(command, "r");
	free(command);
	if(!file) {
		debug(D_BATCH, "condor_rm failed");
		return 0;
	} else {
		char buffer[1024];
		while (fread(buffer, sizeof(char), sizeof(buffer)/sizeof(char), file) > 0)
		  ;
		pclose(file);
		return 1;
	}
}

static int batch_queue_condor_create (struct batch_queue *q)
{
	strncpy(q->logfile, "condor.logfile", sizeof(q->logfile));

	return 0;
}

batch_queue_stub_free(condor);
batch_queue_stub_port(condor);
batch_queue_stub_option_update(condor);

batch_fs_stub_chdir(condor);
batch_fs_stub_getcwd(condor);
batch_fs_stub_mkdir(condor);
batch_fs_stub_putfile(condor);
batch_fs_stub_stat(condor);
batch_fs_stub_unlink(condor);

const struct batch_queue_module batch_queue_condor = {
	BATCH_QUEUE_TYPE_CONDOR,
	"condor",

	batch_queue_condor_create,
	batch_queue_condor_free,
	batch_queue_condor_port,
	batch_queue_condor_option_update,

	{
		batch_job_condor_submit,
		batch_job_condor_submit_simple,
		batch_job_condor_wait,
		batch_job_condor_remove,
	},

	{
		batch_fs_condor_chdir,
		batch_fs_condor_getcwd,
		batch_fs_condor_mkdir,
		batch_fs_condor_putfile,
		batch_fs_condor_stat,
		batch_fs_condor_unlink,
	},
};

/* vim: set noexpandtab tabstop=4: */
