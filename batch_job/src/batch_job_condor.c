#include "batch_job.h"
#include "batch_job_internal.h"
#include "debug.h"
#include "itable.h"
#include "path.h"
#include "process.h"
#include "stringtools.h"
#include "xxmalloc.h"

#include <stdlib.h>
#include <stdio.h>
#include <errno.h>
#include <string.h>
#include <signal.h>
#include <sys/stat.h>

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

static char *blacklisted_expression(struct batch_queue *q) {
	const char *blacklisted     = hash_table_lookup(q->options, "workers-blacklisted");
	static char *last_blacklist = NULL;

	if(!blacklisted)
		return NULL;

	/* print blacklist only when it changes. */
	if(!last_blacklist || strcmp(last_blacklist, blacklisted) != 0) {
		debug(D_BATCH, "Blacklisted hostnames: %s\n", blacklisted);
	}

	buffer_t b;
	buffer_init(&b);

	char *blist = xxstrdup(blacklisted);


	/* strsep updates blist, so we keep the original pointer in binit so we can free it later */
	char *binit = blist;

	char *sep = "";
	char *hostname;

	buffer_printf(&b, "(");
	while((hostname = strsep(&blist, " "))) {
		buffer_printf(&b, "%s(machine != \"%s\")", sep, hostname);

		sep = " && ";
	}
	buffer_printf(&b, ")");

	char *result = xxstrdup(buffer_tostring(&b));

	free(binit);
	buffer_free(&b);

	if(last_blacklist) {
		free(last_blacklist);
	}

	last_blacklist = xxstrdup(blacklisted);

	return result;
}


static batch_job_id_t batch_job_condor_submit (struct batch_queue *q, const char *cmd, const char *extra_input_files, const char *extra_output_files, struct jx *envlist, struct rmsummary *resources )
{
	FILE *file;
	int njobs;
	int jobid;
	const char *options = hash_table_lookup(q->options, "batch-options");

	if(setup_condor_wrapper("condor.sh") < 0) {
		debug(D_BATCH, "could not create condor.sh: %s", strerror(errno));
		return -1;
	}

	if(!string_istrue(hash_table_lookup(q->options, "skip-afs-check"))) {
		char *cwd = path_getcwd();
		if(!strncmp(cwd, "/afs", 4)) {
			debug(D_NOTICE|D_BATCH, "The working directory is '%s':", cwd);
			debug(D_NOTICE|D_BATCH, "This won't work because Condor is not able to write to files in AFS.");
			debug(D_NOTICE|D_BATCH, "Instead, run makeflow from a local disk like /tmp.");
			debug(D_NOTICE|D_BATCH, "Or, use the Work Queue with -T wq and condor_submit_workers.");
			free(cwd);
			exit(EXIT_FAILURE);
		}
		free(cwd);
	}

	file = fopen("condor.submit", "w");
	if(!file) {
		debug(D_BATCH, "could not create condor.submit: %s", strerror(errno));
		return -1;
	}

	fprintf(file, "universe = vanilla\n");
	fprintf(file, "executable = condor.sh\n");
	fprintf(file, "arguments = %s\n",cmd);
	if(extra_input_files)
		fprintf(file, "transfer_input_files = %s\n", extra_input_files);
	// Note that we do not use transfer_output_files, because that causes the job
	// to get stuck in a system hold if the files are not created.
	fprintf(file, "should_transfer_files = yes\n");
	fprintf(file, "when_to_transfer_output = on_exit\n");
	fprintf(file, "notification = never\n");
	fprintf(file, "copy_to_spool = true\n");
	fprintf(file, "transfer_executable = true\n");
	fprintf(file, "keep_claim_idle = 30\n");
	fprintf(file, "log = %s\n", q->logfile);

	const char *c_req = batch_queue_get_option(q, "condor-requirements");
	char *bexp = blacklisted_expression(q);

	if(c_req && bexp) {
		fprintf(file, "requirements = %s && %s\n", c_req, bexp);
	} else if(c_req) {
		fprintf(file, "requirements = %s\n", c_req);
	} else if(bexp) {
		fprintf(file, "requirements = %s\n", bexp);
	}

	if(bexp)
		free(bexp);

	/*
	Getting environment variables formatted for a condor submit
	file is very hairy, due to some strange quoting rules.
	To avoid problems, we simply export vars to the environment,
	and then tell condor getenv=true, which pulls in the environment.
	*/

	fprintf(file, "getenv = true\n");

	if(envlist) {
		jx_export(envlist);
	}

	if(options)
		fprintf(file, "%s\n", options);

	int64_t cores  = -1;
	int64_t memory = -1;
	int64_t disk   = -1;

	if(resources) {
		cores  = resources->cores;
		memory = resources->memory;
		disk   = resources->disk;
	}

	if(batch_queue_get_option(q, "autosize")) {
		fprintf(file, "request_cpus   = ifThenElse(%" PRId64 " > TotalSlotCpus, %" PRId64 ", TotalSlotCpus)\n", cores, cores);
		fprintf(file, "request_memory = ifThenElse(%" PRId64 " > TotalSlotMemory, %" PRId64 ", TotalSlotMemory)\n", memory, memory);
		fprintf(file, "request_disk   = ifThenElse((%" PRId64 "*1024) > TotalSlotDisk, (%" PRId64 "*1024), TotalSlotDisk)\n", disk, disk);
	}
	else {
		if(cores > -1)
			fprintf(file, "request_cpus = %" PRId64 "\n", cores);

		if(memory > -1)
			fprintf(file, "request_memory = %" PRId64 "\n", memory);

		if(disk > -1)
			fprintf(file, "request_disk = (%" PRId64 "*1024)\n", disk);
	}

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
	batch_queue_set_feature(q, "batch_log_name", "%s.condorlog");
	batch_queue_set_feature(q, "autosize", "yes");

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
