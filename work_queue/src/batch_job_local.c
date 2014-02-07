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

static batch_job_id_t batch_job_local_submit_simple (struct batch_queue *q, const char *cmd, const char *extra_input_files, const char *extra_output_files)
{
	batch_job_id_t jobid;

	fflush(NULL);
	jobid = fork();
	if(jobid > 0) {
		debug(D_BATCH, "started process %" PRIbjid ": %s", jobid, cmd);
		struct batch_job_info *info = malloc(sizeof(*info));
		memset(info, 0, sizeof(*info));
		info->submitted = time(0);
		info->started = time(0);
		itable_insert(q->job_table, jobid, info);
		return jobid;
	} else if(jobid < 0) {
		debug(D_BATCH, "couldn't create new process: %s\n", strerror(errno));
		return -1;
	} else {
		/** The following code works but would duplicates the current process because of the system() function.
		int result = system(cmd);
		if(WIFEXITED(result)) {
			_exit(WEXITSTATUS(result));
		} else {
			_exit(1);
		}*/

		/** A note from "man system 3" as of Jan 2012:
		 * Do not use system() from a program with set-user-ID or set-group-ID
		 * privileges, because strange values for some environment variables
		 * might be used to subvert system integrity. Use the exec(3) family of
		 * functions instead, but not execlp(3) or execvp(3). system() will
		 * not, in fact, work properly from programs with set-user-ID or
		 * set-group-ID privileges on systems on which /bin/sh is bash version
		 * 2, since bash 2 drops privileges on startup. (Debian uses a modified
		 * bash which does not do this when invoked as sh.) 
		 */
		execlp("sh", "sh", "-c", cmd, (char *) 0);
		_exit(127);	// Failed to execute the cmd.
	}
	return -1;
}

static batch_job_id_t batch_job_local_submit (struct batch_queue * q, const char *cmd, const char *args, const char *infile, const char *outfile, const char *errfile, const char *extra_input_files, const char *extra_output_files)
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

	batch_job_id_t status = batch_job_local_submit_simple(q, command, extra_input_files, extra_output_files);
	free(command);
	return status;
}

static batch_job_id_t batch_job_local_wait (struct batch_queue * q, struct batch_job_info * info_out, time_t stoptime)
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

static int batch_job_local_remove (struct batch_queue *q, batch_job_id_t jobid)
{
	if(itable_lookup(q->job_table, jobid)) {
		if(kill(jobid, SIGTERM) == 0) {
			debug(D_BATCH, "signalled process %" PRIbjid, jobid);
			return 1;
		} else {
			debug(D_BATCH, "could not signal process %" PRIbjid ": %s\n", jobid, strerror(errno));
			return 0;
		}
	} else {
		debug(D_BATCH, "process %" PRIbjid " is not under my control.\n", jobid);
		return 0;
	}
}

batch_queue_stub_create(local);
batch_queue_stub_free(local);
batch_queue_stub_port(local);
batch_queue_stub_option_update(local);

batch_fs_stub_chdir(local);
batch_fs_stub_getcwd(local);
batch_fs_stub_mkdir(local);
batch_fs_stub_putfile(local);
batch_fs_stub_stat(local);
batch_fs_stub_unlink(local);

const struct batch_queue_module batch_queue_local = {
	BATCH_QUEUE_TYPE_LOCAL,
	"local",

	batch_queue_local_create,
	batch_queue_local_free,
	batch_queue_local_port,
	batch_queue_local_option_update,

	{
		batch_job_local_submit,
		batch_job_local_submit_simple,
		batch_job_local_wait,
		batch_job_local_remove,
	},

	{
		batch_fs_local_chdir,
		batch_fs_local_getcwd,
		batch_fs_local_mkdir,
		batch_fs_local_putfile,
		batch_fs_local_stat,
		batch_fs_local_unlink,
	},
};

/* vim: set noexpandtab tabstop=4: */
