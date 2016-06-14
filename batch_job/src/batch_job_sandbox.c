/*
Copyright (C) 2015- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

/*
The batch_job_sandbox module is very similar to the batch_job_local
module, except that it creates a subdirectory and links/renames files
in and out of the directory, to ensure a clean namespace.
Eventually, this module will supersede "local" as the default.
*/

#include "batch_job.h"
#include "batch_job_internal.h"
#include "debug.h"
#include "process.h"
#include "macros.h"
#include "stringtools.h"
#include "sandbox.h"
#include "itable.h"
#include "jx.h"

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <signal.h>

static struct itable * sandbox_table = 0;

static batch_job_id_t batch_job_sandbox_submit (struct batch_queue *q, const char *cmd, const char *extra_input_files, const char *extra_output_files, struct jx *envlist, const struct rmsummary *resources )
{
	batch_job_id_t jobid;

	struct sandbox *s = sandbox_create(".",extra_input_files,extra_output_files);
	if(!s) {
		debug(D_BATCH,"couldn't create sandbox for %s\n",cmd);
		return -1;
	}

	/* Always flush buffers just before fork, to avoid double output. */
	fflush(NULL);

	jobid = fork();
	if(jobid > 0) {
		/* In parent process after child has started. */
		debug(D_BATCH, "started process %" PRIbjid ": %s", jobid, cmd);

		struct batch_job_info *info = malloc(sizeof(*info));
		memset(info, 0, sizeof(*info));
		info->submitted = time(0);
		info->started = time(0);
		itable_insert(q->job_table, jobid, info);

		if(!sandbox_table) sandbox_table = itable_create(0);
		itable_insert(sandbox_table,jobid, s);
		
		return jobid;
	} else if(jobid < 0) {
		/* Fork failed, child process does not exist. */
		debug(D_BATCH, "couldn't create new process: %s\n", strerror(errno));
		sandbox_delete(s);
		return -1;
	} else {
		/*
		The child process following a fork before an exec
		is a very limited execution environment.
		Generally, we should not produce output, debug statements,
		or do anything that could conflict with actions by the
		parent process on the same file descriptors.
		If an error occurs, we call _exit() so as to end the
		process quickly without performing any cleanup or
		buffer flushes.
		*/

		/* Move to the sandbox directory. */
		if(chdir(s->sandbox_path)<0) {
			_exit(127);
		}

		/* Set up the environment specific to the child. */
		if(envlist) {
			jx_export(envlist);
		}

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

static batch_job_id_t batch_job_sandbox_wait (struct batch_queue * q, struct batch_job_info * info_out, time_t stoptime)
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

			struct sandbox *s = itable_lookup(sandbox_table,jobid);
			if(s) sandbox_cleanup(s);

			return jobid;

		} else if(errno == ESRCH || errno == ECHILD) {
			return 0;
		}

		if(stoptime != 0 && time(0) >= stoptime)
			return -1;
	}
}

static int batch_job_sandbox_remove (struct batch_queue *q, batch_job_id_t jobid)
{
	int status;

	if(kill(jobid, SIGTERM) == 0) {

		if(!itable_lookup(q->job_table, jobid)) {
			debug(D_BATCH, "runaway process %" PRIbjid "?\n", jobid);
			return 0;
		} else {
			debug(D_BATCH, "waiting for process %" PRIbjid, jobid);
			waitpid(jobid, &status, 0);

			struct sandbox *s = itable_lookup(sandbox_table,jobid);
			if(s) sandbox_cleanup(s);

			return 1;
		}
	} else {
		debug(D_BATCH, "could not signal process %" PRIbjid ": %s\n", jobid, strerror(errno));
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

const struct batch_queue_module batch_queue_sandbox = {
	BATCH_QUEUE_TYPE_SANDBOX,
	"sandbox",

	batch_queue_local_create,
	batch_queue_local_free,
	batch_queue_local_port,
	batch_queue_local_option_update,

	{
		batch_job_sandbox_submit,
		batch_job_sandbox_wait,
		batch_job_sandbox_remove,
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
