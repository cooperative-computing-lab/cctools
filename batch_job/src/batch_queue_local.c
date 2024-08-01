/*
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "batch_queue.h"
#include "batch_queue_internal.h"
#include "debug.h"
#include "process.h"
#include "macros.h"
#include "stringtools.h"

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <signal.h>

#if defined(CCTOOLS_OPSYS_DARWIN)
/* no such header */
#elif defined(CCTOOLS_OPSYS_FREEBSD)
#include <sys/procctl.h>
#else
#include <sys/prctl.h>
#endif

static batch_queue_id_t batch_queue_local_submit (struct batch_queue *q, struct batch_task *bt )
{
	batch_queue_id_t jobid;

	fflush(NULL);
	jobid = fork();
	if(jobid > 0) {
		debug(D_BATCH, "started process %" PRIbjid ": %s", jobid, bt->command);
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
		if(bt->envlist) {
			jx_export(bt->envlist);
		}

		/* Force the child process to exit if the parent dies. */
#if defined(CCTOOLS_OPSYS_DARWIN)
		/* no such syscall */
#elif defined (CCTOOLS_OPSYS_FREEBSD)
		const int sig = SIGTERM;
		procctl (P_PID, 0, PROC_PDEATHSIG_CTL, (void *)&sig);
#else
		prctl (PR_SET_PDEATHSIG, SIGTERM);
#endif

		execlp("/bin/sh", "sh", "-c", bt->command, (char *) 0);
		_exit(127);	// Failed to execute the cmd.
	}
	return -1;
}

static batch_queue_id_t batch_queue_local_wait (struct batch_queue * q, struct batch_job_info * info_out, time_t stoptime)
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

static int batch_queue_local_remove (struct batch_queue *q, batch_queue_id_t jobid)
{
	int max_wait = 5; // maximum seconds we wish to wait for a given process
	process_kill_waitpid(jobid, max_wait);
	return 0;

}

static int batch_queue_local_create (struct batch_queue *q)
{
	batch_queue_set_feature(q, "local_job_queue", NULL);
	return 0;
}

batch_queue_stub_free(local);
batch_queue_stub_port(local);
batch_queue_stub_option_update(local);

const struct batch_queue_module batch_queue_local = {
	BATCH_QUEUE_TYPE_LOCAL,
	"local",

	batch_queue_local_create,
	batch_queue_local_free,
	batch_queue_local_port,
	batch_queue_local_option_update,

	batch_queue_local_submit,
	batch_queue_local_wait,
	batch_queue_local_remove,
};

/* vim: set noexpandtab tabstop=8: */
