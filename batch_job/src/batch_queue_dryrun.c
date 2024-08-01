/*
Copyright (C) 2024 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <signal.h>

#include "batch_queue.h"
#include "batch_queue_internal.h"
#include "debug.h"
#include "process.h"
#include "macros.h"
#include "stringtools.h"
#include "path.h"
#include "xxmalloc.h"

static batch_queue_id_t batch_queue_dryrun_submit (struct batch_queue *q, struct batch_job *bt )
{
	FILE *log;
	char *escaped_cmd;
	char *env_assignment;
	char *escaped_env_assignment;
	struct batch_job_info *info;
	batch_queue_id_t jobid = random();

	fflush(NULL);

	debug(D_BATCH, "started dry run of job %" PRIbjid ": %s", jobid, bt->command );

	if ((log = fopen(q->logfile, "a"))) {
		if (!(info = calloc(1, sizeof(*info)))) {
			fclose(log);
			return -1;
		}
		info->submitted = time(0);
		info->started = time(0);
		itable_insert(q->job_table, jobid, info);

		if(bt->envlist && jx_istype(bt->envlist, JX_OBJECT) && bt->envlist->u.pairs) {
			struct jx_pair *p;
			fprintf(log, "env ");
			for(p=bt->envlist->u.pairs;p;p=p->next) {
				if(p->key->type==JX_STRING && p->value->type==JX_STRING) {
					env_assignment = string_format("%s=%s", p->key->u.string_value,p->value->u.string_value);
					escaped_env_assignment = string_escape_shell(env_assignment);
					fprintf(log, "%s", escaped_env_assignment);
					fprintf(log, " ");
					free(env_assignment);
					free(escaped_env_assignment);
				}
			}
		}
		escaped_cmd = string_escape_shell(bt->command);
		fprintf(log, "sh -c %s\n", escaped_cmd);
		free(escaped_cmd);
		fclose(log);
		return jobid;
	} else {
		return -1;
	}
}

static batch_queue_id_t batch_queue_dryrun_wait (struct batch_queue * q, struct batch_job_info * info_out, time_t stoptime)
{
	struct batch_job_info *info;
	UINT64_T jobid;

	itable_firstkey(q->job_table);
	if (itable_nextkey(q->job_table, &jobid, NULL)) {
		info = itable_remove(q->job_table, jobid);
		info->finished = time(0);
		info->exited_normally = 1;
		info->exit_code = 0;
		memcpy(info_out, info, sizeof(*info));
		free(info);
		return jobid;
	} else {
		return 0;
	}
}

static int batch_queue_dryrun_remove (struct batch_queue *q, batch_queue_id_t jobid)
{
	return 0;
}

static int batch_queue_dryrun_create (struct batch_queue *q)
{
	char *cwd = path_getcwd();

	batch_queue_set_feature(q, "local_job_queue", NULL);
	batch_queue_set_feature(q, "batch_log_name", "%s.sh");
	batch_queue_set_option(q, "cwd", cwd);
	return 0;
}

batch_queue_stub_free(dryrun);
batch_queue_stub_port(dryrun);
batch_queue_stub_option_update(dryrun);

const struct batch_queue_module batch_queue_dryrun = {
	BATCH_QUEUE_TYPE_DRYRUN,
	"dryrun",

	batch_queue_dryrun_create,
	batch_queue_dryrun_free,
	batch_queue_dryrun_port,
	batch_queue_dryrun_option_update,

	batch_queue_dryrun_submit,
	batch_queue_dryrun_wait,
	batch_queue_dryrun_remove,
};


/* vim: set noexpandtab tabstop=8: */
