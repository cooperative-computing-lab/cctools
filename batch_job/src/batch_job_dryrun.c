/*
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <signal.h>

#include "batch_job.h"
#include "batch_job_internal.h"
#include "debug.h"
#include "process.h"
#include "macros.h"
#include "stringtools.h"
#include "path.h"
#include "xxmalloc.h"

static batch_job_id_t batch_job_dryrun_submit (struct batch_queue *q, const char *cmd, const char *extra_input_files, const char *extra_output_files, struct jx *envlist, const struct rmsummary *resources )
{
	FILE *log;
	char *escaped_cmd;
	char *env_assignment;
	char *escaped_env_assignment;
	struct batch_job_info *info;
	batch_job_id_t jobid = random();

	fflush(NULL);

	debug(D_BATCH, "started dry run of job %" PRIbjid ": %s", jobid, cmd);

	if ((log = fopen(q->logfile, "a"))) {
		if (!(info = calloc(sizeof(*info), 1))) {
			fclose(log);
			return -1;
		}
		info->submitted = time(0);
		info->started = time(0);
		itable_insert(q->job_table, jobid, info);

		if(envlist && jx_istype(envlist, JX_OBJECT) && envlist->u.pairs) {
			struct jx_pair *p;
			fprintf(log, "env ");
			for(p=envlist->u.pairs;p;p=p->next) {
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
		escaped_cmd = string_escape_shell(cmd);
		fprintf(log, "sh -c %s\n", escaped_cmd);
		free(escaped_cmd);
		fclose(log);
		return jobid;
	} else {
		return -1;
	}
}

static batch_job_id_t batch_job_dryrun_wait (struct batch_queue * q, struct batch_job_info * info_out, time_t stoptime)
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

static int batch_job_dryrun_remove (struct batch_queue *q, batch_job_id_t jobid)
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

static int batch_fs_dryrun_stat (struct batch_queue *q, const char *path, struct stat *buf) {
	struct stat dummy;
	FILE *log;

	if ((log = fopen(q->logfile, "a"))) {
		char *escaped_path = string_escape_shell(path);
		// Since Makeflow only calls stat *after* a file has been created,
		// add a test here as a sanity check. If Makeflow e.g. tries to stat
		// files before running rules to create them, these tests will
		// cripple the shell script representation.
		fprintf(log, "test -e %s\n", escaped_path);
		free(escaped_path);
		fclose(log);
		dummy.st_size = 1;
		memcpy(buf, &dummy, sizeof(dummy));
		return 0;
	} else {
		return -1;
	}
}

static int batch_fs_dryrun_mkdir (struct batch_queue *q, const char *path, mode_t mode, int recursive) {
	FILE *log;

	if ((log = fopen(q->logfile, "a"))) {
		char *escaped_path = string_escape_shell(path);
		if (recursive) {
			fprintf(log, "mkdir -p -m %d %s\n", mode, escaped_path);
		} else {
			fprintf(log, "mkdir -m %d %s\n", mode, escaped_path);
		}
		fclose(log);
		free(escaped_path);
		return 0;
	} else {
		return -1;
	}
}

static int batch_fs_dryrun_chdir (struct batch_queue *q, const char *path) {
	FILE *log;

	if ((log = fopen(q->logfile, "a"))) {
		char *escaped_path = string_escape_shell(path);
		batch_queue_set_option(q, "cwd", xxstrdup(path));

		fprintf(log, "cd %s\n", escaped_path);

		fclose(log);
		free(escaped_path);
		return 0;
	} else {
		return -1;
	}
}

static int batch_fs_dryrun_getcwd (struct batch_queue *q, char *buf, size_t size) {
	const char *cwd = batch_queue_get_option(q, "cwd");
	size_t pathlength = strlen(cwd);
	if (pathlength + 1 > size) {
		errno = ERANGE;
		return -1;
	} else {
		strcpy(buf, cwd);
		return 0;
	}
}

static int batch_fs_dryrun_unlink (struct batch_queue *q, const char *path) {
	FILE *log;

	if ((log = fopen(q->logfile, "a"))) {
		char *escaped_path = string_escape_shell(path);
		fprintf(log, "rm -r %s\n", escaped_path);
		free(escaped_path);
		fclose(log);
		return 0;
	} else {
		return -1;
	}
}

static int batch_fs_dryrun_putfile (struct batch_queue *q, const char *lpath, const char *rpath) {
	FILE *log;

	if ((log = fopen(q->logfile, "a"))) {
		char *escaped_lpath = string_escape_shell(lpath);
		char *escaped_rpath = string_escape_shell(rpath);
		fprintf(log, "cp %s %s\n", escaped_lpath, escaped_rpath);
		free(escaped_lpath);
		free(escaped_rpath);
		fclose(log);
		return 0;
	} else {
		return -1;
	}
}

static int batch_fs_dryrun_rename (struct batch_queue *q, const char *lpath, const char *rpath) {
	FILE *log;

	if ((log = fopen(q->logfile, "a"))) {
		char *escaped_lpath = string_escape_shell(lpath);
		char *escaped_rpath = string_escape_shell(rpath);
		fprintf(log, "mv %s %s\n", escaped_lpath, escaped_rpath);
		free(escaped_lpath);
		free(escaped_rpath);
		fclose(log);
		return 0;
	} else {
		return -1;
	}
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

	{
		batch_job_dryrun_submit,
		batch_job_dryrun_wait,
		batch_job_dryrun_remove,
	},

	{
		batch_fs_dryrun_chdir,
		batch_fs_dryrun_getcwd,
		batch_fs_dryrun_mkdir,
		batch_fs_dryrun_putfile,
		batch_fs_dryrun_rename,
		batch_fs_dryrun_stat,
		batch_fs_dryrun_unlink,
	},
};

/* vim: set noexpandtab tabstop=8: */
