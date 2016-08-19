#include "batch_job.h"
#include "batch_job_internal.h"
#include "work_queue.h"
#include "work_queue_internal.h" /* EVIL */
#include "debug.h"
#include "path.h"
#include "stringtools.h"
#include "macros.h"
#include "rmsummary.h"

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>

static void specify_files(struct work_queue_task *t, const char *input_files, const char *output_files, int caching_flag )
{
	char *f, *p, *files;

	if(input_files) {
		files = strdup(input_files);
		f = strtok(files, " \t,");
		while(f) {
			p = strchr(f, '=');
			if(p) {
				*p = 0;
				work_queue_task_specify_file(t, f, p + 1, WORK_QUEUE_INPUT, caching_flag);
				*p = '=';
			} else {
				work_queue_task_specify_file(t, f, f, WORK_QUEUE_INPUT, caching_flag);
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
				work_queue_task_specify_file(t, f, p + 1, WORK_QUEUE_OUTPUT, caching_flag);
				*p = '=';
			} else {
				work_queue_task_specify_file(t, f, f, WORK_QUEUE_OUTPUT, caching_flag);
			}
			f = strtok(0, " \t,");
		}
		free(files);
	}
}

static void specify_envlist( struct work_queue_task *t, struct jx *envlist )
{
	if(envlist) {
		struct jx_pair *p;
		for(p=envlist->u.pairs;p;p=p->next) {
			work_queue_task_specify_enviroment_variable(t,p->key->u.string_value,p->value->u.string_value);
		}
	}
}

static batch_job_id_t batch_job_wq_submit (struct batch_queue * q, const char *cmd, const char *extra_input_files, const char *extra_output_files, struct jx *envlist, const struct rmsummary *resources)
{
	struct work_queue_task *t;

	int caching_flag = WORK_QUEUE_CACHE;

	if(string_istrue(hash_table_lookup(q->options, "caching"))) {
		caching_flag = WORK_QUEUE_CACHE;
	} else {
		caching_flag = WORK_QUEUE_NOCACHE;
	}

	t = work_queue_task_create(cmd);

	specify_files(t, extra_input_files, extra_output_files, caching_flag);
	specify_envlist(t,envlist);

	if(envlist) {
		const char *category = jx_lookup_string(envlist, "CATEGORY");
		if(category) {
			work_queue_task_specify_category(t, category);
		}
	}

	if(resources)
	{
		work_queue_task_specify_resources(t, resources);
	}

	work_queue_submit(q->data, t);

	return t->taskid;
}

static batch_job_id_t batch_job_wq_wait (struct batch_queue * q, struct batch_job_info * info, time_t stoptime)
{
	static int try_open_log = 0;
	int timeout, taskid = -1;

	if(!try_open_log)
	{
		try_open_log = 1;
		if(!work_queue_specify_log(q->data, q->logfile))
		{
			return -1;
		}
	}

	if(stoptime == 0) {
		timeout = WORK_QUEUE_WAITFORTASK;
	} else {
		timeout = MAX(0, stoptime - time(0));
	}

	struct work_queue_task *t = work_queue_wait(q->data, timeout);
	if(t) {
		info->submitted = t->time_when_submitted / 1000000;
		info->started   = t->time_when_commit_end / 1000000;
		info->finished  = t->time_when_done / 1000000;
		info->exited_normally = 1;
		info->exit_code = t->return_status;
		info->exit_signal = 0;
		info->disk_allocation_exhausted = t->disk_allocation_exhausted;

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

		taskid = t->taskid;
		work_queue_task_delete(t);
	}

	if(taskid >= 0) {
		return taskid;
	}

	if(work_queue_empty(q->data)) {
		return 0;
	} else {
		return -1;
	}
}

static int batch_job_wq_remove (struct batch_queue *q, batch_job_id_t jobid)
{
	return 0;
}

static int batch_queue_wq_create (struct batch_queue *q)
{
	strncpy(q->logfile, "wq.log", sizeof(q->logfile));
	if ((q->data = work_queue_create(0)) == NULL)
		return -1;
	work_queue_enable_process_module(q->data);
	batch_queue_set_feature(q, "absolute_path", NULL);
	batch_queue_set_feature(q, "remote_rename", "%s=%s");
	batch_queue_set_feature(q, "batch_log_name", "%s.wqlog");
	return 0;
}

static int batch_queue_wq_free (struct batch_queue *q)
{
	if (q->data) {
		work_queue_delete(q->data);
		q->data = NULL;
	}
	return 0;
}

static int batch_queue_wq_port (struct batch_queue *q)
{
	return work_queue_port(q->data);
}

static void batch_queue_wq_option_update (struct batch_queue *q, const char *what, const char *value)
{
	if(strcmp(what, "password") == 0) {
		if(value)
			work_queue_specify_password(q->data, value);
	} else if(strcmp(what, "master-mode") == 0) {
		if(strcmp(value, "catalog") == 0)
			work_queue_specify_master_mode(q->data, WORK_QUEUE_MASTER_MODE_CATALOG);
		else if(strcmp(value, "standalone") == 0)
			work_queue_specify_master_mode(q->data, WORK_QUEUE_MASTER_MODE_STANDALONE);
	} else if(strcmp(what, "name") == 0) {
		if(value)
			work_queue_specify_name(q->data, value);
	} else if(strcmp(what, "priority") == 0) {
		if(value)
			work_queue_specify_priority(q->data, atoi(value));
		else
			work_queue_specify_priority(q->data, 0);
	} else if(strcmp(what, "fast-abort") == 0) {
		if(value)
			work_queue_activate_fast_abort(q->data, atof(value));
	} else if(strcmp(what, "estimate-capacity") == 0) {
		work_queue_specify_estimate_capacity_on(q->data, string_istrue(value));
	} else if(strcmp(what, "keepalive-interval") == 0) {
		if(value)
			work_queue_specify_keepalive_interval(q->data, atoi(value));
		else
			work_queue_specify_keepalive_interval(q->data, WORK_QUEUE_DEFAULT_KEEPALIVE_INTERVAL);
	} else if(strcmp(what, "keepalive-timeout") == 0) {
		if(value)
			work_queue_specify_keepalive_timeout(q->data, atoi(value));
		else
			work_queue_specify_keepalive_timeout(q->data, WORK_QUEUE_DEFAULT_KEEPALIVE_TIMEOUT);
	} else if(strcmp(what, "master-preferred-connection") == 0) {
		if(value)
			work_queue_master_preferred_connection(q->data, value);
		else
			work_queue_master_preferred_connection(q->data, "by_ip");
	} else if(strcmp(what, "category-limits") == 0) {
		struct rmsummary *s = rmsummary_parse_string(value);
		if(s) {
			work_queue_specify_category_max_resources(q->data, s->category, s);
			rmsummary_delete(s);
		} else {
			debug(D_NOTICE, "Could no parse '%s' as a summary of resorces encoded in JSON\n", value);
		}
	}
}

batch_fs_stub_chdir(wq);
batch_fs_stub_getcwd(wq);
batch_fs_stub_mkdir(wq);
batch_fs_stub_putfile(wq);
batch_fs_stub_stat(wq);
batch_fs_stub_unlink(wq);

const struct batch_queue_module batch_queue_wq = {
	BATCH_QUEUE_TYPE_WORK_QUEUE,
	"wq",

	batch_queue_wq_create,
	batch_queue_wq_free,
	batch_queue_wq_port,
	batch_queue_wq_option_update,

	{
		batch_job_wq_submit,
		batch_job_wq_wait,
		batch_job_wq_remove,
	},

	{
		batch_fs_wq_chdir,
		batch_fs_wq_getcwd,
		batch_fs_wq_mkdir,
		batch_fs_wq_putfile,
		batch_fs_wq_stat,
		batch_fs_wq_unlink,
	},
};

/* vim: set noexpandtab tabstop=4: */
