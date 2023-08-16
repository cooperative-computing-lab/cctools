/*
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "batch_job.h"
#include "batch_job_internal.h"
#include "taskvine.h"
#include "vine_manager.h" // Internal header for vine_enable_process_module
#include "debug.h"
#include "path.h"
#include "stringtools.h"
#include "macros.h"
#include "rmsummary.h"

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>

static struct vine_file * declare_once( struct batch_queue *q, const char *name, vine_file_flags_t flags )
{
	struct vine_file *f;

	if(!q->tv_file_table) q->tv_file_table = hash_table_create(0,0);

	f = hash_table_lookup(q->tv_file_table,name);
	if(!f) {
		f = vine_declare_file(q->tv_manager,name,flags);
		hash_table_insert(q->tv_file_table,name,f);
	}
	return f;
}

static void specify_files( struct batch_queue *q, struct vine_task *t, const char *input_files, const char *output_files, vine_file_flags_t flags )
{
	char *files;       // Copy of string list of files.
	char *local_name;  // Name of file at manager.
	char *remote_name; // Name of file in task sandbox.

	struct vine_file *f;
	
	if(input_files) {
		files = strdup(input_files);
		local_name = strtok(files, " \t,");
		while(local_name) {
			remote_name = strchr(local_name, '=');
			if(remote_name) {
				*remote_name = 0;
				f = declare_once(q,local_name,flags);
				vine_task_add_input(t,f,remote_name+1,0);
				*remote_name = '=';
			} else {
				f = declare_once(q,local_name,flags);
				vine_task_add_input(t,f,local_name,0);
			}
			local_name = strtok(0, " \t,");
		}
		free(files);
	}

	if(output_files) {
		files = strdup(output_files);
		local_name = strtok(files, " \t,");
		while(local_name) {
			remote_name = strchr(local_name, '=');
			if(remote_name) {
				*remote_name = 0;
				f = declare_once(q,local_name,flags);
				vine_task_add_output(t,f,remote_name+1,0);
				*remote_name = '=';
			} else {
				f = declare_once(q,local_name,flags);
				vine_task_add_output(t,f,local_name,0);
			}
			local_name = strtok(0, " \t,");
		}
		free(files);
	}
}

static void specify_envlist( struct vine_task *t, struct jx *envlist )
{
	if(envlist) {
		struct jx_pair *p;
		for(p=envlist->u.pairs;p;p=p->next) {
			vine_task_set_env_var(t,p->key->u.string_value,p->value->u.string_value);
		}
	}
}

static batch_job_id_t batch_job_vine_submit (struct batch_queue * q, const char *cmd, const char *extra_input_files, const char *extra_output_files, struct jx *envlist, const struct rmsummary *resources)
{
	struct vine_task *t;

	int caching_flag = VINE_CACHE;

	const char *caching_option = hash_table_lookup(q->options,"caching");
	if(!strcmp(caching_option,"never")) {
		caching_flag = VINE_CACHE_NEVER;
	} else if(!strcmp(caching_option,"workflow")) {
		caching_flag = VINE_CACHE;
	} else if(!strcmp(caching_option,"forever")) {
		caching_flag = VINE_CACHE_ALWAYS;
	} else {
		caching_flag = VINE_CACHE;
	}

	t = vine_task_create(cmd);

	specify_files(q, t, extra_input_files, extra_output_files, caching_flag);
	specify_envlist(t,envlist);

	if(envlist) {
		const char *category = jx_lookup_string(envlist, "CATEGORY");
		if(category) {
			vine_task_set_category(t, category);
		}
	}

	if(resources)
	{
		vine_task_set_resources(t, resources);
	}

	return vine_submit(q->tv_manager, t);
}

static batch_job_id_t batch_job_vine_wait (struct batch_queue * q, struct batch_job_info * info, time_t stoptime)
{
	int timeout, taskid = -1;

	if(stoptime == 0) {
		timeout = VINE_WAIT_FOREVER;
	} else {
		timeout = MAX(0, stoptime - time(0));
	}

	struct vine_task *t = vine_wait(q->tv_manager, timeout);
	if(t) {
		info->submitted = vine_task_get_metric(t,"time_when_submitted") / 1000000;
		info->started   = vine_task_get_metric(t,"time_when_commit_end") / 1000000;
		info->finished  = vine_task_get_metric(t,"time_when_done") / 1000000;
		info->exited_normally = 1;
		info->exit_code = vine_task_get_exit_code(t);
		info->exit_signal = 0;
		info->disk_allocation_exhausted = 0;
		
		/*
		   If the standard output of the job is not empty,
		   then print it, because this is analogous to a Unix
		   job, and would otherwise be lost.  Important for
		   capturing errors from the program.
		 */

		const char *s = vine_task_get_stdout(t);
		if(s[1] || s[0]!='\n') {
			printf("%s\n",s);
		}

		taskid = vine_task_get_id(t);
		vine_task_delete(t);
	}

	if(taskid >= 0) {
		return taskid;
	}

	if(vine_empty(q->tv_manager)) {
		return 0;
	} else {
		return -1;
	}
}

static int batch_job_vine_remove (struct batch_queue *q, batch_job_id_t jobid)
{
	return 0;
}

static int batch_queue_vine_create (struct batch_queue *q)
{
	strncpy(q->logfile, "vine.log", sizeof(q->logfile));
	if ((q->tv_manager = vine_create(0)) == NULL)
		return -1;
	vine_manager_enable_process_shortcut(q->tv_manager);
	batch_queue_set_feature(q, "absolute_path", NULL);
	batch_queue_set_feature(q, "remote_rename", "%s=%s");
	batch_queue_set_feature(q, "batch_log_name", "%s.vine.log");
	batch_queue_set_feature(q, "batch_log_transactions", "%s.tr");
	return 0;
}

static int batch_queue_vine_free (struct batch_queue *q)
{
	if (q->tv_file_table) {
		hash_table_delete(q->tv_file_table);
	}
	
	if (q->tv_manager) {
		vine_delete(q->tv_manager);
		q->tv_manager = NULL;
	}

	return 0;
}

static int batch_queue_vine_port (struct batch_queue *q)
{
	return vine_port(q->tv_manager);
}

static void batch_queue_vine_option_update (struct batch_queue *q, const char *what, const char *value)
{
	if(strcmp(what, "password") == 0) {
		if(value)
			vine_set_password(q->tv_manager, value);
	} else if(strcmp(what, "name") == 0) {
		if(value)
			vine_set_name(q->tv_manager, value);
	} else if(strcmp(what, "debug") == 0) {
		if(value)
			vine_enable_debug_log(value);
	} else if(strcmp(what, "priority") == 0) {
		if(value)
			vine_set_priority(q->tv_manager, atoi(value));
		else
			vine_set_priority(q->tv_manager, 0);
	} else if(strcmp(what, "fast-abort") == 0 || strcmp(what,"disconnect-slow-workers")==0 ) {
		if(value)
			vine_enable_disconnect_slow_workers(q->tv_manager, atof(value));
	} else if(strcmp(what, "keepalive-interval") == 0) {
		if(value)
			vine_tune(q->tv_manager, "keepalive-interval",atoi(value));
	} else if(strcmp(what, "keepalive-timeout") == 0) {
		if(value)
			vine_tune(q->tv_manager, "keepalive-timeout",atoi(value));
	} else if(strcmp(what, "manager-preferred-connection") == 0) {
		if(value)
			vine_set_manager_preferred_connection(q->tv_manager, value);
		else
			vine_set_manager_preferred_connection(q->tv_manager, "by_ip");
	} else if(strcmp(what, "category-limits") == 0) {
		struct rmsummary *s = rmsummary_parse_string(value);
		if(s) {
			vine_set_category_resources_max(q->tv_manager, s->category, s);
			rmsummary_delete(s);
		} else {
			debug(D_NOTICE, "Could not parse '%s' as a summary of resorces encoded in JSON\n", value);
		}
        } else if(!strcmp(what,"scheduler")) {
		if(!strcmp(value,"files")) {	
			vine_set_scheduler(q->tv_manager,VINE_SCHEDULE_FILES);
		} else if(!strcmp(value,"time")) {
			vine_set_scheduler(q->tv_manager,VINE_SCHEDULE_TIME);
		} else if(!strcmp(value,"fcfs")) {
			vine_set_scheduler(q->tv_manager,VINE_SCHEDULE_FCFS);
		} else if(!strcmp(value,"random")) {
			vine_set_scheduler(q->tv_manager,VINE_SCHEDULE_RAND);
		} else if(!strcmp(value,"worst")) {
			vine_set_scheduler(q->tv_manager,VINE_SCHEDULE_WORST);
		} else {
			debug(D_NOTICE|D_BATCH,"unknown scheduling mode %s\n",optarg);
		}
	}
}

batch_fs_stub_chdir(vine);
batch_fs_stub_getcwd(vine);
batch_fs_stub_mkdir(vine);
batch_fs_stub_putfile(vine);
batch_fs_stub_rename(vine);
batch_fs_stub_stat(vine);
batch_fs_stub_unlink(vine);

const struct batch_queue_module batch_queue_vine = {
	BATCH_QUEUE_TYPE_VINE,
	"vine",

	batch_queue_vine_create,
	batch_queue_vine_free,
	batch_queue_vine_port,
	batch_queue_vine_option_update,

	{
		batch_job_vine_submit,
		batch_job_vine_wait,
		batch_job_vine_remove,
	},

	{
		batch_fs_vine_chdir,
		batch_fs_vine_getcwd,
		batch_fs_vine_mkdir,
		batch_fs_vine_putfile,
		batch_fs_vine_rename,
		batch_fs_vine_stat,
		batch_fs_vine_unlink,
	},
};

/* vim: set noexpandtab tabstop=8: */
