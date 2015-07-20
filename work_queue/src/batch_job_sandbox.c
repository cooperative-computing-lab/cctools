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
#include <ctype.h>

#define CHAR_BUFF_LEN 4096

static struct nvpair *output_files_list;
static struct nvpair *task_sandbox_name_list;

static batch_job_id_t batch_job_sandbox_submit (struct batch_queue *q, const char *cmd, const char *extra_input_files, const char *extra_output_files, struct nvpair *envlist )
{
	batch_job_id_t jobid;

    // create task sandbox
	const char *local_task_dir = nvpair_lookup_string(envlist, "local_task_dir");
	char *template = string_format("%s/task.sandbox.XXXXXX", local_task_dir);
    char *task_sandbox_name = mkdtemp(template);

    if (output_files_list == NULL)
    	output_files_list = nvpair_create();
    if (task_sandbox_name_list == NULL) 
        task_sandbox_name_list = nvpair_create();

	fflush(NULL);
	jobid = fork();

	if(jobid > 0) {

		debug(D_BATCH, "started process %" PRIbjid ": %s", jobid, cmd);
		struct batch_job_info *info = malloc(sizeof(*info));
		memset(info, 0, sizeof(*info));
		info->submitted = time(0);
		info->started = time(0);
		itable_insert(q->job_table, jobid, info);

        // store the extra output files and task sandbox name
        char *jobid_str = string_format("%d", (int)jobid);
        nvpair_insert_string(output_files_list, jobid_str, extra_output_files);
        nvpair_insert_string(task_sandbox_name_list, jobid_str, task_sandbox_name);

        while(isspace(*extra_input_files)) extra_input_files++;
        char *p;
        char *input_files = strdup(extra_input_files); 
        p = strtok(input_files, ","); 
        while (p != NULL) {

            char *orig_file; 
            char *symlink_path;
            
            // move input files into task sandbox
            if (p[0] != '/') {
                char cwd[CHAR_BUFF_LEN];
            	getcwd(cwd, sizeof(cwd));
                orig_file = string_format("%s/%s", cwd, p);
                symlink_path = string_format("%s/%s", task_sandbox_name, p);
                symlink(orig_file, symlink_path);
            }
            
            p = strtok(NULL, ",");

        }

		return jobid;

	} else if(jobid < 0) {
		debug(D_BATCH, "couldn't create new process: %s\n", strerror(errno));
		return -1;
	} else {

		if(envlist) {
			nvpair_export(envlist);
		}
       
        // change working directory to the task sandbox directory 
        chdir(task_sandbox_name);
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
          
            char *jobid_str = string_format("%d", jobid);
            const char *extra_output_files = nvpair_lookup_string(output_files_list, jobid_str);
            const char *task_sandbox_name = nvpair_lookup_string(task_sandbox_name_list, jobid_str);
          
            while(isspace(*extra_output_files)) extra_output_files++;
            char *p;
            char *output_files = strdup(extra_output_files);
            p = strtok(output_files, ",");
            while ( p != NULL ) {
                // move the output file to makeflow working directory
                char *output_file_path = string_format("%s/%s", task_sandbox_name, p);
                if(rename(output_file_path, p) == -1)
					fatal("Fail to move the output file with the error message %s.\n", strerror(errno));
                p = strtok(NULL, ",");	
            } 
          
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
			return 1;
		}
	} else {
		debug(D_BATCH, "could not signal process %" PRIbjid ": %s\n", jobid, strerror(errno));
		return 0;
	}

}

batch_queue_stub_create(sandbox);
batch_queue_stub_free(sandbox);
batch_queue_stub_port(sandbox);
batch_queue_stub_option_update(sandbox);

batch_fs_stub_chdir(sandbox);
batch_fs_stub_getcwd(sandbox);
batch_fs_stub_mkdir(sandbox);
batch_fs_stub_putfile(sandbox);
batch_fs_stub_stat(sandbox);
batch_fs_stub_unlink(sandbox);

const struct batch_queue_module batch_queue_sandbox = {
	BATCH_QUEUE_TYPE_SANDBOX,
	"sandbox",

	batch_queue_sandbox_create,
	batch_queue_sandbox_free,
	batch_queue_sandbox_port,
	batch_queue_sandbox_option_update,

	{
		batch_job_sandbox_submit,
		batch_job_sandbox_wait,
		batch_job_sandbox_remove,
	},

	{
		batch_fs_sandbox_chdir,
		batch_fs_sandbox_getcwd,
		batch_fs_sandbox_mkdir,
		batch_fs_sandbox_putfile,
		batch_fs_sandbox_stat,
		batch_fs_sandbox_unlink,
	},
};

/* vim: set noexpandtab tabstop=4: */
