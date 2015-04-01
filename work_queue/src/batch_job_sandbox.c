#include "batch_job.h"
#include "batch_job_internal.h"
#include "debug.h"
#include "process.h"
#include "macros.h"
#include "stringtools.h"
#include "unlink_recursive.h"

#include <time.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <signal.h>
#include <sys/stat.h>
#include <dirent.h>

//TODO how to name the sandbox directory
static int count = 0;

int mf_sandbox_link_recursive( const char *source, const char *target ) {
    struct stat info;

    if(stat(source,&info)<0) return 0;

    if(S_ISDIR(info.st_mode)) {
        DIR *dir = opendir(source);
        if(!dir) return 0;

        mkdir(target, 0777);

        struct dirent *d;
        int result = 1;

        while((d = readdir(dir))) {
            if(!strcmp(d->d_name,".")) continue;
            if(!strcmp(d->d_name,"..")) continue;

            char *subsource = string_format("%s/%s",source,d->d_name);
            char *subtarget = string_format("%s/%s",target,d->d_name);

            result = mf_sandbox_link_recursive(subsource,subtarget);

            free(subsource);
            free(subtarget);

            if(!result) break;
        }
        closedir(dir);

        return result;
    } else {
        if(link(source, target)==0) return 1;
        return 0;
    }
}

static batch_job_id_t batch_job_sandbox_submit (struct batch_queue *q, const char *cmd, const char *extra_input_files, const char *extra_output_files, struct nvpair *envlist )
{
	batch_job_id_t jobid;

    char *public_sandbox;
    public_sandbox = nvpair_lookup_string(envlist, "local_task_dir");

    char dir_name_template[2048];
    sprintf(dir_name_template, "%s/t-XXXXXX", public_sandbox);

    char *sandbox_name; 
    sandbox_name = mkdtemp(dir_name_template);
    
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
        
        int sub_proc_id = fork();

        if (sub_proc_id > 0) {

            int return_status;
            waitpid(sub_proc_id, &return_status, 0);  // Parent process waits here for child to terminate.

            if (return_status == 0)  { // Verify child process terminated without error.  

                char cwd[4096];
                getcwd(cwd, sizeof(cwd));

            	debug(D_BATCH, "Current working dir: %s\n", cwd);

                char *oup_f, *oup_p, *oup_files;

                char src_fn[4096];
                char dst_fn[4096];

                if(extra_output_files) {
                    oup_files = strdup(extra_output_files);
                    oup_f = strtok(oup_files, " \t,");
                    while(oup_f) {
                        sprintf(src_fn, "%s/%s", sandbox_name, oup_f);
                        sprintf(dst_fn, "%s/%s", cwd, oup_f);
            	        debug(D_BATCH, "SOURCE FILE IS: %s\n", src_fn);
            	        debug(D_BATCH, "DESTINATION FILE IS: %s\n", dst_fn);

                        oup_p = strchr(oup_f, '=');
                        if(oup_p) {
                            *oup_p = 0;
                            mf_sandbox_link_recursive(src_fn, dst_fn);
                            *oup_p = '=';
                        } else {
                            mf_sandbox_link_recursive(src_fn, dst_fn);
                        }
                        oup_f = strtok(0, " \t,");
                    }
                    free(oup_files);
                }

            	debug(D_BATCH, "REMOVING SANDBOX: %s\n", sandbox_name);
                //unlink_recursive(sandbox_name);
                exit(0);

            } else {     
		        debug(D_BATCH, "Sub-process terminated with error: %s\n", strerror(errno));
                _exit(127);
            }

        } else if (sub_proc_id < 0) {
		    debug(D_BATCH, "couldn't create new sub process: %s\n", strerror(errno));
			return -1;
		} else {

		    if(envlist) {
		    	nvpair_export(envlist);
		    }

            mkdir(sandbox_name, 0777);
            char *f, *p, *files;

            char link_fn_path[4096];
            if(extra_input_files) {
                files = strdup(extra_input_files);
                f = strtok(files, " \t,");

                while(f) {
                    p = strchr(f, '=');
                    if (*f == '/')
                    	sprintf(link_fn_path, "%s%s", sandbox_name, f);
                    else
                		sprintf(link_fn_path, "%s/%s", sandbox_name, f);

                    if(p) {
                        *p = 0;
                        mf_sandbox_link_recursive(f, link_fn_path);
                        *p = '=';
                    } else {
                        mf_sandbox_link_recursive(f, link_fn_path);
                    }
                    f = strtok(0, " \t,");
                }
                free(files);
            }   


            chdir(sandbox_name);
            char test_cwd[1024];
            getcwd(test_cwd, sizeof(test_cwd));

		    execlp("sh", "sh", "-c", cmd, (char *) 0);
		    _exit(127);	// Failed to execute the cmd. 
        }
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
