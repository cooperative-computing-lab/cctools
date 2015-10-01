#include "batch_job_internal.h"
#include "process.h"
#include "batch_job.h"
#include "debug.h"

#include <stdio.h>
#include <string.h>
#include <unistd.h>

static batch_job_id_t batch_job_amazon_submit (struct batch_queue *q, const char *cmd, const char *extra_input_files, const char *extra_output_files, struct nvpair *envlist )
{
    int jobid;
    struct batch_job_info *info = malloc(sizeof(*info));
    memset(info, 0, sizeof(*info));
    debug(D_BATCH, "Forking EC2 script process...");
    // Fork process and spin off shell script
    jobid = fork();
    if (jobid > 0) // parent
    {
        info->submitted = time(0);
        info->started = time(0);
        itable_insert(q->job_table, jobid, info);
        return jobid;
    }
    else { // child
	    execlp("sh", "sh", "-c", cmd, (char *) 0);
    }
}

static batch_job_id_t batch_job_amazon_wait (struct batch_queue * q, struct batch_job_info * info_out, time_t stoptime)
{
    while (1) {
        int timeout = 5;
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
		}
    }
}

static int batch_job_amazon_remove (struct batch_queue *q, batch_job_id_t jobid)
{
    printf("test remove\n");
    struct batch_job_info *info =  itable_lookup(q->job_table, jobid);
    printf("Job started at: %d\n", (int)info->started);
	info->finished = time(0);
	info->exited_normally = 0;
	info->exit_signal = 0;
    return 0;
}


batch_queue_stub_create(amazon);
batch_queue_stub_free(amazon);
batch_queue_stub_port(amazon);
batch_queue_stub_option_update(amazon);

batch_fs_stub_chdir(amazon);
batch_fs_stub_getcwd(amazon);
batch_fs_stub_mkdir(amazon);
batch_fs_stub_putfile(amazon);
batch_fs_stub_stat(amazon);
batch_fs_stub_unlink(amazon);

const struct batch_queue_module batch_queue_amazon = {
	BATCH_QUEUE_TYPE_AMAZON,
	"amazon",

	batch_queue_amazon_create,
	batch_queue_amazon_free,
	batch_queue_amazon_port,
	batch_queue_amazon_option_update,

	{
		batch_job_amazon_submit,
		batch_job_amazon_wait,
		batch_job_amazon_remove,
	},

	{
		batch_fs_amazon_chdir,
		batch_fs_amazon_getcwd,
		batch_fs_amazon_mkdir,
		batch_fs_amazon_putfile,
		batch_fs_amazon_stat,
		batch_fs_amazon_unlink,
	},
};
