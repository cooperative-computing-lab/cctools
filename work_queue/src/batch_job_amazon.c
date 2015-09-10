#include "batch_job_internal.h"
#include "batch_job.h"

#include <stdio.h>

static batch_job_id_t batch_job_amazon_submit (struct batch_queue *q, const char *cmd, const char *extra_input_files, const char *extra_output_files, struct nvpair *envlist )
{
    printf("test submit\n");
}

static batch_job_id_t batch_job_amazon_wait (struct batch_queue * q, struct batch_job_info * info_out, time_t stoptime)
{
    printf("test submit\n");
}

static int batch_job_amazon_remove (struct batch_queue *q, batch_job_id_t jobid)
{
    printf("test submit\n");
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
