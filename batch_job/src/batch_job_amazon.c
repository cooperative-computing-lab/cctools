#include "batch_job_internal.h"
#include "process.h"
#include "batch_job.h"
#include "stringtools.h"
#include "debug.h"
#include "jx_parse.h"

#include <stdio.h>
#include <string.h>
#include <unistd.h>

/*
The build process takes the helper script batch_job_amazon_script.sh
and converts it into an embeddable string that is included into this file.
*/

char *amazon_ec2_script =
#include "batch_job_amazon_script.c"

char *amazon_script_filename = ".makeflow_amazon_ec2_script.sh";

static batch_job_id_t batch_job_amazon_submit(struct batch_queue *q, const char *cmd, const char *extra_input_files, const char *extra_output_files, struct jx *envlist, const struct rmsummary *resources)
{
	// XXX still need to pass in environment somehow

	if(access(amazon_script_filename, F_OK | X_OK) == -1) {
		debug(D_BATCH, "Generating Amazon ec2 script...");
		FILE *f = fopen(amazon_script_filename, "w");
		fprintf(f, "%s", amazon_ec2_script);
		fclose(f);
		chmod(amazon_script_filename, 0755);
	}

	char *shell_cmd = string_format("./%s \"%s\" \"%s\" \"%s\"",
					amazon_script_filename,
					cmd,
					extra_input_files,
					extra_output_files);

	return batch_job_local_submit(q,shell_cmd,extra_input_files,extra_output_files,envlist,resources);
}

static batch_job_id_t batch_job_amazon_wait(struct batch_queue *q, struct batch_job_info *info_out, time_t stoptime)
{
	return batch_job_local_wait(q,info_out,stoptime);
}

static int batch_job_amazon_remove(struct batch_queue *q, batch_job_id_t jobid)
{
	return batch_job_local_remove(q,jobif);
}


batch_queue_stub_create(amazon);
batch_queue_stub_free(amazon);
batch_queue_stub_port(amazon);
batch_queue_stub_option_update(amazon);

batch_fs_stub_chdir(amazon);
batch_fs_stub_getcwd(amazon);
batch_fs_stub_mkdir(amazon);
batch_fs_stub_putfile(amazon);
batch_fs_stub_rename(amazon);
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
	 batch_fs_amazon_rename,
	 batch_fs_amazon_stat,
	 batch_fs_amazon_unlink,
	 },
};
