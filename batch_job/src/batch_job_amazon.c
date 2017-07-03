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

char *amazon_script_filename = "_temp_amazon_ec2_script.sh";

static batch_job_id_t batch_job_amazon_submit(struct batch_queue *q, const char *cmd, const char *extra_input_files, const char *extra_output_files, struct jx *envlist, const struct rmsummary *resources)
{
	int jobid;
	struct batch_job_info *info = malloc(sizeof(*info));
	memset(info, 0, sizeof(*info));
	char *ec2_home = getenv("EC2_HOME");
	if(ec2_home == NULL) {
		fatal("EC2_HOME environment variable must be set to EC2 tools directory");
	}

	char *amazon_credentials_filepath = hash_table_lookup(q->options,
							      "amazon-credentials");
	if(amazon_credentials_filepath == NULL) {
		fatal("No amazon credentials passed. Please pass file containing amazon credentials using --amazon-credentials flag");
	}
	char *amazon_ami = hash_table_lookup(q->options,
					     "amazon-ami");
	if(amazon_ami == NULL) {
		fatal("No ami image id passed. Please pass file containing ami image id using --amazon-ami flag");
	}

	struct jx *config = jx_parse_file(amazon_credentials_filepath);
	if(!config) {
		fatal("Amazon credentials file could not be opened");
	}

	const char *aws_access_key_id = jx_lookup_string(config, "aws_access_key_id");
	const char *aws_secret_access_key = jx_lookup_string(config, "aws_access_key_id");

	if(!aws_access_key_id)
		fatal("credentials file %s does not contain aws_access_key_id");
	if(!aws_secret_access_key)
		fatal("credentials file %s does not contain aws_secret_access_key");

	// Write amazon ec2 script to file if does not already exist
	if(access(amazon_script_filename, F_OK | X_OK) == -1) {
		debug(D_BATCH, "Generating Amazon ec2 script...");
		FILE *f = fopen(amazon_script_filename, "w");
		fprintf(f, "%s", amazon_ec2_script);
		fclose(f);
		// Execute permissions
		chmod(amazon_script_filename, 0755);
	}

	// Run ec2 script
	char *shell_cmd = string_format("./%s %s %s '%s' %s %s %s",
					amazon_script_filename,
					aws_access_key_id,
					aws_secret_access_key,
					cmd,
					amazon_ami,
					extra_input_files,
					extra_output_files);

	debug(D_BATCH, "Forking EC2 script process...");
	// Fork process and spin off shell script
	jobid = fork();
	if(jobid > 0)		// parent
	{
		info->submitted = time(0);
		info->started = time(0);
		itable_insert(q->job_table, jobid, info);
		free(shell_cmd);
		return jobid;
	} else if(jobid == 0) {	// child
		execlp("sh", "sh", "-c", shell_cmd, (char *) 0);
		_exit(1);
	} else {
		return -1;
	}
}

static batch_job_id_t batch_job_amazon_wait(struct batch_queue *q, struct batch_job_info *info_out, time_t stoptime)
{
	while(1) {
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

static int batch_job_amazon_remove(struct batch_queue *q, batch_job_id_t jobid)
{
	struct batch_job_info *info = itable_lookup(q->job_table, jobid);
	printf("Job started at: %d\n", (int) info->started);
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
