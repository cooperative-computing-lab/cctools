#include "batch_job_internal.h"
#include "process.h"
#include "batch_job.h"
#include "stringtools.h"
#include "debug.h"
#include "jx_print.h"
#include "jx_parse.h"

#include <stdio.h>
#include <string.h>
#include <unistd.h>

/*
These determine whether the down/uploads will all be forked at once
(in parallel) or one after another (in series). In my tests series
always performs the best.
*/
#define UPLOAD_IN_PARALLEL 0
#define DOWNLOAD_IN_PARALLEL 0

/*
This is the file where the Lambda function stores it's
response. For now there's no good use for it, but it may be
valuable in the future
*/
static const char *output_file_name = "/dev/null";

/*
Decides what to name the folder where files will be uploaded to and
downloaded from for this particular job.
*/
char *bucket_folder_create(int pid)
{
	char *bucket_folder = malloc(sizeof(*bucket_folder) * 256);
	sprintf(bucket_folder, "%d", pid);
	return bucket_folder;
}

/*
Forks an upload process and waits, or forks them all at once,
depending on UPLOAD_IN_PARALLEL
*/
int upload_file(const char *file_name, const char *profile_name, const char *region_name, const char *bucket_name, const char *bucket_folder)
{
	int jobid = fork();
	/* parent */
	if(jobid > 0) {
		int status;
		if(UPLOAD_IN_PARALLEL) {
			return jobid;
		} else {
			waitpid(jobid, &status, 0);
			if(WIFEXITED(status)) {
				return WEXITSTATUS(status);
			}
			return 1;
		}
	}
	/* child */
	else if(jobid == 0) {
		char *shell_cmd = string_format("aws --profile %s --region %s s3 cp %s s3://%s/%s/%s --quiet", profile_name, region_name, file_name, bucket_name, bucket_folder, file_name);
		execlp("sh", "sh", "-c", shell_cmd, (char *) 0);
		free(shell_cmd);
		_exit(1);
	}
	/* error */
	else {
		return -1;
	}
}

/*
Forks a download process and waits, or forks them all at once,
depending on DOWNLOAD_IN_PARALLEL.

Given that we are using a blocking --invocation-type of the Lambda
function (we are, 'RequestResponse', as seen in invoke_function()),
and that AWS S3 guarantees read-after-write consistency (it should,
see http://docs.aws.amazon.com/AmazonS3/latest/dev/Introduction.html#ConsistencyModel),
then we should not have to request a file more than once. Testing
indicates this is the case.
 */
int download_file(const char *file_name, const char *profile_name, const char *region_name, const char *bucket_name, const char *bucket_folder)
{
	int jobid = fork();

	/* parent */
	if(jobid > 0) {
		int status;
		if(DOWNLOAD_IN_PARALLEL) {
			return jobid;
		} else {
			waitpid(jobid, &status, 0);
			if(WIFEXITED(status)) {
				return WEXITSTATUS(status);
			}
			return 1;
		}
	}
	/* child */
	else if(jobid == 0) {
		char *shell_cmd = string_format("aws --profile %s --region %s s3 cp s3://%s/%s/%s %s --quiet", profile_name, region_name, bucket_name, bucket_folder, file_name, file_name);
		execlp("sh", "sh", "-c", shell_cmd, (char *) 0);
		free(shell_cmd);
		_exit(1);
	}
	/* error */
	else {
		return -1;
	}
}

/*
Forks an invocation process for the Lambda function and waits for
it to finish
*/
int invoke_function(const char *profile_name, const char *region_name, const char *function_name, const char *payload)
{
	char *shell_cmd = string_format("aws --profile %s --region %s lambda invoke --invocation-type RequestResponse --function-name %s --log-type None --payload '%s' %s > /dev/null",
					profile_name,
					region_name,
					function_name,
					payload,
					output_file_name);
	int jobid = fork();

	/* parent */
	if(jobid > 0) {
		int status;
		waitpid(jobid, &status, 0);
		free(shell_cmd);
		if(WIFEXITED(status)) {
			return WEXITSTATUS(status);
		}
		return 1;
	}
	/* child */
	else if(jobid == 0) {
		execlp("sh", "sh", "-c", shell_cmd, (char *) 0);
		free(shell_cmd);
		_exit(1);
	}
	/* error */
	else {
		return -1;
	}

	return 0;
}

/*
Creates the json payload to be sent to the Lambda function. It is the
'event' variable in the Lambda function code
*/
char *payload_create(const char *cmdline, const char *region_name, const char *bucket_folder, const char *bucket_name, struct jx *inputq, struct jx *outputq)
{
	struct jx *payload = jx_object(0);
	jx_insert_string(payload, "cmd", cmdline);
	jx_insert_string(payload, "region_name", region_name);
	jx_insert_string(payload, "bucket_name", bucket_name);
	jx_insert_string(payload, "bucket_folder", bucket_folder);
	jx_insert(payload, jx_string("input_names"), inputq);
	jx_insert(payload, jx_string("output_names"), outputq);
	return jx_print_string(payload);
}

/*
Sanitizes the file lists to retrieve just the filenames in the cases
of an '='
*/
struct jx *process_filestring(const char *filestring)
{
	struct jx *fileq = jx_array(0);

	char *files, *f, *p;
	if(filestring) {
		files = strdup(filestring);
		f = strtok(files, " \t,");
		while(f) {
			p = strchr(f, '=');
			jx_array_append(fileq, jx_string(p ? p + 1 : f));
			f = strtok(0, " \t,");
		}
		free(files);
	}

	return fileq;
}

/*
Uploads files to S3
*/
void perform_uploads(struct jx *uploadq, const char *profile_name, const char *region_name, const char *bucket_name, const char *bucket_folder)
{
	char *file_name;

	int status;
	for(int i = 0; i < jx_array_length(uploadq); ++i) {
		file_name = jx_print_string(jx_array_index(uploadq, i));
		status = upload_file(file_name, profile_name, region_name, bucket_name, bucket_folder);
		if(status != 0) {
			/* there was a problem downloading, I'm not sure how to handle it */
		}
		free(file_name);
	}

	if(UPLOAD_IN_PARALLEL) {
		for(int i = 0; i < jx_array_length(uploadq); ++i) {
			wait(0);
		}
	}
}

/*
Downloads files from S3
*/
void perform_downloads(struct jx *downloadq, const char *profile_name, const char *region_name, const char *bucket_name, const char *bucket_folder)
{
	char *file_name;

	int status;
	for(int i = 0; i < jx_array_length(downloadq); ++i) {
		file_name = jx_print_string(jx_array_index(downloadq, i));
		status = download_file(file_name, profile_name, region_name, bucket_name, bucket_folder);
		if(status != 0) {
			/* there was a problem downloading, I'm not sure how to handle it */
		}
		free(file_name);
	}

	if(DOWNLOAD_IN_PARALLEL) {
		for(int i = 0; i < jx_array_length(downloadq); ++i) {
			wait(0);
		}
	}
}

static batch_job_id_t batch_job_lambda_submit(struct batch_queue *q, const char *cmdline, const char *input_files, const char *output_files, struct jx *envlist, const struct rmsummary *resources)
{
	/* Parse the config file produced by batch_job_lambda_setup.sh */
	char *config_file_name = hash_table_lookup(q->options, "lambda-config");
	if(!config_file_name) {
		fatal("No Lambda config file passed. Please pass config file containing using --lambda-config");
	}

	struct jx *j = jx_parse_file(config_file_name);
	if(!j) {
		fatal("Couldn't parse setup file\n");
	}

	if(!jx_lookup_string(j, "bucket_name")) {
		fatal("Couldn't parse the bucket name\n");
	}
	if(!jx_lookup_string(j, "region_name")) {
		fatal("Couldn't parse the region name\n");
	}
	if(!jx_lookup_string(j, "profile_name")) {
		fatal("Couldn't parse the profile name\n");
	}
	if(!jx_lookup_string(j, "function_name")) {
		fatal("Couldn't parse the function name\n");
	}

	char *bucket_name = strdup(jx_lookup_string(j, "bucket_name"));
	char *region_name = strdup(jx_lookup_string(j, "region_name"));
	char *profile_name = strdup(jx_lookup_string(j, "profile_name"));
	char *function_name = strdup(jx_lookup_string(j, "function_name"));
	jx_delete(j);

	struct batch_job_info *info = malloc(sizeof(*info));
	memset(info, 0, sizeof(*info));

	debug(D_BATCH, "Forking Lambda script process...");

	batch_job_id_t jobid = fork();

	/* parent */
	if(jobid > 0) {
		free(bucket_name);
		free(region_name);
		free(profile_name);
		free(function_name);

		info->submitted = time(0);
		info->started = time(0);
		itable_insert(q->job_table, jobid, info);

		return jobid;
	}
	/* child */
	else if(jobid == 0) {
		char *bucket_folder = bucket_folder_create(getpid());
		struct jx *inputq = process_filestring(input_files);
		struct jx *outputq = process_filestring(output_files);
		char *payload = payload_create(cmdline, region_name, bucket_folder, bucket_name, inputq, outputq);

		/* Upload all the inputs for the job to S3 */
		perform_uploads(inputq, profile_name, region_name, bucket_name, bucket_folder);

		/* Invoke the Lambda function, producing the outputs in S3 */
		int status = invoke_function(profile_name, region_name, function_name, payload);
		if(status != 0) {
			/* there was a problem invoking, I'm not sure how to handle it */
		}

		/* Retrieve the outputs from S3 */
		perform_downloads(outputq, profile_name, region_name, bucket_name, bucket_folder);

		free(bucket_name);
		free(region_name);
		free(profile_name);
		free(function_name);
		free(bucket_folder);
		free(payload);
		jx_delete(inputq);
		jx_delete(outputq);

		exit(0);
	}
	/* error */
	else {
		return -1;
	}
}

static batch_job_id_t batch_job_lambda_wait(struct batch_queue *q, struct batch_job_info *info_out, time_t stoptime)
{
	while(1) {
		int timeout = 10;
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

static int batch_job_lambda_remove(struct batch_queue *q, batch_job_id_t jobid)
{
	struct batch_job_info *info = itable_lookup(q->job_table, jobid);
	printf("Job started at: %d\n", (int) info->started);
	info->finished = time(0);
	info->exited_normally = 0;
	info->exit_signal = 0;
	return 0;
}

batch_queue_stub_create(lambda);
batch_queue_stub_free(lambda);
batch_queue_stub_port(lambda);
batch_queue_stub_option_update(lambda);

batch_fs_stub_chdir(lambda);
batch_fs_stub_getcwd(lambda);
batch_fs_stub_mkdir(lambda);
batch_fs_stub_putfile(lambda);
batch_fs_stub_rename(lambda);
batch_fs_stub_stat(lambda);
batch_fs_stub_unlink(lambda);

const struct batch_queue_module batch_queue_lambda = {
	BATCH_QUEUE_TYPE_LAMBDA,
	"lambda",

	batch_queue_lambda_create,
	batch_queue_lambda_free,
	batch_queue_lambda_port,
	batch_queue_lambda_option_update,

	{
	 batch_job_lambda_submit,
	 batch_job_lambda_wait,
	 batch_job_lambda_remove,
	 },

	{
	 batch_fs_lambda_chdir,
	 batch_fs_lambda_getcwd,
	 batch_fs_lambda_mkdir,
	 batch_fs_lambda_putfile,
	 batch_fs_lambda_rename,
	 batch_fs_lambda_stat,
	 batch_fs_lambda_unlink,
	 },
};
