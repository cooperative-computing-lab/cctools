/*
Copyright (c) 2018- The University of Notre Dame.
This software is distributed under the GNU General Public License.
See the file COPYING for details. 
*/

/*
Theory of operation:

batch_job_lambda assumes that the caller has already set up
an s3 bucket and a generic lambda function.  (This is done by
makeflow_lambda_setup.)  To execute a batch job, this module
uploads the input files to the bucket, then invokes the function,
passing a "payload" JSON object which describes the job.
The generic lambda then pulls the input files from the bucket,
runs the job as a sub-process, and then pushes the output files
back to the bucket.  This module then pulls the output files down
from the bucket, and the job is done.
*/

/*
Things that need to be fixed in this code:
1 - Every input file is transferred for every job.  Instead, we should
only transfer files if they do not exist. 

2 - The function execution side is unable to download directories.

3 - File renaming is not supported. Input/output files must have
the same name inside and outside of the sandbox.
*/

#include "batch_job_internal.h"
#include "process.h"
#include "batch_job.h"
#include "stringtools.h"
#include "debug.h"
#include "path.h"
#include "jx_print.h"
#include "jx_parse.h"

#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>

struct lambda_config {
	const char *bucket_name;
	const char *bucket_folder;
	const char *region_name;
	const char *profile_name;
	const char *function_name;
};

static struct lambda_config * lambda_config_load( const char *filename )
{
	struct jx * j = jx_parse_file(filename);
	if(!j) fatal("%s isn't a valid json file\n",filename);

	struct lambda_config *c = malloc(sizeof(*c));

	c->bucket_name   = jx_lookup_string(j,"bucket_name");
	c->bucket_folder = string_format("makeflow_%d",getpid());
	c->region_name   = jx_lookup_string(j,"region_name");
	c->profile_name  = jx_lookup_string(j,"profile_name");
	c->function_name = jx_lookup_string(j,"function_name");

	if(!c->bucket_name)    fatal("%s doesn't define bucket_name",filename);
	if(!c->region_name)    fatal("%s doesn't define region_name",filename);
	if(!c->profile_name)   fatal("%s doesn't define profile_name",filename);
	if(!c->function_name)  fatal("%s doesn't define function_name",filename);

	return c;
}

/*
Upload a file/dir to the appropriate bucket.  This is a bit complicated b/c "s3 cp" only works on files, and "s3 sync" only works on directories, not files.
*/

static int upload_dir( struct lambda_config *config, const char *file_name)
{
	char *cmd = string_format("aws s3 sync %s s3://%s/%s/%s --quiet", file_name, config->bucket_name, config->bucket_folder, path_basename(file_name));
	debug(D_BATCH,"%s",cmd);
	int r = system(cmd);
	free(cmd);
	return r;
}

static int upload_file( struct lambda_config *config, const char *file_name )
{
	char *cmd = string_format("aws s3 cp %s s3://%s/%s/%s --quiet", file_name, config->bucket_name, config->bucket_folder, file_name);
	debug(D_BATCH,"%s",cmd);
	int r = system(cmd);
	free(cmd);
	return r;
}

static int upload_item( struct lambda_config *config, const char *file_name )
{
	struct stat info;

	int result = stat(file_name,&info);
	if(result!=0) {
		debug(D_BATCH,"couldn't access input file %s: %s",file_name,strerror(errno));
		return -1;
	}

	if(S_ISDIR(info.st_mode)) {
		return upload_dir(config,file_name);
	} else {
		return upload_file(config,file_name);
	}
}
/*
Download a file from the appropriate bucket. Amazingly, S3 does not provide a way to sense whether an object is a file or a directory, so we try "s3 cp" first and then "s3 sync" if it fails.

Given that we are using a blocking --invocation-type of the Lambda
function (we are, 'RequestResponse', as seen in invoke_function()),
and that AWS S3 guarantees read-after-write consistency (it should,
see http://docs.aws.amazon.com/AmazonS3/latest/dev/Introduction.html#ConsistencyModel),
then we should not have to request a file more than once. Testing
indicates this is the case.
 */

static int download_file( struct lambda_config *config, const char *file_name )
{
	char *cmd = string_format("aws s3 cp s3://%s/%s/%s %s --quiet", config->bucket_name, config->bucket_folder, file_name, file_name);
	debug(D_BATCH,"%s",cmd);
	int r = system(cmd);
	free(cmd);
	return r;
}

static int download_dir( struct lambda_config *config, const char *file_name )
{
	char *cmd = string_format("aws s3 sync s3://%s/%s/%s %s --quiet", config->bucket_name, config->bucket_folder, file_name, file_name);
	debug(D_BATCH,"%s",cmd);
	int r = system(cmd);
	free(cmd);
	return r;
}

static int download_item( struct lambda_config *config, const char *file_name )
{
	int r = download_file(config,file_name);
	if(r!=0) r = download_dir(config,file_name);
	return r;
}

/*
Forks an invocation process for the Lambda function and waits for
it to finish.  Returns zero on success.
*/
static int invoke_function( struct lambda_config *config, const char *payload)
{
	char *cmd = string_format("aws lambda invoke --invocation-type RequestResponse --function-name %s --log-type None --payload '%s' /dev/null >/dev/null", config->function_name, payload);
	debug(D_BATCH,"%s",cmd);
	int r = system(cmd);
	free(cmd);
	return r;
}

/*
Creates the json payload to be sent to the Lambda function. It is the
'event' variable in the Lambda function code
*/
char *payload_create(struct lambda_config *config, const char *cmdline, struct jx *input_files, struct jx *output_files)
{
	struct jx *payload = jx_object(0);
	jx_insert_string(payload, "cmd", cmdline);
	jx_insert_string(payload, "region_name", config->region_name);
	jx_insert_string(payload, "bucket_name", config->bucket_name);
	jx_insert_string(payload, "bucket_folder", config->bucket_folder);
	jx_insert(payload, jx_string("input_files"), jx_copy(input_files));
	jx_insert(payload, jx_string("output_files"), jx_copy(output_files));
	char *str = jx_print_string(payload);
       	jx_delete(payload);
	return str;

}

/*
Converts a list of files in the form of a string "a,b=c" into a JX array of the form:
[
{ "inner_name":"a", "outer_name":"a" },
{ "inner_name":"b", "outer_name":"c" }
]
*/

struct jx *filestring_to_jx(const char *filestring)
{
	struct jx *file_array = jx_array(0);

	char *files, *outer_name, *inner_name;
	if(filestring) {
		files = strdup(filestring);
		outer_name = strtok(files, " \t,");
		while(outer_name) {
			inner_name = strchr(outer_name,'=');
			if(inner_name) {
				*inner_name = 0;
				inner_name++;
			} else {
				inner_name = outer_name;
			}

			struct jx *file_object = jx_object(0);
			jx_insert(file_object,jx_string("outer_name"),jx_string(outer_name));
			jx_insert(file_object,jx_string("inner_name"),jx_string(inner_name));
			jx_array_append(file_array,file_object);

			outer_name = strtok(0, " \t,");
		}
		free(files);
	}

	return file_array;
}

/*
Uploads files to S3.  Return zero on success, non-zero otherwise.
*/

int upload_files(struct lambda_config *config, struct jx *file_list )
{
	int i;
	for( i=0; i<jx_array_length(file_list); i++ ) {
		struct jx *file_object = jx_array_index(file_list, i);
		const char *file_name = jx_lookup_string(file_object,"outer_name");
		int status = upload_item(config,file_name);
		if(status!=0) {
			debug(D_BATCH,"upload of %s failed, aborting job submission",file_name);
			return 1;
		}
	}
	return 0;
}

/*
Download files from S3.  If any file fails to download, keep going,
so that the caller will be able to debug the result.  Makeflow will
detect that not all files were returned.
*/

int download_files(struct lambda_config *config, struct jx *file_list )
{
	int nfailures = 0;

	int i;
	for( i=0; i<jx_array_length(file_list); i++ ) {
		struct jx *file_object = jx_array_index(file_list, i);
		const char *file_name = jx_lookup_string(file_object,"outer_name");

		int status = download_item(config,file_name);
		if(status!=0) {
			debug(D_BATCH,"download of %s failed, still continuing",file_name);
			nfailures++;
		}
	}

	return nfailures;
}

static int batch_job_lambda_subprocess( struct lambda_config *config, const char *cmdline, const char *input_file_string, const char *output_file_string )
{
	struct jx *input_files = filestring_to_jx(input_file_string);
	struct jx *output_files = filestring_to_jx(output_file_string);

	char *payload = payload_create(config,cmdline,input_files,output_files);
	int status;

	/* Invoke the Lambda function, producing the outputs in S3 */
	status = invoke_function(config,payload);

	/* Retrieve the outputs from S3 */
	status = download_files(config,output_files);

	return status;
}

static batch_job_id_t batch_job_lambda_submit(struct batch_queue *q, const char *cmdline, const char *input_file_string, const char *output_file_string, struct jx *envlist, const struct rmsummary *resources)
{
	const char *config_file = hash_table_lookup(q->options, "lambda-config");
	if(!config_file) fatal("--lambda-config option is required");

	static struct lambda_config *config = 0;
	if(!config) config = lambda_config_load(config_file);

	struct jx *input_files = filestring_to_jx(input_file_string);
	int status = upload_files(config,input_files);
	jx_delete(input_files);

	if(status!=0) {
		debug(D_BATCH, "failed to upload all input files");
		return -1;
	}

	batch_job_id_t jobid = fork();

	if(jobid > 0) {
		/* parent process */
		debug(D_BATCH, "lambda: forked child process %d",(int)jobid);
		struct batch_job_info *info = malloc(sizeof(*info));
		memset(info, 0, sizeof(*info));
		info->submitted = time(0);
		info->started = time(0);
		itable_insert(q->job_table, jobid, info);
		return jobid;
	} else if(jobid == 0) {
		/* child process */
		int result = batch_job_lambda_subprocess(config,cmdline,input_file_string,output_file_string);
		_exit(result);
	} else {
		debug(D_BATCH,"failed to fork: %s\n",strerror(errno));
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

/*
To remove a job, we must kill its proxy process,
which will then be returned by batch_job_wait when complete.
*/

static int batch_job_lambda_remove(struct batch_queue *q, batch_job_id_t jobid)
{
	if(itable_lookup(q->job_table, jobid)) {
		kill(jobid,SIGKILL);
		return 1;
	} else {
		return 0;
	}
}

static int batch_queue_lambda_create( struct batch_queue *q )
{
	batch_queue_set_feature(q, "remote_rename", "%s=%s");
	return 0;
}

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
