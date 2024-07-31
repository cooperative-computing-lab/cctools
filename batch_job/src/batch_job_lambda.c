/*
Copyright (c) 2018- The University of Notre Dame.
This software is distributed under the GNU General Public License.
See the file COPYING for details. 
*/

/*
Theory of operation:

batch_job_lambda assumes that the a generic lambda function
and an S3 bucket have been created by makeflow_lambda_setup,
and the necessary info recorded to a config file, which is
loaded here.

Each run of a makeflow does in a distinct directory (makeflow_%d)
within the bucket, which is not (yet) deleted at the end of
a run.  The entire bucket is deleted by makeflow_lambda_cleanup.

For each batch job, this module uploads the input files
to the bucket, then invokes the generic lambda function,
passing a "payload" JSON object which describes the job.
The generic lambda then pulls the input files from the bucket,
runs the job as a sub-process, and then pushes the output files
back to the bucket.  This module then pulls the output files down
from the bucket, and the job is done.

A wrinkle is that S3 doesn't treat directories as first class citizens.
To avoid complications of simulating directories, we upload directory
dependencies as tarballs.  For output paths -- which we don't know
in advance if they are directories -- we attempt to download as a file
first, and then look for file.tgz to represent a directory with the same name.

Files are stored in S3 underneath their "outer" name, and are then
downloaded to the function where they are stored with the "inner" name.
*/

#include "batch_job_internal.h"
#include "process.h"
#include "batch_job.h"
#include "stringtools.h"
#include "debug.h"
#include "path.h"
#include "jx_print.h"
#include "jx_parse.h"
#include "nvpair_jx.h"
#include "semaphore.h"

#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>

/*
Load the existing lambda configuration info from the given file name.
*/

struct lambda_config {
	const char *bucket_name;
	const char *bucket_folder;
	const char *function_name;
};

static struct lambda_config * lambda_config_load( const char *filename )
{
	struct jx * j = jx_parse_nvpair_file(filename);
	if(!j) fatal("%s isn't a valid config file\n",filename);

	struct lambda_config *c = malloc(sizeof(*c));

	c->bucket_name   = jx_lookup_string(j,"bucket_name");
	c->bucket_folder = string_format("makeflow_%d",getpid());
	c->function_name = jx_lookup_string(j,"function_name");

	if(!c->bucket_name)    fatal("%s doesn't define bucket_name",filename);
	if(!c->function_name)  fatal("%s doesn't define function_name",filename);

	return c;
}

static int upload_dir( struct lambda_config *config, const char *file_name)
{
	char *cmd = string_format("tar czf - %s | aws s3 cp - s3://%s/%s/%s --quiet", file_name, config->bucket_name, config->bucket_folder, path_basename(file_name));
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

static int isdir( const char *path )
{
	struct stat info;
	int result = stat(path,&info);
	if(result!=0) return 0;
	return S_ISDIR(info.st_mode);
}

static int upload_item( struct lambda_config *config, const char *file_name )
{
	static struct hash_table *uploaded_files = 0;

	if(!uploaded_files) uploaded_files = hash_table_create(0,0);

	if(hash_table_lookup(uploaded_files,file_name)) {
		debug(D_BATCH,"input file %s is already cached in S3",file_name);
		return 0;
	}

	debug(D_BATCH,"uploading %s to S3...",file_name);

	int result;

	if(isdir(file_name)) {
		result = upload_dir(config,file_name);
	} else {
		result = upload_file(config,file_name);
	}

	if(result==0) {
		hash_table_insert(uploaded_files,file_name,strdup(file_name));
	}

	return result;
}

static int download_file( struct lambda_config *config, const char *file_name )
{
	char *cmd = string_format("aws s3 cp s3://%s/%s/%s %s --quiet", config->bucket_name, config->bucket_folder, file_name, file_name);
	debug(D_BATCH,"%s",cmd);
	int r = system(cmd);
	free(cmd);
	return r;
}

static int download_item( struct lambda_config *config, const char *file_name )
{
	int result = 1;

	debug(D_BATCH,"downloading %s from S3",file_name);

	if(download_file(config,file_name)==0) {
		result = 0;
	} else {
		char *tar_name = string_format("%s.tgz",file_name);
		if(download_file(config,tar_name)==0) {
			char *cmd = string_format("tar xzf %s",tar_name);
			debug(D_BATCH,"%s",cmd);
			if(system(cmd)==0) {
				result = 0;
			} else {
				debug(D_BATCH,"unable to un-tar %s!",tar_name);
				result = 1;
			}
			unlink(tar_name);
		} else {
			debug(D_BATCH,"unable to download %s as file or directory",tar_name);
		}
		free(tar_name);
	}
	return result;
}

/*
Forks an invocation process for the Lambda function and waits for
it to finish.  Returns zero on success.
*/
static int invoke_function( struct lambda_config *config, const char *payload)
{
	char *cmd = string_format("aws lambda invoke --invocation-type RequestResponse --function-name %s --log-type None --payload '%s' .lambda.output >/dev/null", config->function_name, payload);
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
{ "inner_name":"a", "outer_name":"a", "type":"file" },
{ "inner_name":"b", "outer_name":"c", "type":"tgz" }
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
			if(isdir(inner_name)) {
				jx_insert(file_object,jx_string("type"),jx_string("tgz"));
			} else {
				jx_insert(file_object,jx_string("type"),jx_string("file"));
			}
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

static int transfer_semaphore = -1;

/*
Within a child process, invoke the lambda function itself,
and then download the output files.  It would be better to
download the outputs as part of wait(), but that information
is not (currently) available within batch_job_lambda_wait().
*/

static int batch_job_lambda_subprocess( struct lambda_config *config, const char *cmdline, const char *input_file_string, const char *output_file_string )
{
	struct jx *input_files = filestring_to_jx(input_file_string);
	struct jx *output_files = filestring_to_jx(output_file_string);

	char *payload = payload_create(config,cmdline,input_files,output_files);
	int status;

	/* Invoke the Lambda function, producing the outputs in S3 */
	status = invoke_function(config,payload);

	/* Retrieve the outputs from S3 */
	semaphore_down(transfer_semaphore);
	status = download_files(config,output_files);
	semaphore_up(transfer_semaphore);

	return status;
}

/*
Submit a lambda job by uploading the input files, forking
a child process, and invoking the subprocess function.
*/

static batch_job_id_t batch_job_lambda_submit(struct batch_queue *q, const char *cmdline, const char *input_file_string, const char *output_file_string, struct jx *envlist, const struct rmsummary *resources)
{
	if(transfer_semaphore==-1) {
		transfer_semaphore = semaphore_create(1);
	}

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

/*
Wait for a completed lambda task by waiting for the
containing subprocess.  Note that the process module is needed
here to avoid accidentally reaping unrelated processes.
*/

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
To remove a job, we take it out of the job table,
kill the proxy process, and then wait for the proxy
process to be returned by waitpid().  If it's not
in the job table, then it's not a valid job.
*/

static int batch_job_lambda_remove(struct batch_queue *q, batch_job_id_t jobid)
{
	struct batch_job_info *info;
	info = itable_remove(q->job_table, jobid);
	if(info) {
		free(info);

		debug(D_BATCH,"killing jobid %d...",(int)jobid);
		kill(jobid,SIGKILL);

		debug(D_BATCH,"waiting for job %d to die...",(int)jobid);
		struct process_info *p = process_waitpid(jobid,60);
		if(p) free(p);

		return 1;
	} else {
		debug(D_BATCH,"no such jobid %d",(int)jobid);
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
	 }
};
