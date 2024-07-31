/*
 * Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "batch_job_internal.h"
#include "process.h"
#include "batch_job.h"
#include "stringtools.h"
#include "debug.h"
#include "jx_parse.h"
#include "jx.h"
#include "jx_pretty_print.h"
#include "itable.h"
#include "hash_table.h"
#include "fast_popen.h"
#include "sh_popen.h"
#include "list.h"


#include <time.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <sys/wait.h>
#include <sys/types.h>
#include <errno.h>

static int initialized = 0;
static char* queue_name = NULL;
static char* compute_env_name = NULL;
static char* vpc = NULL;
static char* sec_group = NULL;
static char* subnet = NULL;
static struct itable* done_jobs;
static struct itable* amazon_job_ids;
static struct itable* done_files;
static int instID;
static char* bucket_name = NULL;
static struct hash_table* submitted_files = NULL;
static int HAS_SUBMITTED_VALUE = 1;


union amazon_batch_ccl_guid{
	char c[8];
	unsigned int ul;
};

static struct internal_amazon_batch_amazon_ids{
	char* aws_access_key_id;
	char* aws_secret_access_key;
	char* aws_region;
	char* aws_email;
	char* env_prefix;
}initialized_data;

static unsigned int gen_guid(){
	FILE* ran = fopen("/dev/urandom","r");
	if(!ran)
		fatal("Cannot open /dev/urandom");
	union amazon_batch_ccl_guid guid;
	size_t k = fread(guid.c,sizeof(char),8,ran);
	if(k<8)
		fatal("couldn't read 8 bytes from /dev/urandom/");
	fclose(ran);
	return guid.ul;	
}

static struct jx* run_command(char* cmd){
	FILE* out = sh_popen(cmd);
	if(out == NULL){
		fatal("fast_popen returned a null FILE* pointer");
	}
	struct jx* jx = jx_parse_stream(out);
	if(jx == NULL){
		fatal("JX parse stream out returned a null jx object");
	}
	sh_pclose(out);
	return jx;
}

static struct list* extract_file_names_from_list(char* in){
	struct list* output = list_create();
	char* tmp = strdup(in);
	char* ta = strtok(tmp,",");
	while(ta != NULL){
		int push_success = list_push_tail(output,strdup(ta));
		if(!push_success){
			fatal("Error appending file name to list due to being out of memory");
		}
		ta = strtok(0,",");
	}
	
	return output;
	
}

static int upload_input_files_to_s3(char* files,char* jobname){
	int success = 1;
	char* env_var = initialized_data.env_prefix;
	struct list* file_list = extract_file_names_from_list(files);
	debug(D_BATCH,"extra input files list: %s, len: %i",files, list_size(file_list));
	list_first_item(file_list);
	char* cur_file = NULL;
	while((cur_file = list_next_item(file_list)) != NULL){
		if(hash_table_lookup(submitted_files,cur_file) == &HAS_SUBMITTED_VALUE){
			continue;
		}
		debug(D_BATCH,"Submitting file: %s",cur_file);
		char* put_file_command = string_format("tar -cvf %s.txz %s && %s aws s3 cp %s.txz s3://%s/%s.txz ",cur_file,cur_file,env_var,cur_file,bucket_name,cur_file);
		int ret = sh_system(put_file_command);
		if(ret != 0){
			debug(D_BATCH,"File Submission: %s FAILURE return code: %i",cur_file,ret);
			success = 0;
		}else{
			debug(D_BATCH,"File Submission: %s SUCCESS return code: %i",cur_file,ret);
		}
		free(put_file_command);
		put_file_command = string_format("rm %s.txz",cur_file);
		sh_system(put_file_command);
		free(put_file_command);
		//assume everything went well?
		hash_table_insert(submitted_files,cur_file,&HAS_SUBMITTED_VALUE);
	}
	list_free(file_list);
	list_delete(file_list);
	return success;
}


static struct internal_amazon_batch_amazon_ids initialize(struct batch_queue* q){
	if(initialized){
		return initialized_data;
	}
	char* config_file = hash_table_lookup(q->options,"amazon-batch-config");
	if(!config_file) {
		fatal("No amazon config file passed!");
	}
	
	struct jx* config = jx_parse_file(config_file);
	
	
	initialized = 1;
	instID = time(NULL);
	queue_name = string_format("%i_ccl_amazon_batch_queue",instID);//should be unique
	done_jobs = itable_create(0);//default size
	amazon_job_ids = itable_create(0);
	done_files = itable_create(0);
	submitted_files = hash_table_create(0,0);
		

	char* amazon_ami = hash_table_lookup(q->options,"amazon-batch-img");
	if(amazon_ami == NULL) {
		fatal("No image id passed. Please pass file containing ami image id using --amazon-batch-img flag");
	}

	char* aws_access_key_id     = (char*)jx_lookup_string(config, "aws_id");
	char* aws_secret_access_key = (char*)jx_lookup_string(config, "aws_key");
	char* aws_region            = (char*)jx_lookup_string(config,"aws_reg");
	bucket_name                 = (char*)jx_lookup_string(config,"bucket");
	vpc                         = (char*)jx_lookup_string(config,"vpc");
	sec_group                   = (char*)jx_lookup_string(config,"sec_group");
	queue_name                  = (char*)jx_lookup_string(config,"queue_name");
	compute_env_name            = (char*)jx_lookup_string(config,"env_name");
	subnet                      = (char*)jx_lookup_string(config,"subnet");	

	if(!aws_access_key_id)
		fatal("credentials file %s does not contain aws_id",config_file);
	if(!aws_secret_access_key)
		fatal("credentials file %s does not contain aws_key",config_file);
	if(!aws_region)
		fatal("credentials file %s does not contain aws_reg",config_file);
	if(!bucket_name)
		fatal("credentials file %s does not contain bucket",config_file);
	if(!queue_name)
		fatal("credentials file %s does not contain queue_name",config_file);
	if(!compute_env_name)
		fatal("credentials file %s does not contain env_name",config_file);
	if(!vpc)
		fatal("credentials file %s does not contain vpc",config_file);
	if(!subnet)
		fatal("credentials file %s does not contain subnet",config_file); 
	
	char* env_var = string_format("AWS_ACCESS_KEY_ID=%s AWS_SECRET_ACCESS_KEY=%s AWS_DEFAULT_REGION=%s ",aws_access_key_id,aws_secret_access_key,aws_region);
	

	initialized_data.aws_access_key_id = aws_access_key_id;
	initialized_data.aws_secret_access_key = aws_secret_access_key;
	initialized_data.aws_region=aws_region;
	initialized_data.env_prefix = env_var;
	return initialized_data;
}

static char* generate_s3_cp_cmds(char* files, char* src, char* dst){
	char* env_var = initialized_data.env_prefix;
	struct list* file_list = extract_file_names_from_list(files);
	list_first_item(file_list);

	char* new_cmd=malloc(sizeof(char)*1);
    new_cmd[0]='\0';
	if(list_size(file_list)> 0){
		char* copy_cmd_prefix = string_format("%s aws s3 cp ", env_var);
		char* cur_file = NULL;
		while((cur_file=list_next_item(file_list)) != NULL){
			char* tmp;
			if(strstr(dst,"s3")){
				tmp = string_format("tar -cvf %s.txz %s && %s %s/%s.txz %s/%s.txz",cur_file,cur_file,copy_cmd_prefix, src, cur_file, dst, cur_file);
			}else{
				tmp = string_format("%s %s/%s.txz %s/%s.txz && tar -xvf %s.txz",copy_cmd_prefix, src, cur_file, dst, cur_file, cur_file);
			}
			char* tmp2 = string_format("%s\n%s\n",new_cmd,tmp);
			free(new_cmd);
			free(tmp);
			new_cmd = tmp2;
		}
	}
	list_free(file_list);
	list_delete(file_list);
	return new_cmd;
}

static char* chmod_all(char* files){
	struct list* file_list = extract_file_names_from_list(files);
	list_first_item(file_list);
	char* new_cmd=malloc(sizeof(char)*1);
	new_cmd[0]='\0';
	char* cur_file = NULL;
	if(list_size(file_list) > 0){
		while((cur_file=list_next_item(file_list)) != NULL){
			char* tmp = string_format("chmod +x %s",cur_file);
			char* tmp2 = string_format("%s\n%s",new_cmd,tmp);
			free(new_cmd);
			free(tmp);
			new_cmd=tmp2;
		}
	}
	list_free(file_list);
	list_delete(file_list);
	return new_cmd;
}

static void upload_cmd_file(char* bucket_name, char* input_files, char* output_files, char* cmd, unsigned int jobid){
	char* env_var = initialized_data.env_prefix;
	//Create command to pull files from s3 and into local space to work on
	char* bucket = string_format("s3://%s",bucket_name);
	char* cpy_in = generate_s3_cp_cmds(input_files,bucket,"./");
	char* chmod = chmod_all(input_files);
	//run the actual command
	char* cmd_tmp = string_format("%s\n%s\n%s\n",cpy_in,chmod,cmd);
	free(cpy_in);
	//copy out any external files
	char* cpy_out = generate_s3_cp_cmds(output_files,"./",bucket);
	cmd_tmp = string_format("%s\n%s\n",cmd_tmp,cpy_out);

	//add headder
	char* final_cmd = string_format("#!/bin/sh\n%s",cmd_tmp);
	free(cmd_tmp);


	//write out to file	
	unsigned int tempuid = gen_guid();
	char* tmpfile_string = string_format("TEMPFILE-%u.sh",tempuid);
	FILE* tmpfile = fopen(tmpfile_string,"w+");
	fwrite(final_cmd,sizeof(char),strlen(final_cmd),tmpfile);
	fclose(tmpfile);
	free(final_cmd);
	
	//make executable and put into s3
	cmd_tmp = string_format("chmod +x %s",tmpfile_string);
	sh_system(cmd_tmp);
	free(cmd_tmp);
	cmd_tmp = string_format("%s aws s3 cp %s s3://%s/COMAND_FILE_%u.sh",env_var,tmpfile_string,bucket_name,jobid);
	sh_system(cmd_tmp);
	free(cmd_tmp);
	remove(tmpfile_string);	
	free(tmpfile_string);

}

static char* aws_submit_job(char* job_name, char* properties_string){
	char* queue = queue_name;
	char* env_var = initialized_data.env_prefix;
	//submit the job-def
	char* tmp = string_format("%s aws batch register-job-definition --job-definition-name %s_def --type container --container-properties \"%s\"",env_var,job_name, properties_string);
	debug(D_BATCH,"Creating the Job Definition: %s",tmp);
    struct jx* jx = run_command(tmp);
	free(tmp);
	
	char* arn = (char*)jx_lookup_string(jx,"jobDefinitionArn");
	if(arn == NULL){
		fatal("Fatal error when trying to create the job definition!");
	}
	jx_delete(jx);
	
	//now that we have create a job-definition, we can submit the job.
	tmp = string_format("%s aws batch submit-job --job-name %s --job-queue %s --job-definition %s_def",env_var,job_name,queue,job_name);
	debug(D_BATCH,"Submitting the job: %s",tmp);
	jx = run_command(tmp);
	free(tmp);
	char* jaid = strdup((char*)jx_lookup_string(jx,"jobId"));
	if(!jaid)
		fatal("NO JOB ID FROM AMAZON GIVEN");
	jx_delete(jx);
	return jaid;
}
enum{
	DESCRIBE_AWS_JOB_SUCCESS = 1, //job exists, succeeded
	DESCRIBE_AWS_JOB_FAILED = 0, //job exists, failed
	DESCRIBE_AWS_JOB_NON_FINAL = -1, //exists, but in non-final state
	DESCRIBE_AWS_JOB_NON_EXIST = -2 //job doesn't exist, should treat as a failure
};

static int finished_aws_job_exit_code(char* aws_jobid, char* env_var){
	char* cmd = string_format("aws batch describe-jobs --jobs %s",aws_jobid);
	struct jx* jx = run_command(cmd);
	free(cmd);
	struct jx* jobs_array = jx_lookup(jx,"jobs");
	if(!jobs_array){
		debug(D_BATCH,"Problem with given aws_jobid: %s",aws_jobid);
		return DESCRIBE_AWS_JOB_NON_EXIST;
	}
	struct jx* first_item = jx_array_index(jobs_array,0);
	if(!first_item){
		debug(D_BATCH,"Problem with given aws_jobid: %s",aws_jobid);
		return DESCRIBE_AWS_JOB_NON_EXIST;
	}
	int ret = (int)jx_lookup_integer(first_item,"exitCode");
	jx_delete(jx);
	return ret;
}

static int describe_aws_job(char* aws_jobid, char* env_var){
	char* cmd = string_format("aws batch describe-jobs --jobs %s",aws_jobid);
	struct jx* jx = run_command(cmd);
	free(cmd);
	int succeed = DESCRIBE_AWS_JOB_NON_FINAL; //default status
	struct jx* jobs_array = jx_lookup(jx,"jobs");
	if(!jobs_array){
		debug(D_BATCH,"Problem with given aws_jobid: %s",aws_jobid);
		return DESCRIBE_AWS_JOB_NON_EXIST;
	}
	struct jx* first_item = jx_array_index(jobs_array,0);
	if(!first_item){
		debug(D_BATCH,"Problem with given aws_jobid: %s",aws_jobid);
		return DESCRIBE_AWS_JOB_NON_EXIST;
	}
	if(strstr((char*)jx_lookup_string(first_item,"status"),"SUCCEEDED")){
		succeed = DESCRIBE_AWS_JOB_SUCCESS;
	}
	if(strstr((char*)jx_lookup_string(first_item,"status"),"FAILED")){
		succeed = DESCRIBE_AWS_JOB_FAILED;
	}

	//start and stop
	if(succeed == DESCRIBE_AWS_JOB_SUCCESS || succeed == DESCRIBE_AWS_JOB_FAILED){	
		int64_t created_string = (int64_t) jx_lookup_integer(first_item,"createdAt");
		int64_t start_string = (int64_t)jx_lookup_integer(first_item,"startedAt");
		int64_t end_string = (int64_t)jx_lookup_integer(first_item,"stoppedAt");
		if(created_string != 0 ){
			debug(D_BATCH,"Job %s was created at: %"PRIi64"",aws_jobid,created_string);
		}
		if(start_string != 0 ){
			debug(D_BATCH,"Job %s started at: %"PRIi64"",aws_jobid,start_string);
		}
		if(end_string != 0 ){
			debug(D_BATCH,"Job %s ended at: %"PRIi64"",aws_jobid,end_string);
		}
	}
	jx_delete(jx);
	return succeed;
}

static char* aws_job_def(char* aws_jobid){
	char* cmd = string_format("aws batch describe-jobs --jobs %s",aws_jobid);
	struct jx* jx = run_command(cmd);
	free(cmd);
	struct jx* jobs_array = jx_lookup(jx,"jobs");
	if(!jobs_array){
		debug(D_BATCH,"Problem with given aws_jobid: %s",aws_jobid);
		return NULL;
	}
	struct jx* first_item = jx_array_index(jobs_array,0);
	if(!first_item){
		debug(D_BATCH,"Problem with given aws_jobid: %s",aws_jobid);
		return NULL;
	}
        char* ret = string_format("%s",(char*)jx_lookup_string(first_item,"jobDefinition"));
	jx_delete(jx);
        return ret;
}

static int del_job_def(char* jobdef){
    char* cmd = string_format("aws batch deregister-job-definition --job-definition %s",jobdef);
    int ret = sh_system(cmd);
    free(cmd);
    return ret;
}

static batch_job_id_t batch_job_amazon_batch_submit(struct batch_queue* q, const char* cmd, const char* extra_input_files, const char* extra_output_files, struct jx* envlist, const struct rmsummary* resources){
	struct internal_amazon_batch_amazon_ids amazon_ids = initialize(q);
	char* env_var = amazon_ids.env_prefix;

	//so, we have the access keys, now we need to either set up the queues and exec environments, or add them.
	unsigned int jobid = gen_guid();
	char* job_name = string_format("%s_%u",queue_name,jobid);
	
	//makeflow specifics
	struct batch_job_info *info = malloc(sizeof(*info));
	memset(info, 0, sizeof(*info));
	
	//specs
	int cpus=1;
	long int mem=1000;
	char* img = hash_table_lookup(q->options,"amazon-batch-img");
	int disk = 1000;
	if(resources){
		cpus = resources->cores;
		mem = resources->memory;
		disk = resources->disk;
		cpus = cpus > 1? cpus:1;
		mem = mem > 1000? mem:1000;
		disk = disk > 1000 ? disk : 1000;
	}
	//upload files to S3
	upload_input_files_to_s3((char*)extra_input_files,job_name);	
	upload_cmd_file(bucket_name,(char*)extra_input_files,(char*)extra_output_files,(char*)cmd,jobid);
	
	//create the fmd string to give to the command
	char* fmt_cmd = string_format("%s aws s3 cp s3://%s/COMAND_FILE_%u.sh ./ && sh ./COMAND_FILE_%u.sh",env_var,bucket_name,jobid,jobid);	

	//combine all properties together
	char* properties_string = string_format("{ \\\"image\\\": \\\"%s\\\", \\\"vcpus\\\": %i, \\\"memory\\\": %li, \\\"privileged\\\":true ,\\\"command\\\": [\\\"sh\\\",\\\"-c\\\",\\\"%s\\\"], \\\"environment\\\":[{\\\"name\\\":\\\"AWS_ACCESS_KEY_ID\\\",\\\"value\\\":\\\"%s\\\"},{\\\"name\\\":\\\"AWS_SECRET_ACCESS_KEY\\\",\\\"value\\\":\\\"%s\\\"},{\\\"name\\\":\\\"REGION\\\",\\\"value\\\":\\\"%s\\\"}] }", img,cpus,mem,fmt_cmd,amazon_ids.aws_access_key_id,amazon_ids.aws_secret_access_key,amazon_ids.aws_region);
	
	char* jaid = aws_submit_job(job_name,properties_string);

	itable_insert(amazon_job_ids,jobid,jaid);
	debug(D_BATCH,"Job %u has amazon id: %s",jobid,jaid);
	itable_insert(done_files,jobid,string_format("%s",extra_output_files));
	debug(D_BATCH,"Job %u successfully Submitted",jobid);
	
	//let makeflow know
	info->submitted = time(0);
	info->started = time(0);
	itable_insert(q->job_table, jobid, info);
	
	//cleanup
	free(job_name);
	free(fmt_cmd);
	
	return jobid;
	
}

static batch_job_id_t batch_job_amazon_batch_wait(struct batch_queue *q, struct batch_job_info *info_out, time_t stoptime){
	struct internal_amazon_batch_amazon_ids amazon_ids = initialize(q);
	//succeeded check
	int done  = 0;
	char* env_var = amazon_ids.env_prefix;
	itable_firstkey(amazon_job_ids);
	char* jaid;
	UINT64_T jobid;
 	while(itable_nextkey(amazon_job_ids,&jobid,(void**)&jaid)){
		done = describe_aws_job(jaid,env_var);
		char* jobname = string_format("%s_%u",queue_name,(unsigned int)jobid);
		unsigned int id = (unsigned int)jobid;
		if(done == DESCRIBE_AWS_JOB_SUCCESS){
			if(itable_lookup(done_jobs,id+1) == NULL){
				//id is done, returning here
				debug(D_BATCH,"Inserting id: %u into done_jobs",id);
				itable_insert(done_jobs,id+1,jobname);
				itable_remove(amazon_job_ids,jobid);
				
				//pull files from s3
				char* output_files = itable_lookup(done_files,id);
				struct list* file_list = extract_file_names_from_list(output_files);
				if(list_size(file_list)> 0){
					list_first_item(file_list);
					char* cur_file = NULL;
					while((cur_file=list_next_item(file_list)) != NULL){
						debug(D_BATCH,"Copying over %s",cur_file);
						char* get_from_s3_cmd = string_format("%s aws s3 cp s3://%s/%s.txz ./%s.txz && tar -xvf %s.txz && rm %s.txz",env_var,bucket_name,cur_file,cur_file, cur_file, cur_file);
						int outputcode = sh_system(get_from_s3_cmd);
						debug(D_BATCH,"output code from calling S3 to pull file %s: %i",cur_file,outputcode);
						FILE* tmpOut = fopen(cur_file,"r");
						if(tmpOut){
							debug(D_BATCH,"File does indeed exist: %s",cur_file);
							fclose(tmpOut);
						}else{
							debug(D_BATCH,"File doesn't exist: %s",cur_file);
						}
						free(get_from_s3_cmd);
					}
				}
				list_free(file_list);
				list_delete(file_list);
				
				//Let Makeflow know we're all done!
				debug(D_BATCH,"Removing the job from the job_table");
				struct batch_job_info* info = itable_remove(q->job_table, id);//got from batch_job_amazon.c
				info->finished = time(0);//get now
				info->exited_normally=1;
				info->exit_code=finished_aws_job_exit_code(jaid,env_var);
				debug(D_BATCH,"copying over the data to info_out");
				memcpy(info_out, info, sizeof(struct batch_job_info));
				free(info);
                                
                                char* jobdef = aws_job_def(jaid);
                                del_job_def(jobdef);
                                free(jobdef);
                                
				return id;
			}
		}else if(done == DESCRIBE_AWS_JOB_FAILED || done == DESCRIBE_AWS_JOB_NON_EXIST){
			if(itable_lookup(done_jobs,id+1)==NULL){
				//id is done, returning here
				itable_insert(done_jobs,id+1,jobname);
				itable_remove(amazon_job_ids,jobid);
				
				debug(D_BATCH,"Failed job: %i",id);
				
				struct batch_job_info* info = itable_remove(q->job_table, id);//got from batch_job_amazon.c
				info->finished = time(0); //get now
				info->exited_normally=0;
				int exc = finished_aws_job_exit_code(jaid,env_var);
				info->exit_code= exc == 0 ? -1 : exc;
				memcpy(info_out, info, sizeof(*info));
				free(info);
                                
                                char* jobdef = aws_job_def(jaid);
                                del_job_def(jobdef);
                                free(jobdef);
                                
				return id;
			}
		}else{
			continue;
		}
	}
	return -1;
}

static int batch_job_amazon_batch_remove(struct batch_queue *q, batch_job_id_t jobid){
	struct internal_amazon_batch_amazon_ids amazon_ids = initialize(q);
	char* env_var = amazon_ids.env_prefix;
	if(itable_lookup(done_jobs,jobid)==NULL){
		char* name = string_format("%s_%i",queue_name,(int)jobid);
		itable_insert(done_jobs,jobid+1,name);
	}
	char* amazon_id;
	if((amazon_id=itable_lookup(amazon_job_ids,jobid))==NULL){
		return -1;
	}
	char* cmd = string_format("%s aws batch terminate-job --job-id %s --reason \"Makeflow Killed\"",env_var,amazon_id);
	debug(D_BATCH,"Terminating the job: %s\n",cmd);
	sh_system(cmd);
	free(cmd);
	return 0;
	
}


batch_queue_stub_create(amazon_batch);
batch_queue_stub_free(amazon_batch);
batch_queue_stub_port(amazon_batch);
batch_queue_stub_option_update(amazon_batch);

const struct batch_queue_module batch_queue_amazon_batch = {
	BATCH_QUEUE_TYPE_AMAZON_BATCH,
	"amazon-batch",

	batch_queue_amazon_batch_create,
	batch_queue_amazon_batch_free,
	batch_queue_amazon_batch_port,
	batch_queue_amazon_batch_option_update,

	{
		batch_job_amazon_batch_submit,
		batch_job_amazon_batch_wait,
		batch_job_amazon_batch_remove,
	}
};

