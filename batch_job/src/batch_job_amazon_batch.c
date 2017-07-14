/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
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

#include <stdio.h>
#include <string.h>
#include <unistd.h>

static int initialized = 0;
static char* queue_name = NULL;
static char* compute_env_name = NULL;
static int ids = 1;
static struct itable* done_jobs;//might need an itable of itables for different runs....
static struct itable* amazon_job_ids;
static struct itable* done_files;
static int instID;
static char* bucket_name = NULL;

static struct internal_amazon_batch_amazon_ids{
	char* aws_access_key_id;
	char* aws_secret_access_key;
	char* aws_region;
	char* aws_email;
	char* master_env_prefix;
}initialized_data;

static char* create_alpha_code(int i){
	char* ret = string_format("");
	char a = 'a';
	int j = i;
	while(j>0){
		char* tmp = string_format("%s%c",ret,a+(j%10));
		free(ret);
		j/=10;
		ret = tmp;
	}
	return ret;
}

static char* wait_for_compute_env_creation(char* name, char* master_env_prefix){
	for(;;){
		char* cmd = string_format("%s aws batch describe-compute-environments --compute-environments %s", master_env_prefix, name);
		FILE* out = popen(cmd,"r");
		struct jx* jx = jx_parse_stream(out);
		struct jx* nxt = jx_array_index(jx_lookup(jx,"computeEnvironments"),0);
		pclose(out);
		free(cmd);
		if(strcmp(jx_lookup_string(nxt,"status"),"VALID") == 0){
			jx_delete(jx);
			return NULL;
		}else if(strcmp(jx_lookup(nxt,"status"),"INVALID") == 0){
			jx_delete(jx);
			return "INVALID CREATION OF COMPUTE_ENVIRONMENT";
		}
	}
	return NULL;
}
static char* wait_for_job_queue_creation(char* name, char* master_env_prefix){
	for(;;){
		char* cmd = string_format("%s aws batch describe-job-queues --job-queues %s", master_env_prefix, name);
		FILE* out = popen(cmd,"r");
		struct jx* jx = jx_parse_stream(out);
		struct jx* nxt = jx_array_index(jx_lookup(jx,"jobQueues"),0);
		pclose(out);
		free(cmd);
		if(strcmp(jx_lookup_string(nxt,"status"),"VALID") == 0){
			jx_delete(jx);
			return NULL;
		}else if(strcmp(jx_lookup(nxt,"status"),"INVALID") == 0){
			jx_delete(jx);
			return "INVALID CREATION OF JOB-QUEUE";
		}
	}
	return NULL;
}

static void split_comma_list(char* in, int* size, char*** output){
	*size = 0;
	char* tmp = strdup(in);
	char* ta = strtok(tmp,",");
	while(ta != NULL){
		if(strlen(ta) < 1) continue;
		*size += 1;
		//free(ta);//hoepfully doesn't crash everything: it did...
		ta = strtok(0,",");
	}
	
	*output = malloc(sizeof(char*) * (*size));
	int i = 0;
	tmp = strdup(in);
	ta = strtok(tmp,",");
	while(ta != NULL){
		if(strlen(ta) < 1) continue;
		(*output)[i] = strdup(ta);
		ta = strtok(0,",");
		i+=1;
	}
	
	return;
	
}

static void clean_str_array(int size, char*** array){
	int i=0;
	for(i=0; i< size; i++){
		free((*array)[i]);
	}
	free(*array);
}

static struct internal_amazon_batch_amazon_ids initialize(struct batch_queue* q){
	if(initialized){
		return initialized_data;
	}
	initialized = 1;
	instID = time(NULL);
	queue_name = string_format("%i_ccl_amazon_batch_queue",instID);//should be unique
	done_jobs = itable_create(0);//default size
	amazon_job_ids = itable_create(0);
	done_files = itable_create(0);
	
	//get amazon stuff
	char* amazon_credentials_filepath = hash_table_lookup(q->options,
							      "amazon-credentials");
	if(amazon_credentials_filepath == NULL) {
		fatal("No amazon credentials passed. Please pass file containing amazon credentials using --amazon-credentials flag");
	}
	char* amazon_ami = hash_table_lookup(q->options,
					     "amazon-ami");
	if(amazon_ami == NULL) {
		fatal("No ami image id passed. Please pass file containing ami image id using --amazon-ami flag");
	}

	struct jx* config = jx_parse_file(amazon_credentials_filepath);
	if(!config) {
		fatal("Amazon credentials file could not be opened");
	}

	const char* aws_access_key_id = jx_lookup_string(config, "aws_access_key");
	const char* aws_secret_access_key = jx_lookup_string(config, "aws_secret_access_key");
	const char* aws_region = jx_lookup_string(config,"aws_ec2_region");
	const char* aws_email = jx_lookup_string(config,"aws_email");

	if(!aws_access_key_id)
		fatal("credentials file %s does not contain aws_access_key");
	if(!aws_secret_access_key)
		fatal("credentials file %s does not contain aws_secret_access_key");
	if(!aws_region)
		fatal("credentials file %s does not contain aws_ec2_region");
	if(!aws_email)
		fatal("credentials file %s does not contain aws_email");
		
	char* env_var = string_format("AWS_ACCESS_KEY_ID=%s AWS_SECRET_ACCESS_KEY=%s AWS_DEFAULT_REGION=%s ",aws_access_key_id,aws_secret_access_key,aws_region);
	
	FILE* out;
	
	//create compute environment
	compute_env_name = string_format("%i_ccl_amazon_batch_compenv",instID);
	//this is wrong, but I just want to try and do the
	char* cmd = string_format("%s aws --region=%s batch create-compute-environment --compute-environment-name %s --type MANAGED --state ENABLED --compute-resources type=EC2,minvCpus=2,maxvCpus=4,desiredvCpus=2,instanceTypes=optimal,subnets=subnet-f8d9e19f,securityGroupIds=sg-8e010af5,instanceRole=ecsInstanceRole --service-role=arn:aws:iam::429641242186:role/service-role/AWSBatchServiceRole",env_var,aws_region,compute_env_name);
	debug(D_BATCH,"Creating the Compute Environment: %s\n",cmd);
	out = popen(cmd,"r");
	struct jx* jx = jx_parse_stream(out);
	jx_pretty_print_stream(jx,stderr);
	pclose(out);
	jx_delete(jx);
	free(cmd);
	
	if(wait_for_compute_env_creation(compute_env_name,env_var) != NULL){
		fatal("ERROR WHEN CREATING THE COMPUTE ENVIRONMENT");
	}
	
	//create queue
	cmd = string_format("%s aws --region=%s batch create-job-queue --state=ENABLED --priority=1 --job-queue-name=%s --compute-environment-order order=1,computeEnvironment=\"%s\"",env_var,aws_region,queue_name,compute_env_name);
	debug(D_BATCH,"Creating the Batch Queue: %s\n",cmd);
	out = popen(cmd,"r");
	jx = jx_parse_stream(out);
	pclose(out);
	jx_pretty_print_stream(jx,stderr);
	jx_delete(jx);

	if(wait_for_job_queue_creation(queue_name,env_var) != NULL){
		fatal("ERROR WHEN CREATING THE QUEUE");
	}
	
	bucket_name = create_alpha_code(instID);
	char* tmpbn = string_format("ccl%s",bucket_name);
	free(bucket_name);
	bucket_name = tmpbn;
	cmd = string_format("%s aws s3 mb s3://%s",env_var,bucket_name);
	debug(D_BATCH,"\nRunning the following command: %s\n",cmd);
	int ret = system(cmd);
	debug(D_BATCH,"Creating new bucket return code: %i",ret);
	free(cmd);
	
	
	
	initialized_data.aws_access_key_id = aws_access_key_id;
	initialized_data.aws_secret_access_key = aws_secret_access_key;
	initialized_data.aws_region=aws_region;
	initialized_data.aws_email = aws_email;
	initialized_data.master_env_prefix = env_var;
	return initialized_data;
}

static batch_job_id_t get_id_from_name(char* name){//name has structure of "QUEUENAME_id"
	int i = strrpos(name,'_');//get last instance
	//int len = strlen(name);
	int k = atoi(&(name[i+1]));
	return k;
}

static batch_job_id_t batch_job_amazon_batch_submit(struct batch_queue* q, const char* cmd, const char* extra_input_files, const char* extra_output_files, struct jx* envlist, const struct rmsummary* resources){
	struct internal_amazon_batch_amazon_ids amazon_ids = initialize(q);
	char* env_var = amazon_ids.master_env_prefix;
	int i;
	FILE* out;
	struct jx* jx;
	
	//so, we have the access keys, now we need to either set up the queues and exec environments, or add them.
	int jobid = ids++;
	char* job_name = string_format("%s_%i",queue_name,jobid);
	char* queue = queue_name;
	
	//makeflow specifics
	struct batch_job_info *info = malloc(sizeof(*info));
	memset(info, 0, sizeof(*info));
	
	//specs
	int cpus=1;
	long int mem=1000;
	char* img = hash_table_lookup(q->options,"amazon-ami");
	if(resources){
		cpus = resources->cores;
		mem = resources->memory;
		cpus = cpus > 1? cpus:1;
		mem = mem > 1000? mem:1000;
	}
	
	//upload files to S3
	int files_split_num = 0;//to prevent dirty data
	char** files_split;
	split_comma_list(extra_input_files,&files_split_num,&files_split);
	debug(D_BATCH,"\nEXTRA INPUT FILES LIST: %s, len: %i\n",extra_input_files, files_split_num);
	for(i=0; i<files_split_num; i++){
		debug(D_BATCH,"\nSubmitting file: %s\n",files_split[i]);
		//using a suggestion from stack overflow
		char* put_file_command = string_format("%s aws s3 sync . s3://%s/ --exclude '*' --include '%s'",env_var,bucket_name,files_split[i]);
		int ret = system(put_file_command);
		debug(D_BATCH,"\nFile Submission: %s return code: %i\n",files_split[i],ret);
	}
	
	//Create command to pull files from s3 and into local space to work on
	char* new_cmd=string_format("");
	if(files_split_num > 0){
		char* copy_cmd_prefix = string_format("%s aws s3 cp ", env_var);
		for(i=0; i<files_split_num; i++){
			char* tmp = string_format("%s s3://%s/%s ./",copy_cmd_prefix, bucket_name, files_split[i]);
			char* tmp2 = string_format("%s\n%s\n",new_cmd,tmp);
			free(new_cmd);
			free(tmp);
			new_cmd = tmp2;
		}
	}
	clean_str_array(files_split_num,&files_split);
	//run the actual command
	char* cmd_tmp = string_format("%s\n%s\n",new_cmd,cmd);
	free(new_cmd);
	new_cmd = cmd_tmp;
	//copy out any external files
	split_comma_list(extra_output_files,&files_split_num,&files_split);
	debug(D_BATCH,"\nNumber of output files: %i\n",files_split_num);
	if(files_split_num > 0){
		char* copy_cmd_prefix = string_format("%s aws s3 cp ", env_var);//AWS_ACCESS_KEY_ID=$ID_KEY AWS_SECRET_ACCESS_KEY=$SECRET AWS_DEFAULT_REGION=$REGION aws s3 cp ";
		for(i=0; i<files_split_num; i++){
			char* tmp = string_format("%s ./%s s3://%s/%s",copy_cmd_prefix, files_split[i], bucket_name, files_split[i]);
			char* tmp2 = string_format("%s\n%s\n",new_cmd,tmp);
			free(new_cmd);
			free(tmp);
			new_cmd = tmp2;
		}
	}
	clean_str_array(files_split_num,&files_split);
	
	new_cmd = string_format("#!/bin/sh\n%s",new_cmd);
	
	FILE* tmpfile = fopen("TEMPFILE.sh","w+");
	fwrite(new_cmd,sizeof(char),strlen(new_cmd),tmpfile);
	fclose(tmpfile);
	free(new_cmd);
	
	system("chmod +x TEMPFILE.sh");
	cmd_tmp = string_format("%s aws s3 cp ./TEMPFILE.sh s3://%s/COMAND_FILE_%i.sh",env_var,bucket_name,jobid);
	system(cmd_tmp);
	free(cmd_tmp);
	remove("TEMPFILE.sh");	

	//turn the massive string into a json array of individual strings
	char* whole_new_command = string_format("aws s3 cp s3://%s/COMAND_FILE_%i.sh ./ && sh ./COMAND_FILE_%i.sh",bucket_name,jobid,jobid);
	int cmd_split_num;
	char** cmd_split;
	string_split(whole_new_command,&cmd_split_num,&cmd_split);
	char* fmt_cmd = string_format("\"%s\"",cmd_split[0]);
	for(i=1; i< cmd_split_num; i++){
		char* tmp = string_format("%s,\"%s\"",fmt_cmd,cmd_split[i]);
		free(fmt_cmd);
		fmt_cmd=tmp;
	}
	free(fmt_cmd);
	fmt_cmd = string_format("%s aws s3 cp s3://%s/COMAND_FILE_%i.sh ./ && sh ./COMAND_FILE_%i.sh",env_var,bucket_name,jobid,jobid);
	free(whole_new_command);
	
	//combine all properties together
	char* properties_string = string_format("{ \\\"image\\\": \\\"%s\\\", \\\"vcpus\\\": %i, \\\"memory\\\": %li, \\\"command\\\": [\\\"sh\\\",\\\"-c\\\",\\\"%s\\\"], \\\"environment\\\":[{\\\"name\\\":\\\"CCL_MASTER_CMD\\\",\\\"value\\\":\\\"%s\\\"},{\\\"name\\\":\\\"CCL_COMMAND_STRING\\\",\\\"value\\\":\\\"%s\\\"},{\\\"name\\\":\\\"AWS_ACCESS_KEY_ID\\\",\\\"value\\\":\\\"%s\\\"},{\\\"name\\\":\\\"AWS_SECRET_ACCESS_KEY\\\",\\\"value\\\":\\\"%s\\\"},{\\\"name\\\":\\\"REGION\\\",\\\"value\\\":\\\"%s\\\"}] }", img,cpus,mem,fmt_cmd,fmt_cmd,cmd,amazon_ids.aws_access_key_id,amazon_ids.aws_secret_access_key,amazon_ids.aws_region);
	char* tmp = string_format("%s aws batch register-job-definition --job-definition-name %s_def --type container --container-properties \"%s\"",env_var,job_name, properties_string);
	debug(D_BATCH,"Creating the Job Definition: %s\n",tmp);
	out = popen(tmp,"r");
	jx = jx_parse_stream(out);
	pclose(out);
	free(tmp);
	
	char* arn = jx_lookup_string(jx,"jobDefinitionArn");
	if(arn == NULL){
		fatal("Fatal error when trying to create the job definition!");
	}
	jx_delete(jx);
	
	//now that we have create a job-definition, we can submit the job.
	tmp = string_format("%s aws batch submit-job --job-name %s --job-queue %s --job-definition %s_def",env_var,job_name,queue,job_name);
	debug(D_BATCH,"Submitting the job: %s\n",tmp);
	out = popen(tmp,"r");
	jx = jx_parse_stream(out);
	free(tmp);
	pclose(out);
	char* jaid = jx_lookup_string(jx,"jobId");
	if(!jaid)
		fatal("NO JOB ID FROM AMAZON GIVEN");
	itable_insert(amazon_job_ids,jobid,jaid);
	jx_pretty_print_stream(jx,stderr);
	itable_insert(done_files,jobid,string_format("%s",extra_output_files));
	debug(D_BATCH,"Job %i successfully Submitted\n",jobid);
	
	//let makeflow know
	info->submitted = time(0);
	info->started = time(0);
	itable_insert(q->job_table, jobid, info);
	
	//cleanup
	free(job_name);
	free(fmt_cmd);
	jx_delete(jx);
	
	return jobid;
	
}

static batch_job_id_t batch_job_amazon_batch_wait(struct batch_queue *q, struct batch_job_info *info_out, time_t stoptime){
	struct internal_amazon_batch_amazon_ids amazon_ids = initialize(q);
	int i=0;
	//succeeded check
	int done  = 0;
	char* env_var = amazon_ids.master_env_prefix; 
	while(!done){
		char* popenstr = string_format("%s aws batch list-jobs --job-queue %s --job-status SUCCEEDED",env_var,queue_name);
		debug(D_BATCH,"Listing the jobs-succeeded: %s\n",popenstr);
		FILE* out = popen(popenstr,"r");//need to decide on the queue name for jobs....
		struct jx* jx= jx_parse_stream(out);
		jx_pretty_print_stream(jx,stderr);
		pclose(out);
		//check to see if we have more to go through
		char* nxt = NULL;
		if((nxt=jx_lookup_string(jx,"nextToken"))==NULL){
			done = 1;
		}
		//checking for our item
		
		struct jx* jx_arr = jx_lookup(jx,"jobSummaryList");
		int len = jx_array_length(jx_arr);
		for(i=0; i<len; ++i){
			struct jx* itm = jx_array_index(jx_arr,i);
			char* jobname = jx_lookup_string(itm, "jobName");
			int id = get_id_from_name(jobname);
			//debug(D_BATCH,"\n\njobName: %s, jobid: %i\n\n",jobname,id);
			if(itable_lookup(done_jobs,id+1) == NULL){
				//id is done, returning here
				debug(D_BATCH,"\n\nInserting id: %i into done_jobs\n\n",id);
				itable_insert(done_jobs,id+1,jobname);
				
				//pull files from s3
				char* output_files = itable_lookup(done_files,id);
				int num_done_files=0;
				char** done_files_list;
				split_comma_list(output_files,&num_done_files,&done_files_list);
				if(num_done_files > 0){
					int j = 0;
					for(j=0; j<num_done_files; j++){
						debug(D_BATCH,"\nCopying over %s",done_files_list[j]);
						char* get_from_s3_cmd = string_format("%s aws s3 cp s3://%s/%s ./%s",env_var,bucket_name,done_files_list[j],done_files_list[j]);
						int outputcode = system(get_from_s3_cmd);
						debug(D_BATCH,"\noutput code from calling S3 to pull file %s: %i",done_files_list[j],outputcode);
						FILE* tmpOut = fopen(done_files_list[j],"r");
						if(tmpOut){
							debug(D_BATCH,"\nFile does indeed exist: %s\n",done_files_list[j]);
							fclose(tmpOut);
						}else{
							debug(D_BATCH,"\nFile doesn't exist: %s\n",done_files_list[j]);
						}
						free(get_from_s3_cmd);
					}
				}
				clean_str_array(num_done_files,&done_files_list);
				
				//Let Makeflow know we're all done!
				debug(D_BATCH,"\n\nRemoving the job from the job_table\n\n");
				struct batch_job_info* info = itable_remove(q->job_table, id);//got from batch_job_amazon.c
				info->finished = time(0);//get now
				info->exited_normally=1;
				info->exit_code=0;//NEED TO FIX EVENTUALLY, and find how to ACTUALLY get the exit code
				debug(D_BATCH,"\n\ncopying over the data to info_out\n\n");
				memcpy(info_out, info, sizeof(struct batch_job_info));
				free(info);
				return id;
			}
		}
	}
	//failed check
	done  = 0;
	while(!done){
		char* popenstr = string_format("%s aws batch list-jobs --job-queue %s --job-status FAILED",env_var,queue_name);
		debug(D_BATCH,"Listing the jobs-failed: %s\n",popenstr);
		FILE* out = popen(popenstr,"r");//need to decide on the queue name for jobs....
		struct jx* jx= jx_parse_stream(out);
		jx_pretty_print_stream(jx,stderr);
		pclose(out);
		//check to see if we have more to go through
		char* nxt = NULL;
		if((nxt=jx_lookup_string(jx,"nextToken"))==NULL){
			done = 1;
		}
		//checking for our item
		struct jx* jx_arr = jx_lookup(jx,"jobSummaryList");
		int len = jx_array_length(jx_arr);
		for(i=0; i<len; ++i){
			struct jx* itm = jx_array_index(jx_arr,i);
			char* jobname = jx_lookup_string(itm, "jobName");
			int id = get_id_from_name(jobname);
			if(itable_lookup(done_jobs,id+1) == NULL){
				//id is done, returning here
				itable_insert(done_jobs,id,jobname);
				
				debug(D_BATCH,"Failed job: %i\n",id);
				
				struct batch_job_info* info = itable_remove(q->job_table, id);//got from batch_job_amazon.c
				info->finished = time(0);//get now
				info->exited_normally=0;
				info->exit_code=1;//NEED TO FIX EVENTUALLY, and find how to ACTUALLY get the exit code
				memcpy(info_out, info, sizeof(*info));
				free(info);
				return id;
			}
		}
	}
	//if we get to this point, we should probably clean up and exit....
	return -1;
}

static int batch_job_amazon_batch_remove(struct batch_queue *q, batch_job_id_t jobid){
	struct internal_amazon_batch_amazon_ids amazon_ids = initialize(q);
	char* env_var = initialized_data.master_env_prefix; 
	if(itable_lookup(done_jobs,jobid)==NULL){
		char* name = string_format("%s_%i",queue_name,jobid);
		itable_insert(done_jobs,jobid+1,name);
	}
	char* amazon_id;
	if((amazon_id=itable_lookup(amazon_job_ids,jobid))==NULL){
		return -1;
	}
	char* cmd = string_format("%s aws batch terminate-job --jobid %s --reason \"Makeflow Killed\"",env_var,amazon_id);
	debug(D_BATCH,"Terminating the job: %s\n",cmd);
	system(cmd);
	free(cmd);
	return 0;
	
}


batch_queue_stub_create(amazon_batch);
batch_queue_stub_free(amazon_batch);
batch_queue_stub_port(amazon_batch);
batch_queue_stub_option_update(amazon_batch);

batch_fs_stub_chdir(amazon_batch);
batch_fs_stub_getcwd(amazon_batch);
batch_fs_stub_mkdir(amazon_batch);
batch_fs_stub_putfile(amazon_batch);
batch_fs_stub_stat(amazon_batch);
batch_fs_stub_unlink(amazon_batch);

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
	 },

	{
	 batch_fs_amazon_batch_chdir,
	 batch_fs_amazon_batch_getcwd,
	 batch_fs_amazon_batch_mkdir,
	 batch_fs_amazon_batch_putfile,
	 batch_fs_amazon_batch_stat,
	 batch_fs_amazon_batch_unlink,
	 },
};
