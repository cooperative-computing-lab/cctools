#include "batch_job_internal.h"
#include "process.h"
#include "batch_job.h"
#include "stringtools.h"
#include "debug.h"
#include "macros.h"
#include "nvpair_jx.h"

#include "jx_parse.h"
#include "jx_print.h"
#include "jx_eval.h"
#include "jx_export.h"
#include "semaphore.h"

#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>

struct batch_job_amazon_info {
	struct batch_job_info info;
	struct aws_config *aws_config;
	char *instance_id;
};

struct aws_instance_type {
	int cores;
	int memory;
	const char *name;
};

struct aws_config {
	const char *subnet;
	const char *ami;
	const char *security_group_id;
	const char *keypair_name;
};

static struct aws_config * aws_config_load( const char *filename )
{
	struct jx * j = jx_parse_nvpair_file(filename);
	if(!j) fatal("%s isn't a valid config file\n",filename);

	struct aws_config *c = malloc(sizeof(*c));

	c->subnet            = jx_lookup_string(j,"subnet");
	c->ami               = jx_lookup_string(j,"ami");
	c->security_group_id = jx_lookup_string(j,"security_group_id");
	c->keypair_name      = jx_lookup_string(j,"keypair_name");

	if(!c->subnet)            fatal("%s doesn't define subnet",filename);
	if(!c->ami)               fatal("%s doesn't define ami",filename);
	if(!c->security_group_id) fatal("%s doesn't define security_group_id",filename);
	if(!c->keypair_name)      fatal("%s doesn't define keypair_name",filename);

	return c;
}

static struct aws_instance_type aws_instance_table[] =
{
	{0,0,"t2.micro"},
	{2,3840,"c4.large"},
	{2,8192,"m4.large"},
	{4,7680,"c4.xlarge"},
	{4,16384,"m4.xlarge"},
	{8,15360,"c4.2xlarge"},
	{8,32768,"m4.2xlarge"},
	{16,30720,"c4.4xlarge"},
	{16,65536,"m4.4xlarge"},
	{36,61440,"c4.8xlarge"},
	{40,163840,"m4.10xlarge"},
	{64,262144,"m4.16xlarge"},
	{0,0,0}
};


/*
Select an instance type that is larger than or equal to
the desired amount of cores, memory, and disk.  Return
the name of the instance, if one exists, otherwise null.
*/


static const char * aws_instance_select( int cores, int memory, int disk )
{
	struct aws_instance_type *i;

	for(i=aws_instance_table;i->name;i++) {
		if(cores<=i->cores && memory<=i->memory) {
			debug(D_BATCH,"job requiring CORES=%d MEMORY=%d matches instance type %s",cores,memory,i->name);
			return i->name;
		}
	}
	return 0;
}

/*
Run an external command that produces json as output.
Parse it and return the corresponding parsed JX object.
*/

static struct jx * json_command( const char *str )
{
	debug(D_BATCH,"executing: %s",str);

	FILE * file = popen(str,"r");
       	if(!file) {
		debug(D_BATCH,"execution failed: %s",strerror(errno));
		return 0;
	}
	struct jx *j = jx_parse_stream(file);
	pclose(file);
	if(!j) {
		debug(D_BATCH,"execution failed: bad json output");
	}
	return j;
}

/*
Create an EC2 instance; on success return the instance id as a string that must be freed.  On failure, return zero.
*/

static char * aws_create_instance( struct aws_config *c, const char *instance_type, const char *ami )
{
	char *str = string_format("aws ec2 run-instances --subnet %s --image-id %s --instance-type %s --key-name %s --security-group-ids %s --associate-public-ip-address --output json",
		c->subnet,
		ami,
		instance_type,
		c->keypair_name,
		c->security_group_id);

	struct jx * jresult = json_command(str);
	if(!jresult) {
		free(str);
		return 0;
	}

	struct jx *jinstance = jx_lookup(jresult,"Instances")->u.items->value;
	if(!jinstance) {
		debug(D_BATCH,"run-instances didn't return an Instances array");
		jx_delete(jresult);
		free(str);
		return 0;
	}
	const char *id = jx_lookup_string(jinstance,"InstanceId");
	if(!id) {
		debug(D_BATCH,"run-instances didn't return an InstanceId!");
		jx_delete(jresult);
		free(str);
		return 0;
	}

	char *result = strdup(id);

	jx_delete(jresult);
	free(str);

	printf("created virtual machine instance %s type %s image %s\n",result,instance_type,ami);
	return result;
}

/*
Get the state of an EC2 instance, on success returns the state as a string that must be freed.  On failure, return zero.
*/

static struct jx * aws_describe_instance( struct aws_config *c, const char *instance_id )
{
	char *str = string_format("aws ec2 describe-instances --instance-ids %s --output json",instance_id);
	struct jx *j = json_command(str);
	free(str);
	return j;
}

/*
Get the state of multiple EC2 instances of a given type, on success returns the state as a string that must be freed.  On failure, return zero.
*/

static struct jx * aws_describe_instances_of_type( const char *instance_type, const char *ami )
{
	char *str = string_format("aws ec2 describe-instances --filters Name=image-id,Values=%s Name=instance-type,Values=%s Name=tag:makeflow_status,Values=idle\n",
		ami,
		instance_type
	);
	// TODO: DEBUG
	printf("aws_describe_instances_of_type: %s", str);
	struct jx *j = json_command(str);
	free(str);
	return j;
}

/*
	Modify the tag of a given EC2 instance id, on success returns one.  On failure, return zero.
*/

static int modify_instance_tag( const char *instance_id, char *makeflow_status )
{

	printf("modifying tag to %s of instance_id: %s\n", instance_id, instance_id);
	char *str = string_format("aws ec2 create-tags --resources %s --tags Key=makeflow_status,Value=%s",
		instance_id,
		makeflow_status
	);
	struct jx *j = json_command(str);
	free(str);
	if (!j) {
		return 0;
	}
	jx_delete(j);
	return 1;
}

/*
	check the tag of a given EC2 instance id, on success returns one.  On failure, return zero.
*/

static int check_instance_tag( const char *instance_id, char *makeflow_status )
{

	printf("check instance tag %s of instance_id: %s\n", makeflow_status, instance_id);
	// TODO
	char *str = string_format("aws ec2 describe-instances --instance-ids %s --filters Name=tag:makeflow_status,Values=%s",
		instance_id,
		makeflow_status
	);
	struct jx *j = json_command(str);
	free(str);
	if (!j) {
		printf("check_instance_tag %s of instance_id: %s did not find\n", makeflow_status, instance_id);
		return 0;
	}
	jx_delete(j);
	printf("check_instance_tag %s of instance_id: %s found\n", makeflow_status, instance_id);
	return 1;
}

/*
Terminate an EC2 instance.  If termination is successfully applied, return true, otherwise return false.
*/

static int aws_terminate_instance( struct aws_config *c, const char *instance_id )
{
	char *str = string_format("aws ec2 terminate-instances --instance-ids %s --output json",instance_id);
	struct jx *jresult = json_command(str);
	if(jresult) {
		str = string_format("aws ec2 delete-tags --resources %s",instance_id);
		jresult = json_command(str);
		jx_delete(jresult);
		printf("deleted virtual machine instance %s\n",instance_id);
		return 1;
	} else {
		return 0;
	}
}

// TODO: DEBUG
static int aws_terminate_idle_instance( struct aws_config *c, const char *instance_id )
{
	// wait 30 seconds
	// check instance type (maybe job type)
	//  if so Terminate return 1s
	//	if not return 1
	//  failure return 0
	printf("aws_terminate_idle_instance: %s going to sleep for 30s\n", instance_id);
	// wait 30 seconds
	sleep(30);

	// check instance type (maybe job type)
	if (check_instance_tag(instance_id, "idle")){
		printf("aws_terminate_idle_instance: terminating idle instance %s\n", instance_id);
		return aws_terminate_instance(c, instance_id);
	}
	return 1;
}



/*
Create an executable script with the necessary variables exported
and the desired command.  This avoids problems with passing commands
through quotes or losing environment variables through ssh.
*/

static int create_script( const char *filename, const char *cmd, struct jx *envlist )
{
	FILE *file = fopen(filename,"w");
	if(!file) return 0;

	fprintf(file,"#!/bin/sh\n");
	jx_export_shell(envlist,file);
	fprintf(file,"exec %s\n",cmd);
	fprintf(file,"exit 127\n");
	fclose(file);

	chmod(filename,0755);
	return 1;
}

/*
Keep attempting to ssh to a host until success is achieved.
*/

static int wait_for_ssh_ready( struct aws_config *c, const char *ip_address )
{
	int result = 0;

	char *cmd = string_format("ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no -i %s.pem ec2-user@%s ls >/dev/null 2>&1",c->keypair_name,ip_address);

	int i;
	for(i=0;i<100;i++) {
		debug(D_BATCH,"test ssh: %s",cmd);
		if(system(cmd)==0) {
			result = 1;
			break;
		}
		sleep(1);
	}

	free(cmd);
	return result;
}

static int put_file( struct aws_config *c, const char *ip_address, const char *localname, const char *remotename )
{
	char *cmd = string_format("scp -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no -i %s.pem \"%s\" \"ec2-user@%s:%s\" >/dev/null 2>&1",c->keypair_name,localname,ip_address,remotename);
	debug(D_BATCH,"put_file: %s\n",cmd);
	int result = system(cmd);
	if(result<0) {
		debug(D_BATCH,"put_file failed");
	}
	free(cmd);
	return result;
}

static int put_files( struct aws_config *aws_config, const char *ip_address, const char *files )
{
	char *filelist = strdup(files);
	char *f = strtok(filelist,",");
	while(f) {
		/*
		Each item in the list could have the format
		"filename" or "filename=remotename"
		*/
		const char *remotename = f;
		char *e = strchr(f,'=');
		if(e) {
			*e = 0;
			remotename = e+1;
		}

		if(put_file(aws_config,ip_address,f,remotename)!=0) return -1;
		f = strtok(0,",");
	}
	free(filelist);
	return 0;
}

static int get_file( struct aws_config *c, const char *ip_address, const char *localname, const char *remotename )
{
	char *cmd = string_format("scp -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no -i %s.pem \"ec2-user@%s:%s\" \"%s\" >/dev/null 2>&1",c->keypair_name,ip_address,remotename,localname);
	debug(D_BATCH,"get file: %s\n",cmd);
	int result = system(cmd);
	if(result<0) {
		debug(D_BATCH,"get_file failed");
	}
	free(cmd);
	return result;
}

static void get_files( struct aws_config *aws_config, const char *ip_address, const char *files )
{
	char *filelist = strdup(files);
	char *f = strtok(filelist,",");
	while(f) {
		/*
		Each item in the list could have the format
		"filename" or "filename=remotename"
		*/
		const char *remotename = f;
		char *e = strchr(f,'=');
		if(e) {
			*e = 0;
			remotename = e+1;
		}
		/*
		In the case of failure, keep going b/c the other output files
		may be necessary to debug the problem.
		*/
		get_file(aws_config,ip_address,f,remotename);
		f = strtok(0,",");
	}
	free(filelist);
}

static int run_task( struct aws_config *c, const char *ip_address, const char *command )
{
	char *cmd = string_format("ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no -i %s.pem \"ec2-user@%s\" \"%s\"",c->keypair_name,ip_address,command);
	debug(D_BATCH,"run task: %s\n",cmd);
	int result = system(cmd);
	free(cmd);
	return result;
}

static const char * get_instance_property( struct jx *j, const char *name )
{
	j = jx_lookup(j,"Reservations");
	if(!j || j->type!=JX_ARRAY || !j->u.items) return 0;

	j = j->u.items->value;
	if(!j || j->type!=JX_OBJECT) return 0;

	j = jx_lookup(j,"Instances");
	if(!j || j->type!=JX_ARRAY || !j->u.items) return 0;

	j = j->u.items->value;
	if(!j || j->type!=JX_OBJECT) return 0;

	return jx_lookup_string(j,name);
}

static const char * get_instance_state_name( struct jx *j )
{
	j = jx_lookup(j,"Reservations");
	if(!j || j->type!=JX_ARRAY || !j->u.items) return 0;

	j = j->u.items->value;
	if(!j) return 0;

	j = jx_lookup(j,"Instances");
	if(!j || j->type!=JX_ARRAY || !j->u.items) return 0;

	j=j->u.items->value;
	if(!j) return 0;

	j = jx_lookup(j,"State");
	if(!j) return 0;

	return jx_lookup_string(j,"Name");
}

static const char * get_idle_instance_id( struct jx *j )
{
	j = jx_lookup(j,"Reservations");
	if(!j || j->type!=JX_ARRAY || !j->u.items) return 0;

	j = j->u.items->value;
	if(!j) return 0;

	j = jx_lookup(j,"Instances");
	if(!j || j->type!=JX_ARRAY || !j->u.items) return 0;

	j=j->u.items->value;
	if(!j) return 0;
	// TODO: DEBUG
	printf("get_idle_instance_id found instance id in jxparsing %s \n", jx_lookup_string(j,"InstanceId"));
	return jx_lookup_string(j,"InstanceId");
}

static const char * idle_instance_type_id( const char *instance_type, const char *ami ){

	// TODO: DEBUG
	printf("idle_instance_type_id\n");
	struct jx *j = aws_describe_instances_of_type(instance_type, ami);
	if(!j) {
		return 0;
	}

	// TODO: DEBUG
	printf("idle_instance_type_id2\n");
	const char* instance_id = get_idle_instance_id(j);

	// TODO: DEBUG
	printf("idle_instance_type_id: returning idle instance id %s \n", instance_id);

	if (!instance_id){
		jx_delete(j);
		return 0;
	} else {
		char* return_instance_id = strdup(instance_id);
		jx_delete(j);
		// TODO: DEBUG
		printf("idle_instance_type_id: returning idle instance id %s \n", return_instance_id);
		return return_instance_id;
	}
}



/*
We use a shared SYSV sempahore here in order to
manage file transfer concurrency.  On one hand,
we want multiple subprocesses running at once,
so that we don't wait long times for images to
be created.  On the other hand, we don't want
multiple file transfers going on at once.
So, each job is managed by a separate subprocess
which acquires and releases a semaphore around file transfers.
*/

static int transfer_semaphore = -1;

/*
This function runs as a child process of makeflow and handles
the execution of one task, after the instance is created.
It waits for the instance to become ready, then probes the
ssh server, sends the input file, runs the command,
and extracts the output file.  We rely on the parent makeflow
process to create and delete the instance as needed.
*/

static int batch_job_amazon_subprocess( struct aws_config *aws_config, const char *instance_id, const char *cmd, const char *extra_input_files, const char *extra_output_files, struct jx *envlist )
{
	char *ip_address = 0;

	// TODO: reset makeflow_status
	// printf("batch_job_amazon_subprocess modify_instance_tag occupied for instance %s\n", instance_id);


	/*
	Put the instance ID into the log file, so that output from
	different concurrent instances can be disentangled.
	*/

	debug_config(instance_id);

	while(1) {
		sleep(5);

		struct jx *j = aws_describe_instance(aws_config,instance_id);
		if(!j) {
			debug(D_BATCH,"unable to get instance state");
			continue;
		}

		const char * state = get_instance_state_name(j);
		if(!state) {
			debug(D_BATCH,"state is not set, keep trying...");
			jx_delete(j);
			continue;
		} else if(!strcmp(state,"pending")) {
			debug(D_BATCH,"state is 'pending', keep trying...");
			jx_delete(j);
			continue;
		} else if(!strcmp(state,"running")) {
			debug(D_BATCH,"state is 'running', checking for ip address");
			const char *i = get_instance_property(j,"PublicIpAddress");
			if(i) {
				debug(D_BATCH,"found ip address %s",i);
				ip_address = strdup(i);
				jx_delete(j);
				break;
			} else {
				debug(D_BATCH,"ip address is not set yet, keep trying...");
				jx_delete(j);
				continue;
			}
		} else {
			debug(D_BATCH,"state is '%s', which is unexpected, so aborting",state);
			jx_delete(j);
			return 127;
		}
	}

	/*
	Even though the instance is running, the ssh service is not necessarily running.
	Probe it periodically until it is ready.
	*/

	wait_for_ssh_ready(aws_config,ip_address);

	/* Send each of the input files to the instance. */
	semaphore_down(transfer_semaphore);
	int result = put_files(aws_config,ip_address,extra_input_files);
	semaphore_up(transfer_semaphore);

	/*
	If we fail to send the files, bail out early indicating
	that the task did not run at all.
	*/

	if(result!=0) return 127;

	/* Generate a unique script with the contents of the task. */
	char *runscript = string_format(".makeflow_task_script_%d",getpid());
	create_script(runscript,cmd,envlist);

	/* Send the script and delete the local copy right away. */
	put_file(aws_config,ip_address,runscript,"makeflow_task_script");
	unlink(runscript);

	/* Run the remote task. */
	int task_result = run_task(aws_config,ip_address,"./makeflow_task_script");

	/* Retreive each of the output files from the instance. */
	semaphore_down(transfer_semaphore);
	get_files(aws_config,ip_address,extra_output_files);
	semaphore_up(transfer_semaphore);

	/*
	Return the task result regardless of the file fetch;
	makeflow will figure out which files were actually produced
	and then do the right thing.
	*/
	return task_result;
}

/*
To ensure that we track all instances correctly and avoid overloading the network,
the setting up of an instance and the sending of input files are done sequentially
within batch_job_amazon_submit.  Once the inputs are successfully sent, we fork
a process in order to execute the desired task, and await its completion.
*/


static batch_job_id_t batch_job_amazon_submit(struct batch_queue *q, const char *cmd, const char *extra_input_files, const char *extra_output_files, struct jx *envlist, const struct rmsummary *resources)
{
	/* Flush output streams before forking, to avoid stale buffered data. */
	fflush(NULL);

	/* Create the job table if it didn't already exist. */
	if(!q->job_table) q->job_table = itable_create(0);

	/* Create the shared transfer semaphore */
	if(transfer_semaphore==-1) {
		transfer_semaphore = semaphore_create(1);
	}

	const char *config_file = batch_queue_get_option(q,"amazon-config");
	if(!config_file) fatal("--amazon-config option is required");

	static struct aws_config * aws_config = 0;
	if(!aws_config) aws_config = aws_config_load(config_file);


	const char *instance_type = jx_lookup_string(envlist,"AMAZON_INSTANCE_TYPE");
	if(!instance_type) {
		instance_type = aws_instance_select(resources->cores,resources->memory,resources->disk);
		if(!instance_type) {
			// TODO: DEBUG
			printf("Couldn't find suitable instance type for job with CORES=%d, MEMORY=%d, DISK=%d\n",(int)resources->cores,(int)resources->memory,(int)resources->disk);
			printf("You can choose one manually with AMAZON_INSTANCE_TYPE.\n");
			return -1;
		}
	}
	const char *ami = jx_lookup_string(envlist,"AMAZON_AMI");
	if(!ami) ami = aws_config->ami;

	const char *instance_id = idle_instance_type_id( instance_type, ami );
	if(!instance_id) {
  	/* Create a new instance and return its unique ID. */
		// TODO: DEBUG
		printf("creating instance\n");
  	instance_id = aws_create_instance(aws_config,instance_type,ami);
    if(!instance_id) {
      debug(D_BATCH,"aws_create_instance failed");
      sleep(1);
      return -1;

    }
		debug(D_BATCH,"created instance %s",instance_id);
  } else
	{
		debug(D_BATCH,"picked up running instance %s",instance_id);
		// TODO: DEBUG
		printf("batch_job_amazon_submit modify instance tag to modify for instance %s \n", instance_id);
		modify_instance_tag(instance_id, "occupied");
	}


	/* Create a new object describing the job */

	struct batch_job_amazon_info *info = malloc(sizeof(*info));
	memset(info,0,sizeof(*info));
	info->aws_config = aws_config;
 	// info->instance_id = instance_id;
	info->instance_id = strdup(instance_id);
	info->info.submitted = time(0);
	info->info.started = time(0);

	/* Now fork a new process to actually execute the task and wait for completion.*/

	batch_job_id_t jobid = fork();
	if(jobid > 0) {
		debug(D_BATCH, "started process %" PRIbjid ": %s", jobid, cmd);
		itable_insert(q->job_table, jobid, info);
		return jobid;
	} else if(jobid < 0) {
		debug(D_BATCH, "couldn't create new process: %s\n", strerror(errno));
		free(info);
		return -1;
	} else {
		/*
		Set signals to default behavior, otherwise we get
		competing behavior in the forked process.
		*/

		signal(SIGINT,SIG_DFL);
		signal(SIGQUIT,SIG_DFL);
		signal(SIGABRT,SIG_DFL);

		_exit(batch_job_amazon_subprocess(aws_config,instance_id,cmd,extra_input_files,extra_output_files,envlist));
	}
	return -1;
}

static batch_job_id_t batch_job_amazon_wait (struct batch_queue * q, struct batch_job_info * info_out, time_t stoptime)
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
			struct batch_job_amazon_info *i = itable_remove(q->job_table, p->pid);
			if(!i) {
				process_putback(p);
				return -1;
			}

			struct batch_job_info *info = &i->info;

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


			printf("batch_job_amazon_wait modify_instance_tag idle for instance %s\n", i->instance_id);
			modify_instance_tag(i->instance_id, "idle");

			/* TODO: check in 30 seconds if the instance is still idle with same job, terminate instance */
			aws_terminate_idle_instance(i->aws_config,i->instance_id);

			/* And clean up the object. */

			free(i);
			return jobid;

		} else if(errno == ESRCH || errno == ECHILD) {
			return 0;
		}

		if(stoptime != 0 && time(0) >= stoptime)
			return -1;
	}
}

/*
To kill an amazon job, we look up the details of the job,
kill the local ssh process forcibly, and then terminate
the Amazon instance.
*/

static int batch_job_amazon_remove (struct batch_queue *q, batch_job_id_t jobid)
{
	struct batch_job_amazon_info *info;

	info = itable_lookup(q->job_table,jobid);
	if(!info) {
		debug(D_BATCH, "runaway process %" PRIbjid "?\n", jobid);
		return 0;
	}

	kill(jobid,SIGKILL);

	aws_terminate_instance(info->aws_config,info->instance_id);

	debug(D_BATCH, "waiting for process %" PRIbjid, jobid);
	struct process_info *p = process_waitpid(jobid,0);
	if(p) free(p);
	if(info) free(info);

	return 1;
}

static int batch_queue_amazon_create (struct batch_queue *q)
{
	batch_queue_set_feature(q, "output_directories", "true");
	batch_queue_set_feature(q, "batch_log_name", "%s.amazonlog");
	batch_queue_set_feature(q, "autosize", "yes");
	batch_queue_set_feature(q, "remote_rename", "%s=%s");
	return 0;
}

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
