/*
Copyright (C) 2017- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "batch_job_internal.h"
#include "process.h"
#include "batch_job.h"
#include "stringtools.h"
#include "debug.h"
#include "macros.h"

#include "jx_parse.h"
#include "jx_print.h"
#include "jx_eval.h"
#include "jx_export.h"

#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>

struct aws_config {
	const char *image_id;
	const char *instance_type;
	const char *security_group_id;
	const char *keypair_name;
};

struct aws_config * aws_config_load( const char *filename )
{
	struct jx * j = jx_parse_file(filename);
	if(!j) fatal("couldn't parse %s\n",filename);

	struct aws_config *c = malloc(sizeof(*c));

	c->image_id          = jx_lookup_string(j,"image_id");
	c->instance_type     = jx_lookup_string(j,"instance_type");
	c->security_group_id = jx_lookup_string(j,"security_group_id");
	c->keypair_name      = jx_lookup_string(j,"keypair_name");

	// XXX check for complete config here

	return c;
}

/*
Run an external command that produces json as output.
Parse it and return the corresponding parsed JX object.
*/

struct jx * json_command( const char *str )
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

char * aws_create_instance( struct aws_config *c )
{
	char *str = string_format("aws ec2 run-instances --image-id %s --instance-type %s --key-name %s --security-group-ids %s --output json",
		c->image_id,
		c->instance_type,
		c->keypair_name,
		c->security_group_id);

	struct jx * jresult = json_command(str);
	free(str);
	if(!jresult) return 0;

	struct jx *jinstance = jx_lookup(jresult,"Instances")->u.items->value;
	const char * result = jx_lookup_string(jinstance,"InstanceId");
	// XXX leak failure yikes
	return strdup(result);
}

/*
Get the state of an EC2 instance, on success returns the state as a string that must be freed.  On failure, return zero.
*/

struct jx * aws_describe_instance( struct aws_config *c, const char *instance_id )
{
	char *str = string_format("aws ec2 describe-instances --instance-ids %s --output json",instance_id);
	struct jx *j = json_command(str);
	free(str);
	return j;
}

/*
Terminate an EC2 instance.  If termination is successfully applied, return true, otherwise return false.
 */

int aws_terminate_instance( struct aws_config *c, const char *instance_id )
{
	char *str = string_format("aws ec2 terminate-instances --instance-ids %s --output json",instance_id);
	struct jx *jresult = json_command(str);
	if(jresult) {
		jx_delete(jresult);
		return 1;
	} else {
		return 0;
	}
}

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

static int wait_for_ssh_ready( struct aws_config *c, const char *ip_address )
{
	int result = 0;

	char *cmd = string_format("ssh -o StrictHostKeyChecking=no -i %s.pem ec2-user@%s ls",c->keypair_name,ip_address);

	int i;
	for(i=0;i<100;i++) {
		debug(D_REMOTE,"testing for ssh ready: %s",cmd);
		if(system(cmd)==0) {
			result = 1;
			break;
		}
		sleep(1);
	}

	free(cmd);
	return result;
}

int put_file( struct aws_config *c, const char *ip_address, const char *localname, const char *remotename )
{
	char *cmd = string_format("scp -o StrictHostKeyChecking=no -i %s.pem \"%s\" \"ec2-user@%s:%s\"",c->keypair_name,localname,ip_address,remotename);
	debug(D_BATCH,"put_file: %s\n",cmd);
	int result = system(cmd);
	free(cmd);
	return result;
}

int get_file( struct aws_config *c, const char *ip_address, const char *localname, const char *remotename )
{
	char *cmd = string_format("scp -o StrictHostKeyChecking=no -i %s.pem \"ec2-user@%s:%s\" \"%s\"",c->keypair_name,ip_address,remotename,localname);
	debug(D_BATCH,"get_file: %s\n",cmd);
	int result = system(cmd);
	free(cmd);
	return result;
}

int run_task( struct aws_config *c, const char *ip_address, const char *command )
{
	char *cmd = string_format("ssh -o StrictHostKeyChecking=no -i %s.pem \"ec2-user@%s\" \"%s\"",c->keypair_name,ip_address,command);
	debug(D_BATCH,"run_task: %s\n",cmd);
	int result = system(cmd);
	free(cmd);
	return result;
}

const char * get_instance_property( struct jx *j, const char *name )
{
	debug(D_BATCH,"DEBUG: %s",jx_print_string(j));

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

const char * get_instance_state_name( struct jx *j )
{
	debug(D_BATCH,"DEBUG: %s",jx_print_string(j));

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

int batch_job_amazon_subprocess( const char *cmd, const char *extra_input_files, const char *extra_output_files, struct jx *envlist )
{
	static struct aws_config * aws_config = 0;

	// XXX load this from json configuration.

	if(!aws_config) aws_config = aws_config_load("amazon.json");

	char *instance_id = aws_create_instance(aws_config);
	if(!instance_id) {
		debug(D_BATCH,"aws_create_instance failed");
		sleep(5);
		return 1;
	}

	char *ip_address = 0;

	while(1) {
		sleep(5);

		debug(D_BATCH,"getting instance state...");
		struct jx *j = aws_describe_instance(aws_config,instance_id);
		if(!j) {
			debug(D_BATCH,"unable to get instance state");
			continue;
		}
	
		const char * state = get_instance_state_name(j);
		if(!state) {
			debug(D_BATCH,"state is not set");
			continue;
		} else if(!strcmp(state,"pending")) {
			debug(D_BATCH,"state is pending, check again later");
			continue;
		} else if(!strcmp(state,"running")) {
			debug(D_BATCH,"state is running, check for ip address");
			const char *i = get_instance_property(j,"PublicIpAddress");
			if(i) {
				debug(D_BATCH,"found ip address %s",i);
				ip_address = strdup(i);
				break;
			} else {
				debug(D_BATCH,"strange, ip address is not set!");
				continue;
			}
		} else {
			debug(D_BATCH,"state is %s, which is unexpected, bailing out",state);
			return 1;
		}
	}

	wait_for_ssh_ready(aws_config,ip_address);

	// XXX this needs to be unique
	char *runscript = string_format("_makeflow_task_script_%d",rand());

	create_script(runscript,cmd,envlist);

	char *filelist = strdup(extra_input_files);
	char *f = strtok(filelist,",");
	while(f) {
		// XXX need to handle remotename
		put_file(aws_config,ip_address,f,f);
		f = strtok(0,",");
	}
	free(filelist);

	put_file(aws_config,ip_address,runscript,"_makeflow_task_script");

	int task_result = run_task(aws_config,ip_address,"./_makeflow_task_script");

	filelist = strdup(extra_output_files);
	f = strtok(filelist,",");
	while(f) {
		// XXX need to handle remotename
		get_file(aws_config,ip_address,f,f);
		f = strtok(0,",");
	}
	free(filelist);

	aws_terminate_instance(aws_config,instance_id);
	free(instance_id);

	return task_result;
}

static batch_job_id_t batch_job_amazon_submit(struct batch_queue *q, const char *cmd, const char *extra_input_files, const char *extra_output_files, struct jx *envlist, const struct rmsummary *resources)
{
	fflush(NULL);

	if(!q->job_table) q->job_table = itable_create(0);

	batch_job_id_t jobid = fork();
	if(jobid > 0) {
		debug(D_BATCH, "started process %" PRIbjid ": %s", jobid, cmd);
		struct batch_job_info *info = malloc(sizeof(*info));
		memset(info, 0, sizeof(*info));
		info->submitted = time(0);
		info->started = time(0);
		itable_insert(q->job_table, jobid, info);
		return jobid;
	} else if(jobid < 0) {
		debug(D_BATCH, "couldn't create new process: %s\n", strerror(errno));
		return -1;
	} else {
		_exit(batch_job_amazon_subprocess(cmd,extra_input_files,extra_output_files,envlist));
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

		} else if(errno == ESRCH || errno == ECHILD) {
			return 0;
		}

		if(stoptime != 0 && time(0) >= stoptime)
			return -1;
	}
}

static int batch_job_amazon_remove (struct batch_queue *q, batch_job_id_t jobid)
{
	int status;

	if(kill(jobid, SIGTERM) == 0) {
		if(!itable_lookup(q->job_table, jobid)) {
			debug(D_BATCH, "runaway process %" PRIbjid "?\n", jobid);
			return 0;
		} else {
			debug(D_BATCH, "waiting for process %" PRIbjid, jobid);
			waitpid(jobid, &status, 0);
			return 1;
		}
	} else {
		debug(D_BATCH, "could not signal process %" PRIbjid ": %s\n", jobid, strerror(errno));
		return 0;
	}

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
