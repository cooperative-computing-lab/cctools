#include "batch_job.h"
#include "batch_job_internal.h"
#include "debug.h"
#include "path.h"
#include "stringtools.h"
#include "process.h"
#include "xxmalloc.h"

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>

#include <sys/stat.h>

static char * cluster_name = NULL;
static char * cluster_submit_cmd = NULL;
static char * cluster_remove_cmd = NULL;
static char * cluster_options = NULL;


static int setup_batch_wrapper(struct batch_queue *q, const char *sysname)
{
	char *wrapperfile = string_format("%s.wrapper", sysname);

	if(access(wrapperfile, R_OK | X_OK) == 0)
		return 0;

	FILE *file = fopen(wrapperfile, "w");
	if(!file) {
		free(wrapperfile);
		return -1;
	}

	fprintf(file, "#!/bin/sh\n");
	if(q->type == BATCH_QUEUE_TYPE_MOAB || q->type == BATCH_QUEUE_TYPE_TORQUE) {
		fprintf(file, "CMD=${BATCH_JOB_COMMAND}\n");
		fprintf(file, "[ -n \"${PBS_JOBID}\" ] && JOB_ID=`echo ${PBS_JOBID} | cut -d . -f 1`\n");
	} else {
		fprintf(file, "CMD=$@\n");
	}

	fprintf(file, "logfile=%s.status.${JOB_ID}\n", sysname);
	fprintf(file, "starttime=`date +%%s`\n");
	fprintf(file, "cat > $logfile <<EOF\n");
	fprintf(file, "start $starttime\n");
	fprintf(file, "EOF\n\n");
	fprintf(file, "eval \"$CMD\"\n\n");
	fprintf(file, "status=$?\n");
	fprintf(file, "stoptime=`date +%%s`\n");
	fprintf(file, "cat >> $logfile <<EOF\n");
	fprintf(file, "stop $status $stoptime\n");
	fprintf(file, "EOF\n");
	fclose(file);

	chmod(wrapperfile, 0755);

	free(wrapperfile);

	return 0;
}

static batch_job_id_t batch_job_cluster_submit_simple (struct batch_queue * q, const char *cmd, const char *extra_input_files, const char *extra_output_files)
{
	batch_job_id_t jobid;
	struct batch_job_info *info;
	const char *options = hash_table_lookup(q->options, "batch-options");

	if(setup_batch_wrapper(q, cluster_name) < 0)
		return -1;

	char *name = xxstrdup(cmd);
	{
		char *s = strchr(name, ' ');
		if(s)
			*s = 0;
	}
	char *command = NULL;
	switch(q->type) {
		case BATCH_QUEUE_TYPE_TORQUE:
			command = string_format("%s %s '%s' %s %s.wrapper", cluster_submit_cmd, cluster_options, path_basename(name), options ? options : "", cluster_name);
			break;
		case BATCH_QUEUE_TYPE_SGE:
		case BATCH_QUEUE_TYPE_MOAB:
		case BATCH_QUEUE_TYPE_CLUSTER:
		default:
			command = string_format("%s %s '%s' %s %s.wrapper \"%s\"", cluster_submit_cmd, cluster_options, path_basename(name), options ? options : "", cluster_name, cmd);
			break;
	}
	free(name);

	debug(D_BATCH, "%s", command);

	setenv("BATCH_JOB_COMMAND", cmd, 1);

	FILE *file = popen(command, "r");
	free(command);
	if(!file) {
		debug(D_BATCH, "couldn't submit job: %s", strerror(errno));
		return -1;
	}

	char line[BATCH_JOB_LINE_MAX] = "";
	while(fgets(line, sizeof(line), file)) {
		if(sscanf(line, "Your job %" SCNbjid, &jobid) == 1 || sscanf(line, "%" SCNbjid, &jobid) == 1) {
			debug(D_BATCH, "job %" PRIbjid " submitted", jobid);
			pclose(file);
			info = malloc(sizeof(*info));
			memset(info, 0, sizeof(*info));
			info->submitted = time(0);
			itable_insert(q->job_table, jobid, info);
			return jobid;
		}
	}

	if(strlen(line)) {
		debug(D_NOTICE, "job submission failed: %s", line);
	} else {
		debug(D_NOTICE, "job submission failed: no output from %s", cluster_name);
	}
	pclose(file);
	return -1;
}

static batch_job_id_t batch_job_cluster_submit (struct batch_queue * q, const char *cmd, const char *args, const char *infile, const char *outfile, const char *errfile, const char *extra_input_files, const char *extra_output_files)
{
	char *command = string_format("%s %s", cmd, args);
	if (infile) {
		char *new = string_format("%s <%s", command, infile);
		free(command);
		command = new;
	}
	if (outfile) {
		char *new = string_format("%s >%s", command, outfile);
		free(command);
		command = new;
	}
	if (errfile) {
		char *new = string_format("%s 2>%s", command, errfile);
		free(command);
		command = new;
	}

	batch_job_id_t status = batch_job_cluster_submit_simple(q, command, extra_input_files, extra_output_files);
	free(command);
	return status;
}

static batch_job_id_t batch_job_cluster_wait (struct batch_queue * q, struct batch_job_info * info_out, time_t stoptime)
{
	struct batch_job_info *info;
	batch_job_id_t jobid;
	int t, c;

	while(1) {
		UINT64_T ujobid;
		itable_firstkey(q->job_table);
		while(itable_nextkey(q->job_table, &ujobid, (void **) &info)) {
			jobid = ujobid;
			char *statusfile = string_format("%s.status.%" PRIbjid, cluster_name, jobid);
			FILE *file = fopen(statusfile, "r");
			if(file) {
				char line[BATCH_JOB_LINE_MAX];
				while(fgets(line, sizeof(line), file)) {
					if(sscanf(line, "start %d", &t)) {
						info->started = t;
					} else if(sscanf(line, "stop %d %d", &c, &t) == 2) {
						debug(D_BATCH, "job %" PRIbjid " complete", jobid);
						if(!info->started)
							info->started = t;
						info->finished = t;
						info->exited_normally = 1;
						info->exit_code = c;
					}
				}
				fclose(file);

				if(info->finished != 0) {
					unlink(statusfile);
					info = itable_remove(q->job_table, jobid);
					*info_out = *info;
					free(info);
					free(statusfile);
					return jobid;
				}
			} else {
				debug(D_BATCH, "could not open status file \"%s\"", statusfile);
			}

			free(statusfile);
		}

		if(itable_size(q->job_table) <= 0)
			return 0;

		if(stoptime != 0 && time(0) >= stoptime)
			return -1;

		if(process_pending())
			return -1;

		sleep(1);
	}

	return -1;
}

static int batch_job_cluster_remove (struct batch_queue *q, batch_job_id_t jobid)
{
	struct batch_job_info *info;

	info = itable_lookup(q->job_table, jobid);
	if(!info)
		return 0;

	if(!info->started)
		info->started = time(0);

	info->finished = time(0);
	info->exited_normally = 0;
	info->exit_signal = 1;

	char *command = string_format("%s %" PRIbjid, cluster_remove_cmd, jobid);
	system(command);
	free(command);

	return 1;
}

static int batch_queue_cluster_create (struct batch_queue *q)
{
	if(cluster_name)
		free(cluster_name);
	if(cluster_submit_cmd)
		free(cluster_submit_cmd);
	if(cluster_remove_cmd)
		free(cluster_remove_cmd);
	if(cluster_options)
		free(cluster_options);

	cluster_name = cluster_submit_cmd = cluster_remove_cmd = cluster_options = NULL;

	switch(q->type) {
		case BATCH_QUEUE_TYPE_SGE:
			cluster_name = strdup("sge");
			cluster_submit_cmd = strdup("qsub");
			cluster_remove_cmd = strdup("qdel");
			cluster_options = strdup("-cwd -o /dev/null -j y -N");
			break;
		case BATCH_QUEUE_TYPE_MOAB:
			cluster_name = strdup("moab");
			cluster_submit_cmd = strdup("msub");
			cluster_remove_cmd = strdup("mdel");
			cluster_options = strdup("-d . -o /dev/null -v BATCH_JOB_COMMAND -j oe -N");
			break;
		case BATCH_QUEUE_TYPE_TORQUE:
			cluster_name = strdup("torque");
			cluster_submit_cmd = strdup("qsub");
			cluster_remove_cmd = strdup("qdel");
			cluster_options = strdup("-d . -o /dev/null -v BATCH_JOB_COMMAND -j oe -N");
			break;
		case BATCH_QUEUE_TYPE_CLUSTER:
			cluster_name = getenv("BATCH_QUEUE_CLUSTER_NAME");
			cluster_submit_cmd = getenv("BATCH_QUEUE_CLUSTER_SUBMIT_COMMAND");
			cluster_remove_cmd = getenv("BATCH_QUEUE_CLUSTER_REMOVE_COMMAND");
			cluster_options = getenv("BATCH_QUEUE_CLUSTER_SUBMIT_OPTIONS");
			break;
		default:
			debug(D_BATCH, "Invalid cluster type: %s\n", batch_queue_type_to_string(q->type));
			return -1;
	}

	if(cluster_name && cluster_submit_cmd && cluster_remove_cmd && cluster_options)
		return 0;

	if(!cluster_name)
		debug(D_NOTICE, "Environment variable BATCH_QUEUE_CLUSTER_NAME unset\n");
	if(!cluster_submit_cmd)
		debug(D_NOTICE, "Environment variable BATCH_QUEUE_CLUSTER_SUBMIT_COMMAND unset\n");
	if(!cluster_remove_cmd)
		debug(D_NOTICE, "Environment variable BATCH_QUEUE_CLUSTER_REMOVE_COMMAND unset\n");
	if(!cluster_options)
		debug(D_NOTICE, "Environment variable BATCH_QUEUE_CLUSTER_SUBMIT_OPTIONS unset\n");

	return -1;
}

batch_queue_stub_free(cluster);
batch_queue_stub_port(cluster);
batch_queue_stub_option_update(cluster);

batch_fs_stub_chdir(cluster);
batch_fs_stub_getcwd(cluster);
batch_fs_stub_mkdir(cluster);
batch_fs_stub_putfile(cluster);
batch_fs_stub_stat(cluster);
batch_fs_stub_unlink(cluster);

const struct batch_queue_module batch_queue_cluster = {
	BATCH_QUEUE_TYPE_CLUSTER,
	"cluster",

	batch_queue_cluster_create,
	batch_queue_cluster_free,
	batch_queue_cluster_port,
	batch_queue_cluster_option_update,

	{
		batch_job_cluster_submit,
		batch_job_cluster_submit_simple,
		batch_job_cluster_wait,
		batch_job_cluster_remove,
	},

	{
		batch_fs_cluster_chdir,
		batch_fs_cluster_getcwd,
		batch_fs_cluster_mkdir,
		batch_fs_cluster_putfile,
		batch_fs_cluster_stat,
		batch_fs_cluster_unlink,
	},
};

const struct batch_queue_module batch_queue_moab = {
	BATCH_QUEUE_TYPE_MOAB,
	"moab",

	batch_queue_cluster_create,
	batch_queue_cluster_free,
	batch_queue_cluster_port,
	batch_queue_cluster_option_update,

	{
		batch_job_cluster_submit,
		batch_job_cluster_submit_simple,
		batch_job_cluster_wait,
		batch_job_cluster_remove,
	},

	{
		batch_fs_cluster_chdir,
		batch_fs_cluster_getcwd,
		batch_fs_cluster_mkdir,
		batch_fs_cluster_putfile,
		batch_fs_cluster_stat,
		batch_fs_cluster_unlink,
	},
};

const struct batch_queue_module batch_queue_sge = {
	BATCH_QUEUE_TYPE_SGE,
	"sge",

	batch_queue_cluster_create,
	batch_queue_cluster_free,
	batch_queue_cluster_port,
	batch_queue_cluster_option_update,

	{
		batch_job_cluster_submit,
		batch_job_cluster_submit_simple,
		batch_job_cluster_wait,
		batch_job_cluster_remove,
	},

	{
		batch_fs_cluster_chdir,
		batch_fs_cluster_getcwd,
		batch_fs_cluster_mkdir,
		batch_fs_cluster_putfile,
		batch_fs_cluster_stat,
		batch_fs_cluster_unlink,
	},
};

const struct batch_queue_module batch_queue_torque = {
	BATCH_QUEUE_TYPE_TORQUE,
	"torque",

	batch_queue_cluster_create,
	batch_queue_cluster_free,
	batch_queue_cluster_port,
	batch_queue_cluster_option_update,

	{
		batch_job_cluster_submit,
		batch_job_cluster_submit_simple,
		batch_job_cluster_wait,
		batch_job_cluster_remove,
	},

	{
		batch_fs_cluster_chdir,
		batch_fs_cluster_getcwd,
		batch_fs_cluster_mkdir,
		batch_fs_cluster_putfile,
		batch_fs_cluster_stat,
		batch_fs_cluster_unlink,
	},
};

/* vim: set noexpandtab tabstop=4: */
