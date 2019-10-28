/*
Copyright (C) 2017- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "batch_job.h"
#include "batch_job_internal.h"
#include "debug.h"
#include "path.h"
#include "stringtools.h"
#include "process.h"
#include "xxmalloc.h"
#include "jx.h"
#include "jx_match.h"

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <ctype.h>

#include <sys/stat.h>

static char * cluster_name = NULL;
static char * cluster_submit_cmd = NULL;
static char * cluster_remove_cmd = NULL;
static char * cluster_options = NULL;
static char * cluster_jobname_var = NULL;

int batch_job_verbose_jobnames = 0;

/*
Principle of operation:
Each batch job that we submit uses a wrapper file.
The wrapper file is kept the same for each job, so
that we do not unduly pollute the filesystem.

The command line to run is passed as the environment
variable BATCH_JOB_COMMAND, because not all batch systems
support precise passing of command line arguments.

The wrapper then writes a status file, which indicates the
starting and ending time of the task to a known log file,
which batch_job_cluster_wait then periodically polls to observe completion.
While this is not particularly elegant, there is no widely
portable API for querying the state of a batch job in PBS-like systems.
This method is simple, cheap, and reasonably effective.
*/

/*
setup_batch_wrapper creates the wrapper file if necessary,
returning true on success and false on failure.
*/

static int setup_batch_wrapper(struct batch_queue *q, const char *sysname )
{
	char wrapperfile[PATH_MAX];
	snprintf(wrapperfile, PATH_MAX, "%s.wrapper", sysname);

	if(access(wrapperfile, R_OK | X_OK) == 0) return 1;

	FILE *file = fopen(wrapperfile, "w");
	if(!file) {
		return 0;
	}
	fchmod(fileno(file), 0755);

	char *path = getenv("PWD");

	fprintf(file, "#!/bin/sh\n");
	fprintf(file, "#$ -S /bin/sh\n");

	if(q->type == BATCH_QUEUE_TYPE_SLURM){
		fprintf(file, "[ -n \"${SLURM_JOB_ID}\" ] && JOB_ID=`echo ${SLURM_JOB_ID} | cut -d . -f 1`\n");
	} else {
		// Some systems set PBS_JOBID, some set JOBID.
		fprintf(file, "[ -n \"${PBS_JOBID}\" ] && JOB_ID=`echo ${PBS_JOBID} | cut -d . -f 1`\n");
	}

	if(q->type == BATCH_QUEUE_TYPE_TORQUE || q->type == BATCH_QUEUE_TYPE_PBS){
		fprintf(file, "cd %s\n", path);
	}

	// Each job writes out to its own log file.
	fprintf(file, "logfile=%s.status.${JOB_ID}\n", sysname);
	fprintf(file, "starttime=`date +%%s`\n");
	fprintf(file, "cat > $logfile <<EOF\n");
	fprintf(file, "start $starttime\n");
	fprintf(file, "EOF\n\n");
	// The command to run is taken from the environment.
	fprintf(file, "eval \"$BATCH_JOB_COMMAND\"\n\n");

	// When done, write the status and time to the logfile.
	fprintf(file, "status=$?\n");
	fprintf(file, "stoptime=`date +%%s`\n");
	fprintf(file, "cat >> $logfile <<EOF\n");
	fprintf(file, "stop $status $stoptime\n");
	fprintf(file, "EOF\n");
	fclose(file);

	return 1;
}

static char *cluster_set_resource_string(struct batch_queue *q, const struct rmsummary *resources)
{
	char *cluster_resources = NULL;
	const char *ignore_mem = hash_table_lookup(q->options, "ignore-mem-spec");
	const char *mem_type = hash_table_lookup(q->options, "mem-type");

	if(q->type == BATCH_QUEUE_TYPE_TORQUE || q->type == BATCH_QUEUE_TYPE_PBS){
		char *mem = string_format(",mem=%" PRId64 "mb", resources->memory);
		char *disk = string_format(",file=%" PRId64 "mb", resources->disk);
		cluster_resources = string_format(" -l nodes=1:ppn=%" PRId64 "%s%s ", 
			resources->cores>0 ? resources->cores : 1,
			resources->memory>0 ? mem : "",
			resources->disk>0 ? disk  : "");
		free(mem);
		free(disk);
	} else if(q->type == BATCH_QUEUE_TYPE_SLURM){
		char *mem = NULL;
		if(!strcmp("no", ignore_mem)){
			mem = string_format(" --mem=%" PRId64 "M", resources->memory);
		}
		// Currently leaving out tmp as SLURM assumes a shared FS and tmp may be limiting
		// char *disk = string_format(" --tmp=%" PRId64 "M", resources->disk);
		cluster_resources = string_format(" -N 1 -n 1 -c %" PRId64 "%s ", 
			resources->cores>0 ? resources->cores : 1,
			(resources->memory>0 && mem) ? mem : "");
		free(mem);
	} else if(q->type == BATCH_QUEUE_TYPE_SGE){
		char *mem = NULL;
		mem = string_format(" -l %s=%" PRId64 "M", (mem_type ? mem_type : "h_vmem"), resources->memory);
		// SGE assumes shared FS, ignoring possible disk specification
		cluster_resources = string_format(" -pe smp %" PRId64 "%s ", 
			resources->cores>0 ? resources->cores : 1,
			(resources->memory>0 && mem) ? mem : "");
		free(mem);

	}
	const char *safe_mode = hash_table_lookup(q->options, "safe-submit-mode");
	if(safe_mode && !strcmp("yes", safe_mode)) {
		free(cluster_resources);
		cluster_resources = NULL;
	}
	if(!cluster_resources) {
		cluster_resources = xxstrdup("");
	}
	return cluster_resources;
}

static batch_job_id_t batch_job_cluster_submit (struct batch_queue * q, const char *cmd, const char *extra_input_files, const char *extra_output_files, struct jx *envlist, const struct rmsummary *resources )
{
	batch_job_id_t jobid;
	struct batch_job_info *info;
	const char *options = hash_table_lookup(q->options, "batch-options");

	if(!setup_batch_wrapper(q, cluster_name)) {
		debug(D_NOTICE|D_BATCH,"couldn't setup wrapper file: %s",strerror(errno));
		return -1;
	}

	char *cluster_resources = cluster_set_resource_string(q, resources);

	/*
	Experiment shows that passing environment variables
	through the command-line doesn't work, due to multiple
	levels of quote interpretation.  So, we export all
	variables into the environment, and rely upon the -V
	option to load the environment into the job.
	*/

	if(envlist) {
		jx_export(envlist);
	}

	/*
	Pass the command to run through the environment as well.
	*/
	setenv("BATCH_JOB_COMMAND", cmd, 1);

	/*
	Re the PBS qsub manpage, the -N name must start with a letter and be <= 15 characters long.
	Unfortunately, work_queue_worker hits this limit.

	Previously, we used the beginning of the command for this.
	The CRC had a wrapper script around qsub to help fix submission issues,
	but their wrapper could mis-identify the script and corrupt other files if the submit name matched an existing file.
	It mistook the the node command for the submission script,
	and tried to adjust the line endings and add a newline.
	The script in question happened to be a self-extracting script,
	so the fixups corrupted the bundled tarball.
	To make sure we don't run into issues with sloppy command line fixups,
	we just use an incrementing counter for naming submissions.

	If there are more than 65,535 jobs submitted at once,
	thei counter could roll over.
	This shouldn't be an issue.

	TODO change this to the nodeid during a refactor to batch_task
	*/
	static uint16_t submit_id = 0;
	char *jobname;

	if (batch_job_verbose_jobnames) {
		char *firstword = strdup(cmd);

		char *end = strchr(firstword, ' ');
		if (end) *end = 0;

		jobname = strdup(string_front(path_basename(firstword), 15));
		if (jobname[0] != 0 && !isalpha(jobname[0])) jobname[0] = 'X';

		free(firstword);
	} else {
		jobname = string_format("makeflow%" PRIu16, submit_id);
	}
	submit_id++;

	char *command = string_format("%s %s %s %s %s %s %s.wrapper",
		cluster_submit_cmd,
		cluster_resources,
		cluster_options,
		cluster_jobname_var,
		jobname,
		options ? options : "",
		cluster_name);

	free(jobname);
	free(cluster_resources);
	debug(D_BATCH, "%s", command);

	FILE *file = popen(command, "r");
	free(command);
	if(!file) {
		debug(D_BATCH, "couldn't submit job: %s", strerror(errno));
		return -1;
	}

	char line[BATCH_JOB_LINE_MAX] = "";
	while(fgets(line, sizeof(line), file)) {
		if(sscanf(line, "Your job %" SCNbjid, &jobid) == 1
		|| sscanf(line, "Submitted batch job %" SCNbjid, &jobid) == 1
		|| sscanf(line, "%" SCNbjid, &jobid) == 1 ) {
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
	if(cluster_jobname_var)
		free(cluster_jobname_var);

	cluster_name = cluster_submit_cmd = cluster_remove_cmd = cluster_options = cluster_jobname_var = NULL;

	switch(q->type) {
		case BATCH_QUEUE_TYPE_SGE:
			cluster_name = strdup("sge");
			cluster_submit_cmd = strdup("qsub");
			cluster_remove_cmd = strdup("qdel");
			cluster_options = strdup("-cwd -o /dev/null -j y -V");
			cluster_jobname_var = strdup("-N");
			break;
		case BATCH_QUEUE_TYPE_MOAB:
			cluster_name = strdup("moab");
			cluster_submit_cmd = strdup("msub");
			cluster_remove_cmd = strdup("mdel");
			cluster_options = strdup("-d . -o /dev/null -j oe -V");
			cluster_jobname_var = strdup("-N");
			break;
		case BATCH_QUEUE_TYPE_PBS:
			cluster_name = strdup("pbs");
			cluster_submit_cmd = strdup("qsub");
			cluster_remove_cmd = strdup("qdel");
			cluster_options = strdup("-o /dev/null -j oe -V");
			cluster_jobname_var = strdup("-N");
			break;
		case BATCH_QUEUE_TYPE_TORQUE:
			cluster_name = strdup("torque");
			cluster_submit_cmd = strdup("qsub");
			cluster_remove_cmd = strdup("qdel");
			cluster_options = strdup("-o /dev/null -j oe -V");
			cluster_jobname_var = strdup("-N");
			break;
		case BATCH_QUEUE_TYPE_SLURM:
			cluster_name = strdup("slurm");
			cluster_submit_cmd = strdup("sbatch");
			cluster_remove_cmd = strdup("scancel");
			cluster_options = strdup("-D . -o /dev/null -e /dev/null --export=ALL");
			cluster_jobname_var = strdup("-J");
			break;
		case BATCH_QUEUE_TYPE_CLUSTER:
			cluster_name = getenv("BATCH_QUEUE_CLUSTER_NAME");
			cluster_submit_cmd = getenv("BATCH_QUEUE_CLUSTER_SUBMIT_COMMAND");
			cluster_remove_cmd = getenv("BATCH_QUEUE_CLUSTER_REMOVE_COMMAND");
			cluster_options = getenv("BATCH_QUEUE_CLUSTER_SUBMIT_OPTIONS");
			cluster_jobname_var = getenv("BATCH_QUEUE_CLUSTER_SUBMIT_JOBNAME_VAR");
			break;
		default:
			debug(D_BATCH, "Invalid cluster type: %s\n", batch_queue_type_to_string(q->type));
			return -1;
	}

	if(cluster_name && cluster_submit_cmd && cluster_remove_cmd && cluster_options && cluster_jobname_var)
		return 0;

	if(!cluster_name)
		debug(D_NOTICE, "Environment variable BATCH_QUEUE_CLUSTER_NAME unset\n");
	if(!cluster_submit_cmd)
		debug(D_NOTICE, "Environment variable BATCH_QUEUE_CLUSTER_SUBMIT_COMMAND unset\n");
	if(!cluster_remove_cmd)
		debug(D_NOTICE, "Environment variable BATCH_QUEUE_CLUSTER_REMOVE_COMMAND unset\n");
	if(!cluster_options)
		debug(D_NOTICE, "Environment variable BATCH_QUEUE_CLUSTER_SUBMIT_OPTIONS unset\n");
	if(!cluster_jobname_var)
		debug(D_NOTICE, "Environment variable BATCH_QUEUE_CLUSTER_SUBMIT_JOBNAME_VAR unset\n");

	return -1;
}

batch_queue_stub_free(cluster);
batch_queue_stub_port(cluster);
batch_queue_stub_option_update(cluster);

batch_fs_stub_chdir(cluster);
batch_fs_stub_getcwd(cluster);
batch_fs_stub_mkdir(cluster);
batch_fs_stub_putfile(cluster);
batch_fs_stub_rename(cluster);
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
		batch_job_cluster_wait,
		batch_job_cluster_remove,
	},

	{
		batch_fs_cluster_chdir,
		batch_fs_cluster_getcwd,
		batch_fs_cluster_mkdir,
		batch_fs_cluster_putfile,
		batch_fs_cluster_rename,
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
		batch_job_cluster_wait,
		batch_job_cluster_remove,
	},

	{
		batch_fs_cluster_chdir,
		batch_fs_cluster_getcwd,
		batch_fs_cluster_mkdir,
		batch_fs_cluster_putfile,
		batch_fs_cluster_rename,
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
		batch_job_cluster_wait,
		batch_job_cluster_remove,
	},

	{
		batch_fs_cluster_chdir,
		batch_fs_cluster_getcwd,
		batch_fs_cluster_mkdir,
		batch_fs_cluster_putfile,
		batch_fs_cluster_rename,
		batch_fs_cluster_stat,
		batch_fs_cluster_unlink,
	},
};

const struct batch_queue_module batch_queue_pbs = {
	BATCH_QUEUE_TYPE_PBS,
	"pbs",

	batch_queue_cluster_create,
	batch_queue_cluster_free,
	batch_queue_cluster_port,
	batch_queue_cluster_option_update,

	{
		batch_job_cluster_submit,
		batch_job_cluster_wait,
		batch_job_cluster_remove,
	},

	{
		batch_fs_cluster_chdir,
		batch_fs_cluster_getcwd,
		batch_fs_cluster_mkdir,
		batch_fs_cluster_putfile,
		batch_fs_cluster_rename,
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
		batch_job_cluster_wait,
		batch_job_cluster_remove,
	},

	{
		batch_fs_cluster_chdir,
		batch_fs_cluster_getcwd,
		batch_fs_cluster_mkdir,
		batch_fs_cluster_putfile,
		batch_fs_cluster_rename,
		batch_fs_cluster_stat,
		batch_fs_cluster_unlink,
	},
};

const struct batch_queue_module batch_queue_slurm = {
	BATCH_QUEUE_TYPE_SLURM,
	"slurm",

	batch_queue_cluster_create,
	batch_queue_cluster_free,
	batch_queue_cluster_port,
	batch_queue_cluster_option_update,

	{
		batch_job_cluster_submit,
		batch_job_cluster_wait,
		batch_job_cluster_remove,
	},

	{
		batch_fs_cluster_chdir,
		batch_fs_cluster_getcwd,
		batch_fs_cluster_mkdir,
		batch_fs_cluster_putfile,
		batch_fs_cluster_rename,
		batch_fs_cluster_stat,
		batch_fs_cluster_unlink,
	},
};

/* vim: set noexpandtab tabstop=4: */
