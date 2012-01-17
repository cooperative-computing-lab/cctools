#include "batch_job.h"
#include "batch_job_internal.h"
#include "debug.h"
#include "stringtools.h"
#include "process.h"

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>

#include <sys/stat.h>

static char * cluster_name = NULL;
static char * cluster_submit_cmd = NULL;
static char * cluster_remove_cmd = NULL;
static char * cluster_options = NULL;

int batch_job_setup_cluster(struct batch_queue * q) {

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
			cluster_options = strdup("-d $CWD -o /dev/null -j oe -N");
			break;
		case BATCH_QUEUE_TYPE_CLUSTER:
			cluster_name = getenv("BATCH_QUEUE_CLUSTER_NAME");
			cluster_submit_cmd = getenv("BATCH_QUEUE_CLUSTER_SUBMIT_COMMAND");
			cluster_remove_cmd = getenv("BATCH_QUEUE_CLUSTER_REMOVE_COMMAND");
			cluster_options = getenv("BATCH_QUEUE_CLUSTER_SUBMIT_OPTIONS");
			break;
		default:
			debug(D_DEBUG, "Invalid cluster type: %s\n", batch_queue_type_to_string(q->type));
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

static int setup_batch_wrapper(const char *sysname)
{
	FILE *file;
	char wrapperfile[BATCH_JOB_LINE_MAX];
	
	sprintf(wrapperfile, "%s.wrapper", sysname);

	if(access(wrapperfile, R_OK | X_OK) == 0)
		return 0;

	file = fopen(wrapperfile, "w");
	if(!file)
		return -1;

	fprintf(file, "#!/bin/sh\n");
	fprintf(file, "logfile=%s.status.${JOB_ID}\n", sysname);
	fprintf(file, "starttime=`date +%%s`\n");
	fprintf(file, "cat > $logfile <<EOF\n");
	fprintf(file, "start $starttime\n");
	fprintf(file, "EOF\n\n");
	fprintf(file, "eval \"$@\"\n\n");
	fprintf(file, "status=$?\n");
	fprintf(file, "stoptime=`date +%%s`\n");
	fprintf(file, "cat >> $logfile <<EOF\n");
	fprintf(file, "stop $status $stoptime\n");
	fprintf(file, "EOF\n");
	fclose(file);

	chmod(wrapperfile, 0755);

	return 0;
}


batch_job_id_t batch_job_submit_simple_cluster(struct batch_queue * q, const char *cmd, const char *extra_input_files, const char *extra_output_files)
{
	char line[BATCH_JOB_LINE_MAX];
	char name[BATCH_JOB_LINE_MAX];
	batch_job_id_t jobid;
	struct batch_job_info *info;

	FILE *file;

	if(setup_batch_wrapper(cluster_name) < 0)
		return -1;

	strcpy(name, cmd);
	char *s = strchr(name, ' ');
	if(s)
		*s = 0;

	sprintf(line, "%s %s '%s' %s %s.wrapper \"%s\"", cluster_submit_cmd, cluster_options, string_basename(name), q->options_text ? q->options_text : "", cluster_name, cmd);

	debug(D_DEBUG, "%s", line);

	file = popen(line, "r");
	if(!file) {
		debug(D_DEBUG, "couldn't submit job: %s", strerror(errno));
		return -1;
	}

	line[0] = 0;
	while(fgets(line, sizeof(line), file)) {
		if(sscanf(line, "Your job %d", &jobid) == 1) {
			debug(D_DEBUG, "job %d submitted", jobid);
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

batch_job_id_t batch_job_submit_cluster(struct batch_queue * q, const char *cmd, const char *args, const char *infile, const char *outfile, const char *errfile, const char *extra_input_files, const char *extra_output_files)
{
	char command[BATCH_JOB_LINE_MAX];

	sprintf(command, "%s %s", cmd, args);

	if(infile)
		sprintf(&command[strlen(command)], " <%s", infile);
	if(outfile)
		sprintf(&command[strlen(command)], " >%s", outfile);
	if(errfile)
		sprintf(&command[strlen(command)], " 2>%s", errfile);

	return batch_job_submit_simple_cluster(q, command, extra_input_files, extra_output_files);
}

batch_job_id_t batch_job_wait_cluster(struct batch_queue * q, struct batch_job_info * info_out, time_t stoptime)
{
	struct batch_job_info *info;
	batch_job_id_t jobid;
	FILE *file;
	char statusfile[BATCH_JOB_LINE_MAX];
	char line[BATCH_JOB_LINE_MAX];
	int t, c;

	while(1) {
		UINT64_T ujobid;
		itable_firstkey(q->job_table);
		while(itable_nextkey(q->job_table, &ujobid, (void **) &info)) {
			jobid = ujobid;
			sprintf(statusfile, "%s.status.%d", cluster_name, jobid);
			file = fopen(statusfile, "r");
			if(file) {
				while(fgets(line, sizeof(line), file)) {
					if(sscanf(line, "start %d", &t)) {
						info->started = t;
					} else if(sscanf(line, "stop %d %d", &c, &t) == 2) {
						debug(D_DEBUG, "job %d complete", jobid);
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
					return jobid;
				}
			}
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

int batch_job_remove_cluster(struct batch_queue *q, batch_job_id_t jobid)
{
	char line[BATCH_JOB_LINE_MAX];
	struct batch_job_info *info;

	info = itable_lookup(q->job_table, jobid);
	if(!info)
		return 0;

	if(!info->started)
		info->started = time(0);

	info->finished = time(0);
	info->exited_normally = 0;
	info->exit_signal = 1;

	sprintf(line, "%s %d", cluster_remove_cmd, jobid);
	system(line);

	return 1;
}

