#include "batch_job.h"
#include "batch_job_internal.h"
#include "debug.h"
#include "process.h"
#include "stringtools.h"

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <fcntl.h>

#include <sys/stat.h>

static int setup_hadoop_wrapper(const char *wrapperfile, const char *cmd)
{
	FILE *file;

	file = fopen(wrapperfile, "w");
	if(!file)
		return -1;

	char *escaped_cmd = escape_shell_string(cmd);
	if (escaped_cmd == NULL) return -1;

	fprintf(file, "#!/bin/sh\n");
	fprintf(file, "cmd=%s\n", escaped_cmd);
	/* fprintf(file, "exec %s -- /bin/sh -c %s\n", getenv("HADOOP_PARROT_PATH"), escaped_cmd); */ /* random bash bug, look into later */
	fprintf(file, "exec %s -- /bin/sh <<EOF\n", getenv("HADOOP_PARROT_PATH"));
	fprintf(file, "$cmd\n");
	fprintf(file, "EOF\n");
	free(escaped_cmd);
	fclose(file);

	chmod(wrapperfile, 0755);

	return 0;
}

static char *get_hadoop_target_file(const char *input_files)
{
	static char result[BATCH_JOB_LINE_MAX];
	char *match_string = string_format("%s%%[^,]", getenv("HDFS_ROOT_DIR"));
	sscanf(input_files, match_string, result);
	free(match_string);
	return result;
}

batch_job_id_t batch_job_fork_hadoop(struct batch_queue *q, const char *cmd)
{
	int childpid;
	time_t finish_time;

	fflush(NULL);
	debug(D_HDFS, "forking hadoop_status_wrapper\n");
	if((childpid = fork()) < 0) {
		return -1;
	} else if(!childpid) {
		// CHILD
		char line[BATCH_JOB_LINE_MAX];
		FILE *cmd_pipe;

		cmd_pipe = popen(cmd, "r");
		if(!cmd_pipe) {
			debug(D_DEBUG, "hadoop_status_wrapper: couldn't submit job: %s", strerror(errno));
			_exit(-1 * time(0));
		}

		while(fgets(line, sizeof(line), cmd_pipe)) {
			if(!strncmp(line, "Streaming Command Failed!", 25)) {
				debug(D_HDFS, "hadoop_status_wrapper: job %d failed", (int)getpid());
				_exit(-1 * time(0));
			}
		}
		finish_time = time(0);

		if(pclose(cmd_pipe)) {
			_exit(-1 * finish_time);
		} else {
			_exit(finish_time);
		}

	} else {
		//PARENT
		struct batch_job_info *info;

		info = malloc(sizeof(*info));
		memset(info, 0, sizeof(*info));
		info->submitted = info->started = time(0);
		itable_insert(q->job_table, childpid, info);
		debug(D_DEBUG, "job %d submitted", childpid);
		return childpid;
	}
	return -1;
}

batch_job_id_t batch_job_submit_simple_hadoop(struct batch_queue *q, const char *cmd, const char *extra_input_files, const char *extra_output_files)
{
	char *target_file;

	target_file = get_hadoop_target_file(extra_input_files);
	if(!target_file) {
		debug(D_DEBUG, "couldn't create new hadoop task: no input file specified\n");
		return -1;
	} else
		debug(D_HDFS, "input file %s specified\n", target_file);

	setup_hadoop_wrapper("./hadoop.wrapper", cmd);

	char *command = string_format("%s/bin/hadoop jar %s/mapred/contrib/streaming/hadoop-*-streaming.jar -D mapreduce.job.maps=1 -D mapreduce.job.reduces=0 -D mapreduce.input.fileinputformat.split.minsize=10000000 -input %s -mapper ./hadoop.wrapper -file ./hadoop.wrapper -output '%s/job-%010d' 2>&1", getenv("HADOOP_HOME"), getenv("HADOOP_HOME"), target_file, getenv("HADOOP_USER_TMP"), (int)time(0));

	debug(D_HDFS, "%s", command);

	batch_job_id_t status = batch_job_fork_hadoop(q, command);
	free(command);
	return status;
}

batch_job_id_t batch_job_submit_hadoop(struct batch_queue *q, const char *cmd, const char *args, const char *infile, const char *outfile, const char *errfile, const char *extra_input_files, const char *extra_output_files)
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

	batch_job_id_t status = batch_job_submit_simple_hadoop(q, command, extra_input_files, extra_output_files);
	free(command);
	return status;
}


batch_job_id_t batch_job_wait_hadoop(struct batch_queue * q, struct batch_job_info * info_out, time_t stoptime)
{
	struct batch_job_info *info;
	batch_job_id_t jobid;

	while(1) {
		UINT64_T ujobid;
		itable_firstkey(q->job_table);
		while(itable_nextkey(q->job_table, &ujobid, (void **) &info)) {
			int status;
			jobid = ujobid;

			if(waitpid(jobid, &status, WNOHANG) > 0){
				if(WIFEXITED(status)) {
					int result = WEXITSTATUS(status);
					if(result > 0) {
						debug(D_DEBUG, "job %d success", jobid);
						info->finished = result;
						info->exited_normally = 1;
					} else {
						debug(D_DEBUG, "job %d failed", jobid);
						info->finished = -1 * result;
						info->exited_normally = 0;
					}
				} else {
					debug(D_DEBUG, "job %d failed", jobid);
					info->finished = time(0);
					info->exited_normally = 0;
				}
			}

			if(info->finished != 0) {
				info = itable_remove(q->job_table, jobid);
				memcpy(info_out, info, sizeof(*info));
				free(info);
				return jobid;
			}
		}

		if(itable_size(q->job_table) <= 0)
			return 0;

		if(stoptime != 0 && time(0) >= stoptime)
			return -1;

		sleep(1);
	}

	return -1;
}


int batch_job_remove_hadoop(struct batch_queue *q, batch_job_id_t jobid)
{
	return 0;
}

