#include "batch_job.h"
#include "batch_job_internal.h"
#include "debug.h"
#include "process.h"
#include "stringtools.h"
#include "xxmalloc.h"

#include <assert.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <unistd.h>

#include <fcntl.h>
#include <glob.h>
#include <sys/stat.h>

static const char WRAPPER_TEMPLATE[] = "./hadoop.wrapper.XXXXXX";

struct hadoop_job {
	FILE *status_file;
	pid_t child;
	struct batch_job_info info;
	char wrapper[sizeof(WRAPPER_TEMPLATE)+1];
};

static int setup_hadoop_wrapper (int fd, const char *cmd)
{
	FILE *file = fdopen(fd, "w");

	if (file == NULL)
		return -1;

	char *escaped_cmd = escape_shell_string(cmd);
	if (escaped_cmd == NULL) return -1;

	fprintf(file, "#!/bin/sh\n");
	fprintf(file, "cmd=%s\n", escaped_cmd);
	free(escaped_cmd);
	/* fprintf(file, "exec %s -- /bin/sh -c %s\n", getenv("HADOOP_PARROT_PATH"), escaped_cmd); */ /* random bash bug, look into later */
	fprintf(file, "exec %s -- /bin/sh <<EOF\n", getenv("HADOOP_PARROT_PATH"));
	fprintf(file, "$cmd\n");
	fprintf(file, "EOF\n");
	fclose(file);

	return 0;
}

batch_job_id_t batch_job_fork_hadoop(struct batch_queue *q, char *hadoop_streaming_command[], struct hadoop_job *job)
{
	int status_pipe[2];
	FILE *status_file;
	if (pipe(status_pipe) == -1)
		return -1;
	if (fcntl(status_pipe[0], F_SETFL, O_NONBLOCK) == -1 || ((status_file = fdopen(status_pipe[0], "r")) == NULL)) {
		close(status_pipe[0]);
		close(status_pipe[1]);
		return -1;
	}
	pid_t pid = fork();
	if (pid == 0) { /* CHILD */
		signal(SIGINT, SIG_IGN); /* if user interrupts parent, we'll kill process manually in batch_job_remove_hadoop */
		close(status_pipe[0]);
		close(STDIN_FILENO);
		if (open("/dev/null", O_RDONLY|O_NOCTTY) != STDIN_FILENO) _exit(1);
		if (dup2(status_pipe[1], STDOUT_FILENO) != STDOUT_FILENO) _exit(1);
		if (dup2(status_pipe[1], STDERR_FILENO) != STDERR_FILENO) _exit(1);
		if (execv(hadoop_streaming_command[0], hadoop_streaming_command) == -1)
			_exit(1);
		return -1;
	} else if (pid > 0) { /* PARENT */
		close(status_pipe[1]);
		job->child = pid;
		job->status_file = status_file;
		job->info.submitted = job->info.started = time(0);
		itable_insert(q->job_table, (int) job->child, job);
		debug(D_BATCH, "job %d submitted", (int) job->child);
		return job->child;
	} else {
		return -1;
	}
}

batch_job_id_t batch_job_submit_simple_hadoop(struct batch_queue *q, const char *cmd, const char *extra_input_files, const char *extra_output_files)
{
	int i;
	struct hadoop_job *job = xxmalloc(sizeof(struct hadoop_job));
	strcpy(job->wrapper, WRAPPER_TEMPLATE);
	int fd = mkstemp(job->wrapper);
	if (fd == -1)
		return -1;
	chmod(job->wrapper, 0644);
	if (setup_hadoop_wrapper(fd, cmd) == -1) { /* closes fd on success */
		close(fd);
		free(job);
		return -1;
	}
	char *hadoop_streaming_jar = string_format("%s/mapred/contrib/streaming/hadoop-*-streaming.jar", getenv("HADOOP_HOME"));
	glob_t g;
	if (glob(hadoop_streaming_jar, 0, NULL, &g) != 0 || g.gl_pathc != 1) {
		close(fd);
		free(job);
		debug(D_HDFS, "could not locate hadoop streaming jar using pattern `%s'.", hadoop_streaming_jar);
		return -1;
	}
	free(hadoop_streaming_jar);
	hadoop_streaming_jar = xxstrdup(g.gl_pathv[0]);
	globfree(&g);

	char *hadoop_streaming_command[] = {
		string_format("%s/bin/hadoop", getenv("HADOOP_HOME")),
		string_format("jar"),
		hadoop_streaming_jar,
		string_format("-Dmapreduce.job.reduces=0"),
		string_format("-input"),
		string_format("file:///dev/null"),
		string_format("-mapper"),
		string_format("%s", job->wrapper),
		string_format("-file"),
		string_format("%s", job->wrapper),
		string_format("-output"),
		string_format("%s/job-%010lu.%010d", getenv("HADOOP_USER_TMP"), (unsigned long) time(0), rand()),
		NULL,
	};

	batch_job_id_t status = batch_job_fork_hadoop(q, hadoop_streaming_command, job);
	for (i = 0; hadoop_streaming_command[i]; i++)
		free(hadoop_streaming_command[i]);
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


batch_job_id_t batch_job_wait_hadoop(struct batch_queue *q, struct batch_job_info *info_out, time_t stoptime)
{
	UINT64_T key;
	struct hadoop_job *job;

restart:
	itable_firstkey(q->job_table);
	while (itable_nextkey(q->job_table, &key, (void **) &job)) {
		/* read the status until empty */
		char line[BATCH_JOB_LINE_MAX];
		while (fgets(line, sizeof(line), job->status_file) > 0) {
			debug(D_BATCH, "hadoop-streaming job %d output: %s", (int) job->child, line);
			if (strstr(line, "Streaming Command Failed!")) {
				debug(D_HDFS, "hadoop-streaming job %d failed", job->child);
				/* job->info.exited_normally = 0; */
				/* job->info.finished = time(0); */
			}
		}

		int status;
		pid_t child = waitpid(job->child, &status, WNOHANG);
		if (child > 0) {
			assert(child == job->child);
			job->info.finished = time(0);
			if (WIFEXITED(status)) {
				int result = WEXITSTATUS(status);
				job->info.exit_code = result;
				job->info.exited_normally = 1;
				if (result == 0)
					debug(D_HDFS, "hadoop-streaming job %d exited successfully.", (int)job->child);
				else
					debug(D_HDFS, "hadoop-streaming job %d failed with exit status %d.", (int)job->child, result);
			} else if (WIFSIGNALED(status)) {
					int sig = WTERMSIG(status);
					debug(D_HDFS, "hadoop-streaming job %d terminated by signal %d.", (int)job->child, sig);
					job->info.exited_normally = 0;
					job->info.exit_signal = sig;
			}
			memcpy(info_out, &job->info, sizeof(job->info));
			fclose(job->status_file);
			unlink(job->wrapper);
			free(job);
			itable_remove(q->job_table, key);
			return key;
		}
	}

	if (stoptime > 0 && time(0) >= stoptime)
		return -1;

	sleep(1);
	goto restart;
}


int batch_job_remove_hadoop(struct batch_queue *q, batch_job_id_t jobid)
{
	UINT64_T key = jobid;
	struct hadoop_job *job;
	if ((job = itable_lookup(q->job_table, key))) {
		int status;
		debug(D_BATCH, "sending hadoop-streaming job %d SIGTERM.", (int)job->child);
		kill(job->child, SIGTERM);
		sleep(2); /* give time to exit gracefully */
		pid_t child = waitpid(job->child, &status, WNOHANG);
		if (child <= 0) {
			debug(D_BATCH, "forcibly killing hadoop-streaming job %d with SIGKILL.", (int)job->child);
			kill(job->child, SIGKILL); /* force exit */
			child = waitpid(job->child, &status, 0); /* wait forever */
		}
		assert(child == job->child);
		fclose(job->status_file);
		unlink(job->wrapper);
		free(job);
		itable_remove(q->job_table, key);
		return 1;
	}
	return 0;
}
