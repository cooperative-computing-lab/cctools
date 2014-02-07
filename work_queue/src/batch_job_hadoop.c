#include "batch_job.h"
#include "batch_job_internal.h"
#include "debug.h"
#include "hdfs_library.h"
#include "macros.h"
#include "process.h"
#include "stringtools.h"
#include "xxmalloc.h"

#include <assert.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>

#include <fcntl.h>
#include <glob.h>
#include <pwd.h>
#include <unistd.h>

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
	struct passwd *pwd = getpwuid(getuid());

	if (file == NULL)
		return -1;

	char *escaped_cmd = escape_shell_string(cmd);
	if (escaped_cmd == NULL) return -1;

	fprintf(file, "#!/bin/sh\n");
	fprintf(file, "cmd=%s\n", escaped_cmd);
	free(escaped_cmd);
	/* fprintf(file, "exec %s -- /bin/sh -c %s\n", getenv("HADOOP_PARROT_PATH"), escaped_cmd); */ /* random bash bug, look into later */
	fprintf(file, "exec %s --work-dir='%s' --username='%s' -- /bin/sh <<EOF\n", getenv("HADOOP_PARROT_PATH"), getenv("HDFS_ROOT_DIR"), pwd->pw_name);
	fprintf(file, "$cmd\n");
	fprintf(file, "EOF\n");
	fclose(file);

	return 0;
}

static batch_job_id_t fork_hadoop(struct batch_queue *q, char *hadoop_streaming_command[], struct hadoop_job *job)
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

static batch_job_id_t batch_job_hadoop_submit_simple (struct batch_queue *q, const char *cmd, const char *extra_input_files, const char *extra_output_files)
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

	batch_job_id_t status = fork_hadoop(q, hadoop_streaming_command, job);
	for (i = 0; hadoop_streaming_command[i]; i++)
		free(hadoop_streaming_command[i]);
	return status;
}

static batch_job_id_t batch_job_hadoop_submit (struct batch_queue *q, const char *cmd, const char *args, const char *infile, const char *outfile, const char *errfile, const char *extra_input_files, const char *extra_output_files)
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

	batch_job_id_t status = batch_job_hadoop_submit_simple(q, command, extra_input_files, extra_output_files);
	free(command);
	return status;
}

static batch_job_id_t batch_job_hadoop_wait (struct batch_queue *q, struct batch_job_info *info_out, time_t stoptime)
{
	UINT64_T key;
	struct hadoop_job *job;

restart:
	itable_firstkey(q->job_table);
	while (itable_nextkey(q->job_table, &key, (void **) &job)) {
		/* read the status until empty */
		char line[BATCH_JOB_LINE_MAX];
		while (fgets(line, sizeof(line), job->status_file)) {
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

static int batch_job_hadoop_remove (struct batch_queue *q, batch_job_id_t jobid)
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

static struct hdfs_library *hdfs_services = 0;
static hdfsFS fs = NULL;
static hdfsFS lfs = NULL; /* local file system, for putfile */
static int batch_queue_hadoop_create (struct batch_queue *Q)
{
	if(!getenv("HADOOP_PARROT_PATH")) {
		/* Note: HADOOP_PARROT_PATH is the path to Parrot on the remote node, not on the local machine. */
		debug(D_NOTICE, "error: environment variable HADOOP_PARROT_PATH not set\n");
		return -1;
	}
	if(!getenv("HADOOP_USER_TMP")) {
		debug(D_NOTICE, "error: environment variable HADOOP_USER_TMP not set\n");
		return -1;
	}
	if(!getenv("HDFS_ROOT_DIR")) {
		debug(D_NOTICE, "error: environment variable HDFS_ROOT_DIR not set\n");
		return -1;
	}

	if(!hdfs_services) {
		if (hdfs_library_envinit() == -1)
			return -1;
		hdfs_services = hdfs_library_open();
		if(!hdfs_services)
			return -1;
	}

	if(!lfs) {
		lfs = hdfs_services->connect(NULL, 0); /* local file system, for putfile */
		if(!lfs)
			return -1;
	}

	/* defaults */
	hash_table_insert(Q->options, "host", xxstrdup("default:50070"));
	hash_table_insert(Q->options, "working-dir", xxstrdup("/"));
	hash_table_insert(Q->options, "replicas", xxstrdup("0"));

	return 0;
}

static int batch_queue_hadoop_free (struct batch_queue *Q)
{
	if (fs) {
		hdfs_services->disconnect(fs);
		fs = NULL;
	}
	if (lfs) {
		hdfs_services->disconnect(lfs);
		lfs = NULL;
	}
	if (hdfs_services) {
		hdfs_library_close(hdfs_services);
		hdfs_services = NULL;
	}
	return 0;
}

static void batch_queue_hadoop_option_update (struct batch_queue *Q, const char *what, const char *value)
{
	if(strcmp(what, "working-dir") == 0) {
		if (string_prefix_is(value, "hdfs://")) {
			char *hostportroot = xxstrdup(value+strlen("hdfs://"));
			char *root = strchr(hostportroot, '/');
			char *host = hostportroot;
			const char *port;
			free(hash_table_remove(Q->options, "host"));
			free(hash_table_remove(Q->options, "port"));
			free(hash_table_remove(Q->options, "root")); /* this is value */

			if (root) {
				hash_table_insert(Q->options, "root", xxstrdup(root));
				*root = '\0'; /* remove root */
			} else {
				hash_table_insert(Q->options, "root", xxstrdup("/"));
			}

			if (strchr(host, ':')) {
				port = strchr(host, ':')+1;
				*strchr(host, ':') = '\0';
			} else {
				port = "50070"; /* default namenode port */
			}
			hash_table_insert(Q->options, "port", xxstrdup(port));

			if (strlen(host))
				hash_table_insert(Q->options, "host", xxstrdup(host));
			else
				hash_table_insert(Q->options, "host", xxstrdup("default"));
			free(hostportroot);
		} else {
			fatal("`%s' is not a valid working-dir", value);
		}
	}
}

batch_queue_stub_port(hadoop);

static hdfsFS getfs (struct batch_queue *Q) {

	if (fs == NULL) {
		static const char *groups[] = { "supergroup" };
		const char *host = hash_table_lookup(Q->options, "host");
		const char *port = hash_table_lookup(Q->options, "port");
		const char *root = hash_table_lookup(Q->options, "root");
		if(host == NULL || port == NULL || root == NULL)
			fatal("To use Hadoop batch execution, you must specify a host and root directory via --working-dir (e.g. hdfs://host:port/data).");

		debug(D_HDFS, "connecting to hdfs://%s:%s%s\n", host, port, root);
		fs = hdfs_services->connect_as_user(host, atoi(port), NULL, groups, 1);
		if (fs == NULL) {
			fatal("could not connect to hdfs: %s", strerror(errno));
		}
		hdfs_services->chdir(fs, root);
	}
	assert(lfs);
	return fs;
}

static const char *getroot (struct batch_queue *Q)
{
	const char *workingdir = hash_table_lookup(Q->options, "root");
	if (workingdir == NULL) {
		workingdir = "/";
	}
	return workingdir;
}

static int batch_fs_hadoop_chdir (struct batch_queue *Q, const char *path)
{
	free(hash_table_remove(Q->options, "root")); /* this is value */
	hash_table_insert(Q->options, "root", xxstrdup(path));
	return hdfs_services->chdir(getfs(Q), path);
}

static int batch_fs_hadoop_getcwd (struct batch_queue *Q, char *buf, size_t size)
{
	strncpy(buf, getroot(Q), size);
	return 0;
}

static void copystat (struct stat *buf, hdfsFileInfo *hs, const char *path)
{
	memset(buf, 0, sizeof(*buf));
	buf->st_dev = -1;
	buf->st_rdev = -2;
	buf->st_ino = hash_string(path);
	buf->st_mode = hs->mKind == kObjectKindDirectory ? S_IFDIR : S_IFREG;

	/* HDFS does not have execute bit, lie and set it for all files */
	buf->st_mode |= hs->mPermissions | S_IXUSR | S_IXGRP;
	buf->st_nlink = hs->mReplication;
	buf->st_uid = 0;
	buf->st_gid = 0;
	buf->st_size = hs->mSize;
	buf->st_blksize = hs->mBlockSize;

	/* If the blocksize is not set, assume 64MB chunksize */
	if(buf->st_blksize < 1)
		buf->st_blksize = 64 * 1024 * 1024;
	buf->st_blocks = MAX(1, buf->st_size / buf->st_blksize);

	/* Note that hs->mLastAccess is typically zero. */
	buf->st_atime = buf->st_mtime = buf->st_ctime = hs->mLastMod;
}

static int do_stat (hdfsFS fs, const char *path, struct stat *buf)
{
	debug(D_HDFS, "stat %s", path);

	hdfsFileInfo *file_info = hdfs_services->stat(fs, path);
	if(file_info == NULL)
		return (errno = ENOENT, -1);
	copystat(buf, file_info, path);
	hdfs_services->free_stat(file_info, 1);
	return 0;
}

int batch_fs_hadoop_mkdir (struct batch_queue *Q, const char *path, mode_t mode, int recursive)
{
	/* NYI recursive */
	const char *root = getroot(Q);
	struct stat buf;

	/* hdfs mkdir incorrectly returns EPERM if it already exists. */
	if(do_stat(getfs(Q), path, &buf) == 0 && S_ISDIR(buf.st_mode)) {
		errno = EEXIST;
		return -1;
	}

	debug(D_HDFS, "mkdir %s/%s", root, path);
	return hdfs_services->mkdir(getfs(Q), path);
}

int batch_fs_hadoop_putfile (struct batch_queue *Q, const char *lpath, const char *rpath)
{
	return hdfs_services->copy(lfs, lpath, getfs(Q), rpath);
}

int batch_fs_hadoop_stat (struct batch_queue *Q, const char *path, struct stat *buf)
{
	return do_stat(getfs(Q), path, buf);
}

int batch_fs_hadoop_unlink (struct batch_queue *Q, const char *path)
{
	/*
	HDFS is known to return bogus errnos from unlink,
	so check for directories beforehand, and set the errno
	properly afterwards if needed.
	*/
	struct stat info;

	if(do_stat(getfs(Q), path, &info) < 0)
		return -1;

	if(S_ISDIR(info.st_mode)) {
		errno = EISDIR;
		return -1;
	}

	debug(D_HDFS, "unlink %s", path);
	if(hdfs_services->unlink(getfs(Q), path, 1) == -1) {
		errno = EACCES;
		return -1;
	}

	return 0;
}

const struct batch_queue_module batch_queue_hadoop = {
	BATCH_QUEUE_TYPE_HADOOP,
	"hadoop",

	batch_queue_hadoop_create,
	batch_queue_hadoop_free,
	batch_queue_hadoop_port,
	batch_queue_hadoop_option_update,

	{
		batch_job_hadoop_submit,
		batch_job_hadoop_submit_simple,
		batch_job_hadoop_wait,
		batch_job_hadoop_remove,
	},

	{
		batch_fs_hadoop_chdir,
		batch_fs_hadoop_getcwd,
		batch_fs_hadoop_mkdir,
		batch_fs_hadoop_putfile,
		batch_fs_hadoop_stat,
		batch_fs_hadoop_unlink,
	},
};

/* vim: set noexpandtab tabstop=4: */
