/*
 * Copyright (C) 2014- The University of Notre Dame
 * This software is distributed under the GNU General Public License.
 * See the file COPYING for details.
*/

#include "batch_job.h"
#include "batch_job_internal.h"

#include "buffer.h"
#include "debug.h"
#include "json.h"
#include "json_aux.h"
#include "stringtools.h"
#include "xxmalloc.h"

#include "chirp_client.h"
#include "chirp_reli.h"

#include <assert.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>

#define jistype(o,t) ((o)->type == (t))

#define STOPTIME  (time(NULL)+5)

static const int BATCH_JOB_CHIRP = 1;

static const char *gethost (struct batch_queue *q)
{
	const char *host = hash_table_lookup(q->options, "host");
	if (host == NULL) {
		fatal("To use Chirp batch execution, you must specify a host via working-dir (e.g. chirp://host:port/data).");
	}
	return host;
}

static const char *getworkingdir (struct batch_queue *q)
{
	const char *workingdir = hash_table_lookup(q->options, "working-dir");
	if (workingdir == NULL) {
		workingdir = "/";
	}
	return workingdir;
}

static batch_job_id_t batch_job_chirp_submit (struct batch_queue *q, const char *cmd, const char *args, const char *infile, const char *outfile, const char *errfile, const char *extra_input_files, const char *extra_output_files)
{
	buffer_t B;

	debug(D_DEBUG, "%s(%p, `%s', `%s', `%s', `%s', `%s', `%s', `%s')", __func__, q, cmd, args, infile, outfile, errfile, extra_input_files, extra_output_files);

	buffer_init(&B);
	buffer_abortonfailure(&B, 1);

	buffer_putliteral(&B, "{");

	buffer_putliteral(&B, "\"executable\":\"/bin/sh\",");

	buffer_putfstring(&B, "\"arguments\":[\"sh\", \"-c\",\"{\n%s", cmd);
	if (args)
		buffer_putfstring(&B, " %s", args);
	buffer_putliteral(&B, "\n}");
	if (infile)
		buffer_putfstring(&B, " <%s", infile);
	if (outfile)
		buffer_putfstring(&B, " >%s", outfile);
	if (errfile)
		buffer_putfstring(&B, " 2>%s", errfile);
	buffer_putliteral(&B, "\"],");

	buffer_putliteral(&B, "\"files\":[");
	if (infile)
		buffer_putfstring(&B, "\"{\"task_path\": \"./%s\", \"serv_path\": \"%s/%s\", \"type\": \"INPUT\"},", infile, getworkingdir(q), infile);
	if (outfile)
		buffer_putfstring(&B, "\"{\"task_path\": \"./%s\", \"serv_path\": \"%s/%s\", \"type\": \"OUTPUT\"},", outfile, getworkingdir(q), outfile);
	if (errfile)
		buffer_putfstring(&B, "\"{\"task_path\": \"./%s\", \"serv_path\": \"%s/%s\", \"type\": \"OUTPUT\"},", errfile, getworkingdir(q), errfile);
	if (extra_input_files) {
		char *file;
		char *list = xxstrdup(extra_input_files);
		while ((file = strsep(&list, ","))) {
			if (strlen(file)) {
				buffer_putfstring(&B, "{\"task_path\": \"%s\", \"serv_path\": \"%s/%s\", \"type\": \"INPUT\"},", file, getworkingdir(q), file);
			}
		}
		free(list);
	}
	if (extra_output_files) {
		char *file;
		char *list = xxstrdup(extra_output_files);
		while ((file = strsep(&list, ","))) {
			if (strlen(file)) {
				buffer_putfstring(&B, "{\"task_path\": \"%s\", \"serv_path\": \"%s/%s\", \"type\": \"OUTPUT\"},", file, getworkingdir(q), file);
			}
		}
		free(list);
	}
	{
		/* JSON does not allow trailing commas. */
		size_t l;
		const char *s = buffer_tostring(&B, &l);
		if (s[l-1] == ',')
			buffer_rewind(&B, l-1);
	}
	buffer_putliteral(&B, "]}");

	chirp_jobid_t id;
	debug(D_DEBUG, "job = `%s'", buffer_tostring(&B, NULL));
	int result = chirp_reli_job_create(gethost(q), buffer_tostring(&B, NULL), &id, STOPTIME);

	buffer_rewind(&B, 0);
	buffer_putfstring(&B, "[%" PRICHIRP_JOBID_T "]", id);
	if (result == 0 && (result = chirp_reli_job_commit(gethost(q), buffer_tostring(&B, NULL), STOPTIME)) == 0) {
		itable_insert(q->job_table, id, &BATCH_JOB_CHIRP);
		buffer_free(&B);
		return (batch_job_id_t) id;
	} else {
		buffer_free(&B);
		return (batch_job_id_t) result;
	}
}

static batch_job_id_t batch_job_chirp_submit_simple (struct batch_queue *q, const char *cmd, const char *extra_input_files, const char *extra_output_files)
{
	/* Here we exploit that batch_job_chirp_submit(...) just concatenates cmd and args, passed to /bin/sh. */
	return batch_job_chirp_submit(q, cmd, NULL, NULL, NULL, NULL, extra_input_files, extra_output_files);
}

static batch_job_id_t batch_job_chirp_wait (struct batch_queue *q, struct batch_job_info *info_out, time_t stoptime)
{
	char *status;
	time_t timeout = stoptime-time(NULL);

	assert(timeout >= 0);
	int result = chirp_reli_job_wait(gethost(q), 0, timeout, &status, stoptime);

	if (result > 0) {
		unsigned i;

		debug(D_DEBUG, "status = `%s'", status);
		assert(strlen(status) == (size_t)result);
		json_value *J = json_parse(status, strlen(status));
		assert(jistype(J, json_array));

		for (i = 0; i < J->u.array.length; i++) {
			json_value *job = J->u.array.values[i];
			assert(jistype(job, json_object));
			chirp_jobid_t id = jsonA_getname(job, "id", json_integer)->u.integer;
			assert(id);
			if (itable_lookup(q->job_table, id) == &BATCH_JOB_CHIRP) {
				buffer_t B;
				int reaprc;

				debug(D_BATCH, "job %" PRICHIRP_JOBID_T " completed", id);

				buffer_init(&B);
				buffer_abortonfailure(&B, 1);
				buffer_putfstring(&B, "[%" PRICHIRP_JOBID_T "]", id);
				reaprc = chirp_reli_job_reap(gethost(q), buffer_tostring(&B, NULL), STOPTIME);
				buffer_free(&B);

				if (reaprc == 0) {
					debug(D_BATCH, "reaped job %" PRICHIRP_JOBID_T, id);

					json_value *status = jsonA_getname(job, "status", json_string);
					assert(status);
					if (strcmp(status->u.string.ptr, "FINISHED") == 0) {
						json_value *exit_status = jsonA_getname(job, "exit_status", json_string);
						assert(exit_status);
						assert(strcmp(exit_status->u.string.ptr, "EXITED") == 0 || strcmp(exit_status->u.string.ptr, "SIGNALED") == 0);
						if (strcmp(exit_status->u.string.ptr, "EXITED") == 0) {
							json_value *exit_code = jsonA_getname(job, "exit_code", json_integer);
							assert(exit_code);
							info_out->exited_normally = 1;
							info_out->exit_code = exit_code->u.integer;
						} else {
							json_value *exit_signal = jsonA_getname(job, "exit_signal", json_integer);
							assert(exit_signal);
							debug(D_BATCH, "job finished with signal %d", (int)exit_signal->u.integer);
							info_out->exited_normally = 0;
							info_out->exit_signal = exit_signal->u.integer;
						}
					} else {
						json_value *error = jsonA_getname(job, "error", json_string);
						if (error)
							debug(D_BATCH, "exited abnormally: %s (%s)", status->u.string.ptr, error->u.string.ptr);
						else
							debug(D_BATCH, "exited abnormally: %s", status->u.string.ptr);
						info_out->exited_normally = 0;
						info_out->exit_signal = 0;
					}
					itable_remove(q->job_table, id);
					json_value_free(J);
					return id;
				} else {
					debug(D_BATCH, "did not reap job %" PRICHIRP_JOBID_T ": %d (%s)", id, errno, strerror(errno));
				}
			}
		}
		json_value_free(J);
	}
	return 0;
}

static int batch_job_chirp_remove (struct batch_queue *q, batch_job_id_t jobid)
{
	int rc = 0;

	if (itable_lookup(q->job_table, jobid) == &BATCH_JOB_CHIRP) {
		int result;
		buffer_t B;

		buffer_init(&B);
		buffer_abortonfailure(&B, 1);
		buffer_putfstring(&B, "[%" PRIbjid "]", jobid);

		debug(D_BATCH, "removing job %" PRIbjid, jobid);

		result = chirp_reli_job_kill(gethost(q), buffer_tostring(&B, NULL), STOPTIME);
		if (result == 0)
			debug(D_BATCH, "forcibly killed job %" PRIbjid, jobid);

		result = chirp_reli_job_reap(gethost(q), buffer_tostring(&B, NULL), STOPTIME);
		if (result == 0) {
			debug(D_BATCH, "reaped job %" PRIbjid, jobid);
			rc = jobid;
		} else {
			debug(D_BATCH, "could not reap job %" PRIbjid ": %d (%s)", jobid, errno, strerror(errno));
		}
		buffer_free(&B);

		itable_remove(q->job_table, jobid);
	}

	return rc;
}

batch_queue_stub_create(chirp);
batch_queue_stub_free(chirp);
batch_queue_stub_port(chirp);

static void batch_queue_chirp_option_update (struct batch_queue *q, const char *what, const char *value)
{
	if (strcmp(what, "working-dir") == 0) {
		if (string_prefix_is(value, "chirp://")) {
			char *hostportroot = xxstrdup(value+strlen("chirp://"));
			char *root = strchr(hostportroot, '/');
			free(hash_table_remove(q->options, "host"));
			free(hash_table_remove(q->options, "working-dir")); /* this is value */
			if (root) {
				hash_table_insert(q->options, "working-dir", xxstrdup(root));
				*root = '\0'; /* remove root */
				hash_table_insert(q->options, "host", xxstrdup(hostportroot));
			} else {
				hash_table_insert(q->options, "working-dir", xxstrdup("/"));
				hash_table_insert(q->options, "host", xxstrdup(hostportroot));
			}
			free(hostportroot);
		} else {
			fatal("`%s' is not a valid working-dir", value);
		}
	}
}


int batch_fs_chirp_chdir (struct batch_queue *q, const char *path)
{
	batch_queue_set_option(q, "working-dir", path);
	return 0;
}

int batch_fs_chirp_getcwd (struct batch_queue *q, char *buf, size_t size)
{
	strncpy(buf, getworkingdir(q), size);
	return 0;
}

int batch_fs_chirp_mkdir (struct batch_queue *q, const char *path, mode_t mode, int recursive)
{
	if (recursive)
		return chirp_reli_mkdir_recursive(gethost(q), path, mode, STOPTIME);
	else
		return chirp_reli_mkdir(gethost(q), path, mode, STOPTIME);
}

int batch_fs_chirp_putfile (struct batch_queue *q, const char *lpath, const char *rpath)
{
	struct stat buf;
	FILE *file = fopen(lpath, "r");
	if (file && fstat(fileno(file), &buf) == 0) {
		int n = chirp_reli_putfile(gethost(q), rpath, file, buf.st_mode, buf.st_size, STOPTIME);
		fclose(file);
		return n;
	}
	return -1;
}

#define COPY_STATC( a, b )\
	memset(&(b),0,sizeof(b));\
	(b).st_dev = (a).cst_dev;\
	(b).st_ino = (a).cst_ino;\
	(b).st_mode = (a).cst_mode;\
	(b).st_nlink = (a).cst_nlink;\
	(b).st_uid = (a).cst_uid;\
	(b).st_gid = (a).cst_gid;\
	(b).st_rdev = (a).cst_rdev;\
	(b).st_size = (a).cst_size;\
	(b).st_blksize = (a).cst_blksize;\
	(b).st_blocks = (a).cst_blocks;\
	(b).st_atime = (a).cst_atime;\
	(b).st_mtime = (a).cst_mtime;\
	(b).st_ctime = (a).cst_ctime;
int batch_fs_chirp_stat (struct batch_queue *q, const char *path, struct stat *buf)
{
	struct chirp_stat cbuf;
	int rc = chirp_reli_stat(gethost(q), path, &cbuf, STOPTIME);
	if (rc >= 0) {
		COPY_STATC(cbuf, *buf);
	}
	return rc;
}

int batch_fs_chirp_unlink (struct batch_queue *q, const char *path)
{
	return chirp_reli_rmall(gethost(q), path, STOPTIME);
}

const struct batch_queue_module batch_queue_chirp = {
	BATCH_QUEUE_TYPE_CHIRP,
	"chirp",

	batch_queue_chirp_create,
	batch_queue_chirp_free,
	batch_queue_chirp_port,
	batch_queue_chirp_option_update,

	{
		batch_job_chirp_submit,
		batch_job_chirp_submit_simple,
		batch_job_chirp_wait,
		batch_job_chirp_remove,
	},

	{
		batch_fs_chirp_chdir,
		batch_fs_chirp_getcwd,
		batch_fs_chirp_mkdir,
		batch_fs_chirp_putfile,
		batch_fs_chirp_stat,
		batch_fs_chirp_unlink,
	},
};

/* vim: set noexpandtab tabstop=4: */
