/*
 * Copyright (C) 2022 The University of Notre Dame
 * This software is distributed under the GNU General Public License.
 * See the file COPYING for details.
*/

#ifdef CCTOOLS_WITH_CHIRP

#include "batch_job.h"
#include "batch_job_internal.h"

#include "buffer.h"
#include "debug.h"
#include "json.h"
#include "json_aux.h"
#include "jx.h"
#include "jx_print.h"
#include "random.h"
#include "sigdef.h"
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

#define STOPTIME  (time(NULL)+30)

static const int BATCH_JOB_CHIRP = 1;

static const char *gethost (struct batch_queue *q)
{
	const char *host = hash_table_lookup(q->options, "host");
	if (host == NULL) {
		fatal("To use Chirp batch execution, you must specify a host via working-dir (e.g. chirp://host:port/data).");
	}
	return host;
}

static const char *getroot (struct batch_queue *q)
{
	const char *root = hash_table_lookup(q->options, "root");
	if (root == NULL) {
		root = "/";
	}
	return root;
}

static void addfile (struct batch_queue *q, buffer_t *B, const char *file, const char *type)
{
	if (file) {
		buffer_putliteral(B, "{\"task_path\":\"./");
		jsonA_escapestring(B, file);
		buffer_putliteral(B, "\",\"serv_path\":\"");
		jsonA_escapestring(B, getroot(q));
		buffer_putliteral(B, "/");
		jsonA_escapestring(B, file);
		buffer_putfstring(B, "\",\"type\":\"%s\"},", type);
	}
}

static batch_job_id_t batch_job_chirp_submit (struct batch_queue *q, const char *cmd, const char *extra_input_files, const char *extra_output_files, struct jx *envlist, const struct rmsummary *resources )
{
	buffer_t B;

	debug(D_DEBUG, "%s(%p, `%s', `%s', `%s')", __func__, q, cmd, extra_input_files, extra_output_files);

	buffer_init(&B);
	buffer_abortonfailure(&B, 1);

	buffer_putliteral(&B, "{");

	buffer_putliteral(&B, "\"executable\":\"/bin/sh\"");

	buffer_putfstring(&B, ",\"arguments\":[\"sh\",\"-c\",\"");
	jsonA_escapestring(&B, cmd);
	buffer_putliteral(&B, "\"]");

	if(envlist) {
		buffer_putliteral(&B,",\"environment\":");
		jx_print_buffer(envlist,&B);
	}

	buffer_putliteral(&B, ",\"files\":[");

	if (extra_input_files) {
		char *file;
		char *list = xxstrdup(extra_input_files);
		while ((file = strsep(&list, ","))) {
			if (strlen(file))
				addfile(q, &B, file, "INPUT");
		}
		free(list);
	}
	if (extra_output_files) {
		char *file;
		char *list = xxstrdup(extra_output_files);
		while ((file = strsep(&list, ","))) {
			if (strlen(file))
				addfile(q, &B, file, "OUTPUT");
		}
		free(list);
	}
	{
		/* JSON does not allow trailing commas. */
		size_t l;
		const char *s = buffer_tolstring(&B, &l);
		if (s[l-1] == ',')
			buffer_rewind(&B, l-1);
	}
	buffer_putliteral(&B, "]");

	buffer_putfstring(&B, ",\"tag\":\"%s\"", (const char *)hash_table_lookup(q->options, "tag"));

	buffer_putliteral(&B, "}");

	chirp_jobid_t id;
	debug(D_DEBUG, "job = `%s'", buffer_tostring(&B));
	int result = chirp_reli_job_create(gethost(q), buffer_tostring(&B), &id, STOPTIME);

	buffer_rewind(&B, 0);
	buffer_putfstring(&B, "[%" PRICHIRP_JOBID_T "]", id);
	if (result == 0 && (result = chirp_reli_job_commit(gethost(q), buffer_tostring(&B), STOPTIME)) == 0) {
		itable_insert(q->job_table, id, &BATCH_JOB_CHIRP);
		buffer_free(&B);
		return (batch_job_id_t) id;
	} else {
		buffer_free(&B);
		return (batch_job_id_t) result;
	}
}

static batch_job_id_t batch_job_chirp_wait (struct batch_queue *q, struct batch_job_info *info_out, time_t stoptime)
{
	char *status;
	time_t timeout = stoptime-time(NULL);

	if (timeout < 0)
		timeout = 0;
	int result = chirp_reli_job_wait(gethost(q), 0, timeout, &status, STOPTIME);

	if (result > 0) {
		unsigned i;

		debug(D_DEBUG, "status = `%s'", status);
		assert(strlen(status) == (size_t)result);
		json_value *J = json_parse(status, result);
		assert(J && jistype(J, json_array));

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
				reaprc = chirp_reli_job_reap(gethost(q), buffer_tostring(&B), STOPTIME);
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
							json_value *exit_signal = jsonA_getname(job, "exit_signal", json_string);
							assert(exit_signal);
							debug(D_BATCH, "job finished with signal %s", exit_signal->u.string.ptr);
							info_out->exited_normally = 0;
							info_out->exit_signal = sigdefint(exit_signal->u.string.ptr);
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

		result = chirp_reli_job_kill(gethost(q), buffer_tostring(&B), STOPTIME);
		if (result == 0)
			debug(D_BATCH, "forcibly killed job %" PRIbjid, jobid);

		result = chirp_reli_job_reap(gethost(q), buffer_tostring(&B), STOPTIME);
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

static int batch_queue_chirp_create (struct batch_queue *q)
{
	BUFFER_STACK(B, 128)
	char tag[21];

	random_hex(tag, sizeof(tag));
	buffer_putfstring(B, "unknown-project:%s", tag);
	batch_queue_set_option(q, "tag", buffer_tostring(B));
	batch_queue_set_feature(q, "local_job_queue", NULL);
	batch_queue_set_feature(q, "gc_size", NULL);
	return 0;
}

batch_queue_stub_free(chirp);
batch_queue_stub_port(chirp);

static void batch_queue_chirp_option_update (struct batch_queue *q, const char *what, const char *value)
{
	if (strcmp(what, "working-dir") == 0) {
		if (string_prefix_is(value, "chirp://")) {
			char *hostportroot = xxstrdup(value+strlen("chirp://"));
			char *root = strchr(hostportroot, '/');
			if (root) {
				batch_queue_set_option(q, "root", root);
				*root = '\0'; /* remove root */
				batch_queue_set_option(q, "host", hostportroot);
				chirp_reli_mkdir_recursive(gethost(q), getroot(q), S_IRWXU, STOPTIME);
			} else {
				batch_queue_set_option(q, "root", "/");
				batch_queue_set_option(q, "host", hostportroot);
			}
			free(hostportroot);
		} else {
			fatal("`%s' is not a valid working-dir", value);
		}
	} else if (strcmp(what, "name") == 0) {
		char tag[21];
		BUFFER_STACK(B, 128)

		random_hex(tag, sizeof(tag));
		buffer_putfstring(B, "%.32s:%s", value == NULL ? "unknown-project" : value, tag);
		batch_queue_set_option(q, "tag", buffer_tostring(B));
	} else if (strcmp(what, "tag") == 0) {
		if (value == NULL)
			fatal("tag value must be set!");
	}
}


int batch_fs_chirp_chdir (struct batch_queue *q, const char *path)
{
	batch_queue_set_option(q, "working-dir", path);
	return 0;
}

int batch_fs_chirp_getcwd (struct batch_queue *q, char *buf, size_t size)
{
	strncpy(buf, getroot(q), size);
	return 0;
}

int batch_fs_chirp_mkdir (struct batch_queue *q, const char *path, mode_t mode, int recursive)
{
	char resolved[CHIRP_PATH_MAX];
	snprintf(resolved, sizeof(resolved), "%s/%s", getroot(q), path);
	if (recursive)
		return chirp_reli_mkdir_recursive(gethost(q), resolved, mode, STOPTIME);
	else
		return chirp_reli_mkdir(gethost(q), resolved, mode, STOPTIME);
}

int batch_fs_chirp_putfile (struct batch_queue *q, const char *lpath, const char *rpath)
{
	char resolved[CHIRP_PATH_MAX];
	struct stat buf;
	FILE *file = fopen(lpath, "r");
	snprintf(resolved, sizeof(resolved), "%s/%s", getroot(q), rpath);
	if (file && fstat(fileno(file), &buf) == 0) {
		int n = chirp_reli_putfile(gethost(q), resolved, file, buf.st_mode, buf.st_size, STOPTIME);
		fclose(file);
		return n;
	}
	return -1;
}

int batch_fs_chirp_rename (struct batch_queue *q, const char *lpath, const char *rpath)
{
	char lresolved[CHIRP_PATH_MAX];
	char rresolved[CHIRP_PATH_MAX];
	snprintf(lresolved, sizeof(lresolved), "%s/%s", getroot(q), lpath);
	snprintf(rresolved, sizeof(rresolved), "%s/%s", getroot(q), rpath);
	if (chirp_reli_rename(gethost(q), lresolved, rresolved, STOPTIME) <= 0) return 0;
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
	char resolved[CHIRP_PATH_MAX];
	snprintf(resolved, sizeof(resolved), "%s/%s", getroot(q), path);
	int rc = chirp_reli_stat(gethost(q), resolved, &cbuf, STOPTIME);
	if (rc >= 0) {
		COPY_STATC(cbuf, *buf);
	}
	debug(D_BATCH, "= %d", rc);
	return rc;
}

int batch_fs_chirp_unlink (struct batch_queue *q, const char *path)
{
	char resolved[CHIRP_PATH_MAX];
	snprintf(resolved, sizeof(resolved), "%s/%s", getroot(q), path);
	return chirp_reli_rmall(gethost(q), resolved, STOPTIME);
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
		batch_job_chirp_wait,
		batch_job_chirp_remove,
	},

	{
		batch_fs_chirp_chdir,
		batch_fs_chirp_getcwd,
		batch_fs_chirp_mkdir,
		batch_fs_chirp_putfile,
		batch_fs_chirp_rename,
		batch_fs_chirp_stat,
		batch_fs_chirp_unlink,
	},
};

#endif

/* vim: set noexpandtab tabstop=8: */
