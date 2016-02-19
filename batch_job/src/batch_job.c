/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2005- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "batch_job.h"
#include "batch_job_internal.h"

#include "debug.h"
#include "itable.h"
#include "stringtools.h"
#include "xxmalloc.h"

#include <sys/stat.h>

#include <stdlib.h>
#include <string.h>

extern const struct batch_queue_module batch_queue_amazon;
extern const struct batch_queue_module batch_queue_chirp;
extern const struct batch_queue_module batch_queue_cluster;
extern const struct batch_queue_module batch_queue_condor;
extern const struct batch_queue_module batch_queue_local;
extern const struct batch_queue_module batch_queue_moab;
extern const struct batch_queue_module batch_queue_sge;
extern const struct batch_queue_module batch_queue_pbs;
extern const struct batch_queue_module batch_queue_torque;
extern const struct batch_queue_module batch_queue_slurm;
extern const struct batch_queue_module batch_queue_wq;

static struct batch_queue_module batch_queue_unknown = {
	BATCH_QUEUE_TYPE_UNKNOWN, "unknown",

	NULL, NULL, NULL, NULL,

	{NULL, NULL, NULL},

	{NULL, NULL, NULL, NULL, NULL, NULL},
};

#define BATCH_JOB_SYSTEMS  "local, wq, condor, sge, torque, moab, slurm, chirp, amazon"

const struct batch_queue_module * const batch_queue_modules[] = {
	&batch_queue_amazon,
	&batch_queue_chirp,
	&batch_queue_cluster,
	&batch_queue_condor,
	&batch_queue_local,
	&batch_queue_moab,
	&batch_queue_sge,
	&batch_queue_pbs,
	&batch_queue_torque,
	&batch_queue_slurm,
	&batch_queue_wq,
	&batch_queue_unknown
};

struct batch_queue *batch_queue_create(batch_queue_type_t type)
{
	int i;
	struct batch_queue *q;

	q = xxmalloc(sizeof(*q));
	q->type = type;
	strncpy(q->logfile, "", sizeof(q->logfile));
	q->options = hash_table_create(0, NULL);
	q->features = hash_table_create(0, NULL);
	q->job_table = itable_create(0);
	q->output_table = itable_create(0);
	q->data = NULL;

	batch_queue_set_feature(q, "local_job_queue", "yes");
	batch_queue_set_feature(q, "batch_log_name", "%s.batchlog");
	batch_queue_set_feature(q, "gc_size", "yes");

	q->module = NULL;
	for (i = 0; batch_queue_modules[i]->type != BATCH_QUEUE_TYPE_UNKNOWN; i++)
		if (batch_queue_modules[i]->type == type)
			q->module = batch_queue_modules[i];
	if (q->module == NULL) {
		batch_queue_delete(q);
		return NULL;
	}

	if(q->module->create(q) == -1) {
		batch_queue_delete(q);
		return NULL;
	}

	debug(D_BATCH, "created queue %p (%s)", q, q->module->typestr);

	return q;
}

void batch_queue_delete(struct batch_queue *q)
{
	if(q) {
		char *key;
		char *value;

		debug(D_BATCH, "deleting queue %p", q);

		q->module->free(q);

		for (hash_table_firstkey(q->options); hash_table_nextkey(q->options, &key, (void **) &value); free(value))
			;
		hash_table_delete(q->options);
		for (hash_table_firstkey(q->features); hash_table_nextkey(q->features, &key, (void **) &value); free(value))
			;
		hash_table_delete(q->features);
		itable_delete(q->job_table);
		itable_delete(q->output_table);
		free(q);
	}
}

const char *batch_queue_get_option (struct batch_queue *q, const char *what)
{
	return hash_table_lookup(q->options, what);
}

const char *batch_queue_supports_feature (struct batch_queue *q, const char *what)
{
	return hash_table_lookup(q->features, what);
}

batch_queue_type_t batch_queue_get_type(struct batch_queue *q)
{
	return q->type;
}

void batch_queue_set_logfile(struct batch_queue *q, const char *logfile)
{
	strncpy(q->logfile, logfile, sizeof(q->logfile));
	q->logfile[sizeof(q->logfile)-1] = '\0';
	debug(D_BATCH, "set logfile to `%s'", logfile);
}

int batch_queue_port(struct batch_queue *q)
{
	return q->module->port(q);
}

void batch_queue_set_option (struct batch_queue *q, const char *what, const char *value)
{
	char *current = hash_table_remove(q->options, what);
	free(current);
	if(value) {
		hash_table_insert(q->options, what, xxstrdup(value));
		debug(D_BATCH, "set option `%s' to `%s'", what, value);
	} else {
		debug(D_BATCH, "cleared option `%s'", what);
	}
	q->module->option_update(q, what, value);
}

void batch_queue_set_feature (struct batch_queue *q, const char *what, const char *value)
{
	char *current = hash_table_remove(q->features, what);
	free(current);
	if(value) {
		hash_table_insert(q->features, what, xxstrdup(value));
		debug(D_BATCH, "set feature `%s' to `%s'", what, value);
	} else {
		debug(D_BATCH, "cleared feature `%s'", what);
	}
}

void batch_queue_set_int_option(struct batch_queue *q, const char *what, int value) {
	char *str_value = string_format("%d", value);
	batch_queue_set_option(q, what, str_value);

	free(str_value);
}

batch_queue_type_t batch_queue_type_from_string(const char *str)
{
	int i;
	for (i = 0; batch_queue_modules[i]->type != BATCH_QUEUE_TYPE_UNKNOWN; i++)
		if (strcmp(batch_queue_modules[i]->typestr, str) == 0)
			return batch_queue_modules[i]->type;
	return BATCH_QUEUE_TYPE_UNKNOWN;
}

const char *batch_queue_type_to_string(batch_queue_type_t t)
{
	int i;
	for (i = 0; batch_queue_modules[i]->type != BATCH_QUEUE_TYPE_UNKNOWN; i++)
		if (batch_queue_modules[i]->type == t)
			return batch_queue_modules[i]->typestr;
	return "unknown";
}

const char *batch_queue_type_string()
{
	return BATCH_JOB_SYSTEMS;
}


batch_job_id_t batch_job_submit(struct batch_queue * q, const char *cmd, const char *extra_input_files, const char *extra_output_files, struct jx *envlist, struct rmsummary *resources)
{
	return q->module->job.submit(q, cmd, extra_input_files, extra_output_files, envlist, resources);
}

batch_job_id_t batch_job_wait(struct batch_queue * q, struct batch_job_info * info)
{
	return q->module->job.wait(q, info, 0);
}

batch_job_id_t batch_job_wait_timeout(struct batch_queue * q, struct batch_job_info * info, time_t stoptime)
{
	return q->module->job.wait(q, info, stoptime);
}

int batch_job_remove(struct batch_queue *q, batch_job_id_t jobid)
{
	return q->module->job.remove(q, jobid);
}


int batch_fs_chdir (struct batch_queue *q, const char *path)
{
	return q->module->fs.chdir(q, path);
}

int batch_fs_getcwd (struct batch_queue *q, char *buf, size_t size)
{
	return q->module->fs.getcwd(q, buf, size);
}

int batch_fs_mkdir (struct batch_queue *q, const char *path, mode_t mode, int recursive)
{
	return q->module->fs.mkdir(q, path, mode, recursive);
}

int batch_fs_putfile (struct batch_queue *q, const char *lpath, const char *rpath)
{
	return q->module->fs.putfile(q, lpath, rpath);
}

int batch_fs_stat (struct batch_queue *q, const char *path, struct stat *buf)
{
	return q->module->fs.stat(q, path, buf);
}

int batch_fs_unlink (struct batch_queue *q, const char *path)
{
	return q->module->fs.unlink(q, path);
}

/* vim: set noexpandtab tabstop=4: */
