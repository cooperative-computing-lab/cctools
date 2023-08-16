/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2022 The University of Notre Dame
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
extern const struct batch_queue_module batch_queue_lambda;
extern const struct batch_queue_module batch_queue_amazon_batch;
extern const struct batch_queue_module batch_queue_chirp;
extern const struct batch_queue_module batch_queue_cluster;
extern const struct batch_queue_module batch_queue_condor;
extern const struct batch_queue_module batch_queue_local;
extern const struct batch_queue_module batch_queue_moab;
extern const struct batch_queue_module batch_queue_sge;
extern const struct batch_queue_module batch_queue_pbs;
extern const struct batch_queue_module batch_queue_lsf;
extern const struct batch_queue_module batch_queue_torque;
extern const struct batch_queue_module batch_queue_blue_waters;
extern const struct batch_queue_module batch_queue_slurm;
extern const struct batch_queue_module batch_queue_wq;
extern const struct batch_queue_module batch_queue_vine;
extern const struct batch_queue_module batch_queue_mesos;
extern const struct batch_queue_module batch_queue_k8s;
extern const struct batch_queue_module batch_queue_dryrun;
#ifdef CCTOOLS_WITH_MPI
extern const struct batch_queue_module batch_queue_mpi;
#endif

static struct batch_queue_module batch_queue_unknown = {
	BATCH_QUEUE_TYPE_UNKNOWN, "unknown",

	NULL, NULL, NULL, NULL,

	{NULL, NULL, NULL},

	{NULL, NULL, NULL, NULL, NULL, NULL, NULL},
};

#define BATCH_JOB_SYSTEMS "local, vine, wq, condor, sge, pbs, lsf, torque, moab, mpi, slurm, chirp, amazon, amazon-batch, lambda, mesos, k8s, dryrun"

const struct batch_queue_module * const batch_queue_modules[] = {
	&batch_queue_amazon,
	&batch_queue_amazon_batch,
	&batch_queue_lambda,
#ifdef CCTOOLS_WITH_CHIRP
	&batch_queue_chirp,
#endif
#ifdef CCTOOLS_WITH_MPI
        &batch_queue_mpi,
#endif
	&batch_queue_cluster,
	&batch_queue_condor,
	&batch_queue_local,
	&batch_queue_moab,
	&batch_queue_sge,
	&batch_queue_pbs,
	&batch_queue_lsf,
	&batch_queue_torque,
	&batch_queue_blue_waters,
	&batch_queue_slurm,
	&batch_queue_wq,
	&batch_queue_vine,
	&batch_queue_mesos,
	&batch_queue_k8s,
	&batch_queue_dryrun,
	&batch_queue_unknown
};

struct batch_job_info *batch_job_info_create()
{
	struct batch_job_info *info = calloc(1,sizeof(*info));
	return info;
}

void batch_job_info_delete(struct batch_job_info *info)
{
	free(info);
}

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
	q->tv_file_table = 0;
	q->tv_manager = 0;
	q->wq_manager = 0;
	
	batch_queue_set_feature(q, "local_job_queue", "yes");
	batch_queue_set_feature(q, "absolute_path", "yes");
	batch_queue_set_feature(q, "output_directories", "yes");
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
		debug(D_BATCH, "deleting queue %p", q);

		q->module->free(q);

		hash_table_clear(q->options,free);
		hash_table_delete(q->options);

		hash_table_clear(q->features,free);
		hash_table_delete(q->features);

		itable_delete(q->job_table);
		free(q);
	}
}

const char *batch_queue_get_option (struct batch_queue *q, const char *what)
{
	return hash_table_lookup(q->options, what);
}

int batch_queue_option_is_yes (struct batch_queue *q, const char *what)
{
	const char *result = batch_queue_get_option(q, what);

	if(!result || strcmp(result, "yes")) {
		return 0;
	}

	return 1;
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

	const char *tr_pattern = batch_queue_supports_feature(q, "batch_log_transactions");
	if(tr_pattern) {
		char *tr_name = string_format(tr_pattern, q->logfile);
		batch_queue_set_option(q, "batch_log_transactions_name", tr_name);
		free(tr_name);
	}
}

int batch_queue_port(struct batch_queue *q)
{
	return q->module->port(q);
}

void batch_queue_set_option (struct batch_queue *q, const char *what, const char *value)
{
	char *current = hash_table_remove(q->options, what);
	if(value) {
		hash_table_insert(q->options, what, xxstrdup(value));
		debug(D_BATCH, "set option `%s' to `%s'", what, value);
	} else {
		debug(D_BATCH, "cleared option `%s'", what);
	}
	free(current);
	q->module->option_update(q, what, value);
}

void batch_queue_set_feature (struct batch_queue *q, const char *what, const char *value)
{
	char *current = hash_table_remove(q->features, what);
	if(value) {
		hash_table_insert(q->features, what, xxstrdup(value));
		debug(D_BATCH, "set feature `%s' to `%s'", what, value);
	} else {
		debug(D_BATCH, "cleared feature `%s'", what);
	}
	free(current);
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


batch_job_id_t batch_job_submit(struct batch_queue * q, const char *cmd, const char *extra_input_files, const char *extra_output_files, struct jx *envlist, const struct rmsummary *resources)
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

int batch_fs_rename (struct batch_queue *q, const char *lpath, const char *rpath)
{
	return q->module->fs.rename(q, lpath, rpath);
}

int batch_fs_stat (struct batch_queue *q, const char *path, struct stat *buf)
{
	return q->module->fs.stat(q, path, buf);
}

int batch_fs_unlink (struct batch_queue *q, const char *path)
{
	return q->module->fs.unlink(q, path);
}

/* vim: set noexpandtab tabstop=8: */
