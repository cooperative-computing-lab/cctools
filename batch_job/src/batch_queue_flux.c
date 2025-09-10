/*
Copyright (C) 2024 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "batch_queue.h"
#include "batch_queue_internal.h"
#include "debug.h"
#include "itable.h"
#include "path.h"
#include "process.h"
#include "macros.h"
#include "stringtools.h"
#include "xxmalloc.h"
#include "jx_parse.h"

#include <stdlib.h>
#include <stdio.h>
#include <errno.h>
#include <string.h>
#include <signal.h>
#include <sys/stat.h>
#include <libgen.h>

// itable mapping flux_job_ids to flux_job_info
static struct itable *flux_job_info_table = NULL;

// itable mapping batch_queue_jobid_t to flux_job_info
static struct itable *batch_queue_jobid_info_table = NULL;

static int job_count = 1;

struct flux_job_info {
	batch_queue_id_t job_id;
	uint64_t flux_job_id;
};

static struct flux_job_info *create_flux_job_info(batch_queue_id_t job_id, uint64_t flux_job_id)
{
	struct flux_job_info *new_job_info = xxmalloc(sizeof(struct flux_job_info));
	new_job_info->job_id = job_id;
	new_job_info->flux_job_id = flux_job_id;

	return new_job_info;
}

static void delete_flux_job_info(struct flux_job_info *job_info)
{
	if (job_info) {
		free(job_info);
	}
}

static batch_queue_id_t batch_queue_flux_submit(struct batch_queue *q, struct batch_job *bt)
{
	// Set same defaults as batch_queue_condor and condor_submit_workers
	// Flux does not support setting memory and disk requirements
	int64_t cores = 1;
	int64_t gpus = 0;

	struct rmsummary *resources = bt->resources;
	if (resources) {
		cores = resources->cores > -1 ? resources->cores : cores;
		gpus = resources->gpus > -1 ? resources->gpus : gpus;
	}

	// Create archive to stage-in to flux job
	// First, delete old archive if it exists
	FILE *archive_remove_pipe = popen("flux archive remove -f", "r");
	if (!archive_remove_pipe) {
		return -1;
	}
	char buffer[BUFSIZ];
	while (fread(buffer, sizeof(char), sizeof(buffer) / sizeof(char), archive_remove_pipe) > 0) {
	}
	pclose(archive_remove_pipe);

	// Only enable the stage-in option if we have files in the archive
	bool flux_stage_in = false;
	if (bt->input_files) {
		struct batch_file *bf;
		LIST_ITERATE(bt->input_files, bf)
		{
			flux_stage_in = true;

			char *dirc = xxstrdup(bf->outer_name);
			char *basec = xxstrdup(bf->outer_name);
			char *dname = dirname(dirc);
			char *bname = basename(basec);

			char *command = string_format("flux archive create --append -C %s %s 2>&1", dname, bname);
			FILE *archive_create_pipe = popen(command, "r");
			if (!archive_create_pipe) {
				return -1;
			}
			while (fread(buffer, sizeof(char), sizeof(buffer) / sizeof(char), archive_create_pipe) > 0) {
			}
			int archive_create_status = pclose(archive_create_pipe);
			archive_create_status = WEXITSTATUS(archive_create_status);
			if (archive_create_status != EXIT_SUCCESS) {
				debug(D_BATCH, "flux failed to create archive with file %s", bf->outer_name);
				return -1;
			}

			free(dirc);
			free(basec);
			free(command);
		}
	}

	// Flux does not support staging files out of the worker environment, so warn for each file
	if (bt->output_files) {
		struct batch_file *bf;
		LIST_ITERATE(bt->output_files, bf)
		{
			debug(D_BATCH, "warn: flux does not support output files (%s)", bf->outer_name);
		}
	}

	// We simply export vars to the environment, and flux-submit pulls in the environment to the worker.
	if (bt->envlist) {
		jx_export(bt->envlist);
	}

	char *submit_command = string_format("flux submit %s --flags=waitable --nodes=1 --cores=%" PRId64 " --gpus-per-node=%" PRId64 " sh -c 'cd $FLUX_JOB_TMPDIR && %s' | flux job id --to=dec", flux_stage_in ? "-o stage-in" : "", cores, gpus, bt->command);
	FILE *submit_pipe = popen(submit_command, "r");
	free(submit_command);

	uint64_t flux_job_id;
	memset(buffer, 0, sizeof(buffer));
	while (fgets(buffer, sizeof(buffer), submit_pipe)) {
		if (sscanf(buffer, "%" PRIu64, &flux_job_id) == 1) {
			batch_queue_id_t job_id = job_count++;
			struct batch_job_info *info = calloc(1, sizeof(*info));
			info->submitted = time(0);
			info->started = time(0);
			itable_insert(q->job_table, job_id, info);

			struct flux_job_info *curr_job_info = create_flux_job_info(job_id, flux_job_id);
			itable_insert(flux_job_info_table, flux_job_id, curr_job_info);
			itable_insert(batch_queue_jobid_info_table, job_id, curr_job_info);

			pclose(submit_pipe);

			debug(D_BATCH, "created job_id %" PRId64 " with flux_job_id %" PRIu64, job_id, flux_job_id);
			return job_id;
		}
	}

	return -1;
}

static void fill_batch_job_info(struct batch_job_info *info_out, uint64_t flux_job_id)
{
	if (!info_out) {
		return;
	}

	char *command = string_format("flux jobs --json %" PRIu64 " 2> /dev/null", flux_job_id);
	FILE *pipe = popen(command, "r");
	free(command);
	command = NULL;

	if (!pipe) {
		return;
	}

	struct jx *json = jx_parse_stream(pipe);
	if (!json) {
		pclose(pipe);
		return;
	}

	info_out->submitted = jx_lookup_double(json, "t_submit");
	info_out->started = jx_lookup_double(json, "t_run");
	info_out->disk_allocation_exhausted = 0;
	info_out->exit_code = jx_lookup_integer(json, "returncode");
	info_out->exit_signal = jx_lookup_integer(json, "waitstatus");
	info_out->exit_signal = WSTOPSIG(info_out->exit_signal);
	info_out->exited_normally = jx_lookup_integer(json, "waitstatus");
	info_out->exited_normally = WIFEXITED(info_out->exited_normally);
	info_out->finished = jx_lookup_boolean(json, "success");

	jx_delete(json);
}

static batch_queue_id_t batch_queue_flux_wait_jobid(struct batch_queue *q, struct batch_job_info *info_out, time_t stoptime, uint64_t wait_flux_job_id)
{
	while (1) {
		int timeout;

		if (stoptime > 0) {
			timeout = MAX(0, stoptime - time(0));
		} else {
			timeout = 5;
		}

		if (timeout <= 0) {
			return -1;
		}

		char *wait_command;
		if (wait_flux_job_id != 0) {
			wait_command = string_format("timeout %ds flux job wait %" PRIu64 " 2>&1", timeout, wait_flux_job_id);
		} else {
			wait_command = string_format("timeout %ds flux job wait 2>&1", timeout);
		}

		FILE *wait_pipe = popen(wait_command, "r");
		free(wait_command);
		if (!wait_pipe) {
			return -1;
		}

		char wait_output[BUFSIZ];
		while (fread(wait_output, sizeof(char), sizeof(wait_output) / sizeof(char), wait_pipe) > 0) {
		}
		string_chomp(wait_output);
		int wait_status = pclose(wait_pipe);
		wait_status = WEXITSTATUS(wait_status);

		if (wait_status == 124) {
			// timeout killed the wait command
			return -1;
		} else if (wait_status == 2) {
			// no more jobs to be waited on
			return 0;
		}

		// convert output flux job id to decimal
		char *convert_command = string_format("echo '%s' | flux job id --to=dec 2>&1", wait_output);
		FILE *convert_pipe = popen(convert_command, "r");
		free(convert_command);
		if (!convert_pipe) {
			return -1;
		}

		char convert_output[BUFSIZ];
		uint64_t flux_job_id;
		while (fgets(convert_output, sizeof(convert_output), convert_pipe)) {
			if (sscanf(convert_output, "%" PRIu64, &flux_job_id) == 1) {
				struct flux_job_info *job_info = itable_lookup(flux_job_info_table, flux_job_id);
				if (job_info) {
					pclose(convert_pipe);
					fill_batch_job_info(info_out, flux_job_id);
					return job_info->job_id;
				}
			}
		}

		pclose(convert_pipe);
	}
}

static batch_queue_id_t batch_queue_flux_wait(struct batch_queue *q, struct batch_job_info *info_out, time_t stoptime)
{
	return batch_queue_flux_wait_jobid(q, info_out, stoptime, 0);
}

static int batch_queue_flux_remove(struct batch_queue *q, batch_queue_id_t jobid, batch_queue_remove_mode_t mode)
{
	struct flux_job_info *info = itable_lookup(batch_queue_jobid_info_table, jobid);
	if (!info) {
		return 0;
	}

	char *kill_command = string_format("flux job kill %" PRIu64 " 2>&1", info->flux_job_id);
	FILE *kill_pipe = popen(kill_command, "r");
	free(kill_command);
	if (!kill_pipe) {
		return 0;
	}

	char buffer[BUFSIZ];
	while (fread(buffer, sizeof(char), sizeof(buffer) / sizeof(char), kill_pipe) > 0) {
	}

	int kill_status = pclose(kill_pipe);
	kill_status = WEXITSTATUS(kill_status);
	if (kill_status == EXIT_SUCCESS) {
		// Kill signal sent successfully, try to wait on specific job
		struct batch_job_info info_out;
		batch_queue_id_t waited_jobid = batch_queue_flux_wait_jobid(q, &info_out, 5, info->flux_job_id);

		if (waited_jobid != -1) {
			return 1;
		}

		// Wait timed out, so kill it for real
		kill_command = string_format("flux job kill -s SIGKILL %" PRIu64 " 2>&1", info->flux_job_id);
		kill_pipe = popen(kill_command, "r");
		free(kill_command);
		if (!kill_pipe) {
			return 0;
		}

		while (fread(buffer, sizeof(char), sizeof(buffer) / sizeof(char), kill_pipe) > 0) {
		}
		pclose(kill_pipe);

		// Wait on job, then return
		waited_jobid = batch_queue_flux_wait_jobid(q, &info_out, 5, info->flux_job_id);
		if (waited_jobid != -1) {
			return 1;
		} else {
			return 0;
		}
	} else {
		return 0;
	}
}

static int batch_queue_flux_create(struct batch_queue *q)
{
	batch_queue_set_option(q, "experimental", "yes");

	FILE *uptime_pipe = popen("flux uptime 2>&1", "r");
	if (!uptime_pipe) {
		return -1;
	}

	char buffer[BUFSIZ];
	while (fread(buffer, sizeof(char), sizeof(buffer) / sizeof(char), uptime_pipe) > 0) {
	}

	int uptime_status = pclose(uptime_pipe);
	uptime_status = WEXITSTATUS(uptime_status);
	if (uptime_status != EXIT_SUCCESS) {
		debug(D_BATCH, "batch_queue_flux_create failed: not connected to flux environment");
		return -1;
	}

	flux_job_info_table = itable_create(0);
	batch_queue_jobid_info_table = itable_create(0);

	return 0;
}

static int batch_queue_flux_free(struct batch_queue *q)
{
	if (flux_job_info_table) {
		struct flux_job_info *info;
		uint64_t flux_job_id;
		ITABLE_ITERATE(flux_job_info_table, flux_job_id, info)
		{
			delete_flux_job_info(info);
		}
		itable_delete(flux_job_info_table);
		flux_job_info_table = NULL;
	}

	if (batch_queue_jobid_info_table) {
		itable_delete(batch_queue_jobid_info_table);
		batch_queue_jobid_info_table = NULL;
	}

	return 0;
}

batch_queue_stub_port(flux);
batch_queue_stub_option_update(flux);

const struct batch_queue_module batch_queue_flux = {
		BATCH_QUEUE_TYPE_FLUX,
		"flux",

		batch_queue_flux_create,
		batch_queue_flux_free,
		batch_queue_flux_port,
		batch_queue_flux_option_update,

		batch_queue_flux_submit,
		batch_queue_flux_wait,
		batch_queue_flux_remove,
};

/* vim: set noexpandtab tabstop=8: */
