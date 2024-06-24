/*
Copyright (C) 2024 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef CHIRP_JOB_H
#define CHIRP_JOB_H

#include "chirp_types.h"
#include "jx.h"

extern unsigned chirp_job_concurrency;
extern int      chirp_job_enabled;
extern pid_t    chirp_job_schedd;
extern int      chirp_job_time_limit;

enum {
	CHIRP_JOB_WAIT_MAX_TIMEOUT = 30
};

int chirp_job_create (chirp_jobid_t *id, struct jx *j, const char *subject);

int chirp_job_commit (chirp_jobid_t id, const char *subject);

int chirp_job_kill (chirp_jobid_t id, const char *subject);

int chirp_job_status (chirp_jobid_t id, const char *subject, buffer_t *B);

int chirp_job_wait (chirp_jobid_t id, const char *subject, INT64_T timeout, buffer_t *B);

int chirp_job_reap (chirp_jobid_t id, const char *subject);

int chirp_job_schedule (void);

#endif

/* vim: set noexpandtab tabstop=8: */
