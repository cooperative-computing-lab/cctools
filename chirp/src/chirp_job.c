/*
Copyright (C) 2024 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

/*
This is a dummy implementation of the chirp_job interface,
meant as a placeholder while we work out a simpler implementation.
*/

#include "chirp_job.h"

#include <errno.h>
#include <unistd.h>

unsigned chirp_job_concurrency = 0;
int      chirp_job_enabled = 0;
pid_t    chirp_job_schedd = 0;
int      chirp_job_time_limit = 0;

int chirp_job_create (chirp_jobid_t *id, struct jx *j, const char *subject)
{
	errno = ENOTSUP;
	return -1;
}

int chirp_job_commit (chirp_jobid_t id, const char *subject)
{
	errno = ENOTSUP;
	return -1;
}

int chirp_job_kill (chirp_jobid_t id, const char *subject)
{
	errno = ENOTSUP;
	return -1;
}

int chirp_job_status (chirp_jobid_t id, const char *subject, buffer_t *B)
{
	errno = ENOTSUP;
	return -1;
}

int chirp_job_wait (chirp_jobid_t id, const char *subject, INT64_T timeout, buffer_t *B)
{
	errno = ENOTSUP;
	return -1;
}


int chirp_job_reap (chirp_jobid_t id, const char *subject)
{
	errno = ENOTSUP;
	return -1;
}

/* Note that the scheduler must run forever otherwise the parent thinks it crashed. */

int chirp_job_schedule (void)
{
	while(1) { sleep(10); }
	return -1;
}
