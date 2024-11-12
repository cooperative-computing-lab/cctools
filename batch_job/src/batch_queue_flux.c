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
#include "stringtools.h"
#include "xxmalloc.h"

#include <stdlib.h>
#include <stdio.h>
#include <errno.h>
#include <string.h>
#include <signal.h>
#include <sys/stat.h>

static batch_queue_id_t batch_queue_flux_submit(struct batch_queue *q, struct batch_job *bt)
{
	return 0;
}

static batch_queue_id_t batch_queue_flux_wait(struct batch_queue *q, struct batch_job_info *info_out, time_t stoptime)
{
	return 0;
}

static int batch_queue_flux_remove(struct batch_queue *q, batch_queue_id_t jobid)
{
	return 0;
}

static int batch_queue_flux_create(struct batch_queue *q)
{
	batch_queue_set_option(q, "experimental", "yes");
	return 0;
}

batch_queue_stub_free(flux);
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
