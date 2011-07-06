/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2005- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "batch_job.h"
#include "batch_job_internal.h"
#include "itable.h"
#include "mpi_queue.h"
#include "work_queue.h"

#include "debug.h"

#include "macros.h"
#include "process.h"
#include "stringtools.h"
#include "timestamp.h"

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <errno.h>
#include <signal.h>
#include <fcntl.h>

#include <sys/wait.h>
#include <sys/stat.h>
#include <sys/signal.h>

const char *batch_queue_type_string()
{
	return "local, condor, sge, moab, wq, hadoop, mpi-queue";
}

batch_queue_type_t batch_queue_type_from_string(const char *str)
{
	if(!strcmp(str, "condor"))
		return BATCH_QUEUE_TYPE_CONDOR;
	if(!strcmp(str, "sge"))
		return BATCH_QUEUE_TYPE_SGE;
	if(!strcmp(str, "moab"))
		return BATCH_QUEUE_TYPE_MOAB;
	if(!strcmp(str, "grid"))
		return BATCH_QUEUE_TYPE_GRID;
	if(!strcmp(str, "local"))
		return BATCH_QUEUE_TYPE_LOCAL;
	if(!strcmp(str, "unix"))
		return BATCH_QUEUE_TYPE_LOCAL;
	if(!strcmp(str, "wq"))
		return BATCH_QUEUE_TYPE_WORK_QUEUE;
	if(!strcmp(str, "workqueue"))
		return BATCH_QUEUE_TYPE_WORK_QUEUE;
	if(!strcmp(str, "wq-sharedfs"))
		return BATCH_QUEUE_TYPE_WORK_QUEUE_SHAREDFS;
	if(!strcmp(str, "workqueue-sharedfs"))
		return BATCH_QUEUE_TYPE_WORK_QUEUE_SHAREDFS;
	if(!strcmp(str, "xgrid"))
		return BATCH_QUEUE_TYPE_XGRID;
	if(!strcmp(str, "hadoop"))
		return BATCH_QUEUE_TYPE_HADOOP;
	if(!strcmp(str, "mpi"))
		return BATCH_QUEUE_TYPE_MPI_QUEUE;
	if(!strcmp(str, "mpi-queue"))
		return BATCH_QUEUE_TYPE_MPI_QUEUE;
	return BATCH_QUEUE_TYPE_UNKNOWN;
}

const char *batch_queue_type_to_string(batch_queue_type_t t)
{
	switch (t) {
	case BATCH_QUEUE_TYPE_LOCAL:
		return "local";
	case BATCH_QUEUE_TYPE_CONDOR:
		return "condor";
	case BATCH_QUEUE_TYPE_SGE:
		return "sge";
	case BATCH_QUEUE_TYPE_MOAB:
		return "moab";
	case BATCH_QUEUE_TYPE_GRID:
		return "grid";
	case BATCH_QUEUE_TYPE_WORK_QUEUE:
		return "wq";
	case BATCH_QUEUE_TYPE_WORK_QUEUE_SHAREDFS:
		return "wq-sharedfs";
	case BATCH_QUEUE_TYPE_XGRID:
		return "xgrid";
	case BATCH_QUEUE_TYPE_HADOOP:
		return "hadoop";
	case BATCH_QUEUE_TYPE_MPI_QUEUE:
		return "mpi-queue";
	default:
		return "unknown";
	}
}

struct batch_queue *batch_queue_create(batch_queue_type_t type)
{
	struct batch_queue *q;

	if(type == BATCH_QUEUE_TYPE_UNKNOWN)
		return 0;

	q = malloc(sizeof(*q));
	q->type = type;
	q->options_text = 0;
	q->job_table = itable_create(0);
	q->output_table = itable_create(0);
	q->hadoop_jobs = NULL;

	if(type == BATCH_QUEUE_TYPE_CONDOR)
		q->logfile = strdup("condor.logfile");
	else if(type == BATCH_QUEUE_TYPE_WORK_QUEUE || type == BATCH_QUEUE_TYPE_WORK_QUEUE_SHAREDFS)
		q->logfile = strdup("wq.log");
	else
		q->logfile = NULL;

	if(type == BATCH_QUEUE_TYPE_WORK_QUEUE || type == BATCH_QUEUE_TYPE_WORK_QUEUE_SHAREDFS) {
		q->work_queue = work_queue_create(0);
		if(!q->work_queue) {
			batch_queue_delete(q);
			return 0;
		}
	} else {
		q->work_queue = 0;
	}
	
	if(type == BATCH_QUEUE_TYPE_MPI_QUEUE) {
		q->mpi_queue = mpi_queue_create(0);
		if(!q->mpi_queue) {
			batch_queue_delete(q);
			return 0;
		}
	} else {
		q->mpi_queue = 0;
	}
	
	if(type == BATCH_QUEUE_TYPE_SGE || type == BATCH_QUEUE_TYPE_MOAB || type == BATCH_QUEUE_TYPE_GRID) {
		batch_job_setup_grid(q);
	}

	if(type == BATCH_QUEUE_TYPE_HADOOP) {
		int fail = 0;

	   if(!getenv("HADOOP_HOME")) {
				debug(D_NOTICE, "error: environment variable HADOOP_HOME not set\n");
				fail = 1;
			}
		if(!getenv("HDFS_ROOT_DIR")) {
				debug(D_NOTICE, "error: environment variable HDFS_ROOT_DIR not set\n");
				fail = 1;
		}
		if(!getenv("HADOOP_PARROT_PATH")) {
			/* Note: HADOOP_PARROT_PATH is the path to Parrot on the remote node, not on the local machine. */
			debug(D_NOTICE, "error: environment variable HADOOP_PARROT_PATH not set\n");
			fail = 1;
		}
	
		if(fail) {
			batch_queue_delete(q);
			return 0;
		}

		q->hadoop_jobs = itable_create(0);
	} else {
		q->hadoop_jobs = NULL;
	}

	return q;
}

void batch_queue_delete(struct batch_queue *q)
{
	if(q) {
		if(q->options_text)
			free(q->options_text);
		if(q->job_table)
			itable_delete(q->job_table);
		if(q->output_table)
			itable_delete(q->output_table);
		if(q->logfile)
			free(q->logfile);
		if(q->work_queue)
			work_queue_delete(q->work_queue);
		if(q->hadoop_jobs)
			itable_delete(q->hadoop_jobs);
		free(q);
	}
}

void batch_queue_set_logfile(struct batch_queue *q, const char *logfile)
{
	if(q->logfile)
		free(q->logfile);
	q->logfile = strdup(logfile);
}

void batch_queue_set_options(struct batch_queue *q, const char *options_text)
{
	if(q->options_text) {
		free(q->options_text);
	}

	if(options_text) {
		q->options_text = strdup(options_text);
	} else {
		q->options_text = 0;
	}
}

batch_job_id_t batch_job_submit(struct batch_queue *q, const char *cmd, const char *args, const char *infile, const char *outfile, const char *errfile, const char *extra_input_files, const char *extra_output_files)
{
	if(!q->job_table)
		q->job_table = itable_create(0);

	if(q->type == BATCH_QUEUE_TYPE_LOCAL) {
		return batch_job_submit_local(q, cmd, args, infile, outfile, errfile, extra_input_files, extra_output_files);
	} else if(q->type == BATCH_QUEUE_TYPE_CONDOR) {
		return batch_job_submit_condor(q, cmd, args, infile, outfile, errfile, extra_input_files, extra_output_files);
	} else if(q->type == BATCH_QUEUE_TYPE_SGE) {
		return batch_job_submit_grid(q, cmd, args, infile, outfile, errfile, extra_input_files, extra_output_files);
	} else if(q->type == BATCH_QUEUE_TYPE_MOAB) {
		return batch_job_submit_grid(q, cmd, args, infile, outfile, errfile, extra_input_files, extra_output_files);
	} else if(q->type == BATCH_QUEUE_TYPE_WORK_QUEUE) {
		return batch_job_submit_work_queue(q, cmd, args, infile, outfile, errfile, extra_input_files, extra_output_files);
	} else if(q->type == BATCH_QUEUE_TYPE_WORK_QUEUE_SHAREDFS) {
		return batch_job_submit_work_queue(q, cmd, args, infile, outfile, errfile, extra_input_files, extra_output_files);
	} else if(q->type == BATCH_QUEUE_TYPE_XGRID) {
		return batch_job_submit_xgrid(q, cmd, args, infile, outfile, errfile, extra_input_files, extra_output_files);
	} else if(q->type == BATCH_QUEUE_TYPE_HADOOP) {
		return batch_job_submit_hadoop(q, cmd, args, infile, outfile, errfile, extra_input_files, extra_output_files);
	} else if(q->type == BATCH_QUEUE_TYPE_MPI_QUEUE) {
		return batch_job_submit_mpi_queue(q, cmd, args, infile, outfile, errfile, extra_input_files, extra_output_files);
	} else {
		errno = EINVAL;
		return -1;
	}
}

batch_job_id_t batch_job_submit_simple(struct batch_queue * q, const char *cmd, const char *extra_input_files, const char *extra_output_files)
{
	if(!q->job_table)
		q->job_table = itable_create(0);

	if(q->type == BATCH_QUEUE_TYPE_LOCAL) {
		return batch_job_submit_simple_local(q, cmd, extra_input_files, extra_output_files);
	} else if(q->type == BATCH_QUEUE_TYPE_CONDOR) {
		return batch_job_submit_simple_condor(q, cmd, extra_input_files, extra_output_files);
	} else if(q->type == BATCH_QUEUE_TYPE_SGE) {
		return batch_job_submit_simple_grid(q, cmd, extra_input_files, extra_output_files);
	} else if(q->type == BATCH_QUEUE_TYPE_MOAB) {
		return batch_job_submit_simple_grid(q, cmd, extra_input_files, extra_output_files);
	} else if(q->type == BATCH_QUEUE_TYPE_WORK_QUEUE) {
		return batch_job_submit_simple_work_queue(q, cmd, extra_input_files, extra_output_files);
	} else if(q->type == BATCH_QUEUE_TYPE_WORK_QUEUE_SHAREDFS) {
		return batch_job_submit_simple_work_queue(q, cmd, extra_input_files, extra_output_files);
	} else if(q->type == BATCH_QUEUE_TYPE_XGRID) {
		return batch_job_submit_simple_xgrid(q, cmd, extra_input_files, extra_output_files);
	} else if(q->type == BATCH_QUEUE_TYPE_HADOOP) {
		return batch_job_submit_simple_hadoop(q, cmd, extra_input_files, extra_output_files);
	} else if(q->type == BATCH_QUEUE_TYPE_MPI_QUEUE) {
		return batch_job_submit_simple_mpi_queue(q, cmd, extra_input_files, extra_output_files);
	} else {
		errno = EINVAL;
		return -1;
	}
}

batch_job_id_t batch_job_wait(struct batch_queue * q, struct batch_job_info * info)
{
	return batch_job_wait_timeout(q, info, 0);
}

batch_job_id_t batch_job_wait_timeout(struct batch_queue * q, struct batch_job_info * info, time_t stoptime)
{
	if(!q->job_table)
		q->job_table = itable_create(0);

	if(q->type == BATCH_QUEUE_TYPE_LOCAL) {
		return batch_job_wait_local(q, info, stoptime);
	} else if(q->type == BATCH_QUEUE_TYPE_CONDOR) {
		return batch_job_wait_condor(q, info, stoptime);
	} else if(q->type == BATCH_QUEUE_TYPE_SGE) {
		return batch_job_wait_grid(q, info, stoptime);
	} else if(q->type == BATCH_QUEUE_TYPE_MOAB) {
		return batch_job_wait_grid(q, info, stoptime);
	} else if(q->type == BATCH_QUEUE_TYPE_WORK_QUEUE) {
		return batch_job_wait_work_queue(q, info, stoptime);
	} else if(q->type == BATCH_QUEUE_TYPE_WORK_QUEUE_SHAREDFS) {
		return batch_job_wait_work_queue(q, info, stoptime);
	} else if(q->type == BATCH_QUEUE_TYPE_XGRID) {
		return batch_job_wait_xgrid(q, info, stoptime);
	} else if(q->type == BATCH_QUEUE_TYPE_HADOOP) {
		return batch_job_wait_hadoop(q, info, stoptime);
	} else if(q->type == BATCH_QUEUE_TYPE_MPI_QUEUE) {
		return batch_job_wait_mpi_queue(q, info, stoptime);
	} else {
		errno = EINVAL;
		return -1;
	}
}

int batch_job_remove(struct batch_queue *q, batch_job_id_t jobid)
{
	if(!q->job_table)
		q->job_table = itable_create(0);

	if(q->type == BATCH_QUEUE_TYPE_LOCAL) {
		return batch_job_remove_local(q, jobid);
	} else if(q->type == BATCH_QUEUE_TYPE_CONDOR) {
		return batch_job_remove_condor(q, jobid);
	} else if(q->type == BATCH_QUEUE_TYPE_SGE) {
		return batch_job_remove_grid(q, jobid);
	} else if(q->type == BATCH_QUEUE_TYPE_MOAB) {
		return batch_job_remove_grid(q, jobid);
	} else if(q->type == BATCH_QUEUE_TYPE_WORK_QUEUE) {
		return batch_job_remove_work_queue(q, jobid);
	} else if(q->type == BATCH_QUEUE_TYPE_WORK_QUEUE_SHAREDFS) {
		return batch_job_remove_work_queue(q, jobid);
	} else if(q->type == BATCH_QUEUE_TYPE_XGRID) {
		return batch_job_remove_xgrid(q, jobid);
	} else if(q->type == BATCH_QUEUE_TYPE_HADOOP) {
		return batch_job_remove_hadoop(q, jobid);
	} else if(q->type == BATCH_QUEUE_TYPE_MPI_QUEUE) {
		return batch_job_remove_mpi_queue(q, jobid);
	} else {
		errno = EINVAL;
		return -1;
	}
}

int batch_queue_port(struct batch_queue *q)
{
	if(q->type == BATCH_QUEUE_TYPE_WORK_QUEUE) {
		return work_queue_port(q->work_queue);
	} else if(q->type == BATCH_QUEUE_TYPE_MPI_QUEUE) {
		return mpi_queue_port(q->mpi_queue);
	} else {
		return 0;
	}
}

