#ifndef BATCH_JOB_INTERNAL_H_
#define BATCH_JOB_INTERNAL_H_

#include "batch_job.h"
#include "itable.h"
#include "mpi_queue.h"
#include "work_queue.h"
#include "hierarchical_work_queue.h"

#define BATCH_JOB_LINE_MAX 8192

struct batch_queue {
	batch_queue_type_t type;
	char *logfile;
	char *options_text;
	struct itable *job_table;
	struct itable *output_table;
	struct itable *hadoop_jobs;
	struct work_queue *work_queue;
	struct mpi_queue *mpi_queue;
	struct hierarchical_work_queue *hierarchical_work_queue;
};

batch_job_id_t batch_job_submit_simple_local(struct batch_queue * q, const char *cmd, const char *extra_input_files, const char *extra_output_files);
batch_job_id_t batch_job_submit_local(struct batch_queue * q, const char *cmd, const char *args, const char *infile, const char *outfile, const char *errfile, const char *extra_input_files, const char *extra_output_files);
batch_job_id_t batch_job_wait_local(struct batch_queue * q, struct batch_job_info * info_out, time_t stoptime);
int batch_job_remove_local(struct batch_queue *q, batch_job_id_t jobid);

batch_job_id_t batch_job_submit_simple_condor(struct batch_queue * q, const char *cmd, const char *extra_input_files, const char *extra_output_files);
batch_job_id_t batch_job_submit_condor(struct batch_queue * q, const char *cmd, const char *args, const char *infile, const char *outfile, const char *errfile, const char *extra_input_files, const char *extra_output_files);
batch_job_id_t batch_job_wait_condor(struct batch_queue * q, struct batch_job_info * info_out, time_t stoptime);
int batch_job_remove_condor(struct batch_queue *q, batch_job_id_t jobid);

int batch_job_setup_grid(struct batch_queue *q);
batch_job_id_t batch_job_submit_simple_grid(struct batch_queue * q, const char *cmd, const char *extra_input_files, const char *extra_output_files);
batch_job_id_t batch_job_submit_grid(struct batch_queue * q, const char *cmd, const char *args, const char *infile, const char *outfile, const char *errfile, const char *extra_input_files, const char *extra_output_files);
batch_job_id_t batch_job_wait_grid(struct batch_queue * q, struct batch_job_info * info_out, time_t stoptime);
int batch_job_remove_grid(struct batch_queue *q, batch_job_id_t jobid);

batch_job_id_t batch_job_submit_simple_moab(struct batch_queue * q, const char *cmd, const char *extra_input_files, const char *extra_output_files);
batch_job_id_t batch_job_submit_moab(struct batch_queue * q, const char *cmd, const char *args, const char *infile, const char *outfile, const char *errfile, const char *extra_input_files, const char *extra_output_files);
batch_job_id_t batch_job_wait_moab(struct batch_queue * q, struct batch_job_info * info_out, time_t stoptime);
int batch_job_remove_moab(struct batch_queue *q, batch_job_id_t jobid);

batch_job_id_t batch_job_submit_simple_work_queue(struct batch_queue * q, const char *cmd, const char *extra_input_files, const char *extra_output_files);
batch_job_id_t batch_job_submit_work_queue(struct batch_queue * q, const char *cmd, const char *args, const char *infile, const char *outfile, const char *errfile, const char *extra_input_files, const char *extra_output_files);
batch_job_id_t batch_job_wait_work_queue(struct batch_queue * q, struct batch_job_info * info_out, time_t stoptime);
int batch_job_remove_work_queue(struct batch_queue *q, batch_job_id_t jobid);

batch_job_id_t batch_job_submit_simple_mpi_queue(struct batch_queue * q, const char *cmd, const char *extra_input_files, const char *extra_output_files);
batch_job_id_t batch_job_submit_mpi_queue(struct batch_queue * q, const char *cmd, const char *args, const char *infile, const char *outfile, const char *errfile, const char *extra_input_files, const char *extra_output_files);
batch_job_id_t batch_job_wait_mpi_queue(struct batch_queue * q, struct batch_job_info * info_out, time_t stoptime);
int batch_job_remove_mpi_queue(struct batch_queue *q, batch_job_id_t jobid);

batch_job_id_t batch_job_submit_simple_hadoop(struct batch_queue * q, const char *cmd, const char *extra_input_files, const char *extra_output_files);
batch_job_id_t batch_job_submit_hadoop(struct batch_queue * q, const char *cmd, const char *args, const char *infile, const char *outfile, const char *errfile, const char *extra_input_files, const char *extra_output_files);
batch_job_id_t batch_job_wait_hadoop(struct batch_queue * q, struct batch_job_info * info_out, time_t stoptime);
int batch_job_remove_hadoop(struct batch_queue *q, batch_job_id_t jobid);

batch_job_id_t batch_job_submit_simple_xgrid(struct batch_queue * q, const char *cmd, const char *extra_input_files, const char *extra_output_files);
batch_job_id_t batch_job_submit_xgrid(struct batch_queue * q, const char *cmd, const char *args, const char *infile, const char *outfile, const char *errfile, const char *extra_input_files, const char *extra_output_files);
batch_job_id_t batch_job_wait_xgrid(struct batch_queue * q, struct batch_job_info * info_out, time_t stoptime);
int batch_job_remove_xgrid(struct batch_queue *q, batch_job_id_t jobid);

batch_job_id_t batch_job_submit_simple_hierarchical_work_queue(struct batch_queue * q, const char *cmd, const char *extra_input_files, const char *extra_output_files);
batch_job_id_t batch_job_submit_hierarchical_work_queue(struct batch_queue * q, const char *cmd, const char *args, const char *infile, const char *outfile, const char *errfile, const char *extra_input_files, const char *extra_output_files);
batch_job_id_t batch_job_wait_hierarchical_work_queue(struct batch_queue * q, struct batch_job_info * info_out, time_t stoptime);
int batch_job_remove_hierarchical_work_queue(struct batch_queue *q, batch_job_id_t jobid);


#endif

