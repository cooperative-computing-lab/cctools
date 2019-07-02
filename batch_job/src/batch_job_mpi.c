/*
Copyright (C) 2019- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "batch_job.h"
#include "batch_job_internal.h"
#include "debug.h"
#include "process.h"
#include "macros.h"
#include "stringtools.h"

#include "hash_table.h"
#include "jx.h"
#include "jx_parse.h"
#include "jx_print.h"
#include "load_average.h"
#include "host_memory_info.h"
#include "int_sizes.h"
#include "itable.h"
#include "hash_table.h"
#include "list.h"
#include "xxmalloc.h"

#include <unistd.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <signal.h>
#include <math.h>
#include <assert.h>

#ifdef CCTOOLS_WITH_MPI

#include <mpi.h>

struct mpi_worker {
	char *name;
	int rank;
	int64_t memory;
	int64_t cores;
	int64_t avail_memory;
	int64_t avail_cores;
};

struct mpi_job {
	int64_t cores;
	int64_t memory;
	struct mpi_worker *worker;
	const char *cmd;
	batch_job_id_t jobid;
	const char *env;
	const char *infiles;
	const char *outfiles;
	time_t submitted;
};

/* Array of workers and properties, indexed by MPI rank. */

static struct mpi_worker *workers = 0;
static int nworkers = 0;

/*
Each job is entered into two data structures:
job_queue is an ordered queue of ready jobs waiting for a worker.
job_table is a table of all jobs in any state, indexed by jobid
*/

static struct list *job_queue = 0;
static struct itable *job_table = 0;

/* Each job is assigned a unique jobid returned by batch_job_submit. */

static batch_job_id_t jobid = 1;

static void mpi_send_string( int rank, const char *str )
{
	unsigned length = strlen(str);
	MPI_Send(&length, 1, MPI_UNSIGNED, rank, 0, MPI_COMM_WORLD);
	MPI_Send(str, length, MPI_CHAR, rank, 0, MPI_COMM_WORLD);
}

static char * mpi_recv_string( int rank )
{
	unsigned length;
	MPI_Recv(&length, 1, MPI_UNSIGNED, rank, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
	char *str = malloc(length+1);
	MPI_Recv(str, length, MPI_CHAR, rank, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
	str[length] = 0;
	return str;
}

static void mpi_send_jx( int rank, struct jx *j )
{
	char *str = jx_print_string(j);
	mpi_send_string(rank,str);
	free(str);
}

static struct jx * mpi_recv_jx( int rank )
{
	char *str = mpi_recv_string(rank);
	if(!str) return 0;
	struct jx * j = jx_parse_string(str);
	free(str);
	return j;
}

static struct mpi_worker * find_worker_for_job(  struct mpi_job *job )
{
	int i;
	for(i=1;i<nworkers;i++) {	
		struct mpi_worker *worker = &workers[i];
		if(job->cores>=worker->avail_cores && job->memory>=worker->avail_memory) {
			return worker;
		}
	}

	return 0;
}

void send_job_to_worker( struct mpi_job *job, struct mpi_worker *worker )
{
	struct jx *j = jx_object(0);
	jx_insert_string(j,"Orders","Execute");
	jx_insert_string(j,"CMD",job->cmd);
	jx_insert_integer(j,"ID",job->jobid);
	if(job->env) jx_insert_string(j,"ENV",job->env);
	if(job->infiles) jx_insert_string(j,"IN",job->infiles);
	if(job->outfiles) jx_insert_string(j,"OUT",job->outfiles);

	mpi_send_jx(worker->rank,j);

	worker->avail_cores -= job->cores;
	worker->avail_memory -= job->memory;
	job->worker = worker;
}

batch_job_id_t receive_result_from_worker( struct mpi_worker *worker, struct batch_job_info *info )
{
	struct jx *j = mpi_recv_jx(worker->rank);

	int jobid = jx_lookup_integer(j, "ID");

	struct mpi_job *job = itable_lookup(job_table,jobid);

	memset(info,0,sizeof(*info));

	info->submitted = job->submitted;
	info->started = jx_lookup_integer(j,"START");
	info->finished = jx_lookup_integer(j,"END");

	info->exited_normally = jx_lookup_integer(j,"NORMAL");
	if(info->exited_normally) {
		info->exit_code = jx_lookup_integer(j, "STATUS");
	} else {
		info->exit_signal = jx_lookup_integer(j, "SIGNAL");
	}

	jx_delete(j);

	worker->avail_cores += job->cores;
	worker->avail_memory += job->memory;

	return jobid;
}

static batch_job_id_t batch_job_mpi_submit(struct batch_queue *q, const char *cmd, const char *extra_input_files, const char *extra_output_files, struct jx *envlist, const struct rmsummary *resources)
{
	struct mpi_job *job = malloc(sizeof(*job));

	job->cmd = xxstrdup(cmd);
	job->cores = resources->cores;
	job->memory = resources->memory;
	job->env = envlist ? jx_print_string(envlist) : 0;
	job->infiles = extra_input_files;
	job->outfiles = extra_output_files;
	job->jobid = jobid++;
	job->submitted = time(0);
	
	list_push_tail(job_queue,job);
	itable_insert(job_table,jobid,job);

	return job->jobid;
}

static batch_job_id_t batch_job_mpi_wait(struct batch_queue *q, struct batch_job_info *info, time_t stoptime)
{
	struct mpi_job *job;
	struct mpi_worker *worker;

	/* Assign as many jobs as possible to available workers */

	list_first_item(job_queue);
	while((job = list_next_item(job_queue))) {
		worker = find_worker_for_job(job);
		if(worker) {
			list_remove(job_queue, job);
			send_job_to_worker(job,worker);
		}
	}

	/* Probe each of the workers for responses.  */

	int i;
	for(i=1;i<nworkers;i++) {
		MPI_Status mstatus;
		int flag=0;
		MPI_Iprobe(i, 0, MPI_COMM_WORLD, &flag, &mstatus);
		if(flag) {
			jobid = receive_result_from_worker(worker,info);
			return jobid;
		}
	}

	return -1;
}

static int batch_job_mpi_remove( struct batch_queue *q, batch_job_id_t jobid )
{
	struct mpi_job *job = itable_remove(job_table,jobid);
	if(job) {
		list_remove(job_queue,job);

		if(job->worker) {
			char *cmd = string_format("{\"Orders\":\"Cancel\", \"ID\":\"%li\"}", jobid);
			mpi_send_string(job->worker->rank,cmd);
			free(cmd);
		}

		// XXX also free job elements
		free(job);
	}

	return 0;
}

static int batch_queue_mpi_create(struct batch_queue *q)
{
	job_queue = list_create();
	job_table = itable_create(0);
	return 0;
}

static void batch_job_mpi_kill_workers()
{
	int i;
	for(i=1;i<nworkers;i++) {
		mpi_send_string(i,"{\"Orders\":\"Terminate\"}");
	}
}

static int batch_queue_mpi_free(struct batch_queue *q)
{
	batch_job_mpi_kill_workers();
	MPI_Finalize();
	return 0;
}

static void mpi_worker_handle_signal(int sig)
{
	//do nothing, so that way we can kill the children and clean it up
}

static void send_config_message()
{
	/* measure available local resources */
	int cores = load_average_get_cpus();
	uint64_t memtotal, memavail;
	host_memory_info_get(&memavail, &memtotal);

	/* send initial configuration message */
	struct jx *jmsg = jx_object(0);
	jx_insert_string(jmsg,"name",procname);
	jx_insert_integer(jmsg,"rank",rank);
	jx_insert_integer(jmsg,"cores",cores);
	jx_insert_integer(jmsg,"memory",memtotal);

	mpi_send_jx(0,jmsg);
	jx_delete(jmsg);
}

static void handle_terminate()
{
	debug(D_BATCH, "%i:%s Being told to terminate, calling finalize and returning\n", rank, procname);
	MPI_Finalize();
	exit(0);
}

static void handle_cancel( struct jx *msg )
{
	int jobid = jx_lookup_integer(msg, "ID");

	uint64_t pid;
	struct jx *job;

	itable_firstkey(job_table);
	while(itable_nextkey(job_table, &pid, (void **)&job)) {
		if(job->jobid==jobid) {
			debug(D_BATCH, "killing jobid %d pid %d",jobid,pid);
			kill(pid, SIGKILL);

			debug(D_BATCH, "waiting for jobid %d pid %d",jobid,pid);
			int status;
			waitpid(pid, &status, 0);

			debug(D_BATCH, "killed jobid %d pid %d",jobid,pid);
			job = itable_remove(job_table,pid);
			jx_delete(job);
			break;
		}
	}

	debug(D_BATCH,"jobid %d not found!",jobid);
}

int sandbox_create( struct jx *job, const char *sandbox )
{
	/*
	create_dir(sandbox);

	char *file_list = strdup(jx_lookup_string(job,"INF"));
	char *file = strtok(file_list, ",");
	while(file) {
		char *sandbox_name = string_format("%s/%s", sandbox, file);
		int result = link(file, sandbox_name);
		if(result<0) {
			copy_file(file,sandbox_name);
		       	if(result<0)  != 0) {
				free(tmp);
			}
			file = strtok(0, ",");
		}

	}
	*/
}

int sandbox_commit( struct jx *job, const char *sandbox )
{
	/*
	char *tmp;

	char *sandbox = string_format("%s/%u", workdir, gen_guid());

	// Create and move into the sandbox directory
	char *cmd = string_format("mkdir %s && cd %s && %s", sandbox, sandbox, jx_lookup_string(job,"CMD");


	// For each output file, perform a copy out of the sandbox
	char *file_list = strdup(jx_lookup_string(job,"OUTF"));
	char *file = strtok(file_list, ",");
	while(file) {
		tmp = string_format("%s && cp -rf ./%s %s/%s", cmd, file, cwd, file);
		free(cmd);
		cmd = tmp;
		file = strtok(0, ",");
	}
	free(file_list);

	// Finally, remove the sandbox when done.
	tmp = string_format("%s && rm -rf %s", cmd, sandbox);
	free(cmd);
	cmd = tmp;

	 free(cmd);
	*/
}

void handle_execute( struct jx *job )
{
	int jobid = jx_lookup_string(job,"ID");
	char *sandbox = 0;

	const char *workdir = jx_lookup_string(job,"WORKDIR");
	if(workdir) {
		sandbox = string_format("%s/job.%d",workdir,jobid);
		jx_insert_string(job,"SANDBOX",sandbox);
	} else {
		sandbox = 0;
	}

	int pid = fork();
	if(pid > 0) {
		debug(D_BATCH, "%i:%s created jobid %d pid %d",jobid,pid);
		itable_insert(job_table,pid,job);
		jx_insert_integer(job,"START",time(0));
	} else if(pid < 0) {
		debug(D_BATCH, "%i:%s there was an error that prevented forking: %s\n", rank, procname, strerror(errno));
		MPI_Finalize();
		exit(1);
	} else {
		if(env) {
			jx_export(env);
		}

		if(sandbox) {
			sandbox_create(job,sandbox);
			chdir(sandbox);
		}

		const char *cmd = jx_lookup_string(job,"CMD");

		execlp("sh", "sh", "-c", cmd, (char *) 0);
		debug(D_BATCH,"%i:%s failed to execute: %s",rank,procname,strerror(errno));
		_exit(127);
	}
}


void handle_complete( pid_t pid, int status )
{{
	struct jx *job = itable_lookup(job_table,pid);
	if(!job) {
		debug(D_BATCH,"%i:%s No job with pid %d found!",rank,procname,pid);
		return;
	}

	debug(D_BATCH,"%i:%s jobid %d pid %d has exited",rank,procname,jx_lookup_integer(job,"ID"),pid);

	jx_insert_integer(job,"END",time(0));

	if(WIFEXITED(status)) {
		jx_insert_integer(job,"NORMAL",1);
		jx_insert_integer(job,"STATUS",WEXITSTATUS(status));
	} else {
		jx_insert_integer(job,"NORMAL",0);
		jx_insert_integer(job,"SIGNAL",WTERMSIG(status));
	}

	const char *sandbox = jx_lookup_string(job,"SANDBOX");
	if(sandbox) {
		sandbox_commit(job,sandbox);
	}

	mpi_send_jx(0,job);
}

/*
Main loop for dedicated worker communicating with master.
*/

static int batch_job_mpi_worker(int worldsize, int rank, char *procname, int procnamelen)
{
	/* set up signal handlers to ignore signals */

	signal(SIGINT, mpi_worker_handle_signal);
	signal(SIGTERM, mpi_worker_handle_signal);
	signal(SIGQUIT, mpi_worker_handle_signal);

	send_config_message();

	// XXX workdir should get come from command line arguments
	char *workdir = "/tmp/makeflow-mpi";
	char *cwd = get_current_dir_name();

	/* job table contains the jx for each job, indexed by the running pid */

	job_table = itable_create(0);

	while(1) {
		MPI_Status mstatus;
		int flag;
		MPI_Iprobe(0, 0, MPI_COMM_WORLD, &flag, &mstatus);
		if(flag) {
			struct jx *msg = mpi_recv_jx(0);
			const char *order = jx_lookup_string(msg,"Orders");

			if(!strcmp(order,"Terminate")) {
				handle_terminate();
			} else if(!strcmp(order,"Cancel")) {
				handle_cancel(msg);
			} else if(!strcmp(order,"Execute")) {
				handle_execute(msg);
			} else {
				debug(D_BATCH,"unexpected order: %s",order);
			}
			jx_delete(msg);
		}

		int status;
		pid_t pid = waitpid(0, &status, WNOHANG);
		if(pid>0) handle_complete(pid,status);

		// XXX we have a busy waiting problem here...
	}

	MPI_Finalize();

	return 0;
}

/*
Perform the setup unique to the master process,
by setting up the table of workers and receiving
basic resource configuration.
*/

static void batch_job_mpi_master_setup(int mpi_world_size, int mpi_cores, int mpi_memory, const char *working_dir)
{
	int i;

	workers = calloc(mpi_world_size,sizeof(*workers));

	for(i = 1; i < mpi_world_size; i++) {
		struct mpi_worker *w = &workers[i];
		struct jx *j = mpi_recv_jx(i);
		w->name       = strdup(jx_lookup_string(j,"name"));
		w->rank       = 1;
		w->memory     = jx_lookup_integer(j,"memory");
		w->cores      = jx_lookup_integer(j,"cores");
		jx_delete(j);
	}
}

/*
Common initialization for both master and workers.
Determine the rank and then (if master) initalize and return,
or (if worker) continue on to the worker code.
*/

void batch_job_mpi_setup(int mpi_cores, int mpi_memory, const char *mpi_task_working_dir)
{
	int mpi_world_size;
	int mpi_rank;
	char procname[MPI_MAX_PROCESSOR_NAME];
	int procnamelen;

	printf("setting up MPI...\n");

	MPI_Init(NULL, NULL);
	MPI_Comm_size(MPI_COMM_WORLD, &mpi_world_size);
	MPI_Comm_rank(MPI_COMM_WORLD, &mpi_rank);
	MPI_Get_processor_name(procname, &procnamelen);

	debug(D_BATCH, "%i:%s My pid is: %i\n", mpi_rank, procname, getpid());

	if(mpi_rank == 0) {
		printf("MPI master process ready.\n");
		batch_job_mpi_master_setup(mpi_world_size, mpi_cores, mpi_memory, mpi_task_working_dir);
	} else {
		printf("MPI worker process ready.\n");
		int r = batch_job_mpi_worker(mpi_world_size, mpi_rank, procname, procnamelen);
		exit(r);
	}
}

batch_queue_stub_port(mpi);
batch_queue_stub_option_update(mpi);

batch_fs_stub_chdir(mpi);
batch_fs_stub_getcwd(mpi);
batch_fs_stub_mkdir(mpi);
batch_fs_stub_putfile(mpi);
batch_fs_stub_rename(mpi);
batch_fs_stub_stat(mpi);
batch_fs_stub_unlink(mpi);

const struct batch_queue_module batch_queue_mpi = {
	BATCH_QUEUE_TYPE_MPI,
	"mpi",

	batch_queue_mpi_create,
	batch_queue_mpi_free,
	batch_queue_mpi_port,
	batch_queue_mpi_option_update,

	{
	 batch_job_mpi_submit,
	 batch_job_mpi_wait,
	 batch_job_mpi_remove,},

	{
	 batch_fs_mpi_chdir,
	 batch_fs_mpi_getcwd,
	 batch_fs_mpi_mkdir,
	 batch_fs_mpi_putfile,
	 batch_fs_mpi_rename,
	 batch_fs_mpi_stat,
	 batch_fs_mpi_unlink,
	 },
};
#else

void batch_job_mpi_setup( int mpi_cores, int mpi_memory, const char *mpi_task_working_dir ) {
  fatal("makeflow: mpi support is not enabled: please reconfigure using --with-mpicc-path");

}

#endif

/* vim: set noexpandtab tabstop=4: */
