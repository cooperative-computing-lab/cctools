/*
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifdef CCTOOLS_WITH_MPI

#include "batch_job.h"
#include "batch_job_internal.h"
#include "debug.h"
#include "macros.h"
#include "stringtools.h"
#include "jx.h"
#include "jx_parse.h"
#include "jx_print.h"
#include "load_average.h"
#include "host_memory_info.h"
#include "itable.h"
#include "list.h"
#include "xxmalloc.h"

#include <unistd.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <signal.h>
#include <sys/wait.h>

#include <mpi.h>

struct mpi_worker {
	char *name;
	int rank;
	int memory;
	int cores;
	int avail_memory;
	int avail_cores;
};

struct mpi_job {
	int cores;
	int memory;
	struct mpi_worker *worker;
	char *cmd;
	batch_job_id_t jobid;
	struct jx *env;
	char *infiles;
	char *outfiles;
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

/* Workers use an exponentially increasing sleep time from 1ms to 100ms. */

#define MIN_SLEEP_TIME 1000
#define MAX_SLEEP_TIME 100000

/* Send a null-delimited C-string. */

static void mpi_send_string( int rank, const char *str )
{
	MPI_Send(str, strlen(str), MPI_CHAR, rank, 0, MPI_COMM_WORLD);
}

/* Receive a null-delimited C-string; result must be free()d when done. */

static char * mpi_recv_string( int rank )
{
	int length=0;
	MPI_Status status;

	MPI_Probe(rank,0,MPI_COMM_WORLD,&status);
	MPI_Get_count(&status,MPI_CHAR,&length);
	char *str = xxmalloc(length+1);
	MPI_Recv(str, length, MPI_CHAR, rank, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
	str[length] = 0;
	return str;
}

/* Send a JX object by serializing it to a string and sending it. */

static void mpi_send_jx( int rank, struct jx *j )
{
	char *str = jx_print_string(j);
	mpi_send_string(rank,str);
	free(str);
}

/*
Receive a jx object by receiving a string and parsing it.
The result must be freed with jx_delete().
*/

static struct jx * mpi_recv_jx( int rank )
{
	char *str = mpi_recv_string(rank);
	if(!str) fatal("failed to receive string from rank %d",rank);
	struct jx * j = jx_parse_string(str);
	if(!j) fatal("failed to parse string: %s",str);
	free(str);
	return j;
}

/*
Scan the list of workers and find the first available worker
with sufficient cores and memory to fit the job.
*/

static struct mpi_worker * find_worker_for_job(  struct mpi_job *job )
{
	int i;
	for(i=1;i<nworkers;i++) {	
		struct mpi_worker *worker = &workers[i];
		if(job->cores<=worker->avail_cores && job->memory<=worker->avail_memory) {
			return worker;
		}
	}

	return 0;
}

/*
Send a given job to a worker by describing it as a JX
object, and then note the committed resources at the worker.
*/

void send_job_to_worker( struct mpi_job *job, struct mpi_worker *worker )
{
	debug(D_BATCH,"assigned job %lld (%d cores, %d memory) to worker %d",job->jobid,job->cores,job->memory,worker->rank);

	struct jx *j = jx_object(0);
	jx_insert_string(j,"Action","Execute");
	jx_insert_string(j,"CMD",job->cmd);
	jx_insert_integer(j,"JOBID",job->jobid);
	if(job->env) jx_insert(j,jx_string("ENV"),job->env);
	if(job->infiles) jx_insert_string(j,"INFILES",job->infiles);
	if(job->outfiles) jx_insert_string(j,"OUTFILES",job->outfiles);

	mpi_send_jx(worker->rank,j);

	worker->avail_cores -= job->cores;
	worker->avail_memory -= job->memory;
	job->worker = worker;

	debug(D_BATCH,"worker %d now has %d cores %d memory available",worker->rank,worker->avail_cores,worker->avail_memory);
}

/*
Receive a result message from the worker, and convert the JX
representation into a batch_job_info structure.  Returns the
jobid of the completed job.
*/

static batch_job_id_t receive_result_from_worker( struct mpi_worker *worker, struct batch_job_info *info )
{
	struct jx *j = mpi_recv_jx(worker->rank);

	batch_job_id_t jobid = jx_lookup_integer(j, "JOBID");

	struct mpi_job *job = itable_lookup(job_table,jobid);
	if(!job) {
		/* This could happen if there is a race between Cancel and Complete. */
		debug(D_BATCH,"ignoring unexpected jobid %lld returned from worker %d",jobid,worker->rank);
		return -1;
	}

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

	debug(D_BATCH,"worker %d now has %d cores %d memory available",worker->rank,worker->avail_cores,worker->avail_memory);

	return jobid;
}

static struct mpi_job * mpi_job_create( const char *cmd, const char *extra_input_files, const char *extra_output_files, struct jx *envlist, const struct rmsummary *resources )
{
	struct mpi_job *job = malloc(sizeof(*job));

	job->cmd = xxstrdup(cmd);
	job->cores = resources->cores;
	job->memory = resources->memory;
	job->env = jx_copy(envlist);
	job->infiles = strdup(extra_input_files);
	job->outfiles = strdup(extra_output_files);
	job->jobid = jobid++;
	job->submitted = time(0);

	/*
	If resources are not set, assume at least one core,
	so as to avoid overcommitting workers.
	*/

	if(job->cores<=0) job->cores = 1;
	if(job->memory<=0) job->memory = 0;

	
	return job;
}

static void mpi_job_delete( struct mpi_job *job )
{
	if(!job) return;

	free(job->cmd);
	jx_delete(job->env);
	free(job->infiles);
	free(job->outfiles);
	free(job);
}

/*
Submit an MPI job by converting it into a struct mpi_job
and adding it to the job_queue and job table.
Returns a generated jobid for the job.
*/

static batch_job_id_t batch_job_mpi_submit(struct batch_queue *q, const char *cmd, const char *extra_input_files, const char *extra_output_files, struct jx *envlist, const struct rmsummary *resources)
{
	struct mpi_job * job = mpi_job_create(cmd,extra_input_files,extra_output_files,envlist,resources);

	list_push_tail(job_queue,job);
	itable_insert(job_table,job->jobid,job);

	return job->jobid;
}

/*
Wait for a job to complete within the given stoptime.
First assigns jobs to workers, if possible, then wait
for a response message from a worker.  Since we cannot
wait for an MPI message with a timeout, use an exponentially
increasing sleep time.
*/

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

	/* Check for incoming completions from workers. */

	int sleep_time = MIN_SLEEP_TIME;

	while(1) {
		MPI_Status mstatus;
		int flag=0;
		MPI_Iprobe(MPI_ANY_SOURCE, 0, MPI_COMM_WORLD, &flag, &mstatus);
		if(flag) {
			struct mpi_worker *worker = &workers[mstatus.MPI_SOURCE];
			jobid = receive_result_from_worker(worker,info);
			if(jobid>0) {
				debug(D_BATCH,"completed job %lld received from worker %d",jobid,worker->rank);
				return jobid;
			}
		}

		/* Return if time has expired. */
		if(time(0)>stoptime) break;

		/* Otherwise sleep for a while */
		usleep(sleep_time);
		sleep_time = MIN(sleep_time*2,MAX_SLEEP_TIME);
	}

	return -1;
}

/*
Remove a running job by taking it out of the job_table and job_queue,
and if necessary, send a remove message to the corresponding worker.
*/

static int batch_job_mpi_remove( struct batch_queue *q, batch_job_id_t jobid )
{
	struct mpi_job *job = itable_remove(job_table,jobid);
	if(job) {
		debug(D_BATCH,"removing job %lld",jobid);
		list_remove(job_queue,job);

		if(job->worker) {
			debug(D_BATCH,"cancelling job %lld on worker %d",jobid,job->worker->rank);
			char *cmd = string_format("{\"Action\":\"Remove\", \"JOBID\":\"%lld\"}", jobid);
			mpi_send_string(job->worker->rank,cmd);
			free(cmd);
		}

		mpi_job_delete(job);
	}

	return 0;
}

static int batch_queue_mpi_create(struct batch_queue *q)
{
	job_queue = list_create();
	job_table = itable_create(0);
	return 0;
}

/*
Send a message to all workers telling them to exit.
XXX This would be better implemented as a broadcast.
*/

static void batch_job_mpi_kill_workers()
{
	int i;
	debug(D_BATCH,"killing all mpi workers");
	for(i=1;i<nworkers;i++) {
		mpi_send_string(i,"{\"Action\":\"Terminate\"}");
	}
}

/*
Free the queue object by flushing the data structures,
and killing all workers.
*/

static int batch_queue_mpi_free(struct batch_queue *q)
{
	uint64_t jobid;
	struct mpi_job *job;

	itable_firstkey(job_table);
	while(itable_nextkey(job_table,&jobid,(void**)&job)) {
		mpi_job_delete(job);
	}

	list_delete(job_queue);
	itable_delete(job_table);

	batch_job_mpi_kill_workers();
	MPI_Finalize();
	return 0;
}

/**********************************/
/* MPI Worker Process Begins Here */
/**********************************/

static void mpi_worker_handle_signal(int sig)
{
	//do nothing, so that way we can kill the children and clean it up
}

/*
Send the initial configuration message from the worker
that describe the local resources and setup.
*/

static void mpi_worker_send_config_message( int rank, const char *procname )
{

	/* measure available local resources */
	int cores = load_average_get_cpus();
	uint64_t memtotal, memavail;
	host_memory_info_get(&memavail, &memtotal);
	memavail /= MEGA;
	memtotal /= MEGA;

	/* send initial configuration message */
	struct jx *jmsg = jx_object(0);
	jx_insert_string(jmsg,"name",procname);
	jx_insert_integer(jmsg,"rank",rank);
	jx_insert_integer(jmsg,"cores",cores);
	jx_insert_integer(jmsg,"memory",memtotal);

	mpi_send_jx(0,jmsg);
	jx_delete(jmsg);
}

/*
Terminate the worker process when requested.
*/

static void mpi_worker_handle_terminate()
{
	debug(D_BATCH,"terminating");
	MPI_Finalize();
	exit(0);
}

/*
Remove the indicated job from the job_table,
and if necessary, killing the process and waiting for it.
*/

static void mpi_worker_handle_remove( struct jx *msg )
{
	batch_job_id_t jobid = jx_lookup_integer(msg, "JOBID");

	uint64_t pid;
	struct jx *job;

	itable_firstkey(job_table);
	while(itable_nextkey(job_table, &pid, (void **)&job)) {
		if(jx_lookup_integer(job,"JOBID")==jobid) {
			debug(D_BATCH, "killing jobid %lld pid %llu",jobid,pid);
			kill(pid, SIGKILL);

			debug(D_BATCH, "waiting for jobid %lld pid %llu",jobid,pid);
			int status;
			waitpid(pid, &status, 0);

			debug(D_BATCH, "killed jobid %lld pid %llu",jobid,pid);
			job = itable_remove(job_table,pid);
			jx_delete(job);
			break;
		}
	}

	debug(D_BATCH,"jobid %lld not found!",jobid);
}

/*
Execute a job by forking a new process, setting
up the job environment, and executing the command.
If successful, the job object is installed in the job
table with the pid as the key.
*/

void mpi_worker_handle_execute( struct jx *job )
{
	batch_job_id_t jobid = jx_lookup_integer(job,"JOBID");
	const char *cmd = jx_lookup_string(job,"CMD");

	pid_t pid = fork();
	if(pid > 0) {
		debug(D_BATCH,"created jobid %lld pid %d: %s",jobid,pid,cmd);
		itable_insert(job_table,pid,jx_copy(job));
		jx_insert_integer(job,"START",time(0));
	} else if(pid < 0) {
		debug(D_BATCH,"error forking: %s\n",strerror(errno));
		MPI_Finalize();
		exit(1);
	} else {
		struct jx *env = jx_lookup(job,"ENV");
		if(env) {
			jx_export(env);
		}

		const char *cmd = jx_lookup_string(job,"CMD");

		execlp("/bin/sh", "sh", "-c", cmd, (char *) 0);
		debug(D_BATCH,"failed to execute: %s",strerror(errno));
		_exit(127);
	}
}

/*
Handle the completion of a given pid by looking up
the corresponding job in the job_table, and if found,
send back a completion message to the manager.
*/

void mpi_worker_handle_complete( pid_t pid, int status )
{
	struct jx *job = itable_lookup(job_table,pid);
	if(!job) {
		debug(D_BATCH,"No job with pid %d found!",pid);
		return;
	}

	debug(D_BATCH,"jobid %lld pid %d has exited",jx_lookup_integer(job,"JOBID"),pid);

	jx_insert_integer(job,"END",time(0));

	if(WIFEXITED(status)) {
		jx_insert_integer(job,"NORMAL",1);
		jx_insert_integer(job,"STATUS",WEXITSTATUS(status));
	} else {
		jx_insert_integer(job,"NORMAL",0);
		jx_insert_integer(job,"SIGNAL",WTERMSIG(status));
	}

	mpi_send_jx(0,job);
}

/*
Main loop for dedicated worker communicating with manager.
Note that there is no clear way to simultaneously wait for
an MPI message and also wait for a child process to exit.
When necessary, we alternate between a non-blocking MPI_Probe
and a non-blocking waitpid(), with an exponentially increasing
sleep in between.
*/

static int mpi_worker_main_loop(int worldsize, int rank, const char *procname )
{
	int sleep_time = MIN_SLEEP_TIME;

	/* set up signal handlers to ignore signals */
	signal(SIGINT, mpi_worker_handle_signal);
	signal(SIGTERM, mpi_worker_handle_signal);
	signal(SIGQUIT, mpi_worker_handle_signal);

	/* send the initial resource information. */
	mpi_worker_send_config_message(rank,procname);

	/* job table contains the jx for each job, indexed by the running pid */
	job_table = itable_create(0);

	while(1) {
		MPI_Status mstatus;
		int flag=0;
		int action_count=0;

		/*
		If jobs are running, do a non-blocking check for messages.
		Otherwise, go right to a blocking message receive.
		*/

		int jobs_running = itable_size(job_table);
		if(jobs_running>0) {
			MPI_Iprobe(0, 0, MPI_COMM_WORLD, &flag, &mstatus);
		}

		if(flag || jobs_running==0) {
			struct jx *msg = mpi_recv_jx(0);
			const char *action = jx_lookup_string(msg,"Action");

			debug(D_BATCH,"got message with action %s",action);

			if(!strcmp(action,"Terminate")) {
				mpi_worker_handle_terminate();
			} else if(!strcmp(action,"Remove")) {
				mpi_worker_handle_remove(msg);
			} else if(!strcmp(action,"Execute")) {
				mpi_worker_handle_execute(msg);
			} else {
				debug(D_BATCH,"unexpected action: %s",action);
			}
			jx_delete(msg);
			action_count++;
		}

		/* Check for any finished jobs and deal with them. */

		int status;
		pid_t pid = waitpid(0, &status, WNOHANG);
		if(pid>0) {
			mpi_worker_handle_complete(pid,status);
			action_count++;
		}

		/* If some jobs are running and nothing happened, then sleep. */

		jobs_running = itable_size(job_table);
		if(action_count==0 && jobs_running) {
			usleep(sleep_time);
			sleep_time = MIN(sleep_time*2,MAX_SLEEP_TIME);
		} else {
			sleep_time = MIN_SLEEP_TIME;
		}
	}

	MPI_Finalize();

	return 0;
}

/*
Perform the setup unique to the manager process,
by setting up the table of workers and receiving
basic resource configuration.

Note that the goal here is to get one active worker
per machine that supervises all of the memory and cores
on that node.  Most MPI implementations will give us
one MPI rank per core on machine, which is not what
we want.

To work around this, we note each duplicate rank on
a system, note it has having no cores and memory,
and send it a terminate message, leaving one active
worker per machine.
*/

static void batch_job_mpi_manager_setup(int mpi_world_size, int manual_cores, int manual_memory )
{
	int i;

	workers = calloc(mpi_world_size,sizeof(*workers));
	nworkers = mpi_world_size;

	const char *prev_name = 0;

	debug(D_BATCH,"rank 0 (manager)");

	for(i = 1; i < mpi_world_size; i++) {
		struct mpi_worker *w = &workers[i];
		struct jx *j = mpi_recv_jx(i);
		w->name       = strdup(jx_lookup_string(j,"name"));
		w->rank       = i;
		w->memory     = manual_memory ? manual_memory : jx_lookup_integer(j,"memory");
		w->cores      = manual_cores ? manual_cores : jx_lookup_integer(j,"cores");
		w->avail_memory = w->memory;
		w->avail_cores = w->cores;
		jx_delete(j);

		if(prev_name && !strcmp(w->name,prev_name)) {
			debug(D_BATCH,"rank %d [merged with %s]",w->rank,w->name);
			w->avail_memory = 0;
			w->avail_cores = 0;
			mpi_send_string(w->rank,"{\"Action\":\"Terminate\"}");
		} else {
			debug(D_BATCH,"rank %d (%s) %d cores %d MB memory",w->rank,w->name,w->cores,w->memory);
			prev_name = w->name;
		}
	}
}

/*
Common initialization for both manager and workers.
Determine the rank and then (if manager) initalize and return,
or (if worker) continue on to the worker code.
*/

void batch_job_mpi_setup( const char *debug_filename, int manual_cores, int manual_memory )
{
	int mpi_world_size;
	int mpi_rank;
	char procname[MPI_MAX_PROCESSOR_NAME];
	int procnamelen;

	MPI_Comm_size(MPI_COMM_WORLD, &mpi_world_size);
	MPI_Comm_rank(MPI_COMM_WORLD, &mpi_rank);
	MPI_Get_processor_name(procname, &procnamelen);

	if(mpi_rank == 0) {
		printf("MPI manager process ready.\n");
		batch_job_mpi_manager_setup(mpi_world_size, manual_cores, manual_memory );
	} else {
		printf("MPI worker process ready.\n");
		procname[procnamelen] = 0;
		debug_config(string_format("%d:%s",mpi_rank,procname));
		debug_config_file(string_format("%s.rank.%d",debug_filename,mpi_rank));
		debug_reopen();
		int r = mpi_worker_main_loop(mpi_world_size, mpi_rank, procname);
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

#include "debug.h"

void batch_job_mpi_setup( const char *debug_file_name, int manual_cores, int manual_memory)
{
	fatal("makeflow: mpi support is not enabled: please reconfigure using --with-mpi-path");
}

#endif

/* vim: set noexpandtab tabstop=8: */
