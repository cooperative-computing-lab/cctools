#include "copy_stream.h"
#include "create_dir.h"
#include "debug.h"
#include "disk_info.h"
#include "domain_name_cache.h"
#include "dpopen.h"
#include "file_cache.h"
#include "full_io.h"
#include "getopt.h"
#include "hierarchical_work_queue.h"
#include "itable.h"
#include "list.h"
#include "link.h"
#include "load_average.h"
#include "macros.h"
#include "memory_info.h"
#include "stringtools.h"
#include "timestamp.h"
#include "worker_comm.h"

#include <errno.h>
#include <fcntl.h>
#include <math.h>
#include <mpi.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#include <sys/select.h>
#include <sys/signal.h>
#include <sys/stat.h>
#include <sys/types.h>



#define FILE_CACHE_DEFAULT_PATH	"/tmp/wqh_cache"



//**********************************************************************
	static int default_comm_interface = WORKER_COMM_TCP;
	static int comm_port = WORK_QUEUE_DEFAULT_PORT;
	static int comm_default_port = WORK_QUEUE_DEFAULT_PORT;
	static char file_cache_path[WORK_QUEUE_LINE_MAX];
	static int abort_flag = 0;

	// Overall time to wait for communications before exiting 
	static int idle_timeout = 900;

	static struct worker *workerdata      = NULL;
	static struct itable *unfinished_jobs = NULL;



	
	static int comm_interface = WORKER_COMM_TCP;

	static struct list   *active_workers  = NULL;

	static struct list   *waiting_jobs    = NULL;
	static struct itable *active_jobs     = NULL;
	static struct list   *complete_jobs   = NULL;

	static struct itable     *file_table  = NULL;
	static struct file_cache *file_store  = NULL;

	// Time to wait for expected communications
	static int active_timeout = 3600;
	// Time to wait for optional communications
	static int short_timeout = 60;

	
//**********************************************************************

struct worker_job * worker_job_lookup_insert(struct itable *jobs, int jobid)
{
	struct worker_job *job = NULL;

	job = itable_lookup(jobs, jobid);
	if(!job) {
		job = malloc(sizeof(*job));
		memset(job, 0, sizeof(*job));
		job->id = jobid;
		job->input_files = list_create();
		job->output_files = list_create();
		itable_insert(jobs, jobid, job);
	}
	return job;
}


void handle_op(struct worker_comm *super_comm, struct worker_op *op)
{
	struct stat st;
	UINT64_T index;
	struct worker_file *fileptr = NULL;
	struct worker_job *job = NULL;
	
	debug(D_WQ, "Handling op type %d\n", op->type);
	switch(op->type) {
		case WORKER_OP_ROLE:
		{	int stats[3];
			debug(D_WQ, "op: ROLE\n");
			worker_comm_disconnect(super_comm);
			worker_comm_connect(super_comm, comm_interface, op->payload, op->id, active_timeout, short_timeout);
			workerdata->role = op->flags;
			worker_comm_send_id(super_comm, workerdata->workerid, workerdata->hostname);
			stats[0] = workerdata->cores;
			stats[1] = workerdata->ram;
			stats[2] = workerdata->disk;
			worker_comm_send_array(super_comm, WORKER_COMM_ARRAY_INT, stats, 3);
			return;
		}
		case WORKER_OP_WORKDIR:
			debug(D_WQ, "op: WORKDIR (%s)\n", op->name);
			if(stat(op->name, &st)) {
				debug(D_WQ, "Working directory (%s) does not exist\n", op->name);
				exit(1);
			}
			chdir(op->name);
			break;
	
		case WORKER_OP_CLEAR_CACHE:
			debug(D_WQ, "op: CLEAR CACHE\n");
			file_cache_cleanup(file_store);
			itable_firstkey(file_table);
			while(itable_nextkey(file_table, &index, (void **)&fileptr)) {
				free(fileptr);
				itable_remove(file_table, index);
			}
			break;
		case WORKER_OP_COMM_INTERFACE:
			debug(D_WQ, "op: SET INTERFACE\n");
			comm_interface = op->id;
			if(op->flags > 0)
				comm_port = op->flags;
			else
				comm_port = comm_default_port;
			break;
		case WORKER_OP_FILE:
			debug(D_WQ, "op: CREATE FILE id:%d (%s)\n", op->id, op->name);
			fileptr = itable_remove(file_table, op->id);
			if(fileptr && op->options & WORKER_FILE_FLAG_NOCLOBBER)
				break;
			if(!fileptr) {
				fileptr = malloc(sizeof(*fileptr));
				memset(fileptr, 0, sizeof(*fileptr));
			}
			else
				free(fileptr->filename);
			
			fileptr->id = op->id;
			
			if(op->options & WORKER_FILE_FLAG_REMOTEFS)
				fileptr->type = WORKER_FILE_REMOTE;
			else
				fileptr->type = WORKER_FILE_NORMAL;

			fileptr->filename = strdup(op->name);
			fileptr->flags = op->options;
			sprintf(fileptr->label, "%d.%s", fileptr->id, fileptr->filename);
		
			if(op->payload) {
				if(fileptr->payload)
					free(fileptr->payload);
				fileptr->payload = strdup(op->payload);
			}
			
			itable_insert(file_table, op->id, fileptr);
			break;
			
		case WORKER_OP_FILE_CHECK:
		{	int stats[4];

			debug(D_WQ, "op: CHECK FILE %d\n", op->id);
			memset(stats, 0, sizeof(stats));
			stats[0] = -1;
		
			fileptr = itable_lookup(file_table, op->id);
			if(fileptr) {
				char label[WORK_QUEUE_LINE_MAX];
				char cachename[WORK_QUEUE_LINE_MAX];
				struct stat64 st;

				stats[1] = fileptr->flags;
				
				if(fileptr->type == WORKER_FILE_REMOTE) {
					stats[1] |= WORKER_FILE_FLAG_REMOTEFS;
					sprintf(cachename, "%s", fileptr->payload);
				} else {
					sprintf(label, "%d.%s", fileptr->id, fileptr->filename);
					file_cache_contains(file_store, label, cachename);
				}
			
				if(!stat64(cachename, &st)) {
					stats[0] = st.st_size;
					stats[2] = st.st_mtime;
					stats[3] = st.st_mode;
				}
				debug(D_WQ, "\tfile %d (%s:%s) exists: %d %d %d %d\n", op->id, fileptr->label, fileptr->payload?fileptr->payload:"", stats[0], stats[1], stats[2], stats[3]);
			} else {
				debug(D_WQ, "\tfile %d missing\n", op->id);
				stats[1] = WORKER_FILE_FLAG_MISSING;
			}
			
			worker_comm_send_array(super_comm, WORKER_COMM_ARRAY_INT, stats, 4);
		
		}	break;
	
		case WORKER_OP_FILE_PUT:
			debug(D_WQ, "op: PUT FILE %d\n", op->id);
			fileptr = itable_lookup(file_table, op->id);
			if(fileptr) {
				char cachename[WORK_QUEUE_LINE_MAX];
				int fd;
				fileptr->type = op->options;
				if(fileptr->type == WORKER_FILE_NORMAL) {
					file_cache_contains(file_store, fileptr->label, cachename);
				} else if(fileptr->type == WORKER_FILE_REMOTE) {
					sprintf(cachename, "%s", fileptr->payload);
				}
				fd = open64(cachename, O_WRONLY | O_CREAT | O_SYNC | O_TRUNC, op->flags);
				full_write(fd, op->payload, op->payloadsize);
				close(fd);
				debug(D_WQ, "\tdone putting file %d\n", op->id);
			}
			break;
	
		case WORKER_OP_FILE_GET:
			debug(D_WQ, "op: RETRIEVE FILE %d\n", op->id);
			fileptr = itable_lookup(file_table, op->id);
			if(fileptr) {
				char cachename[WORK_QUEUE_LINE_MAX];
				struct stat st;
			
				if(fileptr->type == WORKER_FILE_NORMAL) {
					file_cache_contains(file_store, fileptr->label, cachename);
				} else if(fileptr->type == WORKER_FILE_REMOTE) {
					sprintf(cachename, "%s", fileptr->payload);
				}
			
				if(!stat(cachename, &st)) {
					worker_comm_send_file(super_comm, cachename, -1, 1);
				} else {
					worker_comm_send_buffer(super_comm, NULL, 0, 1);
				}
			} else {
				worker_comm_send_buffer(super_comm, NULL, -1, 1);
			}
			break;
	
		case WORKER_OP_RESULTS:
		{	int num_results = list_size(complete_jobs);
			debug(D_WQ, "op: GET RESULTS\n");
			worker_comm_send_array(super_comm, WORKER_COMM_ARRAY_INT, &num_results, 1);

			while(list_size(complete_jobs)) {
				job = list_pop_head(complete_jobs);
				worker_job_send_result(super_comm, job);
				hierarchical_work_queue_job_delete(job);
				job = NULL;
			}
		}	break;

		case WORKER_OP_JOB_DIRMAP:
			debug(D_WQ, "op: set JOB %d DIRMAP\n", op->jobid);
			job = worker_job_lookup_insert(unfinished_jobs, op->jobid);
			job->dirmap = malloc(op->payloadsize);
			job->dirmaplength = op->payloadsize;
			memcpy(job->dirmap, op->payload, op->payloadsize);
			break;
	
		case WORKER_OP_JOB_REQUIRES:
			debug(D_WQ, "op: set JOB %d REQUIRES file %d\n", op->jobid, op->id);
			job = worker_job_lookup_insert(unfinished_jobs, op->jobid);
			fileptr = itable_lookup(file_table, op->id);
			if(!fileptr) {
				fileptr = malloc(sizeof(*fileptr));
				memset(fileptr, 0, sizeof(*fileptr));
				fileptr->id = op->id;
				fileptr->type = WORKER_FILE_INCOMPLETE;
				itable_insert(file_table, op->id, fileptr);
			}
			list_push_tail(job->input_files, fileptr);
			break;
		
		case WORKER_OP_JOB_GENERATES:
			debug(D_WQ, "op: set JOB %d GENERATES file %d\n", op->jobid, op->id);
			job = worker_job_lookup_insert(unfinished_jobs, op->jobid);
			fileptr = itable_lookup(file_table, op->id);
			if(!fileptr) {
				fileptr = malloc(sizeof(*fileptr));
				memset(fileptr, 0, sizeof(*fileptr));
				fileptr->id = op->id;
				fileptr->type = WORKER_FILE_INCOMPLETE;
				itable_insert(file_table, op->id, fileptr);
			}
			list_push_tail(job->output_files, fileptr);
			break;
		
		case WORKER_OP_JOB_CMD:
			debug(D_WQ, "op: set JOB %d COMMAND %s\n", op->jobid, op->payload);
			job = worker_job_lookup_insert(unfinished_jobs, op->jobid);
			if(job->command)
				free(job->command);
			job->command = strdup(op->payload);
			job->commandlength = op->payloadsize;
			job->output_streams = op->options;
			break;
	
		case WORKER_OP_JOB_CLOSE:
			debug(D_WQ, "op: CLOSE JOB %d\n", op->jobid);
			job = itable_remove(unfinished_jobs, op->jobid);
			if(job) {
				job->status = WORKER_JOB_STATUS_READY;
				list_push_tail(waiting_jobs, job);
			}
			break;
	}
	debug(D_WQ, "Finished handling op\n");
}

void foreman_main()
{
	static struct list *checked_workers = NULL;
	struct list        *tmp_workers = NULL;
	struct list        *new_comms = NULL;
	struct worker      *current_worker = NULL;
	struct worker_job  *job = NULL;
	struct worker_file *fileptr = NULL;
	struct worker_op    results_request;
	struct worker_op    op;
	struct worker_comm *comm;
	static struct link *listen_link = NULL;
	static int          listen_port = -1;
	int num_waiting_jobs = 0;
	
	if(!checked_workers)
		checked_workers = list_create();

	memset(&results_request, 0, sizeof(results_request));
	results_request.type = WORKER_OP_RESULTS;

	/* Wait for new connections */
	if(comm_interface == WORKER_COMM_TCP && (!listen_link || listen_port != comm_port)) {
		if(listen_link)
			link_close(listen_link);
		link_serve(comm_port);
		listen_port = comm_port;
	}
	new_comms = worker_comm_accept_connections(comm_interface, listen_link, active_timeout, short_timeout);
	while((comm = list_pop_head(new_comms))) {
		int stats[3];
		current_worker = malloc(sizeof(*current_worker));
		memset(current_worker, 0, sizeof(*current_worker));
		current_worker->comm = comm;
		worker_comm_recv_array(comm, WORKER_COMM_ARRAY_INT, stats, 3);
		current_worker->cores = current_worker->open_cores = stats[0];
		current_worker->ram = stats[1];
		current_worker->disk = stats[2];
		current_worker->workerid = comm->mpi_rank;
		if(comm->hostname) {
			sprintf(current_worker->hostname, "%s", comm->hostname);
		}
	}

	/* Service each worker */
	num_waiting_jobs = list_size(waiting_jobs);
	while((current_worker = list_pop_head(active_workers))) {
		/* If the worker is full, see if has anything done */
		if(!current_worker->open_cores) {
			int num_results;
			num_results = worker_comm_test_results(current_worker->comm);
			/* If it has available results, fetch them */
			if(num_results > 0) {
				int i;
				current_worker->state = WORKER_STATE_AVAILABLE;
				current_worker->open_cores += num_results;
				for(i = 0; i < num_results; i++) {
					job = worker_job_receive_result(current_worker->comm, active_jobs);
					worker_job_fetch_files(current_worker->comm, job->output_files, file_store);
					list_push_tail(complete_jobs, job);
				}
			}
			/* If there was an actual response, but nothing was ready, ask it again for results */
			if(num_results == 0) {
				worker_comm_send_op(current_worker->comm, &results_request);
			}
		}
		/* If there are available jobs and the worker has any open spaces, give it a job */
		if(num_waiting_jobs && current_worker->open_cores) {
			/* May want to implement a more complicated (read: cache-aware) job selection algorithm */
			job = list_pop_head(waiting_jobs);
			num_waiting_jobs--;
			worker_job_send_files(current_worker->comm, job->input_files, job->output_files, file_store);
			if(job->dirmap) {
				memset(&op, 0, sizeof(op));
				op.type = WORKER_OP_JOB_DIRMAP;
				op.jobid = job->id;
				op.payloadsize = job->dirmaplength;
				op.payload = job->dirmap;
				worker_comm_send_op(current_worker->comm, &op);
			}
			list_first_item(job->input_files);
			memset(&op, 0, sizeof(op));
			while((fileptr = list_next_item(job->input_files))) {
				op.type = WORKER_OP_JOB_REQUIRES;
				op.jobid = job->id;
				op.id = fileptr->id;
				worker_comm_send_op(current_worker->comm, &op);
			}
			
			list_first_item(job->output_files);
			memset(&op, 0, sizeof(op));
			while((fileptr = list_next_item(job->output_files))) {
				op.type = WORKER_OP_JOB_GENERATES;
				op.jobid = job->id;
				op.id = fileptr->id;
				worker_comm_send_op(current_worker->comm, &op);
			}
			memset(&op, 0, sizeof(op));
			op.type = WORKER_OP_JOB_CMD;
			op.jobid = job->id;
			op.options = job->output_streams;
			op.payload = job->command;
			op.payloadsize = job->commandlength;
			worker_comm_send_op(current_worker->comm, &op);
			
			memset(&op, 0, sizeof(op));
			op.type = WORKER_OP_JOB_CLOSE;
			op.jobid = job->id;
			worker_comm_send_op(current_worker->comm, &op);
			
			itable_insert(active_jobs, job->id, job);
			
			current_worker->open_cores--;
			if((num_waiting_jobs && current_worker->open_cores))
				worker_comm_send_op(current_worker->comm, &results_request);
		}
		/* If the worker still has open cores, and there are any jobs available, send it to the end of the queue */
		if(num_waiting_jobs && current_worker->open_cores)
			list_push_tail(active_workers, current_worker);
		else /* Otherwise, mark it as visited and full */
			list_push_tail(checked_workers, current_worker);
	}
	
	/* Swap "checked" and "ready" lists */
	tmp_workers = active_workers;
	active_workers = checked_workers;
	checked_workers = tmp_workers;
}

void worker_main()
{
	struct worker_job *job = NULL;
	UINT64_T jobid;
	int num_ready_procs, maxfd = 0;
	fd_set worker_fds;
	struct timeval short_timeout_st;

	short_timeout_st.tv_sec = short_timeout;
	short_timeout_st.tv_usec = 0;
	
	
	/* If there are active jobs, run through the list and figure out which ones have output available */

	if(itable_size(active_jobs)) {
		debug(D_WQ, "Waiting on %d jobs\n", itable_size(active_jobs));
		FD_ZERO(&worker_fds);
		itable_firstkey(active_jobs);
		while(itable_nextkey(active_jobs, &jobid, (void **)&job)) {
			debug(D_WQ, "Checking status of job %d\n", jobid);
			if(job->output_streams & WORKER_JOB_OUTPUT_STDOUT) {
				debug(D_WQ, "\tchecking stdout\n");
				FD_SET(job->out_fd, &worker_fds);
				maxfd = MAX(job->out_fd+1, maxfd);
			}
			if(job->output_streams & WORKER_JOB_OUTPUT_STDERR) {
				debug(D_WQ, "\tchecking stderr\n");
				FD_SET(job->err_fd, &worker_fds);
				maxfd = MAX(job->err_fd+1, maxfd);
			}
		}
		num_ready_procs = select(maxfd, &worker_fds, NULL, NULL, &short_timeout_st);
	} else {
		num_ready_procs = 0;
	}


	/* for each process with any output, copy the output into the job's stdout or stderr buffer, and check for end of file */
	debug(D_WQ, "%d processes have output\n", num_ready_procs);
	if(num_ready_procs > 0) {
		itable_firstkey(active_jobs);
		while(itable_nextkey(active_jobs, &jobid, (void**)&job)) {
			int len;
			char *buffer;
			if(FD_ISSET(job->err_fd, &worker_fds)) {
				len = copy_stream_to_buffer(job->err, &buffer);
				job->stderr_buffer = realloc(job->stderr_buffer, job->stderr_buffersize+len);
				memcpy(job->stderr_buffer + job->stderr_buffersize, buffer, len);
				job->stderr_buffersize += len;
				free(buffer);
			}
			if(FD_ISSET(job->out_fd, &worker_fds)) {
				len = copy_stream_to_buffer(job->out, &buffer);
				job->stdout_buffer = realloc(job->stdout_buffer, job->stdout_buffersize+len);
				memcpy(job->stdout_buffer + job->stdout_buffersize, buffer, len);
				job->stdout_buffersize += len;
				free(buffer);
			}
			if( (!job->err || feof(job->err)) && (!job->out || feof(job->out)) ) {
				job = itable_remove(active_jobs, jobid);
				job->exit_code = multi_pclose(NULL, job->out, job->err, job->pid);
				job->out = job->err = NULL;
				job->status = WORKER_JOB_STATUS_COMPLETE;
				worker_job_check_files(job, file_store, WORKER_FILES_OUTPUT);
				job->finish_time = timestamp_get();
				list_push_tail(complete_jobs, job);
				workerdata->open_cores++;
				debug(D_WQ, "Job %d finished\n", job->id);
			}
		}
	}
	
	/* If cores are open, start running new jobs */
	debug(D_WQ, "worker has %d open cores and %d waiting jobs\n", workerdata->open_cores, list_size(waiting_jobs));
	while(workerdata->open_cores && list_size(waiting_jobs)) {
		job = list_pop_head(waiting_jobs);
		if(job) {
			FILE **out = NULL, **err = NULL;
			
			if(job->dirmap) {
				char *tok, *dir;
				dir = strtok_r(job->dirmap, ";", &tok);
				if(dir) do {
					create_dir(dir, 0700);
				} while((dir = strtok_r(NULL, ";", &tok)));
			}
			
			worker_job_check_files(job, file_store, WORKER_FILES_INPUT);
			if(job->status != WORKER_JOB_STATUS_READY) {
				list_push_tail(complete_jobs, job);
				continue;
			}
			if(job->output_streams == WORKER_JOB_OUTPUT_STDOUT) {
				out = &job->out;
			}
			if(job->output_streams == WORKER_JOB_OUTPUT_STDERR) {
				err = &job->err;
			}
			if(job->output_streams == WORKER_JOB_OUTPUT_COMBINED) {
				out = err = &job->out;
			}
			job->start_time = timestamp_get();
			job->pid = multi_popen(job->command, NULL, out, err);
			if(job->out)
				job->out_fd = fileno(job->out);
			if(job->err)
				job->err_fd = fileno(job->err);
			workerdata->open_cores--;
			itable_insert(active_jobs, job->id, job);
			debug(D_WQ, "started running job %d (pid %d)\n", job->id, job->pid);
		}
	}
}


static void handle_abort(int sig)
{
	abort_flag = 1;
}

static void show_help(const char *cmd)
{
	printf("Use: %s <masterhost> <port>\n", cmd);
	printf("where options are:\n");
	printf(" -a <time>      Abort after this much idle time during an active connection.\n");
	printf(" -d <subsystem> Enable debugging for this subsystem.\n");
	printf(" -f <path>      File cache path.\n");
	printf(" -m             Use MPI communication by default.\n");
	printf(" -o <file>      Send debugging to this file.\n");
	printf(" -p <port>      Listen for incoming connections on this port when in foreman mode.\n");
	printf(" -r <role>      Set initial role for this worker (foreman|worker).  Defaults to worker.\n");
	printf(" -t <time>      Abort after this amount of idle time without connection. (default=%ds)\n", idle_timeout);
	printf(" -v             Show version string\n");
	printf(" -h             Show this help screen\n");
}

int main(int argc, char *argv[])
{
	int super_port = WORK_QUEUE_DEFAULT_PORT;
	int stats[3];
	char* super_host;
	char c;
	UINT64_T memory_avail, disk_total;
	struct worker_comm *super_comm;
	struct worker_op op;
	int stoptime;
	abort_flag = 0;

	workerdata = malloc(sizeof(*workerdata));

	signal(SIGTERM, handle_abort);
	signal(SIGQUIT, handle_abort);
	signal(SIGINT, handle_abort);


	workerdata->role = WORKER_ROLE_WORKER;
	sprintf(file_cache_path, "%s", FILE_CACHE_DEFAULT_PATH);
	
	comm_port = WORK_QUEUE_DEFAULT_PORT;

	debug_config(argv[0]);

	while((c = getopt(argc, argv, "a:d:f:hmo:p:r:t:v")) != (char) -1) {
		switch (c) {
		case 'a':
			active_timeout = string_time_parse(optarg);
			break;
		case 'd':
			debug_flags_set(optarg);
			break;
		case 'f':
			sprintf(file_cache_path, "%s", optarg);
			break;
		case 'm':
			comm_interface = default_comm_interface = WORKER_COMM_MPI;
			break;
		case 'o':
			debug_config_file(optarg);
			break;
		case 'p':
			comm_port = atoi(optarg);
			break;
		case 'r':
			if(!strcmp(optarg, "foreman") || !strcmp(optarg, "f")) {
				workerdata->role = WORKER_ROLE_FOREMAN;
			} else {
				workerdata->role = WORKER_ROLE_WORKER;
			}
			break;
		case 't':
			idle_timeout = string_time_parse(optarg);
			break;
		case 'v':
			//show_version(argv[0]);
			return 0;
		
		case 'h':
		default:
			//show_help(argv[0]);
			return 1;
		}
	}

	
	if((argc - optind) != 2) {
		show_help(argv[0]);
		exit(1);
	}

	if(comm_interface == WORKER_COMM_MPI) {
		super_port = atoi(argv[optind]);
		MPI_Init(&argc, &argv);
		super_comm = worker_comm_connect(NULL, WORKER_COMM_MPI, NULL, super_port, active_timeout, short_timeout);
	} else {
		super_host = argv[optind];
		super_port = atoi(argv[optind + 1]);
		fprintf(stderr, "Attempting to connect via TCP: %s:%d (%d/%d)\n", super_host, super_port, active_timeout, short_timeout);
		stoptime = time(0) + idle_timeout;
		while(time(0) < stoptime && !abort_flag) {
			super_comm = worker_comm_connect(NULL, WORKER_COMM_TCP, super_host, super_port, active_timeout, short_timeout);
			if(!super_comm)
				sleep(5);
		}
	}
	if(!super_comm) {
		fprintf(stderr, "Unable to establish connection.\n");
		exit(1);
	}
	

	workerdata->cores = workerdata->open_cores = load_average_get_cpus();
	disk_info_get(file_cache_path, &workerdata->disk, &disk_total);
	memory_info_get(&memory_avail, &workerdata->ram);
	domain_name_cache_guess_short(workerdata->hostname);

	debug(D_WQ, "Sending worker info (%d, %s, %d, %d, %d)", workerdata->workerid, workerdata->hostname, workerdata->cores, workerdata->ram, workerdata->disk);
	worker_comm_send_id(super_comm, workerdata->workerid, workerdata->hostname);
	stats[0] = workerdata->cores;
	stats[1] = workerdata->ram;
	stats[2] = workerdata->disk;
	worker_comm_send_array(super_comm, WORKER_COMM_ARRAY_INT, stats, 3);


	unfinished_jobs = itable_create(0);
	waiting_jobs = list_create();
	active_jobs = itable_create(0);
	complete_jobs = list_create();

	file_table = itable_create(0);
	file_store = file_cache_init(file_cache_path);

	while(!abort_flag) {
		int result;

		//wait for a new op from current supervisor and handle it if there is one
		memset(&op, 0, sizeof(op));
//		debug(D_WQ, "started waiting for next op\n");
		result = worker_comm_receive_op(super_comm, &op);
//		debug(D_WQ, "finished waiting for next op\n");
		if(result >= 0)
			handle_op(super_comm, &op);

		// Proceed with role-based operation
		if(workerdata->role == WORKER_ROLE_FOREMAN)
			foreman_main();
		else
			worker_main();
		if(result < 0 && !itable_size(active_jobs) && !list_size(waiting_jobs)) {
			if(time(0) > stoptime)
				abort_flag = 1;
			else
				sleep(5);
		} else {
			stoptime = time(0) + idle_timeout;
		}

	}
	
	return 0;
}


