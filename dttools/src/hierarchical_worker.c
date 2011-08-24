#include "debug.h"
#include "disk_info.h"
#include "domain_name_cache.h"
#include "file_cache.h"
#include "getopt.h"
#include "itable.h"
#include "list.h"
#include "link.h"
#include "memory_info.h"
#include "worker_comm.h"

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

#define FILE_CACHE_DEFAULT_PATH "/tmp/wqh_fc"

#define WORKER_ROLE_WORKER	0x01
#define WORKER_ROLE_FOREMAN	0x02

struct worker_file {
	int id;
	int type;
	char *filename;
	int flags;
	int options;
	char *payload;
	int size;
	char label[WQ_FILENAME_MAX];
};


struct worker_job {
	char *command;
	int commandlength;
	char *dirmap
	int dirmaplength;
	int options;

	int status;
	int result;

	char *stdout_buffer;
	int stdout_buffersize;
	char *stderr_buffer;
	int stderr_buffersize;

	struct list *input_files;
	struct list *output_files;
	
	FILE *out;
	int out_fd;
	FILE *err;
	int err_fd;
};

struct worker {
	int workerid;
	char hostname[WQ_FILENAME_MAX];
	int cores;
	int ram;
	int disk;
	
	int role;
	int *jobids;
	struct worker_comm *comm;
};


enum worker_op_type {
	WORKER_OP_ROLE,
	WORKER_OP_WORKDIR,
	WORKER_OP_CLEAR_CACHE,
	WORKER_OP_COMM_INTERFACE,
	WORKER_OP_RESULTS,

	WORKER_OP_FILE,
	WORKER_OP_FILE_CHECK,
	WORKER_OP_FILE_PUT,
	WORKER_OP_FILE_GET,

	WORKER_OP_JOB_DIRMAP,
	WORKER_OP_JOB_REQUIRES,
	WORKER_OP_JOB_GENERATES,
	WORKER_OP_JOB_CMD,
	WORKER_OP_JOB_CLOSE,
	WORKER_OP_JOB_CANCEL
};

WORKER_FILES_INPUT
WORKER_FILES_OUTPUT

WORKER_FILE_INCOMPLETE
WORKER_FILE_NORMAL
WORKER_FILE_REMOTE

#define WORKER_FILE_FLAG_NOCLOBBER		0x01
#define WORKER_FILE_FLAG_REMOTEFS		0x02
#define WORKER_FILE_FLAG_CACHEABLE		0x04
#define WORKER_FILE_FLAG_MISSING		0x08
#define WORKER_FILE_FLAG_OPTIONAL		0x10
#define WORKER_FILE_FLAG_IGNORE		0x20

#define WORKER_JOB_STATUS_READY		0x01
#define WORKER_JOB_STATUS_MISSING_FILE		0x02
#define WORKER_JOB_STATUS_FAILED_SYMLINK	0x03
#define WORKER_JOB_STATUS_COMPLETE		0x04

#define WORKER_JOB_OUTPUT_STDOUT		0x01
#define WORKER_JOB_OUTPUT_STDERR		0x02
#define WORKER_JOB_OUTPUT_COMBINED		0x03


#define WORKER_STATE_AVAILABLE			0x01
#define WORKER_STATE_BUSY			0x02
#define WORKER_STATE_UNRESPONSNIVE		0x03


//**********************************************************************
	static int comm_interface = WORKER_COMM_TCP;
	static int comm_port = WQ_DEFAULT_PORT;
	static int comm_default_port = WQ_DEFAULT_PORT;
	static char file_cache_path[WQ_FILENAME_MAX];

	// Overall time to wait for communications before exiting 
	static int idle_timeout = 900;
	// Time to wait for expected communications
	static int active_timeout = 3600;
	// Time to wait for optional communications
	static int short_timeout = 60;

	static struct worker *workerdata      = NULL;
	static struct list   *active_workers  = NULL;

	static struct itable *unfinished_jobs = NULL;
	static struct list   *waiting_jobs    = NULL;
	static struct itable *active_jobs     = NULL;
	static struct list   *complete_jobs   = NULL;

	static struct itable     *file_table  = NULL;
	static struct file_cache *file_store  = NULL;
//**********************************************************************

struct worker_job * worker_job_lookup(struct itable *jobs, int jobid)
{
	struct worker_job *job = NULL;

	job = itable_lookup(jobs, jobid);
	if(!job) {
		job = malloc(sizeof(*job));
		memset(job, 0, sizeof(*job));
		job->input_files = list_create();
		job->output_files = list_create();
		itable_insert(jobs, jobid, job);
	}
	return job;
}

void worker_job_check_files(struct worker_job *job, int filetype)
{
	struct worker_file *fileptr;
	struct list *files;
	
	if(filetype == WORKER_FILES_INPUT) {
		files = job->input_files;
	} else {
		files = job->output_files;
	}
	
	list_first_item(files);
	while((fileptr = list_next_item(files))) {
		char filename[WQ_FILENAME_MAX];
		struct stat st;
		
		if(fileptr->type == WORKER_FILE_NORMAL) {
			if(filetype == WORKER_FILES_INPUT) {
				file_cache_contains(file_store, fileptr->label, filename);
			} else {
				sprintf(filename, "%s", fileptr->filename);
			}
		} else if(fileptr->type == WORKER_FILE_REMOTE) {
			sprintf(filename, "%s", fileptr->payload);
		} else if(fileptr->type == WORKER_FILE_INCOMPLETE) {
			job->status = WORKER_JOB_STATUS_MISSING_FILE;
			break;
		}
		
		if(stat(filename, &st)) {
			if(fileptr->options & WORKER_FILE_FLAG_OPTIONAL)
				continue;
			job->status = WORKER_JOB_STATUS_MISSING_FILE;
			break;
		}
		
		
		if(fileptr->type == WORKER_FILES_INPUT) {
			if(!strcmp(fileptr->filename, filename)) {
				continue;
			}
			
			if(stat(fileptr->filename, &st) != 0 && symlink(filename, fileptr->filename) != 0) {
				job->stderr_buffer = strdup(strerror(errno));
				job->stderr_buffersize = strlen(job->stderr_buffer);
				job->status = WORKER_JOB_STATUS_FAILED_SYMLINK;
				break;
			}
		} else {
			if(fileptr->type == WORKER_FILE_REMOTE)
				continue;
				
			lstat(fileptr->filename, &st);
			if(st.st_mode & S_IFREG) {
				FILE *reg;
				int cache_fd;
				file_cache_contains(file_store, fileptr->label, filename);
				reg = fopen(fileptr->filename, "r");
				cache_fd = open64(filename, O_WRONLY | O_CREAT | O_SYNC | O_TRUNC, fileptr->flags);
				copy_stream_to_fd(reg, cache_fd);
				fclose(reg);
				close(cache_fd);
			}
		}
	}
}

void worker_job_send_result(struct worker_comm *comm, struct worker_job *job);
{
	int results_buffer[3];
	
	results_buffer[0] = job->id;
	results_buffer[1] = job->status;
	results_buffer[2] = job->result;
	
	worker_comm_send_array(comm, WORK_COMM_ARRAY_INT, results_buffer, 3);
	worker_comm_send_buffer(comm, job->stdout_buffer, job->stdout_buffersize, 1);
	worker_comm_send_buffer(comm, job->stderr_buffer, job->stderr_buffersize, 1);
	return 0;
}

struct worker_job * worker_job_receive_result(struct worker_comm *comm, struct itable *jobs)
{
	int jobid, stdout_buffersize, stderr_buffersize;
	char *stdout_buffer, *stderr_buffer;
	struct worker_job *job;
	
	worker_comm_recv_array(comm, WORKER_COMM_ARRAY_INT, results_buffer, 3);
	worker_comm_recv_buffer(comm, &stdout_buffer, &stdout_buffersize, 1);
	worker_comm_recv_buffer(comm, &stderr_buffer, &stderr_buffersize, 1);

	job = itable_remove(jobs, results_buffer[0]);

	if(!job) {
		job = malloc(sizeof(*job));
		job->id = results_buffer[0]; 
	}

	job->status = results_buffer[1];
	job->result = results_buffer[2];
	job->stdout_buffer = stdout_buffer;
	job->stdout_buffersize = stdout_buffersize;
	job->stderr_buffer = stderr_buffer;
	job->stderr_buffersize = stderr_buffersize;

	return job;
}

void worker_job_delete(struct worker_job *job)
{
	if(job->command)
		free(job->command);
	if(job->dirmap)
		free(job->dirmap);
	if(job->stdout_buffer)
		free(job->stdout_buffer);
	if(job->stderr_buffer)
		free(job->stderr_buffer);
	if(job->input_files)
		list_delete(job->input_files);
	if(job->output_files)
		list_delete(job->output_files);
	if(job->out)
		fclose(job->out);
	if(job->err)
		fclose(job->err);
	free(job);
}

int handle_files(struct worker_comm *comm, struct list *files, int direction)
{
	struct worker_file *file;
	struct worker_op op;
	struct stat st;

	list_first_item(files);
	while((fileptr = list_next_item(files))) {

		int file_status[3];
		char cachename[WQ_FILENAME_MAX];
		char *buffer;
		int buffersize;
		
		memset(&op, 0, sizeof(op));
		op.type = WORKER_OP_FILE_CHECK;
		op.id = fileptr->id;
		worker_comm_send_op(comm, &op);
		worker_comm_recv_array(comm, WORKER_COMM_ARRAY_INT, file_status, 3);
		
		if(fileptr->type == WORKER_FILE_NORMAL) {
			file_cache_contains(file_store, fileptr->label, cachename);
		} else if(fileptr->type == WORKER_FILE_REMOTE) {
			sprintf(cachename, "%s", fileptr->payload);
		} else if(fileptr->type == WORKER_FILE_INCOMPLETE) {
			return -1;
		}
		if(stat(cachename, &st))
			return -1;

		switch(direction) {
			case WORKER_FILES_INPUT:
				
				if(file_status[0] < 0) {
					memset(&op, 0, sizeof(op));
					op.type = WORKER_OP_FILE;
					op.id = fileptr->id;
					op.options = fileptr->options;
					sprintf(op.name, "%s", fileptr->filename);
					op.payload = fileptr->payload;
					worker_comm_send_op(comm, &op);
					
					if(fileptr->type == WORKER_FILE_REMOTE) {
						memset(&op, 0, sizeof(op));
						op.type = WORKER_OP_FILE_CHECK;
						op.id = fileptr->id;
						worker_comm_send_op(comm, &op);
						worker_comm_recv_array(comm, WORKER_COMM_ARRAY_INT, file_status, 3);
					}
				}
				
				
				if(file_status[0] != st.st_size || file_status[2] <= st.st_mtime) {
					memset(&op, 0, sizeof(op));
					op.type = WORKER_OP_FILE_PUT;
					op.id = fileptr->id;
					if(file_status[0] <= 0)
						op.type = WORKER_FILE_NORMAL;
					else
						op.type = fileptr->type;
					
					op.payloadsize = st.st_size;
					op.payload = NULL;
					worker_comm_send_op(comm, &op);
					worker_comm_send_file(comm, cachename, st.st_size, 0);
				}
			break;
			
			case WORKER_FILES_OUTPUT:
			
				if(fileptr->flags & WORKER_FILE_FLAG_IGNORE)
					continue;
				if(file_status[0] <= 0) {
					if(!fileptr->flags & WORKER_FILE_FLAG_OPTIONAL)
						return -1;
				}
				if(file_status[2] <= st.st_mtime && file_status[0] == st.st_size)
					continue;
				memset(&op, 0, sizeof(op));
				op.type = WORKER_OP_FILE_GET;
				op.id = fileptr->id;
				worker_comm_send_op(comm, &op);
				worker_comm_recv_buffer(comm, &buffer, &buffersize, 1);
				if(buffersize) {
					FILE *outfile;
					outfile = fopen(cachename, "w");
					if(!outfile) {
						free(buffer);
						return -1;
					}
					full_fwrite(outfile, buffer, buffersize);
					fclose(outfile);
					free(buffer);
				} else {
					return -1;
				}
			break;
		}
	}
}


void handle_op(struct worker_comm *super_comm, struct worker_op *op)
{
	struct stat st;
	int index;
	struct worker_file *fileptr = NULL;
	

	switch(op->type) {
		case WORKER_OP_ROLE:
			worker_comm_disconnect(super_comm);
			worker_comm_connect(super_comm, comm_interface, op->payload, op->id, active_timeout, short_timeout);
			workerdata->role = op->flags;
			worker_comm_send_worker(super_comm, workerdata);
			return;
	
		case WORKER_OP_WORKDIR:
			if(stat(op->name, &st)) {
				debug(D_WQ, "Working directory (%s) does not exist\n", op->name);
				exit(1);
			}
			chdir(op->name);
			break;
	
		case WORKER_OP_CLEAR_CACHE:
			file_cache_cleanup(file_store);
			itable_firstkey(file_table);
			while(itable_nextkey(file_table, &index, &fileptr)) {
				free(fileptr);
				itable_remove(file_table, index);
			}
			break;
		case WORKER_OP_COMM_INTERFACE:
			comm_interface = op->id;
			if(op->flags > 0)
				comm_port = op->flags;
			else
				comm_port = comm_default_port;
			break;
		case WORKER_OP_FILE:
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
			if(op->options & WORKER_FILE_FLAG_CACHEABLE)
				fileptr->cacheable = 1;
			else
				fileptr->cacheable = 0;
			else if(op->options & WORKER_FILE_FLAG_REMOTEFS)
				fileptr->type = WORKER_FILE_REMOTE;
			else
				fileptr->type = WORKER_FILE_NORMAL;

			fileptr->options = op->options;
			fileptr->filename = strdup(op->name);
			fileptr->flags = op->flags;
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

			memset(stats, 0, sizeof(stats));
			stats[0] = -1;
		
			fileptr = itable_lookup(file_table, op->id);
			if(fileptr) {
				char label[WQ_FILENAME_MAX];
				char cachename[WQ_FILENAME_MAX];
				struct stat64 st;

				if(fileptr->cacheable)
					stats[1] &= WORKER_FILE_FLAG_CACHEABLE;
		
				if(fileptr->type == WORKER_FILE_REMOTE) {
					stats[1] &= WORKER_FILE_FLAG_REMOTEFS;
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
			
			} else {
				stats[1] = WORKER_FILE_FLAG_MISSING;
			}
			
			worker_comm_send_array(super_comm, WORK_COMM_ARRAY_INT, stats, 4);
		
		}	break;
	
		case WORKER_OP_FILE_PUT:
			fileptr = itable_lookup(file_table, op->id);
			if(fileptr) {
				char cachename[WQ_FILENAME_MAX];
				int fd;
			
				if(fileptr->type == WORKER_FILE_NORMAL) {
					file_cache_contains(file_store, fileptr->label, cachename);
				} else if(fileptr->type == WORKER_FILE_REMOTE) {
					sprintf(cachename, "%s", fileptr->payload);
				}
				fd = open64(cachename, O_WRONLY | O_CREAT | O_SYNC | O_TRUNC, op->flags);
				full_write(fd, op->payload, op->payloadsize);
				close(fd);
			}
			break;
	
		case WORKER_OP_FILE_GET:
			fileptr = itable_lookup(file_table, op->id);
			if(fileptr) {
				char label[WQ_FILENAME_MAX];
				char cachename[WQ_FILENAME_MAX];
				struct stat st;
			
				if(fileptr->type == WORKER_FILE_NORMAL) {
					file_cache_contains(file_store, fileptr->label, cachename);
				} else if(fileptr->type == WORKER_FILE_REMOTE) {
					sprintf(cachename, "%s", fileptr->payload);
				}
			
				if(!stat(cachename, &st)) {
					worker_comm_send_file(super_comm, cachename, 1);
				} else {
					worker_comm_send_buffer(super_comm, NULL, 0, 1);
				}
			} else {
				worker_comm_send_buffer(super_comm, NULL, -1, 1);
			}
			break;
	
		case WORKER_OP_RESULTS:
		{	int num_results = list_size(complete_jobs);
			worker_comm_send_array(super_comm, WORK_COMM_ARRAY_INT64, &num_results, 1);

			while(list_size(complete_jobs)) {
				job = list_pop_head(complete_jobs);
				worker_comm_send_result(super_comm, job);
				worker_job_delete(job);
				job = NULL;
			}
		}	break;

		case WORKER_OP_JOB_DIRMAP:
			job = worker_job_lookup(unfinished_jobs, op->jobid);
			job->dirmap = malloc(op->payloadsize);
			job->dirmaplength = op->payloadsize;
			memcpy(job->dirmap, op->payload, op->payloadsize);
			break;
	
		case WORKER_OP_JOB_REQUIRES:
			job = worker_job_lookup(unfinished_jobs, op->jobid);
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
			job = worker_job_lookup(unfinished_jobs, op->jobid);
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
			job = worker_job_lookup(unfinished_jobs, op->jobid);
			if(job->command)
				free(job->command);
			job->command = strdup(op->payload);
			job->commandlength = op->payloadsize;
			job->output = op->options;
			break;
	
		case WORKER_OP_JOB_CLOSE:
			job = itable_remove(unfinished_jobs, op->jobid);
			if(job) {
				job->status = WORKER_JOB_STATUS_READY;
				list_push_tail(waiting_jobs, job);
			}
			break;
	}

}

void foreman_main()
{
	static struct list *checked_workers = NULL;
	struct list        *tmp_workers = NULL;
	struct worker      *current_worker = NULL;
	struct worker_job  *job = NULL;
	struct worker_file *fileptr = NULL;
	struct worker_op    results_request;
	static struct link *listen_link = NULL;
	static int          listen_port = -1;
	
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
	worker_comm_accept_connections(comm_interface, listen_link, active_workers, active_timeout, short_timeout);

	/* Service each worker */
	num_waiting_jobs = list_size(waiting_jobs);
	while(current_worker = list_pop_head(active_workers)) {
		/* If the worker is full, see if has anything done */
		if(!current_worker->open_cores) {
			int num_results;
			num_results = worker_comm_test_results(current_worker->comm);
			/* If it has available results, fetch them */
			if(num_results > 0) {
				current_worker->state = WORKER_STATE_AVAILABLE;
				current_worker->open_cores += num_results;
				for(i = 0; i < num_results; i++) {
					job = worker_job_receive_result(current_worker->comm, active_jobs);
					handle_files(current_worker->comm, job->output_files, WORKER_FILES_OUTPUT);
					list_push_tail(complete_jobs, job);
				}
			}
			/* If there was an actual response, but nothing was ready, ask it again for results */
			if(num_results == 0) {
				worker_comm_send_op(current_worker->comm, &request_results);
			}
		}
		/* If there are available jobs and the worker has any open spaces, give it a job */
		if(num_waiting_jobs && current_worker->open_cores) {
			/* May want to implement a more complicated (read: cache-aware) job selection algorithm */
			job = list_pop_head(waiting_jobs);
			num_waiting_jobs--;
			handle_files(current_worker->comm, job->input_files, WORKER_FILES_INPUT);
			if(job->dirmap) {
				memset(op, 0, sizeof(*op));
				op->type = WORKER_OP_JOB_DIRMAP;
				op->jobid = job->id;
				op->payloadsize = job->dirmaplength;
				op->payload = job->dirmap;
				worker_comm_send_op(current_worker->comm, op);
			}
			list_first_item(job->input_files)
			memset(op, 0, sizeof(*op));
			while((fileptr = list_next_item(job->input_files)) {
				op->type = WORKER_OP_JOB_REQUIRES;
				op->jobid = job->id;
				op->id = fileptr->id;
				worker_comm_send_op(current_worker->comm, op);
			}
			
			list_first_item(job->output_files);
			memset(op, 0, sizeof(*op));
			while((fileptr = list_next_item(job->output_files)) {
				op->type = WORKER_OP_JOB_GENERATES;
				op->jobid = job->id;
				op->id = fileptr->id;
				worker_comm_send_op(current_worker->comm, op);
			}
			memset(op, 0, sizeof(*op));
			op->type = WORKER_OP_JOB_CMD;
			op->jobid = job->id;
			op->options = job->output;
			op->payload = job->command;
			op->payloadsize = job->commandlength;
			worker_comm_send_op(current_worker->comm, op);
			
			memset(op, 0, sizeof(*op));
			op->type = WORKER_OP_JOB_CLOSE;
			op->jobid = job->id;
			worker_comm_send_op(current_worker->comm, op);
			
			worker_comm_send_op(current_worker->comm, &request_results);
			current_worker->open_cores--;
		}
		/* If the worker still has open cores, and there are any jobs available, send it to the end of the queue */
		if(num_waiting_jobs && current_worker->open_cores)
			list_push_tail(active_workers);
		else /* Otherwise, mark it as visited and full */
			list_push_tail(checked_workers);
	}
	
	/* Swap "checked" and "ready" lists */
	tmp_workers = active_workers;
	active_workers = checked_workers;
	checked_workers = active_workers;
}

void worker_main()
{
	struct worker_job *job = NULL;
	int jobid, maxfd = 0;
	fd_set worker_fds;
	struct timeval short_timeout_st;

	short_timeout_st.tv_sec = short_timeout;
	short_timeout_st.tv_usec = 0;
	
	
	/* run through the list of active jobs, figure out which ones have output available */

	FD_ZERO(&worker_fds);
	itable_firstkey(active_jobs);
	while(itable_nextkey(active_jobs, &jobid, &job)) {
		if(job->output & WORKER_JOB_OUTPUT_STDOUT) {
			FD_SET(&worker_fds, job->out_fd);
			maxfd = MAX(job->out_fd, maxfd);
		}
		if(job->output & WORKER_JOB_OUTPUT_STDERR) {
			FD_SET(&worker_fds, job->err_fd);
			maxfd = MAX(job->err_fd, maxfd);
		}
	}
	num_ready_procs = select(maxfd, worker_fds, NULL, NULL, &short_timeout_st);


	/* for each process with any output, copy the output into the job's stdout or stderr buffer, and check for end of file */
	if(num_ready_procs > 0) {
		itable_firstkey(active_jobs);
		while(itable_nextkey(active_jobs, &jobid, &job)) {
			int len;
			char *buffer;
			if(FD_ISSET(&worker_fds, job->err_fd)) {
				len = copy_stream_to_buffer(job->err, &buffer);
				job->stderr_buffer = realloc(job->stderr_buffer, job->stderr_buffersize+len);
				memcpy(job->stderr_buffer + job->stderr_buffersize, buffer, len);
				job->stderr_buffersize += len;
				free(buffer);
			}
			if(FD_ISSET(&worker_fds, job->out_fd)) {
				len = copy_stream_to_buffer(job->out, &buffer);
				job->stdout_buffer = realloc(job->stdout_buffer, job->stdout_buffersize+len);
				memcpy(job->stdout_buffer + job->stdout_buffersize, buffer, len);
				job->stdout_buffersize += len;
				free(buffer);
			}
			if( (!job->err || feof(job->err)) && (!job->out || feof(job->out)) ) {
				job = itable_remove(active_jobs, jobid);
				job->status = WORKER_JOB_STATUS_COMPLETE;
				worker_job_check_files(job, WORKER_FILES_OUTPUT);
				list_push_tail(complete_jobs, job);
			}
		}
	}
	
	/* If cores are open, start running new jobs */
	while(itable_size(active_jobs) < workerdata->available_cores && list_size(waiting_jobs)) {
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
			
			worker_job_check_files(job, WORKER_FILES_INPUT);
			if(job->status != WORKER_JOB_STATUS_READY) {
				list_push_tail(complete_jobs, job);
				continue;
			}
			if(job->output == WORKER_JOB_OUTPUT_STDOUT) {
				out = &job->out;
			}
			if(job->output == WORKER_JOB_OUTPUT_STDERR) {
				err = &job->err;
			}
			if(job->output == WORKER_JOB_OUTPUT_COMBINED) {
				out = err = &job->out;
			}
			multi_popen(job->command, NULL, out, err);
			if(job->out)
				job->out_fd = fileno(job->out);
			if(job->err)
				job->err_fd = fileno(job->err);
		}
	}
}


int main(int argc, char *argv[])
{
	int super_port = MPI_QUEUE_DEFAULT_PORT;
	char super_host[LINK_ADDRESS_MAX];
	char c;
	int w;
	UINT64_T memory_avail, disk_total;

	workerdata = malloc(sizeof(*workerdata));

	signal(SIGTERM, handle_abort);
	signal(SIGQUIT, handle_abort);
	signal(SIGINT, handle_abort);


	workerdata->role = WORKER_ROLE_WORKER;
	sprintf(file_cache_path, "%s", FILE_CACHE_DEFAULT_PATH);
	
	while((c = getopt(argc, argv, "a:d:f:hmo:r:t:v")) != (char) -1) {
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
			show_version(argv[0]);
			return 0;
		
		case 'h':
		default:
			show_help(argv[0]);
			return 1;
		}
	}

	comm_port = WQ_DEFAULT_PORT;
	

	if(comm_interface == WORKER_COMM_MPI) {
		super_port = atoi(argv[optind]);
		MPI_Init(&argc, &argv);
		super_comm = worker_comm_connect(NULL, WORKER_COMM_MPI, NULL, super_port, active_timeout, short_timeout);
	} else {
		super_host = argv[optind];
		super_port = atoi(argv[optind + 1]);
		super_comm = worker_comm_connect(NULL, WORKER_COMM_TCP, super_host, super_port, active_timeout, short_timeout);
	}
	
	debug_config(argv[0]);
	if(!domain_name_cache_lookup(host, addr)) {
		fprintf(stderr, "couldn't lookup address of host %s\n", host);
		exit(1);
	}

	workerdata->cores = load_average_get_cpus();
	disk_info_get(file_cache_path, &workerdata->disk, &disk_total);
	memory_info_get(&memory_avail, &workerdata->ram);
	domain_name_cache_guess(workerdata->hostname);

	unfinished_jobs = itable_create(0);
	waiting_jobs = list_create();
	active_jobs = itable_create(0);
	complete_jobs = list_create();

	file_table = itable_create(0);
	file_store = file_cache_init(file_cache_path);

	while(1) {
		int result;

		//wait for a new op from current supervisor and handle it if there is one
		memset(&op, 0, sizeof(op));
		result = worker_comm_receive_op(&super_comm, &op);
		if(result >= 0)
			result = handle_op(&super_comm, &op);

		// Proceed with role-based operation
		if(workerdata->role == FOREMAN)
			foreman_main();
		else
			worker_main();
	}
	
}

