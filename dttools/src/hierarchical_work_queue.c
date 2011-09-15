/*
Copyright (C) 2008- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/
#include "copy_stream.h"
#include "get_canonical_path.h"
#include "int_sizes.h"
#include "link.h"
#include "debug.h"
#include "stringtools.h"
#include "catalog_query.h"
#include "catalog_server.h"
#include "datagram.h"
#include "domain_name_cache.h"
#include "file_cache.h"
#include "full_io.h"
#include "hash_table.h"
#include "hierarchical_work_queue.h"
#include "itable.h"
#include "list.h"
#include "macros.h"
#include "process.h"
#include "username.h"
#include "create_dir.h"
#include "timestamp.h"
#include "worker_comm.h"
#include "xmalloc.h"

#include <unistd.h>
#include <dirent.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/wait.h>

#include <errno.h>
#include <math.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#define WORK_QUEUE_FILE 0
#define WORK_QUEUE_BUFFER 1
#define WORK_QUEUE_REMOTECMD 2

#define TASK_STATUS_INITIALIZING 0
#define TASK_STATUS_SENDING_INPUT 1
#define TASK_STATUS_EXECUTING 2
#define TASK_STATUS_RECEIVING_OUTPUT 3

struct hierarchical_work_queue {
	char *name;
	int interface_mode;

	struct link *master_link;
	struct list *active_workers;

	struct list *ready_list;
	struct itable *active_list;
	struct list *complete_list;

	struct itable *file_table;
	struct file_cache *file_store;
	
	int active_timeout;
	int short_timeout;
};

struct work_queue_file {
	int type;		// WORK_QUEUE_FILE or WORK_QUEUE_BUFFER
	int flags;		// WORK_QUEUE_CACHE or others in the future.
	int length;		// length of payload
	void *payload;		// name on master machine or buffer of data.
	char *remote_name;	// name on remote machine.
};

static int next_taskid = 1;

struct worker_job *hierarchical_work_queue_job_create(const char *command_line)
{
	struct worker_job *j = malloc(sizeof(*j));
	memset(j, 0, sizeof(*j));
	j->id = next_taskid++;

	j->command = strdup(command_line);
	j->commandlength = strlen(command_line);
	j->dirmap = NULL;
	j->dirmaplength = 0;
	j->output_streams = WORKER_JOB_OUTPUT_STDOUT | WORKER_JOB_OUTPUT_STDERR;

	j->tag = NULL;

	j->options = 0;
	j->status = WORK_QUEUE_RETURN_STATUS_UNSET;
	j->result = WORK_QUEUE_RESULT_UNSET;

	j->input_files = list_create();
	j->output_files = list_create();

	j->stdout_buffer = NULL;
	j->stdout_buffersize = 0;
	j->stderr_buffer = NULL;
	j->stderr_buffersize = 0;
	
	j->out = j->err = NULL;
	j->out_fd = j->err_fd = 0;
	
	return j;
}

void hierarchical_work_queue_job_delete(struct worker_job *job)
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

static void add_workers(struct hierarchical_work_queue *q)
{
	struct worker *w;
	struct list *new_comms;
	struct worker_comm *comm;
	
	debug(D_WQ, "Waiting for new connections\n");
	new_comms = worker_comm_accept_connections(q->interface_mode, q->master_link, q->active_timeout, q->short_timeout);
	
	debug(D_WQ, "Found %d new connections\n", new_comms?list_size(new_comms):0);
	while(new_comms && (comm = list_pop_head(new_comms))) {
		int stats[3];
		w = malloc(sizeof(*w));
		memset(w, 0, sizeof(*w));
		w->comm = comm;
		worker_comm_recv_array(comm, WORKER_COMM_ARRAY_INT, stats, 3);
		w->cores = w->open_cores = stats[0];
		w->ram = stats[1];
		w->disk = stats[2];
		w->workerid = comm->mpi_rank;
		if(comm->hostname) {
			sprintf(w->hostname, "%s", comm->hostname);
			debug(D_WQ, "adding worker %s\n", w->hostname);
		} else {
			debug(D_WQ, "adding worker %d\n", w->workerid);
		}
		w->jobids = itable_create(0);

		list_push_head(q->active_workers, w);
	}
	list_delete(new_comms);
}

void remove_worker(struct hierarchical_work_queue *q, struct worker *w) {
	UINT64_T jobid;
	struct worker_job *j;
	
	itable_firstkey(w->jobids);
	while(itable_nextkey(w->jobids, &jobid, (void**)&j)) {
		itable_remove(q->active_list, jobid);
		list_push_head(q->ready_list, j);
	}
	itable_delete(w->jobids);
	if(w->comm) {
		worker_comm_delete(w->comm);
	}
	free(w);
}


/**
 * Comparison function for sorting by file/dir names in the output files list
 * of a task
 */
/*
int filename_comparator(const void *a, const void *b)
{
	int rv;
	rv = strcmp(*(char *const *) a, *(char *const *) b);
	return rv > 0 ? -1 : 1;
}
*/
/*
static void delete_uncacheable_files(struct work_queue_task *t, struct work_queue_worker *w)
{
	struct work_queue_file *tf;

	if(t->input_files) {
		list_first_item(t->input_files);
		while((tf = list_next_item(t->input_files))) {
			if(!(tf->flags & WORK_QUEUE_CACHE) && !(tf->flags & WORK_QUEUE_PREEXIST)) {
				debug(D_WQ, "%s (%s) unlink %s", w->hostname, w->addrport, tf->remote_name);
				link_putfstring(w->link, "unlink %s\n", time(0) + short_timeout, tf->remote_name);
			}
		}
	}

	if(t->output_files) {
		list_first_item(t->output_files);
		while((tf = list_next_item(t->output_files))) {
			if(!(tf->flags & WORK_QUEUE_CACHE) && !(tf->flags & WORK_QUEUE_PREEXIST)) {
				debug(D_WQ, "%s (%s) unlink %s", w->hostname, w->addrport, tf->remote_name);
				link_putfstring(w->link, "unlink %s\n", time(0) + short_timeout, tf->remote_name);
			}
		}
	}
}
*/

/*static void start_tasks(struct work_queue *q)
{
	struct work_queue_task *t;
	struct work_queue_worker *w;

	while(list_size(q->ready_list)) {
		t = list_peek_head(q->ready_list);
		w = find_best_worker(q, t);
		if(w) {
			start_task_on_worker(q, w);
		} else {
			break;
		}
	}
}
*/

/*
int work_queue_port(struct work_queue *q)
{
	char addr[LINK_ADDRESS_MAX];
	int port;

	if(!q)
		return 0;

	if(link_address_local(q->master_link, addr, &port)) {
		return port;
	} else {
		return 0;
	}
}
*/

struct hierarchical_work_queue *hierarchical_work_queue_create(int mode, int port, const char *file_cache_path, int timeout)
{
	struct hierarchical_work_queue *q = malloc(sizeof(*q));
	char *envstring;

	memset(q, 0, sizeof(*q));

	if(port == 0) {
		envstring = getenv("WORK_QUEUE_PORT");
		if(envstring) {
			port = atoi(envstring);
		} else {
			// indicate using a random available port
			port = -1;
		}
	}

	q->interface_mode = mode;
	q->active_timeout = timeout;
	q->short_timeout = 60;
	
	switch(mode) {
		case WORKER_COMM_TCP:
			
			if(port == -1) {
				int lowport = 9000;
				int highport = 32767;
		
				envstring = getenv("WORK_QUEUE_LOW_PORT");
				if(envstring)
					lowport = atoi(envstring);
		
				envstring = getenv("WORK_QUEUE_HIGH_PORT");
				if(envstring)
					highport = atoi(envstring);
		
				for(port = lowport; port < highport; port++) {
					q->master_link = link_serve(port);
					if(q->master_link)
						break;
				}
			} else {
				q->master_link = link_serve(port);
			}

			if(!q->master_link)
				goto failure;

			debug(D_WQ, "master link successfully created: listening on port %d\n", port);
			break;
		case WORKER_COMM_MPI:
			q->master_link = NULL;
			break;
	}


	
	q->active_workers = list_create();
	q->ready_list = list_create();
	q->active_list = itable_create(0);
	q->complete_list = list_create();
	
	q->file_table = itable_create(0);
	q->file_store = file_cache_init(file_cache_path);

	envstring = getenv("WORK_QUEUE_NAME");
	if(envstring)
		hierarchical_work_queue_specify_name(q, envstring);

	if(mode == WORKER_COMM_TCP)
	{	debug(D_WQ, "Work Queue is listening on port %d.", port);	}
	else
	{	debug(D_WQ, "Work Queue is listening via mpi.", port);	}
	
	return q;

      failure:
	debug(D_NOTICE, "Could not create Work Queue on port %d.", port);
	free(q);
	return 0;
}

int hierarchical_work_queue_specify_name(struct hierarchical_work_queue *q, const char *name)
{
	if(q && name) {
		if(q->name)
			free(q->name);
		q->name = strdup(name);
		setenv("WORK_QUEUE_NAME", q->name, 1);
	}

	return 0;
}

void hierarchical_work_queue_specify_interface(struct hierarchical_work_queue *q, int mode, int port)
{
	q->interface_mode = mode;
	if(mode == WORKER_COMM_TCP && port) {
		if(q->master_link)
			link_close(q->master_link);
		q->master_link = link_serve(port);
	}
}


void hierarchical_work_queue_delete(struct hierarchical_work_queue *q)
{
	
}

void hierarchical_work_queue_submit(struct hierarchical_work_queue *q, struct worker_job *j)
{
	/* If the task has been used before, clear out accumlated state. */

	if(j->stdout_buffer) {
		free(j->stdout_buffer);
		j->stdout_buffer = NULL;
		j->stdout_buffersize = 0;
	}
	if(j->stderr_buffer) {
		free(j->stderr_buffer);
		j->stderr_buffer = NULL;
		j->stderr_buffersize = 0;
	}
	
	j->result = WORK_QUEUE_RESULT_UNSET;
	j->status = WORKER_JOB_STATUS_READY;

	/* Then, add it to the ready list and mark it as submitted. */

	j->submit_time = timestamp_get();
	list_push_tail(q->ready_list, j);
}

struct worker_job *hierarchical_work_queue_wait(struct hierarchical_work_queue *q)
{
	static struct list *checked_workers = NULL;
	struct worker      *current_worker;
	struct worker_job  *j;
	struct worker_op    op;
	struct worker_op    results_request;

	if(!checked_workers)
		checked_workers = list_create();

	results_request.type = WORKER_OP_RESULTS;
	
	while(1) {
		struct list *tmp_workers;
		struct worker_file *fileptr;
		int num_waiting_jobs;
		// If there's something already available return it without waiting.
		j = list_pop_head(q->complete_list);
		if(j)
			return j;

		if(itable_size(q->active_list) == 0 && list_size(q->ready_list) == 0)
			break;

		// See if there are any eager workers waiting for stuff to do.
		add_workers(q);

		/* Service each worker */
		num_waiting_jobs = list_size(q->ready_list);
		while((current_worker = list_pop_head(q->active_workers))) {
			debug(D_WQ, "checking worker %s %d\n", current_worker->hostname, current_worker->workerid);
			debug(D_WQ, "\tworker has %d open cores to handle %d waiting jobs\n", current_worker->open_cores, num_waiting_jobs);
			/* If the worker is full or there are no jobs left to submit and the worker has active jobs remaining, see if it has anything done */
			if(!current_worker->open_cores || (!num_waiting_jobs && itable_size(current_worker->jobids))) {
				int num_results;
				num_results = worker_comm_test_results(current_worker->comm);
				/* If it has available results, fetch them */
				if(num_results > 0) {
					int i;
					current_worker->state = WORKER_STATE_AVAILABLE;
					current_worker->open_cores += num_results;
					for(i = 0; i < num_results; i++) {
						j = worker_job_receive_result(current_worker->comm, q->active_list);
						worker_job_fetch_files(current_worker->comm, j->output_files, q->file_store);
						itable_remove(current_worker->jobids, j->id);
						list_push_tail(q->complete_list, j);
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
				j = list_pop_head(q->ready_list);
				debug(D_WQ, "\tgiving worker %d:%s job number %d\n", current_worker->workerid, current_worker->hostname, j->id);
				num_waiting_jobs--;
				worker_job_send_files(current_worker->comm, j->input_files, j->output_files, q->file_store);
				debug(D_WQ, "\tjob %d input files handled\n", j->id);
				if(j->dirmap) {
					memset(&op, 0, sizeof(op));
					op.type = WORKER_OP_JOB_DIRMAP;
					op.jobid = j->id;
					op.payloadsize = j->dirmaplength;
					op.payload = j->dirmap;
					worker_comm_send_op(current_worker->comm, &op);
				}
				debug(D_WQ, "\tbuilding job (assigning input files)\n");
				list_first_item(j->input_files);
				memset(&op, 0, sizeof(op));
				while((fileptr = list_next_item(j->input_files))) {
					op.type = WORKER_OP_JOB_REQUIRES;
					op.jobid = j->id;
					op.id = fileptr->id;
					worker_comm_send_op(current_worker->comm, &op);
				}
				debug(D_WQ, "\tbuilding job (assigning output files)\n");
				list_first_item(j->output_files);
				memset(&op, 0, sizeof(op));
				while((fileptr = list_next_item(j->output_files))) {
					op.type = WORKER_OP_JOB_GENERATES;
					op.jobid = j->id;
					op.id = fileptr->id;
					worker_comm_send_op(current_worker->comm, &op);
				}
				debug(D_WQ, "\tbuilding job (sending command)\n");
				memset(&op, 0, sizeof(op));
				op.type = WORKER_OP_JOB_CMD;
				op.jobid = j->id;
				op.options = j->output_streams;
				op.payload = j->command;
				op.payloadsize = j->commandlength;
				worker_comm_send_op(current_worker->comm, &op);
				
				memset(&op, 0, sizeof(op));
				op.type = WORKER_OP_JOB_CLOSE;
				op.jobid = j->id;
				worker_comm_send_op(current_worker->comm, &op);
				debug(D_WQ, "\tdone building job\n");
				
				itable_insert(current_worker->jobids, j->id, j);
				itable_insert(q->active_list, j->id, j);

				current_worker->open_cores--;
				/* If there are no more jobs currently available, or the current worker is full, request its results */
				if((!num_waiting_jobs || !current_worker->open_cores))
					worker_comm_send_op(current_worker->comm, &results_request);
			}
			/* If the worker still has open cores, and there are any jobs available, send it to the end of the queue */
			if(num_waiting_jobs && current_worker->open_cores)
				list_push_tail(q->active_workers, current_worker);
			else /* Otherwise, mark it as visited and full */
				list_push_tail(checked_workers, current_worker);
		}
		
		/* Swap "checked" and "ready" lists */
		tmp_workers = q->active_workers;
		q->active_workers = checked_workers;
		checked_workers = tmp_workers;
	}

	return 0;
}

void hierarchical_work_queue_job_specify_tag(struct worker_job *j, const char *tag)
{
	if(j->tag)
		free(j->tag);
	j->tag = strdup(tag);
}

void hierarchical_work_queue_job_specify_output(struct worker_job *j, int output)
{
	output = output & WORKER_JOB_OUTPUT_COMBINED;
	j->output_streams = output;
}

static int current_fileid = 0;

void hierarchical_work_queue_job_specify_file(struct worker_job *j, const char *local_name, const char *remote_name, int type, int flags)
{
	struct worker_file *tf = malloc(sizeof(*tf));

	tf->id = current_fileid++;
	tf->filename = strdup(remote_name);

	tf->type = WORKER_FILE_REMOTE;

	tf->flags = flags;

	tf->size = strlen(local_name);
	tf->payload = strdup(local_name);

	if(type == WORKER_FILES_INPUT) {
		list_push_tail(j->input_files, tf);
	} else {
		list_push_tail(j->output_files, tf);
	}
}

void hierarchical_work_queue_job_specify_buffer(struct worker_job *j, const char *data, int length, const char *remote_name, int flags)
{
	struct worker_file *tf = malloc(sizeof(struct worker_file));
	
	tf->id = current_fileid++;
	tf->filename = strdup(remote_name);

	tf->type = WORKER_FILE_NORMAL;

	tf->flags = flags;

	tf->size = length;
	tf->payload = malloc(length);
	memcpy(tf->payload, data, length);

	list_push_tail(j->input_files, tf);
}


int hierarchical_work_queue_shut_down_workers(struct hierarchical_work_queue *q, int n)
{
//	struct worker *w;
//	char *key;
//	int i = 0;

	if(!q)
		return -1;

	// send worker exit.
/*	hash_table_firstkey(q->worker_table);
	while((n == 0 || i < n) && hash_table_nextkey(q->worker_table, &key, (void **) &w)) {
		link_putliteral(w->link, "exit\n", time(0) + short_timeout);
		remove_worker(q, w);
		i++;
	}

	return i;
*/
	return 0;
}

int hierarchical_work_queue_empty(struct hierarchical_work_queue *q)
{
	return ((list_size(q->ready_list) + list_size(q->complete_list) + itable_size(q->active_list)) == 0);
}



/*******************
Internal Functions
*******************/

void worker_job_check_files(struct worker_job *job, struct file_cache *file_store, int filetype)
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
		char filename[WORK_QUEUE_LINE_MAX];
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
			if(fileptr->flags & WORKER_FILE_FLAG_OPTIONAL)
				continue;
			job->status = WORKER_JOB_STATUS_MISSING_FILE;
			break;
		}
		
		
		if(filetype == WORKER_FILES_INPUT) {
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
				cache_fd = open64(filename, O_WRONLY | O_CREAT | O_SYNC | O_TRUNC, st.st_mode & S_IRWXU);
				copy_stream_to_fd(reg, cache_fd);
				fclose(reg);
				close(cache_fd);
			}
		}
	}
}

void worker_job_send_result(struct worker_comm *comm, struct worker_job *job)
{
	int results_buffer[3];
	
	results_buffer[0] = job->id;
	results_buffer[1] = job->status;
	results_buffer[2] = job->exit_code;
	
	worker_comm_send_array(comm, WORKER_COMM_ARRAY_INT, results_buffer, 3);
	worker_comm_send_buffer(comm, job->stdout_buffer, job->stdout_buffersize, 1);
	worker_comm_send_buffer(comm, job->stderr_buffer, job->stderr_buffersize, 1);
	return;
}

struct worker_job * worker_job_receive_result(struct worker_comm *comm, struct itable *jobs)
{
	int stdout_buffersize, stderr_buffersize;
	int results_buffer[3];
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
	job->exit_code = results_buffer[2];
	job->stdout_buffer = stdout_buffer;
	job->stdout_buffersize = stdout_buffersize;
	job->stderr_buffer = stderr_buffer;
	job->stderr_buffersize = stderr_buffersize;

	return job;
}

int worker_job_send_files(struct worker_comm *comm, struct list *input_files, struct list *output_files, struct file_cache *file_store)
{
	struct worker_file *fileptr;
	struct worker_op op;
	struct stat st;

	list_first_item(input_files);
	while((fileptr = list_next_item(input_files))) {

		int file_status[3];
		char cachename[WORK_QUEUE_LINE_MAX];

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
		
		debug(D_WQ, "checking file %s (%s) for input\n", fileptr->label, cachename);

		if(stat(cachename, &st))
			return -1;
		
		if(file_status[0] < 0) {
			debug(D_WQ, "\tworker doesn't know of file, sending file info\n");
			memset(&op, 0, sizeof(op));
			op.type = WORKER_OP_FILE;
			op.id = fileptr->id;
			op.options = fileptr->flags;
			if(fileptr->type == WORKER_FILE_REMOTE) {
				op.options = op.options & WORKER_FILE_FLAG_REMOTEFS;
			}
			sprintf(op.name, "%s", fileptr->filename);
			op.payload = fileptr->payload;
			worker_comm_send_op(comm, &op);
			
			if(fileptr->type == WORKER_FILE_REMOTE) {
				debug(D_WQ, "\tfile is remote, checking for availability\n");
				memset(&op, 0, sizeof(op));
				op.type = WORKER_OP_FILE_CHECK;
				op.id = fileptr->id;
				worker_comm_send_op(comm, &op);
				debug(D_WQ, "\twaiting for response\n");
				worker_comm_recv_array(comm, WORKER_COMM_ARRAY_INT, file_status, 3);
			}
		}
		
		
		if(file_status[0] != st.st_size || file_status[2] <= st.st_mtime) {
			debug(D_WQ, "\tfile not available on worker, sending file info\n");
			memset(&op, 0, sizeof(op));
			op.type = WORKER_OP_FILE_PUT;
			op.id = fileptr->id;
			if(file_status[0] <= 0)
				op.options = WORKER_FILE_NORMAL;
			else
				op.options = fileptr->type;
			
			op.payloadsize = st.st_size;
			op.payload = NULL;
			op.flags = st.st_mode & S_IRWXU;
			worker_comm_send_op(comm, &op);
			worker_comm_send_file(comm, cachename, st.st_size, 0);
		}
	}
	
	list_first_item(output_files);
	while((fileptr = list_next_item(output_files))) {

		int file_status[3];
		char cachename[WORK_QUEUE_LINE_MAX];
		
		memset(&op, 0, sizeof(op));
		op.type = WORKER_OP_FILE_CHECK;
		op.id = fileptr->id;
		worker_comm_send_op(comm, &op);
		worker_comm_recv_array(comm, WORKER_COMM_ARRAY_INT, file_status, 3);
		
		debug(D_WQ, "checking file %s (%s) for generation\n", fileptr->label, cachename);

		if(fileptr->type == WORKER_FILE_NORMAL) {
			file_cache_contains(file_store, fileptr->label, cachename);
		} else if(fileptr->type == WORKER_FILE_REMOTE) {
			sprintf(cachename, "%s", fileptr->payload);
		} else if(fileptr->type == WORKER_FILE_INCOMPLETE) {
			return -1;
		}
	
		if(file_status[0] < 0) {
			debug(D_WQ, "\tworker doesn't know of file, sending file info\n");
			memset(&op, 0, sizeof(op));
			op.type = WORKER_OP_FILE;
			op.id = fileptr->id;
			op.options = fileptr->flags;
			if(fileptr->type == WORKER_FILE_REMOTE) {
				op.options = op.options & WORKER_FILE_FLAG_REMOTEFS;
			}
			sprintf(op.name, "%s", fileptr->filename);
			op.payload = fileptr->payload;
			worker_comm_send_op(comm, &op);
		}
	}
	
	return 0;
}

int worker_job_fetch_files(struct worker_comm *comm, struct list *files, struct file_cache *file_store)
{
	struct worker_file *fileptr;
	struct worker_op op;
	struct stat st;

	list_first_item(files);
	while((fileptr = list_next_item(files))) {

		int file_status[3];
		char cachename[WORK_QUEUE_LINE_MAX];
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
		
		if(fileptr->flags & WORKER_FILE_FLAG_IGNORE)
			continue;
		if(file_status[0] <= 0) {
			if(!(fileptr->flags & WORKER_FILE_FLAG_OPTIONAL))
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
	}
	return 0;
}

