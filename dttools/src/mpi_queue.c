/*
Copyright (C) 2011- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "mpi_queue.h"
#include "link.h"
#include "debug.h"
#include "domain_name_cache.h"
#include "hash_table.h"
#include "itable.h"
#include "list.h"
#include "create_dir.h"
#include "int_sizes.h"
#include "macros.h"

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


// MPI_QUEUE op codes must be even, as they may be bitwise ORed with
// MPI_QUEUE_JOB_FAILED (0x01) to indicate which operation type failed
#define MPI_QUEUE_OP_WORK     2
#define MPI_QUEUE_OP_STAT     4
#define MPI_QUEUE_OP_UNLINK   6
#define MPI_QUEUE_OP_MKDIR    8
#define MPI_QUEUE_OP_CLOSE   10

#define MPI_QUEUE_JOB_WAITING  0
#define MPI_QUEUE_JOB_FAILED   1
#define MPI_QUEUE_JOB_BUSY     2
#define MPI_QUEUE_JOB_READY    4
#define MPI_QUEUE_JOB_COMPLETE 8

#define MPI_QUEUE_FILE 0
#define MPI_QUEUE_BUFFER 1 //Unsupported at the moment

#define MPI_QUEUE_TASK_STATUS_INITIALIZING 0
#define MPI_QUEUE_TASK_STATUS_READY 1
#define MPI_QUEUE_TASK_STATUS_EXECUTING 2
#define MPI_QUEUE_TASK_STATUS_COMPLETE 3

struct mpi_queue {
	struct link *master_link;
	struct link *mpi_link;

	struct list *ready_list;
	struct itable *active_list;
	struct list *complete_list;

	INT64_T total_tasks_submitted;
	INT64_T total_tasks_complete;
	INT64_T total_task_time;
	INT64_T total_bytes_sent;
	INT64_T total_bytes_received;
	
	timestamp_t total_send_time;
	timestamp_t total_receive_time;
};

struct mpi_queue_file {
	int type;		// MPI_QUEUE_FILE
	int length;		// length of payload
	char *name;		// name on shared filesystem.
};

struct mpi_queue_task *mpi_queue_task_create(const char *command_line)
{
	struct mpi_queue_task *t = malloc(sizeof(*t));
	memset(t, 0, sizeof(*t));

	t->tag = NULL;
	t->command_line = strdup(command_line);
	t->output = NULL;
	t->input_files = list_create();
	t->output_files = list_create();

	t->status = MPI_QUEUE_TASK_STATUS_INITIALIZING;
	t->return_status = MPI_QUEUE_RETURN_STATUS_UNSET;
	t->result = MPI_QUEUE_RESULT_UNSET;

	t->submit_time = t->start_time = t->finish_time = t->transfer_start_time = t->total_transfer_time = t->computation_time = 0;
	t->total_bytes_transferred = 0;

	return t;
}

void mpi_queue_task_delete(struct mpi_queue_task *t)
{
	struct mpi_queue_file *tf;
	if(t) {
		if(t->command_line)
			free(t->command_line);
		if(t->tag)
			free(t->tag);
		if(t->output)
			free(t->output);
		if(t->input_files) {
			while((tf = list_pop_tail(t->input_files))) {
				if(tf->name)
					free(tf->name);
				free(tf);
			}
			list_delete(t->input_files);
		}
		if(t->output_files) {
			while((tf = list_pop_tail(t->output_files))) {
				if(tf->name)
					free(tf->name);
				free(tf);
			}
			list_delete(t->output_files);
		}
		free(t);
	}
}
void mpi_queue_task_specify_tag(struct mpi_queue_task *t, const char *tag)
{
	if(t->tag) {
		free(t->tag);
	}
	t->tag = strdup(tag);
}

void mpi_queue_task_specify_file(struct mpi_queue_task *t, const char *name, int type)
{
	struct mpi_queue_file *tf = malloc(sizeof(struct mpi_queue_file));

	tf->type = MPI_QUEUE_FILE;
	tf->length = strlen(name);
	tf->name = strdup(name);

	if(type == MPI_QUEUE_INPUT) {
		list_push_tail(t->input_files, tf);
	} else {
		list_push_tail(t->output_files, tf);
	}
}








struct mpi_queue *mpi_queue_create(int port)
{
	struct mpi_queue *q = malloc(sizeof(*q));
	char *envstring;

	memset(q, 0, sizeof(*q));

	if(port == 0) {
		envstring = getenv("MPI_QUEUE_PORT");
		if(envstring) {
			port = atoi(envstring);
		} else {
			// indicate using a random available port
			port = -1;
		}
	}

	if(port == -1) {
		int lowport = 9000;
		int highport = 32767;

		envstring = getenv("MPI_QUEUE_LOW_PORT");
		if(envstring)
			lowport = atoi(envstring);

		envstring = getenv("MPI_QUEUE_HIGH_PORT");
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

	q->ready_list = list_create();
	q->active_list = itable_create(0);
	q->complete_list = list_create();

	debug(D_WQ, "MPI Queue is listening on port %d.", port);
	return q;

      failure:
	debug(D_NOTICE, "Could not create mpi_queue on port %i.", port);
	free(q);
	return 0;
}

void mpi_queue_delete(struct mpi_queue *q)
{
	if(q) {
		UINT64_T key;
		void *value;
		
		list_free(q->ready_list);
		list_delete(q->ready_list);
		list_free(q->complete_list);
		list_delete(q->complete_list);
		
		itable_firstkey(q->active_list);
		while(itable_nextkey(q->active_list, &key, &value)) {
			free(value);
			itable_remove(q->active_list, key);
		}
		itable_delete(q->active_list);
		
		link_close(q->master_link);
		free(q);
	}
}

int mpi_queue_port(struct mpi_queue *q)
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

int mpi_queue_empty(struct mpi_queue *q)
{
	return ((list_size(q->ready_list) + itable_size(q->active_list) + list_size(q->complete_list)) == 0);
}



void mpi_queue_submit(struct mpi_queue *q, struct mpi_queue_task *t)
{
	/* If the task has been used before, clear out accumlated state. */
	static int next_taskid = 1;

	if(t->output) {
		free(t->output);
		t->output = 0;
	}
	t->status = MPI_QUEUE_TASK_STATUS_READY;
	t->total_transfer_time = 0;
	t->result = MPI_QUEUE_RESULT_UNSET;

	//Increment taskid. So we get a unique taskid for every submit.
	t->taskid = next_taskid++;
	
	/* Then, add it to the ready list and mark it as submitted. */
	list_push_tail(q->ready_list, t);
	t->submit_time = timestamp_get();
	q->total_tasks_submitted++;
}

int dispatch_task(struct link *mpi_link, struct mpi_queue_task *t, int timeout)
{
	struct mpi_queue_file *tf;
	int stoptime = time(0) + timeout;


	debug(D_MPI, "sending task %d\n", t->taskid);
	if(t->input_files) {
		list_first_item(t->input_files);
		while((tf = list_next_item(t->input_files))) {
			link_putfstring(mpi_link, "stat %d %s\n", stoptime, t->taskid, tf->name);
		}
	}

	t->start_time = timestamp_get();
	link_putfstring(mpi_link, "work %d %zu\n%s", stoptime, t->taskid, strlen(t->command_line), t->command_line);
	t->status = MPI_QUEUE_TASK_STATUS_EXECUTING;
	link_putfstring(mpi_link, "close %d\n", stoptime, t->taskid);

	debug(D_MPI, "'%s' sent as task %d", t->command_line, t->taskid);
	return 1;
}

int get_results(struct link *mpi_link, struct itable *active_list, struct list *complete_list, int timeout)
{
	char line[MPI_QUEUE_LINE_MAX];
	int num_results, n = 0;
	int stoptime = time(0) + timeout;

	debug(D_MPI, "Getting any results\n");
	link_putliteral(mpi_link, "get results\n", stoptime);
	if(link_readline(mpi_link, line, sizeof(line), stoptime)) {
		debug(D_MPI, "received: %s\n", line);
		sscanf(line, "num results %d", &num_results);
	} else {
		return 0;
	}
	debug(D_MPI, "%d results available\n", num_results);

	while(n++ < num_results && link_readline(mpi_link, line, sizeof(line), stoptime)) {
		struct mpi_queue_task *t;
		int taskid, status, result, result_length;
		
		sscanf(line, "result %d %d %d %d", &taskid, &status, &result, &result_length);
		t = itable_remove(active_list, taskid);
		if(!t) {
			debug(D_NOTICE, "Invalid taskid (%d) returned\n", taskid);
			return -1;
		}
		if(result_length) {
			t->output = malloc(result_length+1);
			link_read(mpi_link, t->output, result_length, time(0) + timeout);
			t->output[result_length] = 0;
		}
		t->status = MPI_QUEUE_TASK_STATUS_COMPLETE;
		t->return_status = result;
		t->result = status;
		list_push_tail(complete_list, t);
	}
	return num_results;
}

struct mpi_queue_task *mpi_queue_wait(struct mpi_queue *q, int timeout)
{
	struct mpi_queue_task *t;
	time_t stoptime;
	int result;

	if(timeout == MPI_QUEUE_WAITFORTASK) {
		stoptime = 0;
	} else {
		stoptime = time(0) + timeout;
	}


	while(1) {
		// If a task is already complete, return it
		t = list_pop_head(q->complete_list);
		if(t)
			return t;

		if(list_size(q->ready_list) == 0 && itable_size(q->active_list) == 0)
			break;

		// Wait no longer than the caller's patience.
		int msec;
		int sec;
		if(stoptime) {
			sec = MAX(0, stoptime - time(0));
			msec = sec * 1000;
		} else {
			sec = 5;
			msec = 5000;
		}

		if(!q->mpi_link) {
			q->mpi_link = link_accept(q->master_link, stoptime);
			if(q->mpi_link) {
				char working_dir[MPI_QUEUE_LINE_MAX];
				link_tune(q->mpi_link, LINK_TUNE_INTERACTIVE);
				link_usleep(q->mpi_link, msec, 0, 1);
				getcwd(working_dir, MPI_QUEUE_LINE_MAX);
				link_putfstring(q->mpi_link, "workdir %s\n", stoptime, working_dir);
				result = link_usleep(q->mpi_link, msec, 1, 1);
			} else {
				result = 0;
			}
		} else {
			debug(D_MPI, "Waiting for link to be ready\n");
			result = link_usleep(q->mpi_link, msec, 1, 1);
		}

		// If nothing was awake, restart the loop or return without a task.
		if(result <= 0) {
			if(stoptime && time(0) >= stoptime) {
				return 0;
			} else {
				continue;
			}
		}

		debug(D_MPI, "sending %d tasks to the MPI master process\n", list_size(q->ready_list));
		// Send all ready tasks to the MPI master process
		while(list_size(q->ready_list)) {
			struct mpi_queue_task *t = list_pop_head(q->ready_list);
			result = dispatch_task(q->mpi_link, t, msec/1000);
			if(result <= 0)
				return 0;
			itable_insert(q->active_list, t->taskid, t);
		}

		// Receive any results back
		result = get_results(q->mpi_link, q->active_list, q->complete_list, msec/1000);
		if(result < 0) {
			return 0;
		}
	}

	return 0;
}





