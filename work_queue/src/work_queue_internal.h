/*
Copyright (C) 2008- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "work_queue.h"
#include "work_queue_resources.h"

#include "list.h"

struct work_queue_file {
	int type;		// WORK_QUEUE_FILE, WORK_QUEUE_BUFFER, WORK_QUEUE_REMOTECMD, WORK_QUEUE_FILE_PIECE
	int flags;		// WORK_QUEUE_CACHE or others in the future.
	int length;		// length of payload, only used for non-file objects like buffers and urls
	off_t offset;		// file offset for WORK_QUEUE_FILE_PIECE
	off_t piece_length;	// file piece length for WORK_QUEUE_FILE_PIECE
	char *payload;		// name on master machine or buffer of data.
	char *remote_name;	// name on remote machine.
};

enum wq_file_types {
	WORK_QUEUE_FILE = 1,
	WORK_QUEUE_BUFFER,
	WORK_QUEUE_REMOTECMD,
	WORK_QUEUE_FILE_PIECE,
	WORK_QUEUE_DIRECTORY,
	WORK_QUEUE_URL
};


struct work_queue_task *work_queue_wait_internal(struct work_queue *q, int timeout, struct link *master_link, int *master_active);

/* Adds (arithmetically) all the workers resources (cores, memory, disk) */
void aggregate_workers_resources( struct work_queue *q, struct work_queue_resources *r );

/** Enable use of the process module.
This allows @ref work_queue_wait to call @ref process_pending from @ref process.h, exiting if a process has completed.
Warning: this will reap any child processes, and their information can only be retrieved via @ref process_wait.
@param q A work queue object.
*/
void work_queue_enable_process_module(struct work_queue *q);


