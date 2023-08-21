/*
Copyright (C) 2022- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "vine_manager_get.h"
#include "vine_file.h"
#include "vine_file_replica.h"
#include "vine_file_replica_table.h"
#include "vine_protocol.h"
#include "vine_task.h"
#include "vine_txn_log.h"
#include "vine_worker_info.h"

#include "create_dir.h"
#include "debug.h"
#include "host_disk_info.h"
#include "link.h"
#include "path.h"
#include "rmsummary.h"
#include "stringtools.h"
#include "timestamp.h"
#include "url_encode.h"

#include <errno.h>
#include <fcntl.h>
#include <unistd.h>

static vine_result_code_t vine_manager_get_symlink_contents(struct vine_manager *q, struct vine_worker_info *w,
		struct vine_task *t, const char *filename, int length);
static vine_result_code_t vine_manager_get_dir_contents(struct vine_manager *q, struct vine_worker_info *w,
		struct vine_task *t, const char *dirname, int64_t *totalsize);

/*
Get an output file from the task and return it as a buffer in memory.
The buffer is attached to the f->data element and can then be retrieved
by the application using vine_task_get_output_buffer.
*/

static vine_result_code_t vine_manager_get_buffer(struct vine_manager *q, struct vine_worker_info *w,
		struct vine_task *t, struct vine_file *f, int64_t *total_size)
{
	char line[VINE_LINE_MAX];
	char name_encoded[VINE_LINE_MAX];
	int64_t size;
	int mode;
	int errornum;

	vine_result_code_t r = VINE_WORKER_FAILURE;

	vine_msg_code_t mcode = vine_manager_recv(q, w, line, sizeof(line));
	if (mcode != VINE_MSG_NOT_PROCESSED)
		return VINE_WORKER_FAILURE;

	if (sscanf(line, "file %s %" SCNd64 " 0%o", name_encoded, &size, &mode) == 3) {

		f->size = size;
		debug(D_VINE,
				"Receiving buffer %s (size: %" PRId64 " bytes) from %s (%s) ...",
				f->source,
				(int64_t)f->size,
				w->addrport,
				w->hostname);

		f->data = malloc(size + 1);
		if (f->data) {
			time_t stoptime = time(0) + vine_manager_transfer_time(q, w, f->size);

			ssize_t actual = link_read(w->link, f->data, f->size, stoptime);
			if (actual >= 0 && (size_t)actual == f->size) {
				/* While not strictly necessary, add a null terminator to facilitate printing text data.
				 */
				f->data[f->size] = 0;
				*total_size += f->size;
				r = VINE_SUCCESS;
			} else {
				/* If insufficient data was read, the connection must be broken. */
				free(f->data);
				f->data = 0;
				r = VINE_WORKER_FAILURE;
			}
		} else {
			r = VINE_APP_FAILURE;
		}
	} else if (sscanf(line, "error %s %d", name_encoded, &errornum) == 2) {
		debug(D_VINE,
				"%s (%s): could not access buffer %s (%s)",
				w->hostname,
				w->addrport,
				f->source,
				strerror(errornum));
		/* Mark the task as missing an output, but return success to keep going. */
		vine_task_set_result(t, VINE_RESULT_OUTPUT_MISSING);
		r = VINE_SUCCESS;
	} else {
		r = VINE_WORKER_FAILURE;
	}

	return r;
}

/*
Receive the contents of a single file from a worker.
The "file" header has already been received, just
bring back the streaming data within various constraints.
*/

static vine_result_code_t vine_manager_get_file_contents(struct vine_manager *q, struct vine_worker_info *w,
		struct vine_task *t, const char *local_name, int64_t length, int mode)
{
	// If a bandwidth limit is in effect, choose the effective stoptime.
	timestamp_t effective_stoptime = 0;
	if (q->bandwidth_limit) {
		effective_stoptime = (length / q->bandwidth_limit) * 1000000 + timestamp_get();
	}

	// Choose the actual stoptime.
	time_t stoptime = time(0) + vine_manager_transfer_time(q, w, length);

	// If necessary, create parent directories of the file.
	char dirname[VINE_LINE_MAX];
	path_dirname(local_name, dirname);
	if (strchr(local_name, '/')) {
		if (!create_dir(dirname, 0777)) {
			debug(D_VINE, "Could not create directory - %s (%s)", dirname, strerror(errno));
			link_soak(w->link, length, stoptime);
			return VINE_MGR_FAILURE;
		}
	}

	// Create the local file.
	debug(D_VINE,
			"Receiving file %s (size: %" PRId64 " bytes) from %s (%s) ...",
			local_name,
			length,
			w->addrport,
			w->hostname);

	// Check if there is space for incoming file at manager
	if (!check_disk_space_for_filesize(dirname, length, q->disk_avail_threshold)) {
		debug(D_VINE,
				"Could not receive file %s, not enough disk space (%" PRId64 " bytes needed)\n",
				local_name,
				length);
		return VINE_MGR_FAILURE;
	}

	int fd = open(local_name, O_WRONLY | O_TRUNC | O_CREAT, 0777);
	if (fd < 0) {
		debug(D_NOTICE, "Cannot open file %s for writing: %s", local_name, strerror(errno));
		link_soak(w->link, length, stoptime);
		return VINE_MGR_FAILURE;
	}

	// Write the data on the link to file.
	int64_t actual = link_stream_to_fd(w->link, fd, length, stoptime);

	fchmod(fd, mode);

	if (close(fd) < 0) {
		warn(D_VINE, "Could not write file %s: %s\n", local_name, strerror(errno));
		unlink(local_name);
		return VINE_MGR_FAILURE;
	}

	if (actual != length) {
		debug(D_VINE,
				"Received item size (%" PRId64 ") does not match the expected size - %" PRId64
				" bytes.",
				actual,
				length);
		unlink(local_name);
		return VINE_WORKER_FAILURE;
	}

	// If the transfer was too fast, slow things down.
	timestamp_t current_time = timestamp_get();
	if (effective_stoptime && effective_stoptime > current_time) {
		usleep(effective_stoptime - current_time);
	}

	return VINE_SUCCESS;
}

/*
Get the contents of a symlink back from the worker,
after the "symlink" header has already been received.
*/

static vine_result_code_t vine_manager_get_symlink_contents(struct vine_manager *q, struct vine_worker_info *w,
		struct vine_task *t, const char *filename, int length)
{
	char *target = malloc(length);

	int actual = link_read(w->link, target, length, time(0) + q->short_timeout);
	if (actual != length) {
		free(target);
		return VINE_WORKER_FAILURE;
	}

	int result = symlink(target, filename);
	if (result < 0) {
		debug(D_VINE, "could not create symlink %s: %s", filename, strerror(errno));
		free(target);
		return VINE_MGR_FAILURE;
	}

	free(target);

	return VINE_SUCCESS;
}

/*
Get a single item (file, dir, symlink, etc) back
from the worker by observing the header and then
pulling the appropriate data on the stream.
Note that if forced_name is non-null, then the item
is stored under that filename.  Otherwise, it is placed
in the directory dirname with the filename given by the
worker.  This allows this function to handle both the
top-level case of renamed files as well as interior files
within a directory.
*/

static vine_result_code_t vine_manager_get_any(struct vine_manager *q, struct vine_worker_info *w, struct vine_task *t,
		const char *dirname, const char *forced_name, int64_t *totalsize)
{
	char line[VINE_LINE_MAX];
	char name_encoded[VINE_LINE_MAX];
	char name[VINE_LINE_MAX];
	int64_t size;
	int mode;
	int errornum;

	vine_result_code_t r = VINE_WORKER_FAILURE;

	vine_msg_code_t mcode = vine_manager_recv(q, w, line, sizeof(line));
	if (mcode != VINE_MSG_NOT_PROCESSED)
		return VINE_WORKER_FAILURE;

	if (sscanf(line, "file %s %" SCNd64 " 0%o", name_encoded, &size, &mode) == 3) {

		url_decode(name_encoded, name, sizeof(name));

		char *subname;
		if (forced_name) {
			subname = strdup(forced_name);
		} else {
			subname = string_format("%s/%s", dirname, name);
		}
		r = vine_manager_get_file_contents(q, w, t, subname, size, mode);
		free(subname);

		if (r == VINE_SUCCESS)
			*totalsize += size;

	} else if (sscanf(line, "symlink %s %" SCNd64, name_encoded, &size) == 2) {

		url_decode(name_encoded, name, sizeof(name));

		char *subname;
		if (forced_name) {
			subname = strdup(forced_name);
		} else {
			subname = string_format("%s/%s", dirname, name);
		}
		r = vine_manager_get_symlink_contents(q, w, t, subname, size);
		free(subname);

		if (r == VINE_SUCCESS)
			*totalsize += size;

	} else if (sscanf(line, "dir %s", name_encoded) == 1) {

		url_decode(name_encoded, name, sizeof(name));

		char *subname;
		if (forced_name) {
			subname = strdup(forced_name);
		} else {
			subname = string_format("%s/%s", dirname, name);
		}
		r = vine_manager_get_dir_contents(q, w, t, subname, totalsize);
		free(subname);

	} else if (sscanf(line, "error %s %d", name_encoded, &errornum) == 2) {

		// If the output file is missing, we make a note of that in the task result,
		// but we continue and consider the transfer a 'success' so that other
		// outputs are transferred and the task is given back to the caller.
		url_decode(name_encoded, name, sizeof(name));
		debug(D_VINE,
				"%s (%s): could not access requested file %s (%s)",
				w->hostname,
				w->addrport,
				name,
				strerror(errornum));
		vine_task_set_result(t, VINE_RESULT_OUTPUT_MISSING);

		r = VINE_SUCCESS;

	} else if (!strcmp(line, "end")) {
		r = VINE_END_OF_LIST;

	} else {
		debug(D_VINE, "%s (%s): sent invalid response to get: %s", w->hostname, w->addrport, line);
		r = VINE_WORKER_FAILURE;
	}

	return r;
}

/*
Retrieve the contents of a directory by creating the local
dir, then receiving each item in the directory until an "end"
header is received.
*/

static vine_result_code_t vine_manager_get_dir_contents(struct vine_manager *q, struct vine_worker_info *w,
		struct vine_task *t, const char *dirname, int64_t *totalsize)
{
	int result = mkdir(dirname, 0777);

	/* If the directory exists, no error, keep going. */
	if (result < 0 && errno != EEXIST) {
		debug(D_VINE, "unable to create %s: %s", dirname, strerror(errno));
		return VINE_APP_FAILURE;
	}

	while (1) {
		int r = vine_manager_get_any(q, w, t, dirname, 0, totalsize);
		if (r == VINE_SUCCESS) {
			// Successfully received one item.
			continue;
		} else if (r == VINE_END_OF_LIST) {
			// Sucessfully got end of sequence.
			return VINE_SUCCESS;
		} else {
			// Failed to receive item.
			return r;
		}
	}
}

/*
Get a single output file from a worker, independently of any task.
*/

vine_result_code_t vine_manager_get_single_file(struct vine_manager *q, struct vine_worker_info *w, struct vine_file *f)
{
	int64_t total_bytes;
	vine_manager_send(q, w, "getfile %s\n", f->cached_name);
	return vine_manager_get_buffer(q, w, 0, f, &total_bytes);
}

/*
Get a single output file, located at the worker under 'cached_name'.
*/

vine_result_code_t vine_manager_get_output_file(struct vine_manager *q, struct vine_worker_info *w, struct vine_task *t,
		struct vine_mount *m, struct vine_file *f)
{
	int64_t total_bytes = 0;
	vine_result_code_t result = VINE_SUCCESS; // return success unless something fails below.

	timestamp_t open_time = timestamp_get();

	debug(D_VINE, "%s (%s) sending back %s to %s", w->hostname, w->addrport, f->cached_name, f->source);

	if (f->type == VINE_FILE) {
		vine_manager_send(q, w, "get %s\n", f->cached_name);
		result = vine_manager_get_any(q, w, t, 0, f->source, &total_bytes);
	} else if (f->type == VINE_BUFFER) {
		vine_manager_send(q, w, "getfile %s\n", f->cached_name);
		result = vine_manager_get_buffer(q, w, t, f, &total_bytes);
	} else {
		result = VINE_APP_FAILURE;
	}

	timestamp_t close_time = timestamp_get();
	timestamp_t sum_time = close_time - open_time;

	if (total_bytes > 0) {
		q->stats->bytes_received += total_bytes;

		t->bytes_received += total_bytes;
		t->bytes_transferred += total_bytes;

		w->total_bytes_transferred += total_bytes;
		w->total_transfer_time += sum_time;

		debug(D_VINE,
				"%s (%s) sent %.2lf MB in %.02lfs (%.02lfs MB/s) average %.02lfs MB/s",
				w->hostname,
				w->addrport,
				total_bytes / 1000000.0,
				sum_time / 1000000.0,
				(double)total_bytes / sum_time,
				(double)w->total_bytes_transferred / w->total_transfer_time);

		vine_txn_log_write_transfer(q, w, t, m, f, total_bytes, sum_time, open_time, 0);
	}

	// If we failed to *transfer* the output file, then that is a hard
	// failure which causes this function to return failure and the task
	// to be returned to the queue to be attempted elsewhere.
	// But if we failed to *store* the file, that is a manager failure.

	if (result != VINE_SUCCESS) {
		debug(D_VINE,
				"%s (%s) failed to return output %s to %s",
				w->addrport,
				w->hostname,
				f->cached_name,
				f->source);

		if (result == VINE_APP_FAILURE) {
			vine_task_set_result(t, VINE_RESULT_OUTPUT_MISSING);
		} else if (result == VINE_MGR_FAILURE) {
			vine_task_set_result(t, VINE_RESULT_OUTPUT_TRANSFER_ERROR);
		}
	}

	// If the transfer was successful, make a record of it in the cache.
	if (result == VINE_SUCCESS && m->flags & VINE_CACHE) {
		struct stat local_info;
		if (stat(f->source, &local_info) == 0) {
			struct vine_file_replica *remote_info =
					vine_file_replica_create(local_info.st_size, local_info.st_mtime);
			vine_file_replica_table_insert(w, f->cached_name, remote_info);
		} else {
			debug(D_NOTICE, "Cannot stat file %s: %s", f->source, strerror(errno));
		}
	}

	return result;
}

/* Get all output files produced by a given task on this worker. */

vine_result_code_t vine_manager_get_output_files(
		struct vine_manager *q, struct vine_worker_info *w, struct vine_task *t)
{
	vine_result_code_t result = VINE_SUCCESS;

	if (t->output_mounts) {
		struct vine_mount *m;
		LIST_ITERATE(t->output_mounts, m)
		{
			// non-file objects are handled by the worker.
			if (m->file->type != VINE_FILE && m->file->type != VINE_BUFFER)
				continue;

			int task_succeeded = (t->result == VINE_RESULT_SUCCESS && t->exit_code == 0);

			// skip failure-only files on success
			if (m->flags & VINE_FAILURE_ONLY && task_succeeded)
				continue;

			// skip success-only files on failure
			if (m->flags & VINE_SUCCESS_ONLY && !task_succeeded)
				continue;

			// otherwise, get the file.
			result = vine_manager_get_output_file(q, w, t, m, m->file);

			// if success or app-level failure, continue to get other files.
			// if worker failure, return.
			if (result == VINE_WORKER_FAILURE) {
				break;
			}
		}
	}

	// tell the worker you no longer need that task's output directory.
	vine_manager_send(q, w, "kill %d\n", t->task_id);

	return result;
}

/*
Get only the resource monitor output file for a given task,
usually because the task has failed, and we want to know why.
*/

vine_result_code_t vine_manager_get_monitor_output_file(
		struct vine_manager *q, struct vine_worker_info *w, struct vine_task *t)
{
	vine_result_code_t result = VINE_SUCCESS;

	const char *summary_name = RESOURCE_MONITOR_REMOTE_NAME ".summary";

	struct vine_mount *m;
	if (t->output_mounts) {
		LIST_ITERATE(t->output_mounts, m)
		{
			if (!strcmp(summary_name, m->remote_name)) {
				result = vine_manager_get_output_file(q, w, t, m, m->file);
				break;
			}
		}
	}

	// tell the worker you no longer need that task's output directory.
	vine_manager_send(q, w, "kill %d\n", t->task_id);

	return result;
}
