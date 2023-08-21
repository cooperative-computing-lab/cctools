/*
Copyright (C) 2022- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "vine_manager_put.h"
#include "vine_current_transfers.h"
#include "vine_file.h"
#include "vine_file_replica.h"
#include "vine_file_replica_table.h"
#include "vine_mount.h"
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
#include "xxmalloc.h"

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <unistd.h>

char *vine_monitor_wrap(
		struct vine_manager *q, struct vine_worker_info *w, struct vine_task *t, struct rmsummary *limits);

/*
Send a symbolic link to the remote worker.
Note that the target of the link is sent
as the "body" of the link, following the
message header.
*/

static int vine_manager_put_symlink(struct vine_manager *q, struct vine_worker_info *w, struct vine_task *t,
		const char *localname, const char *remotename, int64_t *total_bytes)
{
	char target[VINE_LINE_MAX];

	int length = readlink(localname, target, sizeof(target));
	if (length < 0)
		return VINE_APP_FAILURE;

	char remotename_encoded[VINE_LINE_MAX];
	url_encode(remotename, remotename_encoded, sizeof(remotename_encoded));

	vine_manager_send(q, w, "symlink %s %d\n", remotename_encoded, length);

	link_write(w->link, target, length, time(0) + q->long_timeout);

	*total_bytes += length;

	return VINE_SUCCESS;
}

/*
Send a single file to the remote worker.
The transfer time is controlled by the size of the file.
If the transfer takes too long, then cancel it.
*/

static int vine_manager_put_file(struct vine_manager *q, struct vine_worker_info *w, struct vine_task *t,
		const char *localname, const char *remotename, struct stat info, int64_t *total_bytes)
{
	time_t stoptime;
	timestamp_t effective_stoptime = 0;
	int64_t actual = 0;

	/* normalize the mode so as not to set up invalid permissions */
	int mode = (info.st_mode | 0x600) & 0777;

	int64_t length = info.st_size;

	int fd = open(localname, O_RDONLY, 0);
	if (fd < 0) {
		debug(D_NOTICE, "Cannot open file %s: %s", localname, strerror(errno));
		return VINE_APP_FAILURE;
	}

	if (q->bandwidth_limit) {
		effective_stoptime = (length / q->bandwidth_limit) * 1000000 + timestamp_get();
	}

	/* filenames are url-encoded to avoid problems with spaces, etc */
	char remotename_encoded[VINE_LINE_MAX];
	url_encode(remotename, remotename_encoded, sizeof(remotename_encoded));

	stoptime = time(0) + vine_manager_transfer_time(q, w, length);
	vine_manager_send(q, w, "file %s %" PRId64 " 0%o\n", remotename_encoded, length, mode);
	actual = link_stream_from_fd(w->link, fd, length, stoptime);
	close(fd);

	*total_bytes += actual;

	if (actual != length)
		return VINE_WORKER_FAILURE;

	timestamp_t current_time = timestamp_get();
	if (effective_stoptime && effective_stoptime > current_time) {
		usleep(effective_stoptime - current_time);
	}

	return VINE_SUCCESS;
}

/* Need prototype here to address mutually recursive code. */

static vine_result_code_t vine_manager_put_file_or_dir(struct vine_manager *q, struct vine_worker_info *w,
		struct vine_task *t, const char *name, const char *remotename, int64_t *total_bytes, int follow_links);

/*
Send a directory and all of its contents using the new streaming protocol.
Do this by sending a "dir" prefix, then all of the directory contents,
and then an "end" marker.
*/

static vine_result_code_t vine_manager_put_directory(struct vine_manager *q, struct vine_worker_info *w,
		struct vine_task *t, const char *localname, const char *remotename, int64_t *total_bytes)
{
	DIR *dir = opendir(localname);
	if (!dir) {
		debug(D_NOTICE, "Cannot open dir %s: %s", localname, strerror(errno));
		return VINE_APP_FAILURE;
	}

	vine_result_code_t result = VINE_SUCCESS;

	char remotename_encoded[VINE_LINE_MAX];
	url_encode(remotename, remotename_encoded, sizeof(remotename_encoded));

	vine_manager_send(q, w, "dir %s\n", remotename_encoded);

	struct dirent *d;
	while ((d = readdir(dir))) {
		if (!strcmp(d->d_name, ".") || !strcmp(d->d_name, ".."))
			continue;

		char *localpath = string_format("%s/%s", localname, d->d_name);

		result = vine_manager_put_file_or_dir(q, w, t, localpath, d->d_name, total_bytes, 0);

		free(localpath);

		if (result != VINE_SUCCESS)
			break;
	}

	vine_manager_send(q, w, "end\n");

	closedir(dir);
	return result;
}

/*
Send a single item, whether it is a directory, symlink, or file.

Note 1: We call stat/lstat here a single time, and then pass it
to the underlying object so as not to minimize syscall work.

Note 2: This function is invoked at the top level with follow_links=1,
since it is common for the user to to pass in a top-level symbolic
link to a file or directory which they want transferred.
However, in recursive calls, follow_links is set to zero,
and internal links are not followed, they are sent natively.
*/

static vine_result_code_t vine_manager_put_file_or_dir(struct vine_manager *q, struct vine_worker_info *w,
		struct vine_task *t, const char *localpath, const char *remotepath, int64_t *total_bytes,
		int follow_links)
{
	struct stat info;
	int result = VINE_SUCCESS;

	if (follow_links) {
		result = stat(localpath, &info);
	} else {
		result = lstat(localpath, &info);
	}

	if (result >= 0) {
		if (S_ISDIR(info.st_mode)) {
			result = vine_manager_put_directory(q, w, t, localpath, remotepath, total_bytes);
		} else if (S_ISLNK(info.st_mode)) {
			result = vine_manager_put_symlink(q, w, t, localpath, remotepath, total_bytes);
		} else if (S_ISREG(info.st_mode)) {
			result = vine_manager_put_file(q, w, t, localpath, remotepath, info, total_bytes);
		} else {
			debug(D_NOTICE, "skipping unusual file: %s", strerror(errno));
		}
	} else {
		debug(D_NOTICE, "cannot stat file %s: %s", localpath, strerror(errno));
		result = VINE_APP_FAILURE;
	}

	return result;
}

/*
Send a url to generate a cached file,
if it has not already been cached there.  Note that the length
may be an estimate at this point and will be updated by return
message once the object is actually loaded into the cache.
*/

static vine_result_code_t vine_manager_put_url(
		struct vine_manager *q, struct vine_worker_info *w, struct vine_task *t, struct vine_file *f)
{
	char source_encoded[VINE_LINE_MAX];
	char cached_name_encoded[VINE_LINE_MAX];

	url_encode(f->source, source_encoded, sizeof(source_encoded));
	url_encode(f->cached_name, cached_name_encoded, sizeof(cached_name_encoded));

	char *transfer_id = vine_current_transfers_add(q, w, f->source);
	vine_manager_send(q,
			w,
			"puturl %s %s %lld %o %s\n",
			source_encoded,
			cached_name_encoded,
			(long long)f->size,
			0777,
			transfer_id);

	free(transfer_id);
	return VINE_SUCCESS;
}

/* Send a buffer object to the remote worker. */

vine_result_code_t vine_manager_put_buffer(struct vine_manager *q, struct vine_worker_info *w, struct vine_task *t,
		struct vine_file *f, int64_t *total_bytes)
{
	time_t stoptime = time(0) + vine_manager_transfer_time(q, w, f->size);
	vine_manager_send(q, w, "file %s %lld %o\n", f->cached_name, (long long)f->size, 0777);
	int64_t actual = link_putlstring(w->link, f->data, f->size, stoptime);
	if (actual >= 0 && (size_t)actual == f->size) {
		*total_bytes = actual;
		return VINE_SUCCESS;
	} else {
		*total_bytes = 0;
		return VINE_WORKER_FAILURE;
	}
}

/*
Send a single input file of any type to the given worker, and record the performance.
If the file has a chained dependency, send that first.
*/

static vine_result_code_t vine_manager_put_input_file(struct vine_manager *q, struct vine_worker_info *w,
		struct vine_task *t, struct vine_mount *m, struct vine_file *f)
{
	int64_t total_bytes = 0;
	vine_result_code_t result = VINE_SUCCESS; // return success unless something fails below

	timestamp_t open_time = timestamp_get();

	switch (f->type) {
	case VINE_FILE:
		debug(D_VINE, "%s (%s) needs file %s as %s", w->hostname, w->addrport, f->source, m->remote_name);
		result = vine_manager_put_file_or_dir(q, w, t, f->source, f->cached_name, &total_bytes, 1);
		break;

	case VINE_BUFFER:
		debug(D_VINE, "%s (%s) needs buffer as %s", w->hostname, w->addrport, m->remote_name);
		result = vine_manager_put_buffer(q, w, t, f, &total_bytes);
		break;

	case VINE_MINI_TASK:
		debug(D_VINE,
				"%s (%s) will produce %s via mini task %d",
				w->hostname,
				w->addrport,
				m->remote_name,
				f->mini_task->task_id);
		result = vine_manager_put_task(q, w, f->mini_task, 0, 0, f);
		break;

	case VINE_URL:
		debug(D_VINE, "%s (%s) will get %s from url %s", w->hostname, w->addrport, m->remote_name, f->source);
		result = vine_manager_put_url(q, w, t, f);
		break;

	case VINE_EMPTY_DIR:
		debug(D_VINE, "%s (%s) will create directory %s", w->hostname, w->addrport, m->remote_name);
		// Do nothing.  Empty directories are handled by the task specification, while recursive directories are
		// implemented as VINE_FILEs
		break;

	case VINE_TEMP:
		debug(D_VINE, "%s (%s) will use temp file %s", w->hostname, w->addrport, f->source);
		// Do nothing.  Temporary files are created and used in place.
		break;
	}

	if (result == VINE_SUCCESS) {
		timestamp_t close_time = timestamp_get();
		timestamp_t elapsed_time = close_time - open_time;

		t->bytes_sent += total_bytes;
		t->bytes_transferred += total_bytes;

		w->total_bytes_transferred += total_bytes;
		w->total_transfer_time += elapsed_time;

		q->stats->bytes_sent += total_bytes;

		// Write to the transaction log.
		if (f->type == VINE_FILE || f->type == VINE_BUFFER) {
			vine_txn_log_write_transfer(q, w, t, m, f, total_bytes, elapsed_time, open_time, 1);
		}

		// Avoid division by zero below.
		if (elapsed_time == 0)
			elapsed_time = 1;

		if (total_bytes > 0) {
			debug(D_VINE,
					"%s (%s) received %.2lf MB in %.02lfs (%.02lfs MB/s) average %.02lfs MB/s",
					w->hostname,
					w->addrport,
					total_bytes / 1000000.0,
					elapsed_time / 1000000.0,
					(double)total_bytes / elapsed_time,
					(double)w->total_bytes_transferred / w->total_transfer_time);
		}
	} else {
		debug(D_VINE,
				"%s (%s) failed to send %s (%" PRId64 " bytes sent).",
				w->hostname,
				w->addrport,
				f->type == VINE_BUFFER ? "literal data" : f->source,
				total_bytes);

		if (result == VINE_APP_FAILURE) {
			vine_task_set_result(t, VINE_RESULT_INPUT_MISSING);
		}
	}

	return result;
}

/*
Send a single input file, if it is not already noted in the worker's cache.
If already cached, check that the file has not changed.
*/

static vine_result_code_t vine_manager_put_input_file_if_needed(struct vine_manager *q, struct vine_worker_info *w,
		struct vine_task *t, struct vine_mount *m, struct vine_file *f)
{
	struct stat info;

	if (f->type == VINE_FILE) {
		/* If a regular file, check its status on the local filesystem. */
		int result = lstat(f->source, &info);
		if (result != 0) {
			debug(D_NOTICE | D_VINE, "Couldn't access input file %s: %s", f->source, strerror(errno));
			vine_task_set_result(t, VINE_RESULT_INPUT_MISSING);
			return VINE_APP_FAILURE;
		}
	} else if (!f->cached_name) {
		debug(D_NOTICE | D_VINE, "Cache name could not be generated for input file %s", f->source);
		vine_task_set_result(t, VINE_RESULT_INPUT_MISSING);
		if (f->type == VINE_URL)
			t->exit_code = 1;
		return VINE_APP_FAILURE;
	} else {
		/* Any other type, just record dummy values for size and time, until we know better. */
		info.st_size = f->size;
		info.st_mtime = time(0);
	}

	/* Has this file already been sent and cached? */
	struct vine_file_replica *remote_info = vine_file_replica_table_lookup(w, f->cached_name);

	/*
	If so, check that it hasn't changed, and return success.
	XXX The mtime might not be set (0) if the file was cached
	from a previous session.  This would work better if the
	mtime was sent in file transfers, and then returned by
	cache-update messages.
	*/
	if (remote_info) {
		if (f->type == VINE_FILE &&
				(info.st_size != remote_info->size ||
						((info.st_mtime != remote_info->mtime) && (remote_info->mtime != 0)))) {
			debug(D_NOTICE | D_VINE, "File %s has changed since it was first cached!", f->source);
			debug(D_NOTICE | D_VINE, "You may be getting inconsistent results.");
		}

		if (!(f->flags & VINE_CACHE)) {
			debug(D_VINE,
					"File %s is not marked as a cachable file, but it is used by more than one task. Marking as cachable.",
					f->source);
			f->flags |= VINE_CACHE;
		}

		/* If the file is already cached, don't send it. */
		return VINE_SUCCESS;
	}

	/*
	If a file has been substituted for a remote copy, send that instead,
	but account for the file using its original object.
	*/

	struct vine_file *file_to_send = m->substitute ? m->substitute : m->file;

	/* Now send the actual file. */
	vine_result_code_t result = vine_manager_put_input_file(q, w, t, m, file_to_send);

	/* If the send succeeded, then record it in the worker */
	if (result == VINE_SUCCESS) {
		struct vine_file_replica *remote_info = vine_file_replica_create(info.st_size, info.st_mtime);
		vine_file_replica_table_insert(w, f->cached_name, remote_info);

		/* If the file came from the manager we already sent it synchronously and we will not receive a cache
		 * update */
		switch (file_to_send->type) {
		case VINE_URL:
		case VINE_TEMP:
		case VINE_EMPTY_DIR:
			break;
		case VINE_FILE:
		case VINE_MINI_TASK:
		case VINE_BUFFER:
			remote_info->in_cache = 1;
			f->created = 1;
		}
	}

	return result;
}

/* Send all input files needed by a task to the given worker. */

vine_result_code_t vine_manager_put_input_files(struct vine_manager *q, struct vine_worker_info *w, struct vine_task *t)
{
	struct vine_mount *m;

	if (t->input_mounts) {
		LIST_ITERATE(t->input_mounts, m)
		{
			vine_result_code_t result = vine_manager_put_input_file_if_needed(q, w, t, m, m->file);
			if (result != VINE_SUCCESS)
				return result;
		}
	}
	return VINE_SUCCESS;
}

/*
Send the details of one task to a worker.
Note that this function just performs serialization of the task definition.
It does not perform any resource management.
This allows it to be used for both regular tasks and mini tasks.
*/

vine_result_code_t vine_manager_put_task(struct vine_manager *q, struct vine_worker_info *w, struct vine_task *t,
		const char *command_line, struct rmsummary *limits, struct vine_file *target)
{
	vine_result_code_t result = vine_manager_put_input_files(q, w, t);
	if (result != VINE_SUCCESS)
		return result;

	if (target) {
		vine_manager_send(q,
				w,
				"mini_task %lld %s %lld %o\n",
				(long long)target->mini_task->task_id,
				target->cached_name,
				(long long)target->size,
				0777);
	} else {
		vine_manager_send(q, w, "task %lld\n", (long long)t->task_id);
	}

	if (!command_line) {
		command_line = t->command_line;
	}

	long long cmd_len = strlen(command_line);
	vine_manager_send(q, w, "cmd %lld\n", (long long)cmd_len);
	link_putlstring(w->link, command_line, cmd_len, time(0) + q->short_timeout);
	debug(D_VINE, "%s\n", command_line);

	if (t->needs_library) {
		vine_manager_send(q, w, "needs_library %s\n", t->needs_library);
	}

	if (t->provides_library) {
		vine_manager_send(q, w, "provides_library %s\n", t->provides_library);
	}

	vine_manager_send(q, w, "category %s\n", t->category);

	if (limits) {
		vine_manager_send(q, w, "cores %s\n", rmsummary_resource_to_str("cores", limits->cores, 0));
		vine_manager_send(q, w, "gpus %s\n", rmsummary_resource_to_str("gpus", limits->gpus, 0));
		vine_manager_send(q, w, "memory %s\n", rmsummary_resource_to_str("memory", limits->memory, 0));
		vine_manager_send(q, w, "disk %s\n", rmsummary_resource_to_str("disk", limits->disk, 0));

		/* Do not set end, wall_time if running the resource monitor. We let the monitor police these resources.
		 */
		if (q->monitor_mode == VINE_MON_DISABLED) {
			if (limits->end > 0) {
				vine_manager_send(q,
						w,
						"end_time %s\n",
						rmsummary_resource_to_str("end", limits->end, 0));
			}
			if (limits->wall_time > 0) {
				vine_manager_send(q,
						w,
						"wall_time %s\n",
						rmsummary_resource_to_str("wall_time", limits->wall_time, 0));
			}
		}
	}

	/* Note that even when environment variables after resources, values for
	 * CORES, MEMORY, etc. will be set at the worker to the values of
	 * set_*, if used. */
	char *var;
	LIST_ITERATE(t->env_list, var) { vine_manager_send(q, w, "env %zu\n%s\n", strlen(var), var); }

	if (t->input_mounts) {
		struct vine_mount *m;
		LIST_ITERATE(t->input_mounts, m)
		{
			if (m->file->type == VINE_EMPTY_DIR) {
				vine_manager_send(q, w, "dir %s\n", m->remote_name);
			} else {
				char remote_name_encoded[PATH_MAX];
				url_encode(m->remote_name, remote_name_encoded, PATH_MAX);
				vine_manager_send(q,
						w,
						"infile %s %s %d\n",
						m->file->cached_name,
						remote_name_encoded,
						m->flags);
			}
		}
	}

	if (t->output_mounts) {
		struct vine_mount *m;
		LIST_ITERATE(t->output_mounts, m)
		{
			char remote_name_encoded[PATH_MAX];
			url_encode(m->remote_name, remote_name_encoded, PATH_MAX);
			vine_manager_send(q,
					w,
					"outfile %s %s %d\n",
					m->file->cached_name,
					remote_name_encoded,
					m->flags);
		}
	}

	// vine_manager_send returns the number of bytes sent, or a number less than
	// zero to indicate errors. We are lazy here, we only check the last
	// message we sent to the worker (other messages may have failed above).

	int r = vine_manager_send(q, w, "end\n");
	if (r >= 0) {
		return VINE_SUCCESS;
	} else {
		return VINE_WORKER_FAILURE;
	}
}
