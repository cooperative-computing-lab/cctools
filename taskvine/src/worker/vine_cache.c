/*
Copyright (C) 2022- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "vine_cache.h"
#include "vine_cache_file.h"
#include "vine_mount.h"
#include "vine_process.h"
#include "vine_sandbox.h"
#include "vine_worker.h"

#include "vine_protocol.h"
#include "vine_transfer.h"

#include "copy_stream.h"
#include "debug.h"
#include "domain_name_cache.h"
#include "hash_table.h"
#include "link.h"
#include "link_auth.h"
#include "path_disk_size_info.h"
#include "stringtools.h"
#include "timestamp.h"
#include "trash.h"
#include "xxmalloc.h"

#include <dirent.h>
#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include <sys/fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

struct vine_cache {
	struct hash_table *table;
	struct list *pending_transfers;
	struct list *processing_transfers;
	char *cache_dir;
	int max_transfer_procs;
};

static void vine_cache_wait_for_file(struct vine_cache *c, struct vine_cache_file *f, const char *cachename, struct link *manager);

/*
Create the cache manager structure for a given cache directory.
*/

struct vine_cache *vine_cache_create(const char *cache_dir, int max_procs)
{
	struct vine_cache *c = malloc(sizeof(*c));
	c->table = hash_table_create(0, 0);
	c->pending_transfers = list_create();
	c->processing_transfers = list_create();
	c->cache_dir = strdup(cache_dir);
	c->max_transfer_procs = max_procs;
	return c;
}

/*
Load existing cache directory into cache structure.
*/
void vine_cache_load(struct vine_cache *c)
{
	DIR *dir = opendir(c->cache_dir);
	if (dir) {
		debug(D_VINE, "loading cache at: %s", c->cache_dir);
		struct dirent *d;
		while ((d = readdir(dir))) {
			if (!strcmp(d->d_name, "."))
				continue;
			if (!strcmp(d->d_name, ".."))
				continue;

			/* If this is a .meta file, skip it, we are looking for the data files. */
			if (!strcmp(string_back(d->d_name, 5), ".meta")) {
				continue;
			}

			char *meta_path = vine_cache_meta_path(c, d->d_name);
			char *data_path = vine_cache_data_path(c, d->d_name);

			/* Note that the type and source here may be updated by metadata load. */
			struct vine_cache_file *f = vine_cache_file_create(VINE_CACHE_FILE, "manager", 0);

			if (vine_cache_file_load_metadata(f, meta_path)) {
				if (f->cache_level < VINE_CACHE_LEVEL_FOREVER) {
					debug(D_VINE, "cache: %s has cache-level %d, deleting", d->d_name, f->cache_level);
					trash_file(meta_path);
					trash_file(data_path);
					vine_cache_file_delete(f);
				} else {
					debug(D_VINE, "cache: %s has cache-level %d, keeping", d->d_name, f->cache_level);
					hash_table_insert(c->table, d->d_name, f);
					f->status = VINE_CACHE_STATUS_READY;
				}
			} else {
				debug(D_VINE, "cache: %s has invalid metadata, deleting", d->d_name);
				trash_file(meta_path);
				trash_file(data_path);
				vine_cache_file_delete(f);
			}

			free(meta_path);
			free(data_path);
		}
	}
	closedir(dir);
}

/*
send cache updates to manager from existing cache_directory
*/

void vine_cache_scan(struct vine_cache *c, struct link *manager)
{
	struct vine_cache_file *f;
	char *cachename;
	HASH_TABLE_ITERATE(c->table, cachename, f)
	{
		vine_worker_send_cache_update(manager, cachename, f->original_type, f->cache_level, f->size, f->mode, f->transfer_time, f->start_time);
	}
}

/*
Remove all entries with a cache level <= level.
*/

void vine_cache_prune(struct vine_cache *c, vine_cache_level_t level)
{
	struct vine_cache_file *f;
	char *cachename;
	HASH_TABLE_ITERATE(c->table, cachename, f)
	{
		if (f->cache_level <= level) {
			vine_cache_remove(c, cachename, 0);
		}
	}
}

/*
Kill off any process associated with this file object.
Used by both vine_cache_remove and vine_cache_delete.
*/

static void vine_cache_kill(struct vine_cache *c, struct vine_cache_file *f, const char *cachename, struct link *manager)
{
	while (f->status == VINE_CACHE_STATUS_PROCESSING) {
		debug(D_VINE, "cache: killing pending transfer process %d...", f->pid);
		kill(f->pid, SIGKILL);
		vine_cache_wait_for_file(c, f, cachename, manager);
		if (f->status == VINE_CACHE_STATUS_PROCESSING) {
			debug(D_VINE, "cache:still not killed, trying again!");
			sleep(1);
		}
	}
}

static int vine_cache_insert_pending_transfer(struct vine_cache *c, const char *cachename)
{
	return list_push_tail(c->pending_transfers, strdup(cachename));
}

static int vine_cache_insert_processing_transfer(struct vine_cache *c, const char *cachename)
{
	return list_push_tail(c->processing_transfers, strdup(cachename));
}

static int vine_cache_remove_pending_transfer(struct vine_cache *c, const char *cachename)
{
	int removed = 0;
	void *item;
	struct list_cursor *cur = list_cursor_create(c->pending_transfers);
	for (list_seek(cur, 0); list_get(cur, &item); list_next(cur)) {
		if (strcmp((char *)item, cachename) == 0) {
			list_drop(cur);
			free(item);
			removed = 1;
			break;
		}
	}
	list_cursor_destroy(cur);

	return removed;
}

static int vine_cache_remove_processing_transfer(struct vine_cache *c, const char *cachename)
{
	int removed = 0;
	void *item;
	struct list_cursor *cur = list_cursor_create(c->processing_transfers);
	for (list_seek(cur, 0); list_get(cur, &item); list_next(cur)) {
		if (strcmp((char *)item, cachename) == 0) {
			list_drop(cur);
			free(item);
			removed = 1;
			break;
		}
	}
	list_cursor_destroy(cur);

	return removed;
}

/*
Process pending transfers until we reach the maximum number of processing transfers or there are no more pending transfers.
*/
int vine_cache_process_pending_transfers(struct vine_cache *c)
{
	int processed = 0;

	while (list_size(c->processing_transfers) < c->max_transfer_procs && list_size(c->pending_transfers) > 0) {
		char *queue_cachename = list_pop_head(c->pending_transfers);
		if (!queue_cachename) {
			break;
		}
		vine_cache_status_t status = vine_cache_ensure(c, queue_cachename);
		if (status == VINE_CACHE_STATUS_PROCESSING) {
			processed++;
		}
		free(queue_cachename);
	}

	return processed;
}

/*
Delete the cache manager structure, though not the underlying files.
*/
void vine_cache_delete(struct vine_cache *c)
{
	/* Ensure that all child processes are killed off. */
	char *cachename;
	struct vine_cache_file *file;
	HASH_TABLE_ITERATE(c->table, cachename, file)
	{
		vine_cache_kill(c, file, cachename, 0);
	}

	/* clean up processing and pending transfers */
	char *queue_cachename;
	while ((queue_cachename = list_pop_head(c->pending_transfers))) {
		free(queue_cachename);
	}
	list_delete(c->pending_transfers);
	while ((queue_cachename = list_pop_head(c->processing_transfers))) {
		free(queue_cachename);
	}
	list_delete(c->processing_transfers);

	hash_table_clear(c->table, (void *)vine_cache_file_delete);
	hash_table_delete(c->table);
	free(c->cache_dir);
	free(c);
}

/* Get the full path to the data of a cached file. This result must be freed. */

char *vine_cache_data_path(struct vine_cache *c, const char *cachename)
{
	return string_format("%s/%s", c->cache_dir, cachename);
}

/* Get the full path to the metadata of a cached file. This result must be freed. */

char *vine_cache_meta_path(struct vine_cache *c, const char *cachename)
{
	return string_format("%s/%s.meta", c->cache_dir, cachename);
}

/* Get the full path to the transfer location of a cached file. This result must be freed. */

char *vine_cache_transfer_path(struct vine_cache *c, const char *cachename)
{
	return string_format("%s/%s", workspace->transfer_dir, cachename);
}

/* Get the full path to the error message from a transfer.  This result must be freed. */

char *vine_cache_error_path(struct vine_cache *c, const char *cachename)
{
	return string_format("%s/%s.error", workspace->transfer_dir, cachename);
}

/*
Add a file to the cache manager by renaming it from its current location
and writing out the metadata to the proper location.
*/

int vine_cache_add_file(
		struct vine_cache *c, const char *cachename, const char *transfer_path, vine_cache_level_t level, int mode, uint64_t size, time_t mtime, timestamp_t transfer_time)
{
	char *data_path = vine_cache_data_path(c, cachename);
	char *meta_path = vine_cache_meta_path(c, cachename);

	int result = 0;

	if (rename(transfer_path, data_path) == 0) {
		struct vine_cache_file *f = hash_table_lookup(c->table, cachename);
		if (f) {
			/* If the file object is already present, we are providing the missing data. */
		} else {
			/* If not, we are declaring a completely new file. */
			f = vine_cache_file_create(VINE_CACHE_FILE, "manager", 0);
			hash_table_insert(c->table, cachename, f);
		}

		/* Fill in the missing metadata. */
		f->cache_level = level;
		f->mode = mode;
		f->size = size;
		f->mtime = mtime;
		f->transfer_time = transfer_time;

		/* File has data and is ready to use. */
		f->status = VINE_CACHE_STATUS_READY;

		vine_cache_file_save_metadata(f, meta_path);

		result = 1;
	} else {
		result = 0;
	}

	free(data_path);
	free(meta_path);

	return result;
}

/*
Return true if the cache contains the requested item.
*/

int vine_cache_contains(struct vine_cache *c, const char *cachename)
{
	return hash_table_lookup(c->table, cachename) != 0;
}

/*
Queue a remote file transfer to produce a file.
This entry will be materialized later in vine_cache_ensure.
*/

int vine_cache_add_transfer(struct vine_cache *c, const char *cachename, const char *source, vine_cache_level_t level, int mode, uint64_t size, vine_cache_flags_t flags)
{
	/* Has this transfer already been queued? */
	struct vine_cache_file *f = hash_table_lookup(c->table, cachename);
	if (f) {
		/* The transfer is already queued up. */
		return 1;
	}

	/* Create the object and fill in the metadata. */

	f = vine_cache_file_create(VINE_CACHE_TRANSFER, source, 0);

	/*
	XXX Note that VINE_URL may not be right b/c puturl may be used to
	perform a worker-to-worker transfer of an object that was originally
	some other type.  We don't have that type here, and maybe we should.
	*/

	f->original_type = VINE_URL;
	f->cache_level = level;
	f->mode = mode;
	f->size = size;
	f->mtime = 0;
	f->transfer_time = 0;

	hash_table_insert(c->table, cachename, f);

	/* Note metadata is not saved here but when transfer is completed. */

	if (flags & VINE_CACHE_FLAGS_NOW) {
		vine_cache_ensure(c, cachename);
	}

	return 1;
}

/*
Queue a mini-task to produce a file.
This entry will be materialized later in vine_cache_ensure.
*/

int vine_cache_add_mini_task(struct vine_cache *c, const char *cachename, const char *source, struct vine_task *mini_task, vine_cache_level_t level, int mode, uint64_t size)
{
	/* Has this minitask already been queued? */
	struct vine_cache_file *f = hash_table_lookup(c->table, cachename);
	if (f) {
		/* The minitask is already queued up. */
		return 0;
	}

	/* Create the object and fill in the metadata. */

	f = vine_cache_file_create(VINE_CACHE_MINI_TASK, source, mini_task);
	f->original_type = VINE_MINI_TASK;
	f->cache_level = level;
	f->mode = mode;
	f->size = size;

	hash_table_insert(c->table, cachename, f);

	/* Note metadata is not saved here but when mini task is completed. */

	return 1;
}

/*
Remove a named item from the cache, regardless of its type.
*/

int vine_cache_remove(struct vine_cache *c, const char *cachename, struct link *manager)
{
	/* Careful: Don't remove the item rightaway, otherwise the cachename key becomes invalid. */

	struct vine_cache_file *f = hash_table_lookup(c->table, cachename);
	if (!f)
		return 0;

	/* Ensure that any child process associated with the entry is stopped. */
	vine_cache_kill(c, f, cachename, manager);

	/* Then remove the disk state associated with the file. */
	char *data_path = vine_cache_data_path(c, cachename);
	char *meta_path = vine_cache_meta_path(c, cachename);
	trash_file(data_path);
	trash_file(meta_path);
	free(data_path);
	free(meta_path);

	/* Now we can remove the data structure. */
	f = hash_table_remove(c->table, cachename);
	vine_cache_file_delete(f);

	return 1;
}

/*
Execute a shell command via popen and capture its output.
On success, return true.
On failure, return false with the string error_message filled in.
*/

static int do_internal_command(struct vine_cache *c, const char *command, char **error_message)
{
	int result = 0;
	*error_message = 0;

	debug(D_VINE, "cache: executing: %s", command);

	FILE *stream = popen(command, "r");
	if (stream) {
		copy_stream_to_buffer(stream, error_message, 0);
		int exit_status = pclose(stream);
		if (exit_status == 0) {
			if (*error_message) {
				free(*error_message);
				*error_message = 0;
			}
			result = 1;
		} else {
			debug(D_VINE, "cache: command failed with output: %s", *error_message);
			result = 0;
		}
	} else {
		*error_message = string_format("couldn't execute \"%s\": %s", command, strerror(errno));
		result = 0;
	}

	return result;
}

/*
Transfer a single input file from a url to the local transfer path via curl.
-s Do not show progress bar.  (Also disables errors.)
-S Show errors.
-L Follow redirects as needed.
--stderr Send errors to /dev/stdout so that they are observed by popen.
*/

static int do_curl_transfer(struct vine_cache *c, struct vine_cache_file *f, const char *transfer_path, const char *cache_path, char **error_message)
{
	char *command = string_format("curl -sSL --stderr /dev/stdout -o \"%s\" \"%s\"", transfer_path, f->source);
	int result = do_internal_command(c, command, error_message);
	free(command);

	return result;
}

/*
Create a file by executing a mini_task, which should produce the desired cachename.
The mini_task uses all the normal machinery to run a task synchronously,
which should result in the desired file being placed into the cache.
This will be double-checked below.
*/

static int do_mini_task(struct vine_cache *c, struct vine_cache_file *f, char **error_message)
{
	if (vine_process_execute_and_wait(f->process)) {
		*error_message = 0;
		return 1;
	} else {
		const char *str = vine_task_get_stdout(f->mini_task);
		if (str) {
			*error_message = xxstrdup(str);
		} else {
			*error_message = 0;
		}
		return 0;
	}
}

// rewrite hostname of source as seen from this worker
static int rewrite_source_to_ip(struct vine_cache_file *f, char **error_message)
{
	int port_num;
	char host[VINE_LINE_MAX], source_path[VINE_LINE_MAX];
	char addr[LINK_ADDRESS_MAX];

	// expect the form: worker://host:port/path/to/file
	sscanf(f->source, "worker://%256[^:]:%d/%s", host, &port_num, source_path);

	if (!domain_name_cache_lookup(host, addr)) {
		*error_message = string_format("Couldn't resolve hostname %s for %s", host, source_path);
		debug(D_VINE, "%s", *error_message);
		return 0;
	}

	free(f->source);
	f->source = string_format("workerip://%s:%d/%s", addr, port_num, source_path);

	return 1;
}

/*
Transfer a single input file from a worker url to a local file name.
*/
static int do_worker_transfer(struct vine_cache *c, struct vine_cache_file *f, const char *cachename, char **error_message)
{
	int port_num;
	char addr[VINE_LINE_MAX], source_path[VINE_LINE_MAX];
	int stoptime;
	struct link *worker_link;

	// expect the form: workerip://host:port/path/to/file
	sscanf(f->source, "workerip://%256[^:]:%d/%s", addr, &port_num, source_path);

	debug(D_VINE, "cache: setting up worker transfer file %s", f->source);

	stoptime = time(0) + 15;
	worker_link = link_connect(addr, port_num, stoptime);

	if (worker_link == NULL) {
		*error_message = string_format("Could not establish connection with worker at: %s:%d", addr, port_num);
		return 0;
	}

	if (options->password) {
		if (!link_auth_password(worker_link, options->password, time(0) + 5)) {
			*error_message = string_format("Could not authenticate to peer worker at %s:%d", addr, port_num);
			link_close(worker_link);
			return 0;
		}
	}

	/* XXX A fixed timeout of 900 certainly can't be right! */

	char *transfer_dir = vine_cache_transfer_path(c, ".");
	int64_t totalsize;
	int mode, mtime;

	if (!vine_transfer_request_any(worker_link, source_path, transfer_dir, &totalsize, &mode, &mtime, time(0) + 900)) {
		*error_message = string_format("Could not transfer file from %s", f->source);
		link_close(worker_link);
		return 0;
	}

	free(transfer_dir);

	/* At this point, the file is in the transfer path, but not yet in the cache. */

	link_close(worker_link);

	return 1;
}

/*
Transfer a single object into the cache,
whether by worker or via curl.
Use a temporary transfer path while downloading,
and then rename it into the proper place.
*/

static int do_transfer(struct vine_cache *c, struct vine_cache_file *f, const char *cachename, char **error_message)
{
	char *transfer_path = vine_cache_transfer_path(c, cachename);
	char *cache_path = vine_cache_data_path(c, cachename);

	int result = 0;

	if (strncmp(f->source, "workerip://", 11) == 0) {
		result = do_worker_transfer(c, f, cachename, error_message);
	} else if (strncmp(f->source, "worker://", 9) == 0) {
		result = rewrite_source_to_ip(f, error_message);
		if (result) {
			result = do_worker_transfer(c, f, cachename, error_message);
		}
	} else {
		result = do_curl_transfer(c, f, transfer_path, cache_path, error_message);
	}

	if (!result)
		trash_file(transfer_path);

	free(transfer_path);
	free(cache_path);

	return result;
}

/*
Child process that materializes the proper file.
*/

static void vine_cache_worker_process(struct vine_cache_file *f, struct vine_cache *c, const char *cachename)
{
	char *error_message = 0;
	char *cache_path = vine_cache_data_path(c, cachename);
	int result = 0;

	switch (f->cache_type) {
	case VINE_CACHE_FILE:
		result = 1;
		break;
	case VINE_CACHE_TRANSFER:
		result = do_transfer(c, f, cachename, &error_message);
		break;
	case VINE_CACHE_MINI_TASK:
		result = do_mini_task(c, f, &error_message);
		break;
	}

	/* At this point the file is now in the transfer path.  The parent will move it to the cache path. */

	/* If an error message was generated, save it to {transfer_path}.error for recovery by the parent process. */

	if (error_message) {
		debug(D_VINE, "cache: error when creating %s via mini task: %s", cachename, error_message);

		char *error_path = vine_cache_error_path(c, cachename);
		FILE *file = fopen(error_path, "w");
		if (file) {
			fprintf(file, "error creating file at worker: %s\n", error_message);
			fclose(file);
		}
		free(error_path);
		free(error_message);
	}

	free(cache_path);

	/* Exit status should be zero on success. */
	exit(result == 0);
}

/*
Ensure that a given cached entry is fully materialized in the cache,
downloading files or executing commands as needed.  If complete, return
VINE_CACHE_STATUS_READY, If downloading return VINE_CACHE_STATUS_PROCESSING
or VINE_CACHE_STATUS_TRANSFERRED. On failure return VINE_CACHE_STATUS_FAILED.
*/

vine_cache_status_t vine_cache_ensure(struct vine_cache *c, const char *cachename)
{
	if (!strcmp(cachename, "0"))
		return VINE_CACHE_STATUS_READY;

	struct vine_cache_file *f = hash_table_lookup(c->table, cachename);
	if (!f) {
		debug(D_VINE, "cache: %s is unknown, perhaps it failed to transfer earlier?", cachename);
		return VINE_CACHE_STATUS_UNKNOWN;
	}

	switch (f->status) {
	case VINE_CACHE_STATUS_PROCESSING:
	case VINE_CACHE_STATUS_TRANSFERRED:
	case VINE_CACHE_STATUS_READY:
	case VINE_CACHE_STATUS_FAILED:
	case VINE_CACHE_STATUS_UNKNOWN:
		return f->status;
	case VINE_CACHE_STATUS_PENDING:
		/* keep going */
		break;
	}

	/* For a mini-task, we must also insure the inputs to the task exist. */
	if (f->cache_type == VINE_CACHE_MINI_TASK) {
		if (f->mini_task->input_mounts) {
			struct vine_mount *m;
			vine_cache_status_t result;
			LIST_ITERATE(f->mini_task->input_mounts, m)
			{
				result = vine_cache_ensure(c, m->file->cached_name);
				if (result != VINE_CACHE_STATUS_READY)
					return result;
			}
		}
	}

	f->start_time = timestamp_get();

	debug(D_VINE, "cache: forking transfer process to create %s", cachename);

	if (f->cache_type == VINE_CACHE_MINI_TASK) {
		struct vine_process *p = vine_process_create(f->mini_task, VINE_PROCESS_TYPE_MINI_TASK);
		if (!vine_sandbox_stagein(p, c)) {
			debug(D_VINE, "cache: can't stage input files for task %d.", p->task->task_id);
			p->task = 0;
			vine_process_delete(p);
			f->status = VINE_CACHE_STATUS_FAILED;
			return f->status;
		}
		f->process = p;
	}

	int num_processing = list_size(c->processing_transfers);
	if (num_processing >= c->max_transfer_procs) {
		vine_cache_insert_pending_transfer(c, cachename);
		return VINE_CACHE_STATUS_PENDING;
	}

	f->pid = fork();

	if (f->pid < 0) {
		debug(D_VINE, "cache: failed to fork transfer process");
		f->status = VINE_CACHE_STATUS_FAILED;
		return f->status;
	} else if (f->pid > 0) {
		vine_cache_remove_pending_transfer(c, cachename);
		vine_cache_insert_processing_transfer(c, cachename);
		f->status = VINE_CACHE_STATUS_PROCESSING;
		switch (f->cache_type) {
		case VINE_CACHE_TRANSFER:
			debug(D_VINE, "cache: transferring %s to %s", f->source, cachename);
			break;
		case VINE_CACHE_MINI_TASK:
			debug(D_VINE, "cache: creating %s via mini task", cachename);
			break;
		case VINE_CACHE_FILE:
			debug(D_VINE, "cache: checking if %s is present in cache", cachename);
			break;
		}
		return f->status;
	} else {
		vine_cache_worker_process(f, c, cachename);
		_exit(1);
	}
}

/*
Check the outputs of a transfer process to make sure they are valid.
*/

static void vine_cache_check_outputs(struct vine_cache *c, struct vine_cache_file *f, const char *cachename, struct link *manager)
{
	char *cache_path = vine_cache_data_path(c, cachename);
	char *transfer_path = vine_cache_transfer_path(c, cachename);

	timestamp_t transfer_time = f->stop_time - f->start_time;

	/* If a mini-task move the output from sandbox to transfer path. */

	if (f->cache_type == VINE_CACHE_MINI_TASK) {
		if (f->status == VINE_CACHE_STATUS_TRANSFERRED) {

			char *source_path = vine_sandbox_full_path(f->process, f->source);

			debug(D_VINE, "cache: extracting %s from mini-task sandbox to %s\n", f->source, transfer_path);
			int result = rename(source_path, transfer_path);
			if (result != 0) {
				debug(D_VINE, "cache: unable to rename %s to %s: %s\n", source_path, transfer_path, strerror(errno));
				f->status = VINE_CACHE_STATUS_FAILED;
			}

			free(source_path);
		}

		/* Clean up the minitask process, but keep the defining task. */

		f->process->task = 0;
		vine_process_delete(f->process);
		f->process = 0;
	}

	/*
	At this point, all transfer types should result in a file at transfer path.
	Now measure and verify the file, and move it into the cache.
	*/

	if (f->status == VINE_CACHE_STATUS_TRANSFERRED) {

		int mode;
		time_t mtime;
		int64_t size;

		chmod(cache_path, f->mode);

		debug(D_VINE, "cache: measuring %s", transfer_path);
		if (vine_cache_file_measure_metadata(transfer_path, &mode, &size, &mtime)) {
			debug(D_VINE, "cache: created %s with size %lld in %lld usec", cachename, (long long)size, (long long)transfer_time);
			if (vine_cache_add_file(c, cachename, transfer_path, f->cache_level, mode, size, mtime, transfer_time)) {
				f->status = VINE_CACHE_STATUS_READY;
			} else {
				debug(D_VINE, "cache: unable to move %s to %s: %s\n", transfer_path, cache_path, strerror(errno));
				f->status = VINE_CACHE_STATUS_FAILED;
			}
		} else {
			debug(D_VINE, "cache: command succeeded but didn't create %s: %s\n", cachename, strerror(errno));
			f->status = VINE_CACHE_STATUS_FAILED;
		}
	} else {
		debug(D_VINE, "cache: command failed to complete for %s", cachename);
		f->status = VINE_CACHE_STATUS_FAILED;
	}

	/* Finally send a cache update message one way or the other. */
	/* Note that manager could be null if we are in a shutdown situation. */

	if (manager) {
		if (f->status == VINE_CACHE_STATUS_READY) {
			vine_worker_send_cache_update(manager, cachename, f->original_type, f->cache_level, f->size, f->mode, transfer_time, f->start_time);
		} else {
			char *error_path = vine_cache_error_path(c, cachename);
			char *error_message = NULL;
			size_t error_length;

			if (copy_file_to_buffer(error_path, &error_message, &error_length) > 0 && error_message) {
				/* got a message string */
			} else {
				error_message = strdup("unknown error");
			}

			vine_worker_send_cache_invalid(manager, cachename, error_message);

			trash_file(error_path);

			free(error_message);
			free(error_path);
		}
	}

	/* The transfer path is either moved to the cache or failed, so we can delete it safely. */
	trash_file(transfer_path);

	free(cache_path);
	free(transfer_path);
}

/*
Evaluate the exit status of a transfer process to determine if it succeeded.
*/

static void vine_cache_handle_exit_status(struct vine_cache *c, struct vine_cache_file *f, const char *cachename, int status, struct link *manager)
{
	f->stop_time = timestamp_get();

	if (!WIFEXITED(status)) {
		int sig = WTERMSIG(status);
		debug(D_VINE, "cache: transfer process (pid %d) exited abnormally with signal %d", f->pid, sig);
		f->status = VINE_CACHE_STATUS_FAILED;
	} else {
		int exit_code = WEXITSTATUS(status);
		debug(D_VINE, "cache: transfer process for %s (pid %d) exited normally with exit code %d", cachename, f->pid, exit_code);
		if (exit_code == 0) {
			debug(D_VINE, "cache: transfer process for %s completed", cachename);
			f->status = VINE_CACHE_STATUS_TRANSFERRED;
		} else {
			debug(D_VINE, "cache: transfer process for %s failed", cachename);
			f->status = VINE_CACHE_STATUS_FAILED;
		}
	}

	/* Reset pid so we know to stop scanning this entry. */
	f->pid = 0;
}

/*
Consider one cache table entry to determine if the transfer process has completed.
*/

static void vine_cache_wait_for_file(struct vine_cache *c, struct vine_cache_file *f, const char *cachename, struct link *manager)
{
	int status;
	if (f->status == VINE_CACHE_STATUS_PROCESSING) {
		int result = waitpid(f->pid, &status, WNOHANG);
		if (result == 0) {
			// process still executing
		} else if (result < 0) {
			debug(D_VINE, "cache: wait4 on pid %d returned an error: %s", (int)f->pid, strerror(errno));
		} else if (result > 0) {
			vine_cache_remove_processing_transfer(c, cachename);
			vine_cache_handle_exit_status(c, f, cachename, status, manager);
			vine_cache_check_outputs(c, f, cachename, manager);
		}
	}
}

/*
Search the cache table to determine if any transfer processes have completed.
*/

int vine_cache_wait(struct vine_cache *c, struct link *manager)
{
	struct vine_cache_file *f;
	char *cachename;
	HASH_TABLE_ITERATE(c->table, cachename, f)
	{
		vine_cache_wait_for_file(c, f, cachename, manager);
	}
	return 1;
}
