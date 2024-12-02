/*
Copyright (C) 2022- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "vine_manager.h"
#include "vine_blocklist.h"
#include "vine_counters.h"
#include "vine_current_transfers.h"
#include "vine_factory_info.h"
#include "vine_fair.h"
#include "vine_file.h"
#include "vine_file_replica.h"
#include "vine_file_replica_table.h"
#include "vine_manager_factory.h"
#include "vine_manager_get.h"
#include "vine_manager_put.h"
#include "vine_manager_summarize.h"
#include "vine_mount.h"
#include "vine_perf_log.h"
#include "vine_protocol.h"
#include "vine_resources.h"
#include "vine_runtime_dir.h"
#include "vine_schedule.h"
#include "vine_task.h"
#include "vine_task_info.h"
#include "vine_taskgraph_log.h"
#include "vine_txn_log.h"
#include "vine_worker_info.h"

#include "address.h"
#include "buffer.h"
#include "catalog_query.h"
#include "category_internal.h"
#include "cctools.h"
#include "copy_stream.h"
#include "create_dir.h"
#include "debug.h"
#include "domain_name_cache.h"
#include "envtools.h"
#include "hash_table.h"
#include "priority_queue.h"
#include "int_sizes.h"
#include "interfaces_address.h"
#include "itable.h"
#include "jx_parse.h"
#include "jx_print.h"
#include "link.h"
#include "link_auth.h"
#include "list.h"
#include "load_average.h"
#include "macros.h"
#include "path.h"
#include "pattern.h"
#include "process.h"
#include "random.h"
#include "rmonitor.h"
#include "rmonitor_poll.h"
#include "rmonitor_types.h"
#include "set.h"
#include "shell.h"
#include "stringtools.h"
#include "unlink_recursive.h"
#include "url_encode.h"
#include "username.h"
#include "xxmalloc.h"

#include <assert.h>
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <float.h>
#include <math.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

/* Default value for seconds between updates to the catalog. */
#define VINE_UPDATE_INTERVAL 60

/* Default value for seconds between measurement of manager local resources. */
#define VINE_RESOURCE_MEASUREMENT_INTERVAL 30

/* Default value for keepalive interval in seconds. */
#define VINE_DEFAULT_KEEPALIVE_INTERVAL 120

/* Default value for keepalive timeout in seconds. */
#define VINE_DEFAULT_KEEPALIVE_TIMEOUT 900

/* Default value before entity is considered again after last failure, in usecs */
#define VINE_DEFAULT_TRANSIENT_ERROR_INTERVAL (15 * ONE_SECOND)

/* Default maximum time that a library template can fail and retry, it over this number the template should be removed
 */
#define VINE_TASK_MAX_LIBRARY_RETRIES 15

/* Default value before disconnecting a worker that keeps forsaking tasks without any completions */
#define VINE_DEFAULT_MAX_FORSAKEN_PER_WORKER 10

/* Default value for maximum size of standard output from task.  (If larger, send to a separate file.) */
#define MAX_TASK_STDOUT_STORAGE (1 * GIGABYTE)

/* Default value for maximum number of workers to add in a single cycle before dealing with other matters. */
#define MAX_NEW_WORKERS 10

/* Default value for how frequently to check for tasks that do not fit any worker. */
#define VINE_LARGE_TASK_CHECK_INTERVAL 180000000 // 3 minutes in usecs

/* Default value for how frequently to allow calls to vine_hungry_computation. */
#define VINE_HUNGRY_CHECK_INTERVAL 5000000 // 5 seconds in usecs

/* Default timeout for slow workers to come back to the pool, can be set prior to creating a manager. */
double vine_option_blocklist_slow_workers_timeout = 900;

/* Forward prototypes for functions that are called out of order. */
/* Many of these should be removed if forward declaration is not needed. */

static void handle_failure(struct vine_manager *q, struct vine_worker_info *w, struct vine_task *t, vine_result_code_t fail_type);
static void handle_worker_failure(struct vine_manager *q, struct vine_worker_info *w);

static void reap_task_from_worker(struct vine_manager *q, struct vine_worker_info *w, struct vine_task *t, vine_task_state_t new_state);
static void reset_task_to_state(struct vine_manager *q, struct vine_task *t, vine_task_state_t new_state);
static void count_worker_resources(struct vine_manager *q, struct vine_worker_info *w);
static vine_result_code_t get_stdout(struct vine_manager *q, struct vine_worker_info *w, struct vine_task *t, int64_t output_length);
static vine_result_code_t retrieve_output(struct vine_manager *q, struct vine_worker_info *w, struct vine_task *t);

static void find_max_worker(struct vine_manager *q);
static void update_max_worker(struct vine_manager *q, struct vine_worker_info *w);

static vine_task_state_t change_task_state(struct vine_manager *q, struct vine_task *t, vine_task_state_t new_state);

static int task_request_count(struct vine_manager *q, const char *category, category_allocation_t request);

static vine_msg_code_t handle_http_request(struct vine_manager *q, struct vine_worker_info *w, const char *path, time_t stoptime);
static vine_msg_code_t handle_taskvine(struct vine_manager *q, struct vine_worker_info *w, const char *line);
static vine_msg_code_t handle_manager_status(struct vine_manager *q, struct vine_worker_info *w, const char *line, time_t stoptime);
static vine_msg_code_t handle_resources(struct vine_manager *q, struct vine_worker_info *w, time_t stoptime);
static vine_msg_code_t handle_feature(struct vine_manager *q, struct vine_worker_info *w, const char *line);
static void handle_library_update(struct vine_manager *q, struct vine_worker_info *w, const char *line);

static struct jx *manager_to_jx(struct vine_manager *q);
static struct jx *manager_lean_to_jx(struct vine_manager *q);

char *vine_monitor_wrap(struct vine_manager *q, struct vine_worker_info *w, struct vine_task *t, struct rmsummary *limits);

void vine_accumulate_task(struct vine_manager *q, struct vine_task *t);
struct category *vine_category_lookup_or_create(struct vine_manager *q, const char *name);

void vine_disable_monitoring(struct vine_manager *q);
static void aggregate_workers_resources(
		struct vine_manager *q, struct vine_resources *rtotal, struct vine_resources *rmin, struct vine_resources *rmax, int64_t *inuse_cache, struct hash_table *features);
static struct vine_task *vine_wait_internal(struct vine_manager *q, int timeout, const char *tag, int task_id);
static void release_all_workers(struct vine_manager *q);

static int vine_manager_check_inputs_available(struct vine_manager *q, struct vine_task *t);
static void vine_manager_consider_recovery_task(struct vine_manager *q, struct vine_file *lost_file, struct vine_task *rt);

static void delete_uncacheable_files(struct vine_manager *q, struct vine_worker_info *w, struct vine_task *t);
static int delete_worker_file(struct vine_manager *q, struct vine_worker_info *w, const char *filename, vine_cache_level_t cache_level, vine_cache_level_t delete_upto_level);

struct vine_task *send_library_to_worker(struct vine_manager *q, struct vine_worker_info *w, const char *name, vine_result_code_t *result);

/* Return the number of workers matching a given type: WORKER, STATUS, etc */

static int count_workers(struct vine_manager *q, vine_worker_type_t type)
{
	struct vine_worker_info *w;
	char *id;

	int count = 0;

	HASH_TABLE_ITERATE(q->worker_table, id, w)
	{
		if (w->type & type) {
			count++;
		}
	}

	return count;
}

/* Round up a resource value based on the overcommit multiplier currently in effect. */

int64_t overcommitted_resource_total(struct vine_manager *q, int64_t total)
{
	int64_t r = 0;
	if (total != 0) {
		r = ceil(total * q->resource_submit_multiplier);
	}

	return r;
}

/* Returns count of workers that are running at least 1 task. */

static int workers_with_tasks(struct vine_manager *q)
{
	struct vine_worker_info *w;
	char *id;
	int workers_with_tasks = 0;

	HASH_TABLE_ITERATE(q->worker_table, id, w)
	{
		if (strcmp(w->hostname, "unknown")) {
			if (itable_size(w->current_tasks)) {
				workers_with_tasks++;
			}
		}
	}

	return workers_with_tasks;
}

/* Convert a link pointer into a string that can be used as a key into a hash table. */

static char *link_to_hash_key(struct link *link)
{
	return string_format("0x%p", link);
}

/*
This function sends a message to the worker and records the time the message is
successfully sent. This timestamp is used to determine when to send keepalive checks.
*/

__attribute__((format(printf, 3, 4))) int vine_manager_send(struct vine_manager *q, struct vine_worker_info *w, const char *fmt, ...)
{
	va_list va;
	time_t stoptime;
	buffer_t B[1];
	buffer_init(B);
	buffer_abortonfailure(B, 1);
	buffer_max(B, VINE_LINE_MAX);

	va_start(va, fmt);
	buffer_putvfstring(B, fmt, va);
	va_end(va);

	debug(D_VINE, "tx to %s (%s): %s", w->hostname, w->addrport, buffer_tostring(B));

	stoptime = time(0) + q->short_timeout;

	int result = link_putlstring(w->link, buffer_tostring(B), buffer_pos(B), stoptime);

	buffer_free(B);

	return result;
}

/* Handle a name message coming back from the worker, requesting the manager's project name. */

static vine_msg_code_t handle_name(struct vine_manager *q, struct vine_worker_info *w, char *line)
{
	debug(D_VINE, "Sending project name to worker (%s)", w->addrport);

	// send project name (q->name) if there is one. otherwise send blank line
	vine_manager_send(q, w, "%s\n", q->name ? q->name : "");

	return VINE_MSG_PROCESSED;
}

/*
Handle a timeout request from a worker. Check if the worker has any important data before letting it go.
*/

static void handle_worker_timeout(struct vine_manager *q, struct vine_worker_info *w)
{
	// Look at the files and check if any are endangered temps.
	char *cachename;
	struct vine_file_replica *replica;
	// debug(D_VINE, "Handling timeout request");
	HASH_TABLE_ITERATE(w->current_files, cachename, replica)
	{
		if (replica->type == VINE_TEMP) {
			int c = vine_file_replica_table_count_replicas(q, cachename, VINE_FILE_REPLICA_STATE_READY);
			if (c == 1) {
				debug(D_VINE, "Rejecting timeout request from worker %s (%s). Has unique file %s", w->hostname, w->addrport, cachename);
				return;
			}
		}
	}

	if (itable_size(w->current_tasks) == 0) {
		debug(D_VINE, "Accepting timeout request from worker %s (%s).", w->hostname, w->addrport);
		q->stats->workers_idled_out++;
		vine_manager_shut_down_worker(q, w);
	}

	return;
}

/* Handle an info message coming from the worker that provides a variety of metrics. */

static vine_msg_code_t handle_info(struct vine_manager *q, struct vine_worker_info *w, char *line)
{
	char field[VINE_LINE_MAX];
	char value[VINE_LINE_MAX];

	int n = sscanf(line, "info %s %[^\n]", field, value);

	if (n != 2)
		return VINE_MSG_FAILURE;

	if (string_prefix_is(field, "tasks_running")) {
		w->dynamic_tasks_running = atoi(value);
	} else if (string_prefix_is(field, "idle-disconnect-request")) {
		handle_worker_timeout(q, w);
	} else if (string_prefix_is(field, "worker-id")) {
		free(w->workerid);
		w->workerid = xxstrdup(value);
		vine_txn_log_write_worker(q, w, 0, 0);
	} else if (string_prefix_is(field, "worker-end-time")) {
		w->end_time = MAX(0, atoll(value));
	} else if (string_prefix_is(field, "from-factory")) {
		vine_manager_factory_worker_arrive(q, w, value);
	} else if (string_prefix_is(field, "library-update")) {
		handle_library_update(q, w, value);
	}

	// Note we always mark info messages as processed, as they are optional.
	return VINE_MSG_PROCESSED;
}

/*
A cache-update message coming from the worker means that a requested
remote transfer or command was successful, and know we know the size
of the file for the purposes of cache storage management.
*/

static int handle_cache_update(struct vine_manager *q, struct vine_worker_info *w, const char *line)
{
	char cachename[VINE_LINE_MAX];
	int type;
	int cache_level;
	long long size;
	long long mtime;
	long long transfer_time;
	long long start_time;
	char id[VINE_LINE_MAX];

	if (sscanf(line, "cache-update %s %d %d %lld %lld %lld %lld %s", cachename, &type, &cache_level, &size, &mtime, &transfer_time, &start_time, id) == 8) {
		struct vine_file_replica *replica = vine_file_replica_table_lookup(w, cachename);

		if (!replica) {
			/*
			If an unsolicited cache-update arrives, there are several possibilities:
			- The worker is telling us about an item from a previous run.
			- The file was created as an output of a task.
			*/
			replica = vine_file_replica_create(type, cache_level, size, mtime);
			vine_file_replica_table_insert(q, w, cachename, replica);
		}

		replica->type = type;
		replica->cache_level = cache_level;
		replica->size = size;
		replica->mtime = mtime;
		replica->transfer_time = transfer_time;
		replica->state = VINE_FILE_REPLICA_STATE_READY;

		vine_current_transfers_set_success(q, id);
		vine_current_transfers_remove(q, id);

		vine_txn_log_write_cache_update(q, w, size, transfer_time, start_time, cachename);

		w->resources->disk.inuse += size / 1e6;

		/* If the replica corresponds to a declared file. */

		struct vine_file *f = hash_table_lookup(q->file_table, cachename);
		if (f) {
			/* We know it exists and how large it is now. */
			f->state = VINE_FILE_STATE_CREATED;
			f->size = size;

			/* And if the file is a newly created temporary, replicate as needed. */
			if (f->type == VINE_TEMP && *id == 'X' && q->temp_replica_count > 1) {
				hash_table_insert(q->temp_files_to_replicate, f->cached_name, NULL);
			}
		}
	}

	return VINE_MSG_PROCESSED;
}

/*
A cache-invalid message coming from the worker means that a requested
remote transfer or command did not succeed, and the intended file is
not in the cache.  It is accompanied by a (presumably short) string
message that further explains the failure.
So, we remove the corresponding note for that worker and log the error.
We should expect to soon receive some failed tasks that were unable
set up their own input sandboxes.
*/

static int handle_cache_invalid(struct vine_manager *q, struct vine_worker_info *w, const char *line)
{
	char cachename[VINE_LINE_MAX];
	char transfer_id[VINE_LINE_MAX];
	int length;

	/* The third field (transfer_id) is optional. */
	int n = sscanf(line, "cache-invalid %s %d %s", cachename, &length, transfer_id);

	/* If we got two or more fields... */
	if (n >= 2) {

		/* Read back the error message following. */

		char *message = malloc(length + 1);
		time_t stoptime = time(0) + q->long_timeout;

		int actual = link_read(w->link, message, length, stoptime);
		if (actual != length) {
			free(message);
			return VINE_MSG_FAILURE;
		}

		message[length] = 0;
		debug(D_VINE, "%s (%s) invalidated %s with error: %s", w->hostname, w->addrport, cachename, message);
		free(message);

		/* Remove the replica from our records. */
		struct vine_file_replica *replica = vine_file_replica_table_remove(q, w, cachename);
		if (replica) {
			vine_file_replica_delete(replica);
		}

		/* If the third argument was given, also remove the transfer record */
		if (n >= 3) {
			vine_current_transfers_set_failure(q, transfer_id);
			vine_current_transfers_remove(q, transfer_id);
		} else {
			/* throttle workers that could transfer a file */
			w->last_failure_time = timestamp_get();
		}

		/* Successfully processed this message. */
		return VINE_MSG_PROCESSED;
	} else {
		/* Otherwise this is an invalid message format. */
		return VINE_MSG_FAILURE;
	}
}

/*
A transfer-port message indicates that the worker is listening
on its own port to receive get requests from other workers.
*/

static int handle_transfer_port(struct vine_manager *q, struct vine_worker_info *w, const char *line)
{
	int dummy_port;

	int n = sscanf(line, "transfer-port %d", &w->transfer_port);
	if (n != 1) {
		return VINE_MSG_FAILURE;
	}

	w->transfer_port_active = 1;
	link_address_remote(w->link, w->transfer_host, &dummy_port);

	free(w->transfer_url);
	w->transfer_url = string_format("workerip://%s:%d", w->transfer_host, w->transfer_port);

	return VINE_MSG_PROCESSED;
}

/*
A transfer-hostport message indicates that the worker is listening
on one address, but the connections are made to an explicitely set
host and port, because of rerouting.
*/

static int handle_transfer_hostport(struct vine_manager *q, struct vine_worker_info *w, const char *line)
{
	int n = sscanf(line, "transfer-hostport %s %d", w->transfer_host, &w->transfer_port);
	if (n != 2) {
		return VINE_MSG_FAILURE;
	}

	w->transfer_port_active = 1;

	int is_ip = address_is_valid_ip(w->transfer_host);
	free(w->transfer_url);
	w->transfer_url = string_format("worker%s://%s:%d", is_ip ? "ip" : "", w->transfer_host, w->transfer_port);

	return VINE_MSG_PROCESSED;
}

static vine_result_code_t get_completion_result(struct vine_manager *q, struct vine_worker_info *w, const char *line)
{
	if (!q || !w || !line)
		return VINE_WORKER_FAILURE;

	struct vine_task *t;
	int task_status, exit_status;
	uint64_t task_id;
	int64_t output_length, bytes_sent, sandbox_used;

	timestamp_t execution_time, start_time, end_time;
	timestamp_t observed_execution_time;

	// Format: task completion status, exit status (exit code or signal), output length, bytes_sent, execution time,
	// task_id
	int n = sscanf(line,
			"complete %d %d %" SCNd64 " %" SCNd64 " %" SCNd64 " %" SCNd64 " %" SCNd64 " %" SCNd64 "",
			&task_status,
			&exit_status,
			&output_length,
			&bytes_sent,
			&start_time,
			&end_time,
			&sandbox_used,
			&task_id);

	if (n < 7) {
		debug(D_VINE, "Invalid message from worker %s (%s): %s", w->hostname, w->addrport, line);
		return VINE_WORKER_FAILURE;
	}

	execution_time = end_time - start_time;

	/* If the worker sent back a task we have never heard of, then discard the following data. */
	t = itable_lookup(w->current_tasks, task_id);
	if (!t) {
		debug(D_VINE, "Unknown task completion from worker %s (%s): no task %" PRId64 " assigned to worker. Ignoring result.", w->hostname, w->addrport, task_id);

		time_t stoptime = time(0) + vine_manager_transfer_time(q, w, output_length);
		link_soak(w->link, output_length, stoptime);
		return VINE_SUCCESS;
	}

	if (task_status != VINE_RESULT_SUCCESS) {
		w->last_failure_time = timestamp_get();
		t->time_when_last_failure = w->last_failure_time;
	}

	/* If the task was forsaken by the worker or couldn't exeute, it didn't really complete.*/
	if (task_status == VINE_RESULT_FORSAKEN) {
		t->forsaken_count++;
	} else if (task_status == VINE_RESULT_LIBRARY_EXIT) {
		debug(D_VINE, "Task %d library %s failed", t->task_id, t->provides_library);
		struct vine_task *original = hash_table_lookup(q->library_templates, t->provides_library);
		if (original) {
			original->library_failed_count++;
			original->time_when_last_failure = timestamp_get();
		}
		printf("Library %s failed on worker %s (%s)", t->provides_library, w->hostname, w->addrport);
		if (q->watch_library_logfiles)
			printf(", check the library log file %s\n", t->library_log_path);
		else
			printf(", enable watch-library-logfiles for debug\n");
	} else {
		/* Update task stats for this completion. */
		observed_execution_time = timestamp_get() - t->time_when_commit_end;

		t->time_workers_execute_last = observed_execution_time > execution_time ? execution_time : observed_execution_time;
		t->time_workers_execute_last_start = start_time;
		t->time_workers_execute_last_end = end_time;
		t->time_workers_execute_all += t->time_workers_execute_last;
		t->output_length = output_length;
		t->result = task_status;
		t->exit_code = exit_status;

		/* If output is less than 1KB stdout is sent along with completion msg. retrieve it from the link. */
		if (bytes_sent) {
			get_stdout(q, w, t, bytes_sent);
			t->output_received = 1;
			/* worker sent no bytes as output length is 0 */
		} else if (!bytes_sent && !t->output_length) {
			get_stdout(q, w, t, bytes_sent);
			t->output_received = 1;
		}

		/* Update queue stats for this completion. */
		q->stats->time_workers_execute += t->time_workers_execute_last;

		/* Update worker stats for this completion. */
		w->finished_tasks++;

		// Convert resource_monitor status into taskvine status if needed.
		if (q->monitor_mode) {
			if (t->exit_code == RM_OVERFLOW) {
				task_status = VINE_RESULT_RESOURCE_EXHAUSTION;
			} else if (t->exit_code == RM_TIME_EXPIRE) {
				task_status = VINE_RESULT_MAX_END_TIME;
			}
		}

		t->sandbox_measured = sandbox_used;

		/* Update category disk info */
		struct category *c = vine_category_lookup_or_create(q, t->category);
		if (sandbox_used > c->min_vine_sandbox) {
			c->min_vine_sandbox = sandbox_used;
		}

		hash_table_insert(q->workers_with_complete_tasks, w->hashkey, w);
	}

	/* Finally update data structures to reflect the completion. */
	change_task_state(q, t, VINE_TASK_WAITING_RETRIEVAL);
	itable_remove(q->running_table, t->task_id);
	vine_task_set_result(t, task_status);

	return VINE_SUCCESS;
}

/*
A completion message is an asynchronous message that indicates a task has completed.
The manager decides how to handle completion based on the task.
*/

static vine_msg_code_t handle_complete(struct vine_manager *q, struct vine_worker_info *w, const char *line)
{
	vine_result_code_t result = get_completion_result(q, w, line);
	if (result == VINE_SUCCESS) {
		return VINE_MSG_PROCESSED;
	}
	return VINE_MSG_NOT_PROCESSED;
}

/*
This function receives a message from worker and records the time a message is successfully
received. This timestamp is used in keepalive timeout computations.
*/

static vine_msg_code_t vine_manager_recv_no_retry(struct vine_manager *q, struct vine_worker_info *w, char *line, size_t length)
{
	time_t stoptime;
	stoptime = time(0) + q->long_timeout;

	int result = link_readline(w->link, line, length, stoptime);

	if (result <= 0) {
		return VINE_MSG_FAILURE;
	}

	w->last_msg_recv_time = timestamp_get();

	debug(D_VINE, "rx from %s (%s): %s", w->hostname, w->addrport, line);

	char path[length];

	// Check for status updates that can be consumed here.
	if (string_prefix_is(line, "alive")) {
		result = VINE_MSG_PROCESSED;
	} else if (string_prefix_is(line, "taskvine")) {
		result = handle_taskvine(q, w, line);
	} else if (string_prefix_is(line, "manager_status") || string_prefix_is(line, "worker_status") || string_prefix_is(line, "task_status") ||
			string_prefix_is(line, "wable_status") || string_prefix_is(line, "resources_status")) {
		result = handle_manager_status(q, w, line, stoptime);
	} else if (string_prefix_is(line, "available_results")) {
		hash_table_insert(q->workers_with_watched_file_updates, w->hashkey, w);
		result = VINE_MSG_PROCESSED;
	} else if (string_prefix_is(line, "resources")) {
		result = handle_resources(q, w, stoptime);
	} else if (string_prefix_is(line, "feature")) {
		result = handle_feature(q, w, line);
	} else if (string_prefix_is(line, "auth")) {
		debug(D_VINE | D_NOTICE, "worker (%s) is attempting to use a password, but I do not have one.", w->addrport);
		result = VINE_MSG_FAILURE;
	} else if (string_prefix_is(line, "name")) {
		result = handle_name(q, w, line);
	} else if (string_prefix_is(line, "info")) {
		result = handle_info(q, w, line);
	} else if (string_prefix_is(line, "cache-update")) {
		result = handle_cache_update(q, w, line);
	} else if (string_prefix_is(line, "cache-invalid")) {
		result = handle_cache_invalid(q, w, line);
	} else if (string_prefix_is(line, "transfer-hostport")) {
		result = handle_transfer_hostport(q, w, line);
	} else if (string_prefix_is(line, "transfer-port")) {
		result = handle_transfer_port(q, w, line);
	} else if (sscanf(line, "GET %s HTTP/%*d.%*d", path) == 1) {
		result = handle_http_request(q, w, path, stoptime);
	} else if (string_prefix_is(line, "complete")) {
		result = handle_complete(q, w, line);
	} else {
		// Message is not a status update: return it to the user.
		result = VINE_MSG_NOT_PROCESSED;
	}

	return result;
}

/*
Call vine_manager_recv_no_retry and silently retry if the result indicates
an asynchronous update message like 'keepalive' or 'resource'.
*/

vine_msg_code_t vine_manager_recv(struct vine_manager *q, struct vine_worker_info *w, char *line, int length)
{
	vine_msg_code_t result = VINE_MSG_PROCESSED;

	do {
		result = vine_manager_recv_no_retry(q, w, line, length);
	} while (result == VINE_MSG_PROCESSED);

	return result;
}

/*
Compute the expected transfer rate of the manage in bytes/second,
and return the basis of that computation in *data_source.
*/

static double get_manager_transfer_rate(struct vine_manager *q, char **data_source)
{
	double manager_transfer_rate; // bytes per second
	int64_t q_total_bytes_transferred = q->stats->bytes_sent + q->stats->bytes_received;
	timestamp_t q_total_transfer_time = q->stats->time_send + q->stats->time_receive;

	// Note q_total_transfer_time is timestamp_t with units of microseconds.
	if (q_total_transfer_time > 1000000) {
		manager_transfer_rate = 1000000.0 * q_total_bytes_transferred / q_total_transfer_time;
		if (data_source) {
			*data_source = xxstrdup("overall manager");
		}
	} else {
		manager_transfer_rate = q->default_transfer_rate;
		if (data_source) {
			*data_source = xxstrdup("conservative default");
		}
	}

	return manager_transfer_rate;
}

/*
Select an appropriate timeout value for the transfer of a certain number of bytes.
We do not know in advance how fast the system will perform.

So do this by starting with an assumption of bandwidth taken from the worker,
from the manager, or from a (slow) default number, depending on what information is available.
The timeout is chosen to be a multiple of the expected transfer time from the assumed bandwidth.

The overall effect is to reject transfers that are 10x slower than what has been seen before.

Two exceptions are made:
- The transfer time cannot be below a configurable minimum time.
*/

int vine_manager_transfer_time(struct vine_manager *q, struct vine_worker_info *w, int64_t length)
{
	double avg_transfer_rate; // bytes per second
	char *data_source;

	if (w->total_transfer_time > 1000000) {
		// Note w->total_transfer_time is timestamp_t with units of microseconds.
		avg_transfer_rate = 1000000 * w->total_bytes_transferred / w->total_transfer_time;
		data_source = xxstrdup("worker's observed");
	} else {
		avg_transfer_rate = get_manager_transfer_rate(q, &data_source);
	}

	double tolerable_transfer_rate = avg_transfer_rate / q->transfer_outlier_factor; // bytes per second

	int timeout = length / tolerable_transfer_rate;

	// An ordinary manager has a lower minimum timeout b/c it responds immediately to the manager.
	timeout = MAX(q->minimum_transfer_timeout, timeout);

	/* Don't bother printing anything for transfers of less than 1MB, to avoid excessive output. */

	if (length >= 1048576) {
		debug(D_VINE, "%s (%s) using %s average transfer rate of %.2lf MB/s\n", w->hostname, w->addrport, data_source, avg_transfer_rate / MEGABYTE);

		debug(D_VINE, "%s (%s) will try up to %d seconds to transfer this %.2lf MB file.", w->hostname, w->addrport, timeout, length / 1000000.0);
	}

	free(data_source);
	return timeout;
}

/* Read from the catalog if fetch_factory is enabled. */

static void update_read_catalog(struct vine_manager *q)
{
	time_t stoptime = time(0) + 5; // Short timeout for query

	if (q->fetch_factory) {
		vine_manager_factory_update_all(q, stoptime);
	}
}

/*
Send an update to the catalog describing the state of this manager.
*/

static void update_write_catalog(struct vine_manager *q)
{
	// Only write if we have a name.
	if (!q->name)
		return;

	// Generate the manager status in an jx, and print it to a buffer.

	struct jx *j = manager_to_jx(q);
	char *str = jx_print_string(j);

	// Send the buffer.
	debug(D_VINE, "Advertising manager status to the catalog server(s) at %s ...", q->catalog_hosts);
	if (!catalog_query_send_update(q->catalog_hosts, str, CATALOG_UPDATE_BACKGROUND | CATALOG_UPDATE_CONDITIONAL)) {

		// If the send failed b/c the buffer is too big, send the lean version instead.
		struct jx *lj = manager_lean_to_jx(q);
		char *lstr = jx_print_string(lj);
		catalog_query_send_update(q->catalog_hosts, lstr, CATALOG_UPDATE_BACKGROUND);
		free(lstr);
		jx_delete(lj);
	}

	// Clean up.
	free(str);
	jx_delete(j);
}

/* Send and receive updates from the catalog server as needed. */

static void update_catalog(struct vine_manager *q, int force_update)
{
	// Only update every last_update_time seconds.
	if (!force_update && (time(0) - q->catalog_last_update_time) < q->update_interval)
		return;

	// If host and port are not set, pick defaults.
	if (!q->catalog_hosts)
		q->catalog_hosts = xxstrdup(CATALOG_HOST);

	// Update the catalog.
	update_write_catalog(q);

	update_read_catalog(q);

	q->catalog_last_update_time = time(0);
}

void vine_update_catalog(struct vine_manager *m)
{
	if (m) {
		update_catalog(m, 1);
	}
}

static void cleanup_worker_files(struct vine_manager *q, struct vine_worker_info *w)
{
	int i = 0;
	int nfiles = hash_table_size(w->current_files);

	if (nfiles < 1) {
		return;
	}

	char *cached_name = NULL;
	struct vine_file_replica *replica = NULL;
	char **cached_names = malloc(nfiles * sizeof(char *));

	HASH_TABLE_ITERATE(w->current_files, cached_name, replica)
	{
		cached_names[i] = cached_name;
		i++;
	}

	for (i = 0; i < nfiles; i++) {
		cached_name = cached_names[i];
		struct vine_file *f = hash_table_lookup(q->file_table, cached_name);

		// check that the manager actually knows about that file, as the file
		// may correspond to a cache-update of a file that has not been declared
		// yet.
		if (!f || !delete_worker_file(q, w, f->cached_name, f->cache_level, VINE_CACHE_LEVEL_WORKFLOW)) {
			replica = vine_file_replica_table_remove(q, w, cached_name);
			if (replica) {
				vine_file_replica_delete(replica);
			}
		}
	}

	free(cached_names);
}

/* Remove all tasks and other associated state from a given worker. */
static void cleanup_worker(struct vine_manager *q, struct vine_worker_info *w)
{
	struct vine_task *t;
	uint64_t task_id;

	if (!q || !w)
		return;

	vine_current_transfers_wipe_worker(q, w);

	ITABLE_ITERATE(w->current_tasks, task_id, t)
	{
		if (t->time_when_commit_end >= t->time_when_commit_start) {
			timestamp_t delta_time = timestamp_get() - t->time_when_commit_end;
			t->time_workers_execute_failure += delta_time;
			t->time_workers_execute_all += delta_time;
		}

		reap_task_from_worker(q, w, t, VINE_TASK_READY);

		// recreate inputs lost
		if (q->immediate_recovery) {
			vine_manager_check_inputs_available(q, t);
		}

		vine_task_clean(t);

		itable_firstkey(w->current_tasks);
	}

	itable_clear(w->current_tasks, 0);

	w->finished_tasks = 0;

	cleanup_worker_files(q, w);
}

/* Start replicating files that may need replication */

static int recover_temp_files(struct vine_manager *q)
{
	char *cached_name = NULL;
	void *empty_val = NULL;
	int total_replication_count = 0;

	static char key_start[PATH_MAX] = "random init";
	int iter_control;
	int iter_count_var;

	HASH_TABLE_ITERATE_FROM_KEY(q->temp_files_to_replicate, iter_control, iter_count_var, key_start, cached_name, empty_val)
	{
		struct vine_file *f = hash_table_lookup(q->file_table, cached_name);

		if (f) {
			int round_replication_count = vine_file_replica_table_replicate(q, f);

			/* Worker busy or no replicas found */
			if (round_replication_count < 1) {
				/*
				If no replicas are found, it indicates that the file doesn't exist, either pruned or lost.
				Because a pruned file is removed from the recovery queue, so it definitely indicates that the file is lost.
				*/
				if (!vine_file_replica_table_exists_somewhere(q, f->cached_name) && q->transfer_temps_recovery) {
					vine_manager_consider_recovery_task(q, f, f->recovery_task);
				}
				hash_table_remove(q->temp_files_to_replicate, cached_name);
			} else {
				if (iter_count_var > q->attempt_schedule_depth) {
					strncpy(key_start, cached_name, PATH_MAX - 1);
					key_start[PATH_MAX - 1] = '\0';
					break;
				}
			}

			total_replication_count += round_replication_count;
		}
	}

	return total_replication_count;
}

/* Insert into hashtable temp files that may need replication. */

static void recall_worker_lost_temp_files(struct vine_manager *q, struct vine_worker_info *w)
{
	char *cached_name = NULL;
	struct vine_file_replica *info = NULL;

	debug(D_VINE, "Recalling worker %s's temp files", w->hostname);

	// Iterate over files we want might want to recover
	HASH_TABLE_ITERATE(w->current_files, cached_name, info)
	{
		struct vine_file *f = hash_table_lookup(q->file_table, cached_name);

		if (f && f->type == VINE_TEMP) {
			hash_table_insert(q->temp_files_to_replicate, cached_name, NULL);
		}
	}
}

/* Remove a worker from this master by removing all remote state, all local state, and disconnecting. */

void vine_manager_remove_worker(struct vine_manager *q, struct vine_worker_info *w, vine_worker_disconnect_reason_t reason)
{
	if (!q || !w)
		return;

	debug(D_VINE, "worker %s (%s) removed", w->hostname, w->addrport);

	if (w->type == VINE_WORKER_TYPE_WORKER) {
		q->stats->workers_removed++;
	}

	vine_txn_log_write_worker(q, w, 1, reason);

	hash_table_remove(q->worker_table, w->hashkey);
	hash_table_remove(q->workers_with_watched_file_updates, w->hashkey);
	hash_table_remove(q->workers_with_complete_tasks, w->hashkey);

	if (q->transfer_temps_recovery) {
		recall_worker_lost_temp_files(q, w);
	}

	cleanup_worker(q, w);

	vine_manager_factory_worker_leave(q, w);

	vine_worker_delete(w);

	/* update the largest worker seen */
	find_max_worker(q);

	debug(D_VINE, "%d workers connected in total now", count_workers(q, VINE_WORKER_TYPE_WORKER));
}

/* Gently release a worker by sending it a release message, and then removing it. */

static int release_worker(struct vine_manager *q, struct vine_worker_info *w)
{
	if (!w)
		return 0;

	vine_manager_send(q, w, "release\n");

	vine_manager_remove_worker(q, w, VINE_WORKER_DISCONNECT_EXPLICIT);

	q->stats->workers_released++;

	return 1;
}

/* Check for new connections on the manager's port, and add a worker if one is there. */

static void add_worker(struct vine_manager *q)
{
	char addr[LINK_ADDRESS_MAX];
	int port;

	struct link *link = link_accept(q->manager_link, time(0) + q->short_timeout);
	if (!link) {
		return;
	}

	link_keepalive(link, 1);
	link_tune(link, LINK_TUNE_INTERACTIVE);

	if (!link_address_remote(link, addr, &port)) {
		link_close(link);
		return;
	}

	debug(D_VINE, "worker %s:%d connected", addr, port);

	if (q->ssl_enabled) {
		if (link_ssl_wrap_accept(link, q->ssl_key, q->ssl_cert)) {
			debug(D_VINE, "worker %s:%d completed ssl connection", addr, port);
		} else {
			debug(D_VINE, "worker %s:%d failed ssl connection", addr, port);
			link_close(link);
			return;
		}
	} else {
		/* nothing to do */
	}

	if (q->password) {
		debug(D_VINE, "worker %s:%d authenticating", addr, port);
		if (!link_auth_password(link, q->password, time(0) + q->short_timeout)) {
			debug(D_VINE | D_NOTICE, "worker %s:%d presented the wrong password", addr, port);
			link_close(link);
			return;
		}
	}

	struct vine_worker_info *w = vine_worker_create(link);
	if (!w) {
		debug(D_NOTICE, "Cannot allocate memory for worker %s:%d.", addr, port);
		link_close(link);
		return;
	}

	w->hashkey = link_to_hash_key(link);
	w->addrport = string_format("%s:%d", addr, port);

	hash_table_insert(q->worker_table, w->hashkey, w);
}

/* Delete a single file on a remote worker except those with greater delete_upto_level cache level */

static int delete_worker_file(struct vine_manager *q, struct vine_worker_info *w, const char *filename, vine_cache_level_t cache_flags, vine_cache_level_t delete_upto_level)
{
	if (cache_flags <= delete_upto_level) {
		vine_manager_send(q, w, "unlink %s\n", filename);
		struct vine_file_replica *replica;
		replica = vine_file_replica_table_remove(q, w, filename);
		vine_file_replica_delete(replica);
		return 1;
	}

	return 0;
}

/* Delete all files in a list except those with greater delete_upto_level cache level */

static void delete_worker_files(struct vine_manager *q, struct vine_worker_info *w, struct list *mount_list, vine_cache_level_t delete_upto_level)
{
	if (!mount_list)
		return;
	struct vine_mount *m;
	LIST_ITERATE(mount_list, m)
	{
		delete_worker_file(q, w, m->file->cached_name, m->file->cache_level, delete_upto_level);
	}
}

/* Delete all output files of a given task. */

static void delete_task_output_files(struct vine_manager *q, struct vine_worker_info *w, struct vine_task *t)
{
	delete_worker_files(q, w, t->output_mounts, 0);
}

/* Delete only the uncacheable output files of a given task. */
static void delete_uncacheable_files(struct vine_manager *q, struct vine_worker_info *w, struct vine_task *t)
{
	delete_worker_files(q, w, t->input_mounts, VINE_CACHE_LEVEL_TASK);
	delete_worker_files(q, w, t->output_mounts, VINE_CACHE_LEVEL_TASK);
}

/* Determine the resource monitor file name that should be associated with this task. */

static char *monitor_file_name(struct vine_manager *q, struct vine_task *t, const char *ext, int series)
{
	char *dir;
	if (t->monitor_output_directory) {
		/* if output directory from task, we always keep the summaries generated. */
		dir = xxstrdup(t->monitor_output_directory);
	} else {
		if (series) {
			dir = vine_get_path_log(q, "time-series");
		} else {
			dir = vine_get_path_staging(q, NULL);
		}
	}

	char *name = string_format("%s/" RESOURCE_MONITOR_TASK_LOCAL_NAME "%s", dir, t->task_id, ext ? ext : "");
	free(dir);

	return name;
}

/* Extract the resources consumed by a task by reading the appropriate resource monitor file. */
static void read_measured_resources(struct vine_manager *q, struct vine_task *t)
{
	char *summary = monitor_file_name(q, t, ".summary", 0);

	if (t->resources_measured) {
		rmsummary_delete(t->resources_measured);
	}

	t->resources_measured = rmsummary_parse_file_single(summary);

	if (t->resources_measured) {
		t->exit_code = t->resources_measured->exit_status;

		/* cleanup noise in cores value, otherwise small fluctuations trigger new
		 * maximums */
		if (t->resources_measured->cores > 0) {
			t->resources_measured->cores = MIN(t->resources_measured->cores, ceil(t->resources_measured->cores - 0.1));
		}
	} else {
		/* if no resources were measured, then we don't overwrite the return
		 * status, and mark the task as with error from monitoring. */
		t->resources_measured = rmsummary_create(-1);
	}

	/* remove summary file, unless it is kept explicitly by the task */
	if (!t->monitor_output_directory) {
		unlink(summary);
	}

	free(summary);
}

/* Compress old time series files so as to avoid accumulating infinite resource monitoring data. */
static void resource_monitor_compress_logs(struct vine_manager *q, struct vine_task *t)
{
	char *series = monitor_file_name(q, t, ".series", 1);
	char *debug_log = monitor_file_name(q, t, ".debug", 1);

	char *command = string_format("gzip -9 -q %s %s", series, debug_log);

	int status;
	int rc = shellcode(command, NULL, NULL, 0, NULL, NULL, &status);

	if (rc) {
		debug(D_NOTICE, "Could no successfully compress '%s', and '%s'\n", series, debug_log);
	}

	free(series);
	free(debug_log);
	free(command);
}

void exit_debug_message(struct vine_manager *q, struct vine_worker_info *w, struct vine_task *t)
{
	if (t->result == VINE_RESULT_SUCCESS && t->time_workers_execute_last < 1000000) {
		switch (t->exit_code) {
		case (126):
			warn(D_VINE, "Task %d ran for a very short time and exited with code %d.\n", t->task_id, t->exit_code);
			warn(D_VINE, "This usually means that the task's command is not an executable,\n");
			warn(D_VINE, "or that the worker's scratch directory is on a no-exec partition.\n");
			break;
		case (127):
			warn(D_VINE, "Task %d ran for a very short time and exited with code %d.\n", t->task_id, t->exit_code);
			warn(D_VINE, "This usually means that the task's command could not be found, or that\n");
			warn(D_VINE, "it uses a shared library not available at the worker, or that\n");
			warn(D_VINE, "it uses a version of the glibc different than the one at the worker.\n");
			break;
		case (139):
			warn(D_VINE, "Task %d ran for a very short time and exited with code %d.\n", t->task_id, t->exit_code);
			warn(D_VINE, "This usually means that the task's command had a segmentation fault,\n");
			warn(D_VINE, "either because it has a memory access error (segfault), or because\n");
			warn(D_VINE, "it uses a version of a shared library different from the one at the worker.\n");
			break;
		default:
			break;
		}
	}

	debug(D_VINE,
			"%s (%s) done in %.02lfs total tasks %lld average %.02lfs",
			w->hostname,
			w->addrport,
			(t->time_when_done - t->time_when_commit_start) / 1000000.0,
			(long long)w->total_tasks_complete,
			w->total_task_time / w->total_tasks_complete / 1000000.0);

	return;
}

static int fetch_outputs_from_worker(struct vine_manager *q, struct vine_worker_info *w, int task_id)
{
	struct vine_task *t;
	vine_result_code_t result = VINE_SUCCESS;

	t = itable_lookup(w->current_tasks, task_id);
	if (!t) {
		debug(D_VINE, "Failed to find task %d at worker %s (%s).", task_id, w->hostname, w->addrport);
		handle_failure(q, w, t, VINE_WORKER_FAILURE);
		return 0;
	}
	t->time_when_retrieval = timestamp_get();

	/* Determine what subset of outputs to retrieve based on status. */

	switch (t->result) {
	case VINE_RESULT_INPUT_MISSING:
	case VINE_RESULT_FORSAKEN:
		/* If the worker didn't run the task don't bother fetching outputs. */
		result = VINE_SUCCESS;
		break;
	case VINE_RESULT_RESOURCE_EXHAUSTION:
		/* On resource exhaustion, just get the monitor files to figure out what happened. */
		result = vine_manager_get_monitor_output_file(q, w, t);
		break;
	default:
		/* Otherwise get all of the output files. */
		if (!t->output_received) {
			result = retrieve_output(q, w, t);
			if (result == VINE_SUCCESS) {
				t->output_received = 1;
			}
		}
		result = vine_manager_get_output_files(q, w, t);
		break;
	}

	if (result != VINE_SUCCESS) {
		debug(D_VINE, "Failed to receive output from worker %s (%s).", w->hostname, w->addrport);
		handle_failure(q, w, t, result);
	}

	if (result == VINE_WORKER_FAILURE) {
		t->time_when_done = timestamp_get();
		return 0;
	}
	delete_uncacheable_files(q, w, t);

	/* if q is monitoring, update t->resources_measured, and delete the task
	 * summary. */
	if (q->monitor_mode) {
		read_measured_resources(q, t);

		/* Further, if we got debug and series files, gzip them. */
		if (q->monitor_mode & VINE_MON_FULL)
			resource_monitor_compress_logs(q, t);
	}

	// fill in measured disk as it comes from a different info source.
	t->resources_measured->disk = MAX(t->resources_measured->disk, t->sandbox_measured);

	// Finish receiving output.
	t->time_when_done = timestamp_get();

	vine_accumulate_task(q, t);

	// At this point, a task is completed.
	reap_task_from_worker(q, w, t, VINE_TASK_RETRIEVED);
	vine_manager_send(q, w, "kill %d\n", t->task_id);

	switch (t->result) {
	case VINE_RESULT_INPUT_MISSING:
	case VINE_RESULT_FORSAKEN:
		/* do not count tasks that didn't execute as complete, or finished tasks */
		break;
	default:
		w->finished_tasks--;
		w->total_tasks_complete++;

		// At least one task has finished without triggering a slow worker disconnect, thus we
		// now have evidence that worker is not slow (e.g., it was probably the
		// previous task that was slow).
		w->alarm_slow_worker = 0;

		vine_task_info_add(q, t);
		break;
	}

	exit_debug_message(q, w, t);

	if (w->forsaken_tasks > VINE_DEFAULT_MAX_FORSAKEN_PER_WORKER && w->total_tasks_complete == 0) {
		debug(D_VINE, "Disconnecting worker that keeps forsaking tasks %s (%s).", w->hostname, w->addrport);
		handle_failure(q, w, t, VINE_WORKER_FAILURE);
		return 0;
	}

	return 1;
}

/*
Consider the set of tasks that are waiting but not running.
Cancel those that have exceeded their expressed end time,
exceeded the maximum number of retries, or other policy issues.
*/

static int expire_waiting_tasks(struct vine_manager *q)
{
	struct vine_task *t;
	int t_idx;
	int expired = 0;

	double current_time = timestamp_get() / ONE_SECOND;

	int iter_count = 0;
	int iter_depth = q->attempt_schedule_depth;
	PRIORITY_QUEUE_STATIC_ITERATE(q->ready_tasks, t_idx, t, iter_count, iter_depth)
	{
		if (t->resources_requested->end > 0 && t->resources_requested->end <= current_time) {
			vine_task_set_result(t, VINE_RESULT_MAX_END_TIME);
			priority_queue_remove(q->ready_tasks, t_idx);
			change_task_state(q, t, VINE_TASK_RETRIEVED);
			expired++;
		}
	}

	return expired;
}

/*
Consider the set of tasks that are waiting with strict inputs
Terminate those to which no such worker exists.
*/
static int enforce_waiting_fixed_locations(struct vine_manager *q)
{
	int t_idx;
	struct vine_task *t;
	int terminated = 0;

	int iter_count = 0;
	int iter_depth = priority_queue_size(q->ready_tasks);

	PRIORITY_QUEUE_BASE_ITERATE(q->ready_tasks, t_idx, t, iter_count, iter_depth)
	{
		if (t->has_fixed_locations && !vine_schedule_check_fixed_location(q, t)) {
			vine_task_set_result(t, VINE_RESULT_FIXED_LOCATION_MISSING);
			change_task_state(q, t, VINE_TASK_RETRIEVED);
			priority_queue_remove(q->ready_tasks, t_idx);
			terminated++;
		}
	}

	return terminated;
}

/*
This function handles app-level failures. It remove the task from WQ and marks
the task as complete so it is returned to the application.
*/

static void handle_app_failure(struct vine_manager *q, struct vine_worker_info *w, struct vine_task *t)
{
	// remove the task from tables that track dispatched tasks.
	// and add the task to complete list so it is given back to the application.
	reap_task_from_worker(q, w, t, VINE_TASK_RETRIEVED);

	/*If the failure happened after a task execution, we remove all the output
	files specified for that task from the worker's cache.  This is because the
	application may resubmit the task and the resubmitted task may produce
	different outputs. */
	if (t) {
		if (t->time_when_commit_end > 0) {
			delete_task_output_files(q, w, t);
		}
	}

	return;
}

/*
Failures happen in the manager-worker interactions. In this case,
we remove the worker and retry the tasks dispatched to it elsewhere.
*/

static void handle_worker_failure(struct vine_manager *q, struct vine_worker_info *w)
{
	vine_manager_remove_worker(q, w, VINE_WORKER_DISCONNECT_FAILURE);
	return;
}

/*
Handle the failure of a task, taking different actions depending on whether
this is due to an application-level issue or a problem with the worker alone.
*/

static void handle_failure(struct vine_manager *q, struct vine_worker_info *w, struct vine_task *t, vine_result_code_t fail_type)
{
	if (fail_type == VINE_APP_FAILURE) {
		handle_app_failure(q, w, t);
	} else {
		handle_worker_failure(q, w);
	}
	return;
}

/*
Handle the initial connection message from a worker, which reports
basic information about the hostname, operating system, and so forth.
Once this message is processed, the manager knows it is a valid connection
and can begin sending tasks and data.
*/

static vine_msg_code_t handle_taskvine(struct vine_manager *q, struct vine_worker_info *w, const char *line)
{
	char items[4][VINE_LINE_MAX];
	int worker_protocol;

	int n = sscanf(line, "taskvine %d %s %s %s %s", &worker_protocol, items[0], items[1], items[2], items[3]);
	if (n != 5)
		return VINE_MSG_FAILURE;

	if (worker_protocol != VINE_PROTOCOL_VERSION) {
		debug(D_VINE | D_NOTICE, "rejecting worker (%s) as it uses protocol %d. The manager is using protocol %d.", w->addrport, worker_protocol, VINE_PROTOCOL_VERSION);
		vine_block_host(q, w->hostname);
		return VINE_MSG_FAILURE;
	}

	if (w->hostname)
		free(w->hostname);
	if (w->os)
		free(w->os);
	if (w->arch)
		free(w->arch);
	if (w->version)
		free(w->version);

	w->hostname = strdup(items[0]);
	w->os = strdup(items[1]);
	w->arch = strdup(items[2]);
	w->version = strdup(items[3]);

	w->type = VINE_WORKER_TYPE_WORKER;

	q->stats->workers_joined++;
	debug(D_VINE, "%d workers are connected in total now", count_workers(q, VINE_WORKER_TYPE_WORKER));

	debug(D_VINE, "%s (%s) running CCTools version %s on %s (operating system) with architecture %s is ready", w->hostname, w->addrport, w->version, w->os, w->arch);

	if (cctools_version_cmp(CCTOOLS_VERSION, w->version) != 0) {
		debug(D_DEBUG,
				"Warning: potential worker version mismatch: worker %s (%s) is version %s, and manager is version %s",
				w->hostname,
				w->addrport,
				w->version,
				CCTOOLS_VERSION);
	}

	return VINE_MSG_PROCESSED;
}

/*
If the manager has requested that a file be watched with VINE_WATCH,
the worker will periodically send back update messages indicating that
the file has been written to.  There are a variety of ways in which the
message could be stale (e.g. task was cancelled) so if the message does
not line up with an expected task and file, then we discard it and keep
going.
*/

static vine_result_code_t get_update(struct vine_manager *q, struct vine_worker_info *w, const char *line)
{
	int64_t task_id;
	char path[VINE_LINE_MAX];
	int64_t offset;
	int64_t length;

	int n = sscanf(line, "update %" PRId64 " %s %" PRId64 " %" PRId64, &task_id, path, &offset, &length);
	if (n != 4) {
		debug(D_VINE, "Invalid message from worker %s (%s): %s", w->hostname, w->addrport, line);
		return VINE_WORKER_FAILURE;
	}

	struct vine_task *t = itable_lookup(w->current_tasks, task_id);
	if (!t) {
		debug(D_VINE, "worker %s (%s) sent output for unassigned task %" PRId64, w->hostname, w->addrport, task_id);
		link_soak(w->link, length, time(0) + vine_manager_transfer_time(q, w, length));
		return VINE_SUCCESS;
	}

	time_t stoptime = time(0) + vine_manager_transfer_time(q, w, length);

	struct vine_mount *m;
	const char *local_name = 0;

	LIST_ITERATE(t->output_mounts, m)
	{
		if (!strcmp(path, m->remote_name)) {
			local_name = m->file->source;
			break;
		}
	}

	if (!local_name) {
		debug(D_VINE, "worker %s (%s) sent output for unwatched file %s", w->hostname, w->addrport, path);
		link_soak(w->link, length, stoptime);
		return VINE_SUCCESS;
	}

	int fd = open(local_name, O_WRONLY | O_CREAT, 0777);
	if (fd < 0) {
		debug(D_VINE, "unable to update watched file %s: %s", local_name, strerror(errno));
		link_soak(w->link, length, stoptime);
		return VINE_SUCCESS;
	}

	lseek(fd, offset, SEEK_SET);
	link_stream_to_fd(w->link, fd, length, stoptime);
	ftruncate(fd, offset + length);

	if (close(fd) < 0) {
		debug(D_VINE, "unable to update watched file %s: %s\n", local_name, strerror(errno));
		return VINE_SUCCESS;
	}

	return VINE_SUCCESS;
}

/*
make a synchronus connection with a worker to retrieve the stdout of a task
*/

static vine_result_code_t retrieve_output(struct vine_manager *q, struct vine_worker_info *w, struct vine_task *t)
{
	int64_t output_length;
	uint64_t task_id;
	char line[VINE_LINE_MAX];

	vine_manager_send(q, w, "send_stdout %d\n", t->task_id);

	vine_result_code_t result = VINE_SUCCESS;
	vine_msg_code_t mcode;
	mcode = vine_manager_recv(q, w, line, sizeof(line));

	if (mcode != VINE_MSG_NOT_PROCESSED) {
		return VINE_WORKER_FAILURE;
	}
	if (string_prefix_is(line, "error")) {
		return VINE_WORKER_FAILURE;

	} else if (string_prefix_is(line, "stdout")) {
		result = VINE_SUCCESS;
	} else {
		debug(D_VINE, "%s (%s): sent invalid response to send_stdout: %s", w->hostname, w->addrport, line);
		return VINE_WORKER_FAILURE;
	}

	int n = sscanf(line, "stdout  %" SCNd64 " %" SCNd64 "", &task_id, &output_length);

	if (n < 2) {
		debug(D_VINE, "Invalid message from worker %s (%s): %s", w->hostname, w->addrport, line);
		return VINE_WORKER_FAILURE;
	}
	result = get_stdout(q, w, t, output_length);
	return result;
}

/*
Get the standard output of a task, as part of retrieving the result.
This seems awfully complicated and could be simplified.
*/

static vine_result_code_t get_stdout(struct vine_manager *q, struct vine_worker_info *w, struct vine_task *t, int64_t output_length)
{
	int64_t retrieved_output_length;
	timestamp_t effective_stoptime = 0;
	time_t stoptime;
	int64_t actual;

	if (q->bandwidth_limit) {
		effective_stoptime = (output_length / q->bandwidth_limit) * 1000000 + timestamp_get();
	}

	if (output_length <= q->max_task_stdout_storage) {
		retrieved_output_length = output_length;
	} else {
		retrieved_output_length = q->max_task_stdout_storage;
		fprintf(stderr,
				"warning: stdout of task %d"
				" requires %2.2lf GB of storage. This exceeds maximum supported size of %d GB. Only %d GB will be retrieved.\n",
				t->task_id,
				((double)output_length) / q->max_task_stdout_storage,
				q->max_task_stdout_storage / GIGABYTE,
				q->max_task_stdout_storage / GIGABYTE);
		vine_task_set_result(t, VINE_RESULT_STDOUT_MISSING);
	}

	t->output = malloc(retrieved_output_length + 1);
	if (t->output == NULL) {
		fprintf(stderr, "error: allocating memory of size %" PRId64 " bytes failed for storing stdout of task %d.\n", retrieved_output_length, t->task_id);
		// drop the entire length of stdout on the link
		stoptime = time(0) + vine_manager_transfer_time(q, w, output_length);
		link_soak(w->link, output_length, stoptime);
		retrieved_output_length = 0;
		vine_task_set_result(t, VINE_RESULT_STDOUT_MISSING);
	}

	if (retrieved_output_length > 0) {
		debug(D_VINE, "Receiving stdout of task %d (size: %" PRId64 " bytes) from %s (%s) ...", t->task_id, retrieved_output_length, w->addrport, w->hostname);

		// First read the bytes we keep.
		stoptime = time(0) + vine_manager_transfer_time(q, w, retrieved_output_length);
		actual = link_read(w->link, t->output, retrieved_output_length, stoptime);
		if (actual != retrieved_output_length) {
			debug(D_VINE, "Failure: actual received stdout size (%" PRId64 " bytes) is different from expected (%" PRId64 " bytes).", actual, retrieved_output_length);
			t->output[actual] = '\0';
			return VINE_WORKER_FAILURE;
		}
		debug(D_VINE, "Retrieved %" PRId64 " bytes from %s (%s)", actual, w->hostname, w->addrport);

		// Then read the bytes we need to throw away.
		if (output_length > retrieved_output_length) {
			debug(D_VINE,
					"Dropping the remaining %" PRId64 " bytes of the stdout of task %d"
					" since stdout length is limited to %d bytes.\n",
					(output_length - q->max_task_stdout_storage),
					t->task_id,
					q->max_task_stdout_storage);
			stoptime = time(0) + vine_manager_transfer_time(q, w, (output_length - retrieved_output_length));
			link_soak(w->link, (output_length - retrieved_output_length), stoptime);

			// overwrite the last few bytes of buffer to signal truncated stdout.
			char *truncate_msg = string_format("\n>>>>>> STDOUT TRUNCATED AFTER THIS POINT.\n>>>>>> MAXIMUM OF %d BYTES REACHED, %" PRId64 " BYTES TRUNCATED.",
					q->max_task_stdout_storage,
					output_length - retrieved_output_length);
			memcpy(t->output + q->max_task_stdout_storage - strlen(truncate_msg) - 1, truncate_msg, strlen(truncate_msg));
			*(t->output + q->max_task_stdout_storage - 1) = '\0';
			free(truncate_msg);
		}

		timestamp_t current_time = timestamp_get();
		if (effective_stoptime && effective_stoptime > current_time) {
			usleep(effective_stoptime - current_time);
		}
	} else {
		actual = 0;
	}
	if (t->output)
		t->output[actual] = 0;

	return VINE_SUCCESS;
}

/*
Send to this worker a request for task results.
The worker will respond with all completed tasks and updates
on watched output files.  Process those results as they come back.
*/

static vine_result_code_t get_available_results(struct vine_manager *q, struct vine_worker_info *w)
{
	// max_count == -1, tells the worker to send all available results.
	vine_manager_send(q, w, "send_results %d\n", -1);
	debug(D_VINE, "Reading result(s) from %s (%s)", w->hostname, w->addrport);

	char line[VINE_LINE_MAX];

	vine_result_code_t result = VINE_SUCCESS; // return success unless something fails below.

	while (1) {
		vine_msg_code_t mcode;
		mcode = vine_manager_recv(q, w, line, sizeof(line));
		if (mcode != VINE_MSG_NOT_PROCESSED) {
			result = VINE_WORKER_FAILURE;
			break;
		}
		if (string_prefix_is(line, "update")) {
			result = get_update(q, w, line);
			if (result != VINE_SUCCESS)
				break;
		} else if (!strcmp(line, "end")) {
			// Only return success if last message is end.
			break;
		} else {
			debug(D_VINE, "%s (%s): sent invalid response to send_results: %s", w->hostname, w->addrport, line);
			result = VINE_WORKER_FAILURE;
			break;
		}
	}

	return result;
}

/*
Compute the total quantity of resources needed by all tasks in
the ready and running states.  This gives us a complete picture
of the manager's resource consumption for status reporting.
*/

static struct rmsummary *total_resources_needed(struct vine_manager *q)
{
	int t_idx;
	struct vine_task *t;

	struct rmsummary *total = rmsummary_create(0);

	int iter_count = 0;
	int iter_depth = priority_queue_size(q->ready_tasks);

	/* for waiting tasks, we use what they would request if dispatched right now. */
	PRIORITY_QUEUE_BASE_ITERATE(q->ready_tasks, t_idx, t, iter_count, iter_depth)
	{
		const struct rmsummary *s = vine_manager_task_resources_min(q, t);
		rmsummary_add(total, s);
	}

	/* for running tasks, we use what they have been allocated already. */
	char *key;
	struct vine_worker_info *w;

	HASH_TABLE_ITERATE(q->worker_table, key, w)
	{
		if (w->resources->tag < 0) {
			continue;
		}

		total->cores += w->resources->cores.inuse;
		total->memory += w->resources->memory.inuse;
		total->disk += w->resources->disk.inuse;
		total->gpus += w->resources->gpus.inuse;
	}

	return total;
}

/*
Compute the largest resource request for any task in a given category.
*/

static const struct rmsummary *largest_seen_resources(struct vine_manager *q, const char *category)
{
	char *key;
	struct category *c;

	if (category) {
		c = vine_category_lookup_or_create(q, category);
		return c->max_allocation;
	} else {
		HASH_TABLE_ITERATE(q->categories, key, c)
		{
			rmsummary_merge_max(q->max_task_resources_requested, c->max_allocation);
		}
		return q->max_task_resources_requested;
	}
}

/* Return true if this worker can satisfy the given resource request. */

static int check_worker_fit(struct vine_worker_info *w, const struct rmsummary *s)
{

	if (w->resources->workers.total < 1)
		return 0;

	if (!s)
		return w->resources->workers.total;

	if (s->cores > w->resources->cores.total)
		return 0;
	if (s->memory > w->resources->memory.total)
		return 0;
	if (s->disk > w->resources->disk.total)
		return 0;
	if (s->gpus > w->resources->gpus.total)
		return 0;

	return w->resources->workers.total;
}

static int count_workers_for_waiting_tasks(struct vine_manager *q, const struct rmsummary *s)
{

	int count = 0;

	char *key;
	struct vine_worker_info *w;

	HASH_TABLE_ITERATE(q->worker_table, key, w)
	{
		count += check_worker_fit(w, s);
	}

	return count;
}

static void category_jx_insert_max(struct jx *j, struct category *c, const char *field, const struct rmsummary *largest)
{

	double l = rmsummary_get(largest, field);
	double m = -1;
	double e = -1;

	if (c) {
		m = rmsummary_get(c->max_resources_seen, field);
		if (c->max_resources_seen->limits_exceeded) {
			e = rmsummary_get(c->max_resources_seen->limits_exceeded, field);
		}
	}

	char *field_str = string_format("max_%s", field);

	if (l > -1) {
		char *max_str = string_format("%s", rmsummary_resource_to_str(field, l, 0));
		jx_insert_string(j, field_str, max_str);
		free(max_str);
	} else if (c && !category_in_steady_state(c) && e > -1) {
		char *max_str = string_format(">%s", rmsummary_resource_to_str(field, m - 1, 0));
		jx_insert_string(j, field_str, max_str);
		free(max_str);
	} else if (c && m > -1) {
		char *max_str = string_format("~%s", rmsummary_resource_to_str(field, m, 0));
		jx_insert_string(j, field_str, max_str);
		free(max_str);
	} else {
		jx_insert_string(j, field_str, "na");
	}

	free(field_str);
}

/* Create a dummy task to obtain first allocation that category would get if using largest worker */

static struct rmsummary *category_alloc_info(struct vine_manager *q, struct category *c, category_allocation_t request)
{
	struct vine_task *t = vine_task_create("nop");
	vine_task_set_category(t, c->name);
	t->resource_request = request;

	/* XXX this seems like a hack: a vine_worker is being created by malloc instead of vine_worker_create */

	struct vine_worker_info *w = malloc(sizeof(*w));
	w->resources = vine_resources_create();
	w->resources->cores.total = q->current_max_worker->cores;
	w->resources->memory.total = q->current_max_worker->memory;
	w->resources->disk.total = q->current_max_worker->disk;
	w->resources->gpus.total = q->current_max_worker->gpus;

	struct rmsummary *allocation = vine_manager_choose_resources_for_task(q, w, t);

	vine_task_delete(t);
	vine_resources_delete(w->resources);
	free(w);

	return allocation;
}

/* Convert an allocation of resources into a JX record. */

static struct jx *alloc_to_jx(struct vine_manager *q, struct category *c, struct rmsummary *resources)
{
	struct jx *j = jx_object(0);

	jx_insert_double(j, "cores", resources->cores);
	jx_insert_integer(j, "memory", resources->memory);
	jx_insert_integer(j, "disk", resources->disk);
	jx_insert_integer(j, "gpus", resources->gpus);

	return j;
}

/* Convert a resource category into a JX record for reporting to the catalog. */

static struct jx *category_to_jx(struct vine_manager *q, const char *category)
{
	struct vine_stats s;
	struct category *c = NULL;
	const struct rmsummary *largest = largest_seen_resources(q, category);

	c = vine_category_lookup_or_create(q, category);
	vine_get_stats_category(q, category, &s);

	if (s.tasks_waiting + s.tasks_on_workers + s.tasks_done < 1) {
		return NULL;
	}

	struct jx *j = jx_object(0);

	jx_insert_string(j, "category", category);
	jx_insert_integer(j, "tasks_waiting", s.tasks_waiting);
	jx_insert_integer(j, "tasks_running", s.tasks_running);
	jx_insert_integer(j, "tasks_on_workers", s.tasks_on_workers);
	jx_insert_integer(j, "tasks_dispatched", s.tasks_dispatched);
	jx_insert_integer(j, "tasks_done", s.tasks_done);
	jx_insert_integer(j, "tasks_failed", s.tasks_failed);
	jx_insert_integer(j, "tasks_cancelled", s.tasks_cancelled);
	jx_insert_integer(j, "workers_able", s.workers_able);

	category_jx_insert_max(j, c, "cores", largest);
	category_jx_insert_max(j, c, "memory", largest);
	category_jx_insert_max(j, c, "disk", largest);
	category_jx_insert_max(j, c, "gpus", largest);

	struct rmsummary *first_allocation = category_alloc_info(q, c, CATEGORY_ALLOCATION_FIRST);
	struct jx *jr = alloc_to_jx(q, c, first_allocation);
	rmsummary_delete(first_allocation);
	jx_insert(j, jx_string("first_allocation"), jr);

	struct rmsummary *max_allocation = category_alloc_info(q, c, CATEGORY_ALLOCATION_MAX);
	jr = alloc_to_jx(q, c, max_allocation);
	rmsummary_delete(max_allocation);
	jx_insert(j, jx_string("max_allocation"), jr);

	if (q->monitor_mode) {
		jr = alloc_to_jx(q, c, c->max_resources_seen);
		jx_insert(j, jx_string("max_seen"), jr);
	}

	jx_insert_integer(j, "first_allocation_count", task_request_count(q, c->name, CATEGORY_ALLOCATION_FIRST));
	jx_insert_integer(j, "max_allocation_count", task_request_count(q, c->name, CATEGORY_ALLOCATION_MAX));

	return j;
}

/* Convert all resource categories into a JX record. */

static struct jx *categories_to_jx(struct vine_manager *q)
{
	struct jx *a = jx_array(0);

	struct category *c;
	char *category_name;

	HASH_TABLE_ITERATE(q->categories, category_name, c)
	{
		struct jx *j = category_to_jx(q, category_name);
		if (j) {
			jx_array_insert(a, j);
		}
	}

	return a;
}

/*
manager_to_jx examines the overall manager status and creates
an jx expression which can be sent directly to the
user that connects via vine_status.
*/

static struct jx *manager_to_jx(struct vine_manager *q)
{
	struct jx *j = jx_object(0);
	if (!j)
		return 0;

	// Insert all properties from vine_stats

	struct vine_stats info;
	vine_get_stats(q, &info);

	// Add special properties expected by the catalog server
	char owner[USERNAME_MAX];
	username_get(owner);

	jx_insert_string(j, "type", "vine_manager");
	if (q->name)
		jx_insert_string(j, "project", q->name);
	jx_insert_integer(j, "starttime",
			(q->stats->time_when_started / 1000000)); // catalog expects time_t not timestamp_t
	jx_insert_string(j, "working_dir", q->workingdir);
	jx_insert_string(j, "owner", owner);
	jx_insert_string(j, "version", CCTOOLS_VERSION);
	jx_insert_integer(j, "port", vine_port(q));
	jx_insert_integer(j, "priority", q->priority);
	jx_insert_string(j, "manager_preferred_connection", q->manager_preferred_connection);
	jx_insert_string(j, "taskvine_uuid", q->uuid);
	jx_insert_integer(j, "protocol", VINE_PROTOCOL_VERSION);

	char *name, *key;
	HASH_TABLE_ITERATE(q->properties, name, key)
	{
		jx_insert_string(j, name, key);
	}

	int use_ssl = 0;
#ifdef HAS_OPENSSL
	if (q->ssl_enabled) {
		use_ssl = 1;
	}
#endif
	jx_insert_boolean(j, "ssl", use_ssl);

	struct jx *interfaces = interfaces_of_host();
	if (interfaces) {
		jx_insert(j, jx_string("network_interfaces"), interfaces);
	}

	// send info on workers
	jx_insert_integer(j, "workers", info.workers_connected);
	jx_insert_integer(j, "workers_connected", info.workers_connected);
	jx_insert_integer(j, "workers_init", info.workers_init);
	jx_insert_integer(j, "workers_idle", info.workers_idle);
	jx_insert_integer(j, "workers_busy", info.workers_busy);
	jx_insert_integer(j, "workers_able", info.workers_able);

	jx_insert_integer(j, "workers_joined", info.workers_joined);
	jx_insert_integer(j, "workers_removed", info.workers_removed);
	jx_insert_integer(j, "workers_released", info.workers_released);
	jx_insert_integer(j, "workers_idled_out", info.workers_idled_out);
	jx_insert_integer(j, "workers_slow", info.workers_slow);
	jx_insert_integer(j, "workers_lost", info.workers_lost);

	// workers_blocked adds host names, not a count
	struct jx *blocklist = vine_blocklist_to_jx(q);
	if (blocklist) {
		jx_insert(j, jx_string("workers_blocked"), blocklist);
	}

	// send info on tasks
	jx_insert_integer(j, "tasks_waiting", info.tasks_waiting);
	jx_insert_integer(j, "tasks_on_workers", info.tasks_on_workers);
	jx_insert_integer(j, "tasks_running", info.tasks_running);
	jx_insert_integer(j, "tasks_with_results", info.tasks_with_results);
	jx_insert_integer(j, "tasks_left", q->num_tasks_left);

	jx_insert_integer(j, "tasks_submitted", info.tasks_submitted);
	jx_insert_integer(j, "tasks_dispatched", info.tasks_dispatched);
	jx_insert_integer(j, "tasks_done", info.tasks_done);
	jx_insert_integer(j, "tasks_failed", info.tasks_failed);
	jx_insert_integer(j, "tasks_cancelled", info.tasks_cancelled);
	jx_insert_integer(j, "tasks_exhausted_attempts", info.tasks_exhausted_attempts);

	// tasks_complete is deprecated, but the old vine_status expects it.
	jx_insert_integer(j, "tasks_complete", info.tasks_done);

	// send info on manager
	jx_insert_integer(j, "time_when_started", info.time_when_started);
	jx_insert_integer(j, "time_send", info.time_send);
	jx_insert_integer(j, "time_receive", info.time_receive);
	jx_insert_integer(j, "time_send_good", info.time_send_good);
	jx_insert_integer(j, "time_receive_good", info.time_receive_good);
	jx_insert_integer(j, "time_status_msgs", info.time_status_msgs);
	jx_insert_integer(j, "time_internal", info.time_internal);
	jx_insert_integer(j, "time_polling", info.time_polling);
	jx_insert_integer(j, "time_application", info.time_application);
	jx_insert_integer(j, "time_scheduling", info.time_scheduling);

	jx_insert_integer(j, "time_workers_execute", info.time_workers_execute);
	jx_insert_integer(j, "time_workers_execute_good", info.time_workers_execute_good);
	jx_insert_integer(j, "time_workers_execute_exhaustion", info.time_workers_execute_exhaustion);

	jx_insert_integer(j, "bytes_sent", info.bytes_sent);
	jx_insert_integer(j, "bytes_received", info.bytes_received);

	jx_insert_integer(j, "inuse_cache", info.inuse_cache);

	jx_insert_integer(j, "capacity_tasks", info.capacity_tasks);
	jx_insert_integer(j, "capacity_cores", info.capacity_cores);
	jx_insert_integer(j, "capacity_memory", info.capacity_memory);
	jx_insert_integer(j, "capacity_disk", info.capacity_disk);
	jx_insert_integer(j, "capacity_gpus", info.capacity_gpus);
	jx_insert_integer(j, "capacity_instantaneous", info.capacity_instantaneous);
	jx_insert_integer(j, "capacity_weighted", info.capacity_weighted);

	// Add the resources computed from tributary workers.
	struct vine_resources r, rmin, rmax;
	int64_t inuse_cache = 0;
	aggregate_workers_resources(q, &r, &rmin, &rmax, &inuse_cache, NULL);
	vine_resources_add_to_jx(&r, j);

	// add the stats per category
	jx_insert(j, jx_string("categories"), categories_to_jx(q));

	// add total resources used/needed by the manager
	struct rmsummary *total = total_resources_needed(q);

	jx_insert_integer(j, "tasks_total_cores", total->cores);
	jx_insert_integer(j, "tasks_total_memory", total->memory);
	jx_insert_integer(j, "tasks_total_disk", total->disk);
	jx_insert_integer(j, "tasks_total_gpus", total->gpus);
	rmsummary_delete(total);

	return j;
}

/*
manager_lean_to_jx examines the overall manager status and creates
an jx expression which can be sent to the catalog.
It different from manager_to_jx in that only the minimum information that
workers, vine_status and the vine_factory need.
*/

static struct jx *manager_lean_to_jx(struct vine_manager *q)
{
	struct jx *j = jx_object(0);
	if (!j)
		return 0;

	// Insert all properties from vine_stats

	struct vine_stats info;
	vine_get_stats(q, &info);

	// information regarding how to contact the manager
	jx_insert_string(j, "version", CCTOOLS_VERSION);
	jx_insert_string(j, "type", "vine_manager");
	jx_insert_integer(j, "port", vine_port(q));
	jx_insert_integer(j, "protocol", VINE_PROTOCOL_VERSION);

	char *name, *key;
	HASH_TABLE_ITERATE(q->properties, name, key)
	{
		jx_insert_string(j, name, key);
	}

	int use_ssl = 0;
#ifdef HAS_OPENSSL
	if (q->ssl_enabled) {
		use_ssl = 1;
	}
#endif
	jx_insert_boolean(j, "ssl", use_ssl);

	char owner[USERNAME_MAX];
	username_get(owner);
	jx_insert_string(j, "owner", owner);

	if (q->name)
		jx_insert_string(j, "project", q->name);
	jx_insert_integer(j, "starttime",
			(q->stats->time_when_started / 1000000)); // catalog expects time_t not timestamp_t
	jx_insert_string(j, "manager_preferred_connection", q->manager_preferred_connection);

	struct jx *interfaces = interfaces_of_host();
	if (interfaces) {
		jx_insert(j, jx_string("network_interfaces"), interfaces);
	}

	// task information for general vine_status report
	jx_insert_integer(j, "tasks_waiting", info.tasks_waiting);
	jx_insert_integer(j, "tasks_running", info.tasks_running);
	jx_insert_integer(j, "tasks_complete",
			info.tasks_done); // tasks_complete is deprecated, but the old vine_status expects it.

	// additional task information for vine_factory
	jx_insert_integer(j, "tasks_on_workers", info.tasks_on_workers);
	jx_insert_integer(j, "tasks_left", q->num_tasks_left);

	// capacity information the factory needs
	jx_insert_integer(j, "capacity_tasks", info.capacity_tasks);
	jx_insert_integer(j, "capacity_cores", info.capacity_cores);
	jx_insert_integer(j, "capacity_memory", info.capacity_memory);
	jx_insert_integer(j, "capacity_disk", info.capacity_disk);
	jx_insert_integer(j, "capacity_gpus", info.capacity_gpus);
	jx_insert_integer(j, "capacity_weighted", info.capacity_weighted);

	// resources information the factory needs
	struct rmsummary *total = total_resources_needed(q);
	jx_insert_integer(j, "tasks_total_cores", total->cores);
	jx_insert_integer(j, "tasks_total_memory", total->memory);
	jx_insert_integer(j, "tasks_total_disk", total->disk);
	jx_insert_integer(j, "tasks_total_gpus", total->gpus);

	// worker information for general vine_status report
	jx_insert_integer(j, "workers", info.workers_connected);
	jx_insert_integer(j, "workers_connected", info.workers_connected);

	// additional worker information the factory needs
	struct jx *blocklist = vine_blocklist_to_jx(q);
	if (blocklist) {
		jx_insert(j, jx_string("workers_blocked"), blocklist); // danger! unbounded field
	}

	return j;
}

/*
Send a brief human-readable index listing the data
types that can be queried via this API.
*/

static void handle_data_index(struct vine_manager *q, struct vine_worker_info *w, time_t stoptime)
{
	buffer_t buf;
	buffer_init(&buf);

	buffer_printf(&buf, "<h1>taskvine data API</h1>");
	buffer_printf(&buf, "<ul>\n");
	buffer_printf(&buf, "<li> <a href=\"/manager_status\">Queue Status</a>\n");
	buffer_printf(&buf, "<li> <a href=\"/task_status\">Task Status</a>\n");
	buffer_printf(&buf, "<li> <a href=\"/worker_status\">Worker Status</a>\n");
	buffer_printf(&buf, "<li> <a href=\"/resources_status\">Resources Status</a>\n");
	buffer_printf(&buf, "</ul>\n");

	vine_manager_send(q, w, buffer_tostring(&buf), buffer_pos(&buf), stoptime);

	buffer_free(&buf);
}

/*
Process an HTTP request that comes in via a worker port.
This represents a web browser that connected directly
to the manager to fetch status data.
*/

static vine_msg_code_t handle_http_request(struct vine_manager *q, struct vine_worker_info *w, const char *path, time_t stoptime)
{
	char line[VINE_LINE_MAX];

	// Consume (and ignore) the remainder of the headers.
	while (link_readline(w->link, line, VINE_LINE_MAX, stoptime)) {
		if (line[0] == 0)
			break;
	}

	vine_manager_send(q, w, "HTTP/1.1 200 OK\nConnection: close\n");
	if (!strcmp(path, "/")) {
		// Requests to root get a simple human readable index.
		vine_manager_send(q, w, "Content-type: text/html\n\n");
		handle_data_index(q, w, stoptime);
	} else {
		// Other requests get raw JSON data.
		vine_manager_send(q, w, "Access-Control-Allow-Origin: *\n");
		vine_manager_send(q, w, "Content-type: text/plain\n\n");
		handle_manager_status(q, w, &path[1], stoptime);
	}

	// Return success but require a disconnect now.
	return VINE_MSG_PROCESSED_DISCONNECT;
}

/*
Process a manager status request which returns raw JSON.
This could come via the HTTP interface, or via a plain request.
*/

static struct jx *construct_status_message(struct vine_manager *q, const char *request)
{
	struct jx *a = jx_array(NULL);

	if (!strcmp(request, "manager_status") || !strcmp(request, "manager") || !strcmp(request, "resources_status")) {
		struct jx *j = manager_to_jx(q);
		if (j) {
			jx_array_insert(a, j);
		}
	} else if (!strcmp(request, "task_status") || !strcmp(request, "tasks")) {
		struct vine_task *t;
		uint64_t task_id;

		ITABLE_ITERATE(q->tasks, task_id, t)
		{
			struct jx *j = vine_task_to_jx(q, t);
			if (j)
				jx_array_insert(a, j);
		}
	} else if (!strcmp(request, "worker_status") || !strcmp(request, "workers")) {
		struct vine_worker_info *w;
		struct jx *j;
		char *key;

		HASH_TABLE_ITERATE(q->worker_table, key, w)
		{
			// If the worker has not been initialized, ignore it.
			if (!strcmp(w->hostname, "unknown"))
				continue;
			j = vine_worker_to_jx(w);
			if (j) {
				jx_array_insert(a, j);
			}
		}
	} else if (!strcmp(request, "wable_status") || !strcmp(request, "categories")) {
		jx_delete(a);
		a = categories_to_jx(q);
	} else {
		debug(D_VINE, "Unknown status request: '%s'", request);
		jx_delete(a);
		a = NULL;
	}

	return a;
}

/*
Handle a manager status message by composing a response and sending it.
*/

static vine_msg_code_t handle_manager_status(struct vine_manager *q, struct vine_worker_info *target, const char *line, time_t stoptime)
{
	struct link *l = target->link;

	struct jx *a = construct_status_message(q, line);
	target->type = VINE_WORKER_TYPE_STATUS;

	free(target->hostname);
	target->hostname = xxstrdup("QUEUE_STATUS");

	if (!a) {
		debug(D_VINE, "Unknown status request: '%s'", line);
		return VINE_MSG_FAILURE;
	}

	jx_print_link(a, l, stoptime);
	jx_delete(a);

	return VINE_MSG_PROCESSED_DISCONNECT;
}

/*
Handle a resource update message from the worker describing
its cores, memory, disk, etc.
*/

static vine_msg_code_t handle_resources(struct vine_manager *q, struct vine_worker_info *w, time_t stoptime)
{
	while (1) {
		char line[VINE_LINE_MAX];
		int64_t total;

		int result = link_readline(w->link, line, sizeof(line), stoptime);
		if (result <= 0)
			return VINE_MSG_FAILURE;

		debug(D_VINE, "%s", line);

		if (sscanf(line, "cores %" PRId64, &total)) {
			w->resources->cores.total = total;
		} else if (sscanf(line, "memory %" PRId64, &total)) {
			w->resources->memory.total = total;
		} else if (sscanf(line, "disk %" PRId64, &total)) {
			w->resources->disk.total = total;
		} else if (sscanf(line, "gpus %" PRId64, &total)) {
			w->resources->gpus.total = total;
		} else if (sscanf(line, "workers %" PRId64, &total)) {
			w->resources->workers.total = total;
		} else if (sscanf(line, "tag %" PRId64, &total)) {
			w->resources->tag = total;
		} else if (!strcmp(line, "end")) {
			/* Stop when we get an end marker. */
			break;
		} else {
			debug(D_VINE, "unexpected data in resource update!");
			/* But keep going until we get an "end" */
		}
	}

	/* Update the queue total since one worker changed. */
	count_worker_resources(q, w);

	/* Record the update into the transaction log. */
	vine_txn_log_write_worker_resources(q, w);

	return VINE_MSG_PROCESSED;
}

/*
Handle a feature report from a worker, which describes properties set
manually by the user, like a particular GPU model, software installed, etc.
*/

static vine_msg_code_t handle_feature(struct vine_manager *q, struct vine_worker_info *w, const char *line)
{
	char feature[VINE_LINE_MAX];
	char fdec[VINE_LINE_MAX];

	int n = sscanf(line, "feature %s", feature);

	if (n != 1) {
		return VINE_MSG_FAILURE;
	}

	if (!w->features)
		w->features = hash_table_create(4, 0);

	url_decode(feature, fdec, VINE_LINE_MAX);

	debug(D_VINE, "Feature found: %s\n", fdec);

	hash_table_insert(w->features, fdec, (void **)1);

	return VINE_MSG_PROCESSED;
}

/*
Handle activity on a network connection by looking up the mapping
between the link and the vine_worker, then processing on or more
messages available.
*/

static vine_result_code_t handle_worker(struct vine_manager *q, struct link *l)
{
	char line[VINE_LINE_MAX];
	struct vine_worker_info *w;

	char *key = link_to_hash_key(l);
	w = hash_table_lookup(q->worker_table, key);
	free(key);

	vine_msg_code_t mcode;
	mcode = vine_manager_recv_no_retry(q, w, line, sizeof(line));

	// We only expect asynchronous status queries and updates here.

	switch (mcode) {
	case VINE_MSG_PROCESSED:
		// A status message was received and processed.
		return VINE_SUCCESS;
		break;

	case VINE_MSG_PROCESSED_DISCONNECT:
		// A status query was received and processed, so disconnect.
		vine_manager_remove_worker(q, w, VINE_WORKER_DISCONNECT_STATUS_WORKER);
		return VINE_SUCCESS;

	case VINE_MSG_NOT_PROCESSED:
		debug(D_VINE, "Invalid message from worker %s (%s): %s", w->hostname, w->addrport, line);
		q->stats->workers_lost++;
		vine_manager_remove_worker(q, w, VINE_WORKER_DISCONNECT_FAILURE);
		return VINE_WORKER_FAILURE;
		break;

	case VINE_MSG_FAILURE:
		debug(D_VINE, "Failed to read from worker %s (%s)", w->hostname, w->addrport);
		q->stats->workers_lost++;
		vine_manager_remove_worker(q, w, VINE_WORKER_DISCONNECT_FAILURE);
		return VINE_WORKER_FAILURE;
	}

	return VINE_SUCCESS;
}

/*
Construct the table of network links to be considered,
including the manager's accepting link, and one for
each active worker.
*/

static int build_poll_table(struct vine_manager *q)
{
	int n = 0;
	char *key;
	struct vine_worker_info *w;

	// Allocate a small table, if it hasn't been done yet.
	if (!q->poll_table) {
		q->poll_table = malloc(sizeof(*q->poll_table) * q->poll_table_size);
		if (!q->poll_table) {
			// if we can't allocate a poll table, we can't do anything else.
			fatal("allocating memory for poll table failed.");
		}
	}

	// The first item in the poll table is the manager link, which accepts new connections.
	q->poll_table[0].link = q->manager_link;
	q->poll_table[0].events = LINK_READ;
	q->poll_table[0].revents = 0;
	n = 1;

	// For every worker in the hash table, add an item to the poll table
	HASH_TABLE_ITERATE(q->worker_table, key, w)
	{
		// If poll table is not large enough, reallocate it
		if (n >= q->poll_table_size) {
			q->poll_table_size *= 2;
			q->poll_table = realloc(q->poll_table, sizeof(*q->poll_table) * q->poll_table_size);
			if (q->poll_table == NULL) {
				// if we can't allocate a poll table, we can't do anything else.
				fatal("reallocating memory for poll table failed.");
			}
		}

		q->poll_table[n].link = w->link;
		q->poll_table[n].events = LINK_READ;
		q->poll_table[n].revents = 0;
		n++;
	}

	return n;
}

static void vine_manager_compute_input_size(struct vine_manager *q, struct vine_task *t)
{
	t->input_files_size = -1;

	int64_t input_size = 0;
	struct vine_mount *m;
	LIST_ITERATE(t->input_mounts, m)
	{
		if (m->file->state == VINE_FILE_STATE_CREATED) {
			input_size += m->file->size;
		}
	}

	t->input_files_size = (int64_t)ceil(((double)input_size) / ONE_MEGABYTE);
}

/*
 * Determine the resources to allocate for a given task when assigned to a specific worker.
 * @param q The manager structure.
 * @param w The worker info structure.
 * @param t The task structure.
 * @return A pointer to a struct rmsummary describing the chosen resources for the given task.
 */

struct rmsummary *vine_manager_choose_resources_for_task(struct vine_manager *q, struct vine_worker_info *w, struct vine_task *t)
{
	struct rmsummary *limits = rmsummary_create(-1);

	/* Special case: A function-call task consumes no resources. */
	/* Return early, otherwise these zeroes are expanded to use the whole worker. */

	if (t->needs_library) {
		limits->cores = 0;
		limits->memory = 0;
		limits->disk = 0;
		limits->gpus = 0;
		return limits;
	}

	if (t->input_files_size < 0) {
		vine_manager_compute_input_size(q, t);
	}

	/* Compute the minimum and maximum resources for this task. */
	const struct rmsummary *min = vine_manager_task_resources_min(q, t);
	const struct rmsummary *max = vine_manager_task_resources_max(q, t);

	/* available disk for all sandboxes */
	int64_t available_disk = w->resources->disk.total - BYTES_TO_MEGABYTES(w->inuse_cache);

	/* do not count the size of input files as available.
	 * TODO: efficiently discount the size of files already at worker. */
	if (t->input_files_size > 0) {
		available_disk -= t->input_files_size;
	}

	rmsummary_merge_override_basic(limits, max);

	int use_whole_worker = 1;

	int proportional_whole_tasks = q->proportional_whole_tasks;
	if (t->resources_requested->memory > -1 || t->resources_requested->disk > -1) {
		/* if mem or disk are specified explicitely, do not expand resources to fill an integer number of tasks. With this,
		 * the task is assigned exactly the memory and disk specified. We do not do this for cores and gpus, as the use case
		 * here is to specify the number of cores and allocated the rest of the resources evenly. */
		proportional_whole_tasks = 0;
	}

	/* Proportionally assign the worker's resources to the task if configured. */
	if (q->proportional_resources) {

		/* Compute the proportion of the worker the task shall have across resource types. */
		double max_proportion = -1;
		double min_proportion = -1;

		if (w->resources->cores.total > 0) {
			max_proportion = MAX(max_proportion, limits->cores / w->resources->cores.total);
			min_proportion = MAX(min_proportion, min->cores / w->resources->cores.total);
		}

		if (w->resources->memory.total > 0) {
			max_proportion = MAX(max_proportion, limits->memory / w->resources->memory.total);
			min_proportion = MAX(min_proportion, min->memory / w->resources->memory.total);
		}

		if (available_disk > 0) {
			max_proportion = MAX(max_proportion, limits->disk / available_disk);
			min_proportion = MAX(min_proportion, min->disk / available_disk);
		}

		if (w->resources->gpus.total > 0) {
			max_proportion = MAX(max_proportion, limits->gpus / w->resources->gpus.total);
			min_proportion = MAX(min_proportion, min->gpus / w->resources->gpus.total);
		}

		/* If a max_proportion was defined, it cannot be less than a proportion using the minimum
		 * resources for the category. If it was defined, then the min_proportion is not relevant as the
		 * task will try to use the whole worker. */
		if (max_proportion != -1) {
			max_proportion = MAX(max_proportion, min_proportion);
		}

		/* If max_proportion or min_proportion > 1, then the task does not fit the worker for the
		 * specified resources. For the unspecified resources we use the whole
		 * worker as not to trigger a warning when checking for tasks that can't
		 * run on any available worker. */
		if (max_proportion > 1 || min_proportion > 1) {
			use_whole_worker = 1;
		} else if (max_proportion > 0) {
			use_whole_worker = 0;

			// adjust max_proportion so that an integer number of tasks fit the worker.
			if (proportional_whole_tasks) {
				max_proportion = 1.0 / (floor(1.0 / max_proportion));
			}

			/* when cores are unspecified, they are set to 0 if gpus are specified.
			 * Otherwise they get a proportion according to specified
			 * resources. Tasks will get at least one core. */
			if (limits->cores < 0 && limits->gpus > 0) {
				limits->cores = 0;
			} else {
				limits->cores = MAX(1, MAX(limits->cores, floor(w->resources->cores.total * max_proportion)));
			}

			/* unspecified gpus are always 0 */
			if (limits->gpus < 0) {
				limits->gpus = 0;
			}

			limits->memory = MAX(1, MAX(limits->memory, floor(w->resources->memory.total * max_proportion)));

			/* worker's disk is shared evenly among tasks that are not running,
			 * thus the proportion is modified by the current overcommit
			 * multiplier */
			limits->disk = MAX(1, MAX(limits->disk, floor(available_disk * max_proportion / q->resource_submit_multiplier)));
		}
	}

	/* If no resource was specified, use whole worker. */
	if (limits->cores < 1 && limits->gpus < 1 && limits->memory < 1 && limits->disk < 1) {
		use_whole_worker = 1;
	}
	/* At least one specified resource would use the whole worker, thus
	 * using whole worker for all unspecified resources. */
	if ((limits->cores > 0 && limits->cores >= w->resources->cores.total) || (limits->gpus > 0 && limits->gpus >= w->resources->gpus.total) ||
			(limits->memory > 0 && limits->memory >= w->resources->memory.total) || (limits->disk > 0 && limits->disk >= available_disk)) {

		use_whole_worker = 1;
	}

	if (use_whole_worker) {
		/* default cores for tasks that define gpus is 0 */
		if (limits->cores <= 0) {
			limits->cores = limits->gpus > 0 ? 0 : w->resources->cores.total;
		}

		/* default gpus is 0 */
		if (limits->gpus <= 0) {
			limits->gpus = 0;
		}

		if (limits->memory <= 0) {
			limits->memory = w->resources->memory.total;
		}

		if (limits->disk <= 0) {
			limits->disk = available_disk;
		}
	} else if (vine_schedule_in_ramp_down(q)) {
		/* if in ramp down, use all the free space of that worker. note that we don't use
		 * resource_submit_multiplier, as by definition in ramp down there are more workers than tasks. */
		limits->cores = limits->gpus > 0 ? 0 : (w->resources->cores.total - w->resources->cores.inuse);

		/* default gpus is 0 */
		if (limits->gpus <= 0) {
			limits->gpus = 0;
		}

		limits->memory = w->resources->memory.total - w->resources->memory.inuse;
		limits->disk = available_disk;
	}

	/* never go below specified min resources. */
	rmsummary_merge_max(limits, min);

	/* assume the user knows what they are doing... */
	rmsummary_merge_override_basic(limits, t->resources_requested);

	return limits;
}

/*
Start one task on a given worker by specializing the task to the worker,
sending the appropriate input files, and then sending the details of the task.
Note that the "infile" and "outfile" components of the task refer to
files that have already been uploaded into the worker's cache by the manager.
*/

static vine_result_code_t start_one_task(struct vine_manager *q, struct vine_worker_info *w, struct vine_task *t)
{
	struct rmsummary *limits = vine_manager_choose_resources_for_task(q, w, t);

	/*
	If this is a library task, then choose the number of slots to either
	match the explicit request, or set it to the number of cores.
	*/

	if (t->provides_library) {
		if (t->func_exec_mode == VINE_TASK_FUNC_EXEC_MODE_DIRECT) {
			t->function_slots_total = 1;
		} else if (t->function_slots_requested <= 0) {
			t->function_slots_total = limits->cores;
		} else {
			t->function_slots_total = t->function_slots_requested;
		}
	}

	char *command_line;

	if (q->monitor_mode && !t->needs_library) {
		command_line = vine_monitor_wrap(q, w, t, limits);
	} else {
		command_line = xxstrdup(t->command_line);
	}

	vine_result_code_t result = vine_manager_put_task(q, w, t, command_line, limits, 0);

	free(command_line);

	if (result == VINE_SUCCESS) {
		t->current_resource_box = limits;
		rmsummary_merge_override_basic(t->resources_allocated, limits);
		debug(D_VINE, "%s (%s) busy on '%s'", w->hostname, w->addrport, t->command_line);
	} else {
		rmsummary_delete(limits);
	}

	return result;
}

static void count_worker_resources(struct vine_manager *q, struct vine_worker_info *w)
{
	w->resources->cores.inuse = 0;
	w->resources->memory.inuse = 0;
	w->resources->disk.inuse = 0;
	w->resources->gpus.inuse = 0;

	update_max_worker(q, w);

	if (w->resources->workers.total < 1) {
		return;
	}

	uint64_t task_id;
	struct vine_task *task;

	ITABLE_ITERATE(w->current_tasks, task_id, task)
	{
		struct rmsummary *box = task->current_resource_box;
		if (!box)
			continue;
		w->resources->cores.inuse += box->cores;
		w->resources->memory.inuse += box->memory;
		w->resources->disk.inuse += box->disk;
		w->resources->gpus.inuse += box->gpus;
	}

	w->resources->disk.inuse += ceil(BYTES_TO_MEGABYTES(w->inuse_cache));
}

static void update_max_worker(struct vine_manager *q, struct vine_worker_info *w)
{
	if (!w)
		return;

	if (w->resources->workers.total < 1) {
		return;
	}

	if (q->current_max_worker->cores < w->resources->cores.total) {
		q->current_max_worker->cores = w->resources->cores.total;
	}

	if (q->current_max_worker->memory < w->resources->memory.total) {
		q->current_max_worker->memory = w->resources->memory.total;
	}

	if (q->current_max_worker->disk < (w->resources->disk.total - w->inuse_cache)) {
		q->current_max_worker->disk = w->resources->disk.total - w->inuse_cache;
	}

	if (q->current_max_worker->gpus < w->resources->gpus.total) {
		q->current_max_worker->gpus = w->resources->gpus.total;
	}
}

/* we call this function when a worker is disconnected. For efficiency, we use
 * update_max_worker when a worker sends resource updates. */
static void find_max_worker(struct vine_manager *q)
{
	q->current_max_worker->cores = 0;
	q->current_max_worker->memory = 0;
	q->current_max_worker->disk = 0;
	q->current_max_worker->gpus = 0;

	char *key;
	struct vine_worker_info *w;

	HASH_TABLE_ITERATE(q->worker_table, key, w)
	{
		if (w->resources->workers.total > 0) {
			update_max_worker(q, w);
		}
	}
}

/* Tell worker to kill all empty libraries except the case where
 * the task is a function call and the library can run it.
 * This code corresponds to the assumption of
 * @vine.schedule.c:check_worker_have_enough_resources() - empty libraries
 * are not counted towards the resources in use and will be killed if needed. */
static void kill_empty_libraries_on_worker(struct vine_manager *q, struct vine_worker_info *w, struct vine_task *t)
{
	uint64_t task_id;
	struct vine_task *task;
	ITABLE_ITERATE(w->current_tasks, task_id, task)
	{
		if (task->provides_library && task->function_slots_inuse == 0 && (!t->needs_library || strcmp(t->needs_library, task->provides_library))) {
			vine_cancel_by_task_id(q, task->task_id);
		}
	}
}

/*
Commit a given task to a worker by sending the task details,
then updating all auxiliary data structures to note the
assignment and the new task state.
*/

static vine_result_code_t commit_task_to_worker(struct vine_manager *q, struct vine_worker_info *w, struct vine_task *t)
{
	vine_result_code_t result = VINE_SUCCESS;

	/* Kill unused libraries on this worker to reclaim resources. */
	/* Matches assumption in vine_schedule.c:check_available_resources() */
	kill_empty_libraries_on_worker(q, w, t);

	/* If this is a function needing a library, dispatch the library. */
	if (t->needs_library) {
		/* Consider whether the library task is already on that machine. */
		t->library_task = vine_schedule_find_library(q, w, t->needs_library);
		if (!t->library_task) {
			/* Otherwise send the library to the worker. */
			/* Note that this call will re-enter commit_task_to_worker. */
			t->library_task = send_library_to_worker(q, w, t->needs_library, &result);

			/* Careful: if the above failed, then w may no longer be valid */
			/* In that case return immediately without making further changes. */
			if (!t->library_task)
				return result;
		}
		/* If start_one_task_fails, this will be decremented in handle_failure below. */
		t->library_task->function_slots_inuse++;
	}

	t->hostname = xxstrdup(w->hostname);
	t->addrport = xxstrdup(w->addrport);

	t->time_when_commit_start = timestamp_get();
	result = start_one_task(q, w, t);
	t->time_when_commit_end = timestamp_get();

	itable_insert(w->current_tasks, t->task_id, t);
	t->worker = w;

	change_task_state(q, t, VINE_TASK_RUNNING);

	t->try_count += 1;
	q->stats->tasks_dispatched += 1;

	count_worker_resources(q, w);

	if (result != VINE_SUCCESS) {
		debug(D_VINE, "Failed to send task %d to worker %s (%s).", t->task_id, w->hostname, w->addrport);
		handle_failure(q, w, t, result);
	}

	return result;
}

/* 1 if task resubmitted, 0 otherwise */
static int resubmit_task_on_exhaustion(struct vine_manager *q, struct vine_worker_info *w, struct vine_task *t)
{
	if (t->result != VINE_RESULT_RESOURCE_EXHAUSTION) {
		return 0;
	}

	if (t->resources_measured && t->resources_measured->limits_exceeded) {
		struct jx *j = rmsummary_to_json(t->resources_measured->limits_exceeded, 1);
		if (j) {
			char *str = jx_print_string(j);
			debug(D_VINE, "Task %d exhausted resources on %s (%s): %s\n", t->task_id, w->hostname, w->addrport, str);
			free(str);
			jx_delete(j);
		}
	} else {
		debug(D_VINE, "Task %d exhausted resources on %s (%s), but not resource usage was available.\n", t->task_id, w->hostname, w->addrport);
	}

	struct category *c = vine_category_lookup_or_create(q, t->category);
	category_allocation_t next = category_next_label(c,
			t->resource_request,
			/* resource overflow */ 1,
			t->resources_requested,
			t->resources_measured);

	if (next == CATEGORY_ALLOCATION_ERROR) {
		debug(D_VINE, "Task %d failed given max resource exhaustion.\n", t->task_id);
	} else {
		debug(D_VINE, "Task %d resubmitted using new resource allocation.\n", t->task_id);
		t->resource_request = next;
		change_task_state(q, t, VINE_TASK_READY);
		return 1;
	}

	return 0;
}

/* 1 if task resubmitted, 0 otherwise */
static int resubmit_task_on_sandbox_exhaustion(struct vine_manager *q, struct vine_worker_info *w, struct vine_task *t)
{
	if (t->result != VINE_RESULT_SANDBOX_EXHAUSTION) {
		return 0;
	}

	struct category *c = vine_category_lookup_or_create(q, t->category);

	/* on sandbox exhausted, the resources allocated correspond to the overflown sandbox */
	double sandbox = t->resources_allocated->disk;

	/* grow sandbox by given factor (default is two) */
	sandbox *= q->sandbox_grow_factor * sandbox;

	/* take the MAX in case min_vine_sandbox was updated before th result of this task was processed */
	c->min_vine_sandbox = MAX(c->min_vine_sandbox, sandbox);

	debug(D_VINE, "Task %d exhausted disk sandbox on %s (%s).\n", t->task_id, w->hostname, w->addrport);
	double max_allowed_disk = MAX(t->resources_requested->disk, c->max_allocation->disk);

	if (max_allowed_disk > -1 && c->min_vine_sandbox < max_allowed_disk) {
		debug(D_VINE, "Task %d failed given max disk limit for sandbox.\n", t->task_id);
		return 0;
	}

	change_task_state(q, t, VINE_TASK_READY);

	return 1;
}

static int resubmit_if_needed(struct vine_manager *q, struct vine_worker_info *w, struct vine_task *t)
{
	/* in this function, any change_task_state should only be to VINE_TASK_READY */
	if (t->result == VINE_RESULT_FORSAKEN) {
		if (t->max_forsaken > -1 && t->forsaken_count > t->max_forsaken) {
			return 0;
		}

		/* forsaken tasks get a retry back as they are victims of circumstance */
		t->try_count -= 1;
		change_task_state(q, t, VINE_TASK_READY);
		return 1;
	}

	if (t->max_retries > 0 && t->try_count > t->max_retries) {
		// tasks returns to user with the VINE_RESULT_* of the last attempt
		return 0;
	}

	/* special handlings per result. note that most results are terminal, that is tasks are not retried even if they
	 * have not reached max_retries. */
	switch (t->result) {
	case VINE_RESULT_RESOURCE_EXHAUSTION:
		return resubmit_task_on_exhaustion(q, w, t);
		break;
	case VINE_RESULT_SANDBOX_EXHAUSTION:
		return resubmit_task_on_sandbox_exhaustion(q, w, t);
		break;
	default:
		/* by default tasks are not resumitted */
		return 0;
	}
}

/*
Collect a completed task from a worker, and then update
all auxiliary data structures to remove the association
and change the task state.
*/

static void reap_task_from_worker(struct vine_manager *q, struct vine_worker_info *w, struct vine_task *t, vine_task_state_t new_state)
{
	/* Make sure the task and worker agree before changing anything. */
	assert(t->worker == w);

	w->total_task_time += t->time_workers_execute_last;

	rmsummary_delete(t->current_resource_box);
	t->current_resource_box = 0;

	itable_remove(w->current_tasks, t->task_id);

	/*
	If this was a function call assigned to a library,
	then decrease the count of functions assigned,
	and disassociate the task from the library.
	*/

	if (t->needs_library) {
		t->library_task->function_slots_inuse = MAX(0, t->library_task->function_slots_inuse - 1);
	}

	t->worker = 0;

	switch (t->state) {
	case VINE_TASK_RUNNING:
		itable_remove(q->running_table, t->task_id);
		break;
	case VINE_TASK_WAITING_RETRIEVAL:
		list_remove(q->waiting_retrieval_list, t);
		break;
	default:
		assert(t->state > VINE_TASK_READY);
		break;
	}

	/*
	When a normal task or recovery task leaves a worker, it goes back
	into the proper queue.  But a library task was generated just for
	that worker, so it always goes into the RETRIEVED state because it
	is not going back.
	*/

	switch (t->type) {
	case VINE_TASK_TYPE_STANDARD:
	case VINE_TASK_TYPE_RECOVERY:
		if (new_state != VINE_TASK_RETRIEVED || !resubmit_if_needed(q, w, t)) {
			change_task_state(q, t, new_state);
		}
		break;
	case VINE_TASK_TYPE_LIBRARY_INSTANCE:
		change_task_state(q, t, VINE_TASK_RETRIEVED);
		break;
		return;

	case VINE_TASK_TYPE_LIBRARY_TEMPLATE:
		/* A library template should not be scheduled... */
		change_task_state(q, t, VINE_TASK_RETRIEVED);
		break;
		return;
	}

	count_worker_resources(q, w);
}

/*
Determine whether there is transfer capacity to assign this task to this worker.
Returns true on success, false if there are insufficient transfer sources.
If a file can be fetched from a substitute source,
this function modifies the file->substitute field to reflect that source.
*/

static int vine_manager_transfer_capacity_available(struct vine_manager *q, struct vine_worker_info *w, struct vine_task *t)
{
	struct vine_mount *m;

	LIST_ITERATE(t->input_mounts, m)
	{
		/* Is the file already present on that worker? */
		struct vine_file_replica *replica;

		if ((replica = vine_file_replica_table_lookup(w, m->file->cached_name)))
			continue;

		struct vine_worker_info *peer;
		int found_match = 0;

		/* If there is a singly declared mini task dependency linked to multiple created tasks, they
		 * will all share the same reference to it, and consequently share its input file(s).
		 * We modify the object each time we schedule a peer transfer by adding a substitute url.
		 * We must clear the substitute pointer each task we send to ensure we aren't using
		 * a previously scheduled url. */
		vine_file_delete(m->substitute);
		m->substitute = NULL;

		/* Provide a substitute file object to describe the peer. */
		if (!(m->file->flags & VINE_PEER_NOSHARE) && (m->file->cache_level > VINE_CACHE_LEVEL_TASK)) {
			if ((peer = vine_file_replica_table_find_worker(q, m->file->cached_name))) {
				char *peer_source = string_format("%s/%s", peer->transfer_url, m->file->cached_name);
				m->substitute = vine_file_substitute_url(m->file, peer_source, peer);
				free(peer_source);
				found_match = 1;
			}
		}

		/* If that resulted in a match, move on to the next file. */
		if (found_match)
			continue;

		/*
		If no match was found, the behavior depends on the original file type.
		URLs can fetch from the original if capacity is available.
		TEMPs can only fetch from peers, so no match is fatal.
		Any other kind can be provided by the manager at dispatch.
		*/
		if (m->file->type == VINE_URL) {
			/* For a URL transfer, we can fall back to the original if capacity is available. */
			if (vine_current_transfers_url_in_use(q, m->file->source) >= q->file_source_max_transfers) {
				return 0;
			} else {
				/* keep going */
			}
		} else if (m->file->type == VINE_TEMP) {
			//  debug(D_VINE,"task %lld has no ready transfer source for temp %s",(long
			//  long)t->task_id,m->file->cached_name);
			return 0;
		} else if (m->file->type == VINE_MINI_TASK) {
			if (!vine_manager_transfer_capacity_available(q, w, m->file->mini_task)) {
				return 0;
			}
		} else {
			/* keep going */
		}
	}

	debug(D_VINE, "task %lld has a ready transfer source for all files", (long long)t->task_id);
	return 1;
}

/*
If this task produces temporary files, then we must create a recovery task as a copy
of the original task that can be used to re-create those files if they are lost.
The recovery task must be a distinct copy of the original, because the original will
be returned to the user and may be deleted, modified, etc before the recovery task
even needs to be used.
*/

static void vine_manager_create_recovery_tasks(struct vine_manager *q, struct vine_task *t)
{
	struct vine_mount *m;
	struct vine_task *recovery_task = 0;

	/* Only regular tasks get recovery tasks */
	if (t->type != VINE_TASK_TYPE_STANDARD) {
		return;
	}

	LIST_ITERATE(t->output_mounts, m)
	{
		if (m->file->type == VINE_TEMP) {
			if (!recovery_task) {
				recovery_task = vine_task_copy(t);
				recovery_task->type = VINE_TASK_TYPE_RECOVERY;
			}

			m->file->recovery_task = vine_task_addref(recovery_task);
		}
	}

	/*
	Remove the original reference to the recovery task,
	so that only the file pointers carry the needed reference.
	The recovery task does not get entered into the task table
	unless it is needed for execution.
	*/

	vine_task_delete(recovery_task);
}

/*
Consider whether a given recovery task rt should be submitted, so as to re-generate
the necessary output files.  This should only happen if the output files have not been
generated yet.
*/

static void vine_manager_consider_recovery_task(struct vine_manager *q, struct vine_file *lost_file, struct vine_task *rt)
{
	if (!rt)
		return;

	switch (rt->state) {
	case VINE_TASK_INITIAL:
		/* The recovery task has never been run, so submit it now. */
		vine_submit(q, rt);
		notice(D_VINE, "Submitted recovery task %d (%s) to re-create lost temporary file %s.", rt->task_id, rt->command_line, lost_file->cached_name);
		break;
	case VINE_TASK_READY:
	case VINE_TASK_RUNNING:
	case VINE_TASK_WAITING_RETRIEVAL:
	case VINE_TASK_RETRIEVED:
		/* The recovery task is in the process of running, just wait until it is done. */
		break;
	case VINE_TASK_DONE:
		/* The recovery task previously ran to completion, so it must be reset and resubmitted. */
		/* Note that the recovery task has already "left" the manager and so we do not manipulate internal state
		 * here. */
		vine_task_reset(rt);
		vine_submit(q, rt);
		notice(D_VINE, "Submitted recovery task %d (%s) to re-create lost temporary file %s.", rt->task_id, rt->command_line, lost_file->cached_name);
		break;
	}
}

/*
Determine whether the input files needed for this task are available in some form.
Most file types (FILE, URL, BUFFER) we can materialize on demand.
But TEMP files must have been created by a prior task.
If they are no longer present, we cannot run this task,
and should consider re-creating it via a recovery task.
*/

static int vine_manager_check_inputs_available(struct vine_manager *q, struct vine_task *t)
{
	struct vine_mount *m;
	/* all input files are available, if not, consider recovery tasks for them at once */
	int all_available = 1;
	LIST_ITERATE(t->input_mounts, m)
	{
		struct vine_file *f = m->file;
		if (f->type == VINE_FILE && f->state == VINE_FILE_STATE_PENDING) {
			all_available = 0;
		} else if (f->type == VINE_TEMP && f->state == VINE_FILE_STATE_CREATED) {
			if (!vine_file_replica_table_exists_somewhere(q, f->cached_name)) {
				vine_manager_consider_recovery_task(q, f, f->recovery_task);
				all_available = 0;
			}
		}
	}
	return all_available;
}

/*
Determine whether there is a suitable library task for a function call task.
*/

static int vine_manager_check_library_for_function_call(struct vine_manager *q, struct vine_task *t)
{
	if (!t->needs_library || hash_table_lookup(q->library_templates, t->needs_library))
		return 1;
	return 0;
}

/*
Consider if a task is eligible to run, and if so, find the best worker for it.
*/
static struct vine_worker_info *consider_task(struct vine_manager *q, struct vine_task *t)
{
	timestamp_t now_usecs = timestamp_get();
	double now_secs = ((double)now_usecs) / ONE_SECOND;

	// Skip task if min requested start time not met.
	if (t->resources_requested->start > now_secs) {
		return NULL;
	}

	// Skip if this task failed recently
	if (t->time_when_last_failure + q->transient_error_interval > now_usecs) {
		return NULL;
	}

	// Skip if category already running maximum allowed tasks
	struct category *c = vine_category_lookup_or_create(q, t->category);
	if (c->max_concurrent > -1 && c->max_concurrent <= c->vine_stats->tasks_running) {
		return NULL;
	}

	// Skip task if temp input files have not been materialized.
	if (!vine_manager_check_inputs_available(q, t)) {
		return NULL;
	}

	// Skip function call task if no suitable library template was installed
	if (!vine_manager_check_library_for_function_call(q, t)) {
		return NULL;
	}

	// Find the best worker for the task
	q->stats_measure->time_scheduling = timestamp_get();
	struct vine_worker_info *w = vine_schedule_task_to_worker(q, t);
	if (!w) {
		return NULL;
	}
	q->stats->time_scheduling += timestamp_get() - q->stats_measure->time_scheduling;

	// Check if there is transfer capacity available.
	if (q->peer_transfers_enabled) {
		if (!vine_manager_transfer_capacity_available(q, w, t))
			return NULL;
	}

	// All checks passed
	return w;
}

/*
Advance the state of the system by selecting one task available
to run, finding the best worker for that task, and then committing
the task to the worker.
*/

static int send_one_task(struct vine_manager *q)
{
	int t_idx;
	struct vine_task *t;
	struct vine_worker_info *w = NULL;

	int iter_count = 0;
	int iter_depth = MIN(priority_queue_size(q->ready_tasks), q->attempt_schedule_depth);

	// Iterate over the ready tasks by priority.
	// The first time we arrive here, the task with the highest priority is considered. However, there may be various reasons
	// 	that this particular task is not eligible to run, such as: 1) the task requires more resources than the workers have;
	// 	2) the task requires input files that are not available; 3) the task failed recently; etc. (check consider_task function)
	// Therefore, we may permit occasional skips of the highest priority task, and consider the next one in the queue. Similarly,
	// 	other tasks may be skipped, too, until we find a task that is able to run.
	// For a priority queue, iterating over tasks by priority is expensive, as it requires a full sort of the queue. Therefore,
	// 	we simply iterate by numerical index if the task at the top is unable to run, and reset the cursor to the top if events
	// 	that may enable tasks prior to the current cursor to run occur. Specifically, the following events should trigger a reset:
	// 	1. Task retrieval from worker (resources released or inputs available)
	// 	2. New worker connection (more resources available)
	//  3. Delete/Insert an element prior/equal to the rotate cursor (tasks prior to the current cursor changed)
	// 1 and 2 are explicitly handled by the manager where calls priority_queue_rotate_reset, while 3 is implicitly handled by
	// the priority queue data structure where also invokes priority_queue_rotate_reset.
	PRIORITY_QUEUE_ROTATE_ITERATE(q->ready_tasks, t_idx, t, iter_count, iter_depth)
	{
		w = consider_task(q, t);
		if (w) {
			priority_queue_remove(q->ready_tasks, t_idx);
			commit_task_to_worker(q, w, t);
			return 1;
		}
	}

	return 0;
}

/*
get available results from a worker. This is typically used for signaling watched files.
*/
int get_results_from_worker(struct vine_manager *q, struct vine_worker_info *w)
{
	/* get available results from the worker, bail out if that also fails. */
	vine_result_code_t r = get_available_results(q, w);
	if (r != VINE_SUCCESS) {
		handle_worker_failure(q, w);
		return 0;
	}
	return 1;
}

/*
Finding a worker that has tasks waiting to be retrieved, then fetch the outputs
of those tasks. Returns the number of tasks received.
*/
static int receive_tasks_from_worker(struct vine_manager *q, struct vine_worker_info *w, int count_received_so_far)
{
	struct vine_task *t;
	uint64_t task_id;

	int tasks_received = 0;

	/* if the function was called, receive at least one task */
	int max_to_receive = MAX(1, q->max_retrievals - count_received_so_far);

	/* if appropriate, receive all the tasks from the worker */
	if (q->worker_retrievals) {
		max_to_receive = itable_size(w->current_tasks);
	}

	/* Reset the available results table now that the worker is removed */
	hash_table_remove(q->workers_with_complete_tasks, w->hashkey);
	hash_table_firstkey(q->workers_with_complete_tasks);

	/* Now consider all tasks assigned to that worker .*/
	ITABLE_ITERATE(w->current_tasks, task_id, t)
	{
		/* If the task is waiting to be retrieved... */
		if (t->state == VINE_TASK_WAITING_RETRIEVAL) {
			/* Attempt to fetch it. */
			if (fetch_outputs_from_worker(q, w, task_id)) {
				/* If it was fetched, update stats and keep going. */
				tasks_received++;

				if (tasks_received >= max_to_receive) {
					break;
				}
			} else {
				/* But if the fetch failed, the worker is no longer vaild, bail out. */
				return tasks_received;
			}
		}
	}

	/* Consider removing the worker if it is empty. */
	vine_manager_factory_worker_prune(q, w);

	return tasks_received;
}

/*
Advance the state of the system by finding any task that is
waiting to be retrieved, then fetch the outputs of that task,
and mark it as done.
*/

static int receive_one_task(struct vine_manager *q)
{
	struct vine_task *t;

	if ((t = list_peek_head(q->waiting_retrieval_list))) {
		struct vine_worker_info *w = t->worker;
		/* Attempt to fetch from this worker. */
		if (fetch_outputs_from_worker(q, w, t->task_id)) {
			/* Consider whether this worker should be removed. */
			vine_manager_factory_worker_prune(q, w);
			/* If we got a task, then we are done. */
			return 1;
		} else {
			/* But if not, the worker pointer is no longer valid. */
		}
	}

	return 0;
}

/*
Sends keepalives to check if connected workers are responsive,
and ask for updates If not, removes those workers.
*/

static void ask_for_workers_updates(struct vine_manager *q)
{
	struct vine_worker_info *w;
	char *key;
	timestamp_t current_time = timestamp_get();

	HASH_TABLE_ITERATE(q->worker_table, key, w)
	{

		if (q->keepalive_interval > 0) {

			/* we have not received taskvine message from worker yet, so we
			 * simply check again its start_time. */
			if (!strcmp(w->hostname, "unknown")) {
				if ((int)((current_time - w->start_time) / 1000000) >= q->keepalive_timeout) {
					debug(D_VINE, "Removing worker %s (%s): hasn't sent its initialization in more than %d s", w->hostname, w->addrport, q->keepalive_timeout);
					handle_worker_failure(q, w);
				}
				continue;
			}

			// send new keepalive check only (1) if we received a response since last keepalive check AND
			// (2) we are past keepalive interval
			if (w->last_msg_recv_time > w->last_update_msg_time) {
				int64_t last_update_elapsed_time = (int64_t)(current_time - w->last_update_msg_time) / 1000000;
				if (last_update_elapsed_time >= q->keepalive_interval) {
					if (vine_manager_send(q, w, "check\n") < 0) {
						debug(D_VINE, "Failed to send keepalive check to worker %s (%s).", w->hostname, w->addrport);
						handle_worker_failure(q, w);
					} else {
						debug(D_VINE, "Sent keepalive check to worker %s (%s)", w->hostname, w->addrport);
						w->last_update_msg_time = current_time;
					}
				}
			} else {
				// we haven't received a message from worker since its last keepalive check. Check if
				// time since we last polled link for responses has exceeded keepalive timeout. If so,
				// remove worker.
				if (q->link_poll_end > w->last_update_msg_time) {
					if ((int)((q->link_poll_end - w->last_update_msg_time) / 1000000) >= q->keepalive_timeout) {
						debug(D_VINE,
								"Removing worker %s (%s): hasn't responded to keepalive check for more than %d s",
								w->hostname,
								w->addrport,
								q->keepalive_timeout);
						handle_worker_failure(q, w);
					}
				}
			}
		}
	}
}

/*
If disconnect slow workers is enabled, then look for workers that
have taken too long to execute a task, and disconnect
them, under the assumption that they are halted or faulty.
*/

static int disconnect_slow_workers(struct vine_manager *q)
{
	struct category *c;
	char *category_name;

	struct vine_worker_info *w;
	struct vine_task *t;
	uint64_t task_id;

	int removed = 0;

	/* optimization. If no category has a multiplier, simply return. */
	int disconnect_slow_flag = 0;

	HASH_TABLE_ITERATE(q->categories, category_name, c)
	{

		struct vine_stats *stats = c->vine_stats;
		if (!stats) {
			/* no stats have been computed yet */
			continue;
		}

		if (stats->tasks_done < 10) {
			c->average_task_time = 0;
			continue;
		}

		c->average_task_time = (stats->time_workers_execute_good + stats->time_send_good + stats->time_receive_good) / stats->tasks_done;

		if (c->fast_abort > 0)
			disconnect_slow_flag = 1;
	}

	if (!disconnect_slow_flag)
		return 0;

	struct category *c_def = vine_category_lookup_or_create(q, "default");

	timestamp_t current = timestamp_get();

	ITABLE_ITERATE(q->tasks, task_id, t)
	{

		c = vine_category_lookup_or_create(q, t->category);
		/* disconnect slow workers is not enabled for this category */
		if (c->fast_abort == 0)
			continue;

		timestamp_t runtime = current - t->time_when_commit_start;
		timestamp_t average_task_time = c->average_task_time;

		/* Not enough samples, skip the task. */
		if (average_task_time < 1)
			continue;

		double multiplier;
		if (c->fast_abort > 0) {
			multiplier = c->fast_abort;
		} else if (c_def->fast_abort > 0) {
			/* This category uses the default multiplier. (< 0 use default, 0 deactivate). */
			multiplier = c_def->fast_abort;
		} else {
			/* deactivated for the default category. */
			continue;
		}

		if (runtime >= (average_task_time * (multiplier + t->workers_slow))) {
			w = t->worker;
			if (w && (w->type == VINE_WORKER_TYPE_WORKER)) {
				debug(D_VINE, "Task %d is taking too long. Removing from worker.", t->task_id);
				reset_task_to_state(q, t, VINE_TASK_READY);
				t->workers_slow++;

				/* a task cannot mark two different workers as suspect */
				if (t->workers_slow > 1) {
					continue;
				}

				if (w->alarm_slow_worker > 0) {
					/* this is the second task in a row that triggered a disconnection
					 * as a slow worker, therefore we have evidence that this
					 * indeed a slow worker (rather than a task) */

					debug(D_VINE,
							"Removing worker %s (%s): takes too long to execute the current task - %.02lf s (average task execution time by other workers is %.02lf s)",
							w->hostname,
							w->addrport,
							runtime / 1000000.0,
							average_task_time / 1000000.0);
					vine_block_host_with_timeout(q, w->hostname, q->option_blocklist_slow_workers_timeout);
					vine_manager_remove_worker(q, w, VINE_WORKER_DISCONNECT_FAST_ABORT);

					q->stats->workers_slow++;
					removed++;
				}

				w->alarm_slow_worker = 1;
			}
		}
	}

	return removed;
}

/* Forcibly shutdown a worker by telling it to exit, then disconnect it. */

int vine_manager_shut_down_worker(struct vine_manager *q, struct vine_worker_info *w)
{
	if (!w)
		return 0;

	vine_manager_send(q, w, "exit\n");
	vine_manager_remove_worker(q, w, VINE_WORKER_DISCONNECT_EXPLICIT);
	q->stats->workers_released++;

	return 1;
}

static int shutdown_drained_workers(struct vine_manager *q)
{
	char *worker_hashkey = NULL;
	struct vine_worker_info *w = NULL;

	int removed = 0;

	HASH_TABLE_ITERATE(q->worker_table, worker_hashkey, w)
	{
		if (w->draining && itable_size(w->current_tasks) == 0) {
			removed++;
			vine_manager_shut_down_worker(q, w);
		}
	}

	return removed;
}

/* Comparator function for checking if a task matches a given tag. */

static int task_tag_comparator(struct vine_task *t, const char *tag)
{

	if (!t->tag && !tag) {
		return 1;
	}

	if (!t->tag || !tag) {
		return 0;
	}

	return !strcmp(t->tag, tag);
}

/*
Reset a specific task and return it to a known state.
The task could be in any state in any data structure,
including already running on a worker.  So, it must be
removed from its current data structure and then
transitioned to a new state and the corresponding
data structure.
*/

static void reset_task_to_state(struct vine_manager *q, struct vine_task *t, vine_task_state_t new_state)
{
	int t_idx;
	struct vine_worker_info *w = t->worker;

	switch (t->state) {
	case VINE_TASK_INITIAL:
		/* should not happen: this means task was never submitted */
		break;

	case VINE_TASK_READY:
		t_idx = priority_queue_find_idx(q->ready_tasks, t);
		priority_queue_remove(q->ready_tasks, t_idx);
		change_task_state(q, t, new_state);
		break;

	case VINE_TASK_RUNNING:
		// t->worker must be set if in RUNNING state.
		assert(w);

		// send message to worker asking to kill its task.
		vine_manager_send(q, w, "kill %d\n", t->task_id);
		debug(D_VINE, "Task with id %d has been cancelled at worker %s (%s) and removed.", t->task_id, w->hostname, w->addrport);

		// Delete any input files that are not to be cached.
		delete_worker_files(q, w, t->input_mounts, VINE_CACHE_LEVEL_TASK);

		// Delete all output files since they are not needed as the task was cancelled.
		delete_worker_files(q, w, t->output_mounts, VINE_CACHE_LEVEL_FOREVER);

		// Collect task structure from worker.
		// Note that this calls change_task_state internally.
		reap_task_from_worker(q, w, t, new_state);

		break;

	case VINE_TASK_WAITING_RETRIEVAL:
		list_remove(q->waiting_retrieval_list, t);
		change_task_state(q, t, new_state);
		break;

	case VINE_TASK_RETRIEVED:
		list_remove(q->retrieved_list, t);
		change_task_state(q, t, new_state);
		break;

	case VINE_TASK_DONE:
		/* should not happen: this means task was already returned */
		break;
	}
}

/* Search for any one task that matches the given tag string. */

static struct vine_task *find_task_by_tag(struct vine_manager *q, const char *task_tag)
{
	struct vine_task *t;
	uint64_t task_id;

	ITABLE_ITERATE(q->tasks, task_id, t)
	{
		if (task_tag_comparator(t, task_tag)) {
			return t;
		}
	}

	return NULL;
}

/******************************************************/
/************* taskvine public functions *************/
/******************************************************/

struct vine_manager *vine_create(int port)
{
	return vine_ssl_create(port, NULL, NULL);
}

struct vine_manager *vine_ssl_create(int port, const char *key, const char *cert)
{
	struct vine_manager *q = malloc(sizeof(*q));
	if (!q) {
		fprintf(stderr, "Error: failed to allocate memory for manager.\n");
		return 0;
	}
	char *envstring;

	random_init();

	memset(q, 0, sizeof(*q));

	if (port == 0) {
		envstring = getenv("VINE_PORT");
		if (envstring) {
			port = atoi(envstring);
		}
	}

	/* compatibility code */
	if (getenv("VINE_LOW_PORT"))
		setenv("TCP_LOW_PORT", getenv("VINE_LOW_PORT"), 0);
	if (getenv("VINE_HIGH_PORT"))
		setenv("TCP_HIGH_PORT", getenv("VINE_HIGH_PORT"), 0);

	char *runtime_dir = vine_runtime_directory_create();
	if (!runtime_dir) {
		debug(D_NOTICE, "Could not create runtime directories");
		return 0;
	}

	// set debug logfile as soon as possible need to manually use runtime_dir
	// as the manager has not been created yet, but we would like to have debug
	// information of its creation.
	char *debug_tmp = string_format("%s/vine-logs/debug", runtime_dir);
	vine_enable_debug_log(debug_tmp);
	free(debug_tmp);

	q->manager_link = link_serve(port);
	if (!q->manager_link) {
		debug(D_NOTICE, "Could not create manager on port %i.", port);
		free(q);
		return 0;
	} else {
		char address[LINK_ADDRESS_MAX];
		link_address_local(q->manager_link, address, &q->port);
	}

	debug(D_VINE, "manager start");

	q->runtime_directory = runtime_dir;

	q->ssl_key = key ? strdup(key) : 0;
	q->ssl_cert = cert ? strdup(cert) : 0;

	if (q->ssl_key || q->ssl_cert)
		q->ssl_enabled = 1;

	getcwd(q->workingdir, PATH_MAX);

	q->properties = hash_table_create(3, 0);

	/*
	Do it the long way around here so that m->uuid
	is a plain string pointer and we don't end up polluting
	taskvine.h with dttools/uuid.h
	*/
	cctools_uuid_t local_uuid;
	cctools_uuid_create(&local_uuid);
	q->uuid = strdup(local_uuid.str);
	q->duplicated_libraries = 0;

	q->next_task_id = 1;
	q->fixed_location_in_queue = 0;

	q->ready_tasks = priority_queue_create(0);
	q->running_table = itable_create(0);
	q->waiting_retrieval_list = list_create();
	q->retrieved_list = list_create();

	q->tasks = itable_create(0);
	q->library_templates = hash_table_create(0, 0);

	q->worker_table = hash_table_create(0, 0);
	q->file_worker_table = hash_table_create(0, 0);
	q->temp_files_to_replicate = hash_table_create(0, 0);
	q->worker_blocklist = hash_table_create(0, 0);

	q->file_table = hash_table_create(0, 0);

	q->factory_table = hash_table_create(0, 0);
	q->current_transfer_table = hash_table_create(0, 0);
	q->fetch_factory = 0;

	q->measured_local_resources = rmsummary_create(-1);
	q->current_max_worker = rmsummary_create(-1);
	q->max_task_resources_requested = rmsummary_create(-1);

	q->sandbox_grow_factor = 2.0;

	q->stats = calloc(1, sizeof(struct vine_stats));
	q->stats_measure = calloc(1, sizeof(struct vine_stats));

	q->workers_with_watched_file_updates = hash_table_create(0, 0);
	q->workers_with_complete_tasks = hash_table_create(0, 0);

	// The poll table is initially null, and will be created
	// (and resized) as needed by build_poll_table.
	q->poll_table_size = 8;

	q->worker_selection_algorithm = VINE_SCHEDULE_FILES;
	q->process_pending_check = 0;

	q->short_timeout = 5;
	q->long_timeout = 3600;

	q->stats->time_when_started = timestamp_get();
	q->time_last_large_tasks_check = timestamp_get();
	q->task_info_list = list_create();

	q->time_last_wait = 0;
	q->time_last_log_stats = 0;

	q->catalog_hosts = 0;

	q->keepalive_interval = VINE_DEFAULT_KEEPALIVE_INTERVAL;
	q->keepalive_timeout = VINE_DEFAULT_KEEPALIVE_TIMEOUT;

	q->monitor_mode = VINE_MON_DISABLED;

	q->hungry_minimum = 10;
	q->hungry_minimum_factor = 2;

	q->wait_for_workers = 0;
	q->attempt_schedule_depth = 100;

	q->max_retrievals = 1;
	q->worker_retrievals = 1;

	q->proportional_resources = 1;

	/* This option assumes all tasks have similar resource needs.
	 * Turn off by default. */
	q->proportional_whole_tasks = 0;

	q->allocation_default_mode = VINE_ALLOCATION_MODE_FIXED;
	q->categories = hash_table_create(0, 0);

	// The value -1 indicates that disconnecting slow workers is inactive by
	// default
	vine_enable_disconnect_slow_workers(q, -1);

	q->password = 0;

	// peer transfers enabled by default
	q->peer_transfers_enabled = 1;

	q->load_from_shared_fs_enabled = 0;

	q->file_source_max_transfers = VINE_FILE_SOURCE_MAX_TRANSFERS;
	q->worker_source_max_transfers = VINE_WORKER_SOURCE_MAX_TRANSFERS;
	q->perf_log_interval = VINE_PERF_LOG_INTERVAL;

	q->temp_replica_count = 1;
	q->transfer_temps_recovery = 0;
	q->transfer_replica_per_cycle = 10;

	q->resource_submit_multiplier = 1.0;

	q->minimum_transfer_timeout = 60;
	q->transfer_outlier_factor = 10;
	q->default_transfer_rate = 1 * MEGABYTE;
	q->disk_avail_threshold = 100;

	q->update_interval = VINE_UPDATE_INTERVAL;
	q->max_library_retries = VINE_TASK_MAX_LIBRARY_RETRIES;
	q->resource_management_interval = VINE_RESOURCE_MEASUREMENT_INTERVAL;
	q->transient_error_interval = VINE_DEFAULT_TRANSIENT_ERROR_INTERVAL;
	q->max_task_stdout_storage = MAX_TASK_STDOUT_STORAGE;
	q->max_new_workers = MAX_NEW_WORKERS;
	q->large_task_check_interval = VINE_LARGE_TASK_CHECK_INTERVAL;
	q->hungry_check_interval = VINE_HUNGRY_CHECK_INTERVAL;
	q->option_blocklist_slow_workers_timeout = vine_option_blocklist_slow_workers_timeout;

	q->manager_preferred_connection = xxstrdup("by_ip");

	if ((envstring = getenv("VINE_BANDWIDTH"))) {
		q->bandwidth_limit = string_metric_parse(envstring);
		if (q->bandwidth_limit < 0) {
			q->bandwidth_limit = 0;
		}
	}

	vine_enable_perf_log(q, "performance");
	vine_enable_transactions_log(q, "transactions");
	vine_enable_taskgraph_log(q, "taskgraph");

	vine_perf_log_write_update(q, 1);

	q->time_last_wait = timestamp_get();

	debug(D_VINE, "Manager is listening on port %d.", q->port);

	return q;
}

int vine_enable_monitoring(struct vine_manager *q, int watchdog, int series)
{
	if (!q)
		return 0;

	if (series) {
		char *series_file = vine_get_path_log(q, "time-series");
		if (!create_dir(series_file, 0777)) {
			warn(D_VINE, "could not create monitor output directory - %s (%s)", series_file, strerror(errno));
			return 0;
		}
		free(series_file);
	}

	q->monitor_mode = VINE_MON_DISABLED;
	char *exe = resource_monitor_locate(NULL);
	if (!exe) {
		warn(D_VINE, "Could not find the resource monitor executable. Disabling monitoring.\n");
		return 0;
	}

	q->monitor_exe = vine_declare_file(q, exe, VINE_CACHE_LEVEL_WORKFLOW, 0);
	free(exe);

	if (q->measured_local_resources) {
		rmsummary_delete(q->measured_local_resources);
	}
	q->measured_local_resources = rmonitor_measure_process(getpid(), /* do not include disk */ 0);

	q->monitor_mode = VINE_MON_SUMMARY;
	if (series) {
		q->monitor_mode = VINE_MON_FULL;
	}

	if (watchdog) {
		q->monitor_mode |= VINE_MON_WATCHDOG;
	}

	return 1;
}

int vine_enable_peer_transfers(struct vine_manager *q)
{
	debug(D_VINE, "Peer Transfers enabled");
	q->peer_transfers_enabled = 1;
	return 1;
}

int vine_disable_peer_transfers(struct vine_manager *q)
{
	debug(D_VINE, "Peer Transfers disabled");
	fprintf(stderr, "warning: Peer Transfers disabled. Temporary files will be returned to the manager upon creation.");
	q->peer_transfers_enabled = 0;
	return 1;
}

int vine_enable_proportional_resources(struct vine_manager *q)
{
	debug(D_VINE, "Proportional resources enabled");
	q->proportional_resources = 1;
	q->proportional_whole_tasks = 1;
	return 1;
}

int vine_disable_proportional_resources(struct vine_manager *q)
{
	debug(D_VINE, "Proportional resources disabled");
	q->proportional_resources = 0;
	q->proportional_whole_tasks = 0;
	return 1;
}

int vine_enable_disconnect_slow_workers_category(struct vine_manager *q, const char *category, double multiplier)
{
	struct category *c = vine_category_lookup_or_create(q, category);

	if (multiplier >= 1) {
		debug(D_VINE, "Enabling disconnect slow workers for '%s': %3.3lf\n", category, multiplier);
		c->fast_abort = multiplier;
		return 0;
	} else if (multiplier == 0) {
		debug(D_VINE, "Disabling disconnect slow workers for '%s'.\n", category);
		c->fast_abort = 0;
		return 1;
	} else {
		debug(D_VINE, "Using default disconnect slow workers factor for '%s'.\n", category);
		c->fast_abort = -1;
		return 0;
	}
}

int vine_enable_disconnect_slow_workers(struct vine_manager *q, double multiplier)
{
	return vine_enable_disconnect_slow_workers_category(q, "default", multiplier);
}

int vine_port(struct vine_manager *q)
{
	char addr[LINK_ADDRESS_MAX];
	int port;

	if (!q)
		return 0;

	if (link_address_local(q->manager_link, addr, &port)) {
		return port;
	} else {
		return 0;
	}
}

void vine_set_scheduler(struct vine_manager *q, vine_schedule_t algorithm)
{
	q->worker_selection_algorithm = algorithm;
}

void vine_set_name(struct vine_manager *q, const char *name)
{
	if (q->name)
		free(q->name);
	if (name) {
		q->name = xxstrdup(name);
		setenv("VINE_NAME", q->name, 1);
	} else {
		q->name = 0;
	}
}

const char *vine_get_name(struct vine_manager *q)
{
	return q->name;
}

void vine_set_priority(struct vine_manager *q, int priority)
{
	q->priority = priority;
}

void vine_set_tasks_left_count(struct vine_manager *q, int ntasks)
{
	if (ntasks < 1) {
		q->num_tasks_left = 0;
	} else {
		q->num_tasks_left = ntasks;
	}
}

void vine_set_catalog_servers(struct vine_manager *q, const char *hosts)
{
	if (hosts) {
		if (q->catalog_hosts)
			free(q->catalog_hosts);
		q->catalog_hosts = strdup(hosts);
		setenv("CATALOG_HOST", hosts, 1);
	}
}

void vine_set_property(struct vine_manager *m, const char *name, const char *value)
{
	char *oldvalue = hash_table_remove(m->properties, name);
	if (oldvalue)
		free(oldvalue);

	hash_table_insert(m->properties, name, strdup(value));
}

void vine_set_password(struct vine_manager *q, const char *password)
{
	q->password = xxstrdup(password);
}

int vine_set_password_file(struct vine_manager *q, const char *file)
{
	return copy_file_to_buffer(file, &q->password, NULL) > 0;
}

static void delete_task_at_exit(struct vine_task *t)
{
	if (!t) {
		return;
	}

	vine_task_delete(t);

	if (t->type == VINE_TASK_TYPE_LIBRARY_INSTANCE) {
		/* manager created this task, so it is not the API caller's reponsibility. */
		vine_task_delete(t);
	}
}

void vine_delete(struct vine_manager *q)
{
	if (!q)
		return;

	vine_fair_write_workflow_info(q);

	release_all_workers(q);

	vine_perf_log_write_update(q, 1);

	if (q->name)
		update_catalog(q, 1);

	/* we call this function here before any of the structures are freed. */
	vine_disable_monitoring(q);

	if (q->catalog_hosts)
		free(q->catalog_hosts);

	hash_table_clear(q->worker_table, (void *)vine_worker_delete);
	hash_table_delete(q->worker_table);

	hash_table_clear(q->file_worker_table, (void *)set_delete);
	hash_table_delete(q->file_worker_table);

	hash_table_clear(q->temp_files_to_replicate, 0);
	hash_table_delete(q->temp_files_to_replicate);

	hash_table_clear(q->factory_table, (void *)vine_factory_info_delete);
	hash_table_delete(q->factory_table);

	hash_table_clear(q->worker_blocklist, (void *)vine_blocklist_info_delete);
	hash_table_delete(q->worker_blocklist);

	vine_current_transfers_clear(q);
	hash_table_delete(q->current_transfer_table);

	itable_clear(q->tasks, (void *)delete_task_at_exit);
	itable_delete(q->tasks);

	hash_table_clear(q->library_templates, (void *)vine_task_delete);
	hash_table_delete(q->library_templates);

	/* delete files after deleting tasks so that rc are correctly updated. */
	hash_table_clear(q->file_table, (void *)vine_file_delete);
	hash_table_delete(q->file_table);

	char *key;
	struct category *c;
	HASH_TABLE_ITERATE(q->categories, key, c)
	{
		category_delete(q->categories, key);
	}
	hash_table_delete(q->categories);

	priority_queue_delete(q->ready_tasks);
	itable_delete(q->running_table);
	list_delete(q->waiting_retrieval_list);
	list_delete(q->retrieved_list);
	hash_table_delete(q->workers_with_watched_file_updates);
	hash_table_delete(q->workers_with_complete_tasks);

	list_clear(q->task_info_list, (void *)vine_task_info_delete);
	list_delete(q->task_info_list);

	char *staging = vine_get_path_staging(q, NULL);
	if (!access(staging, F_OK)) {
		debug(D_VINE, "deleting %s", staging);
		unlink_recursive(staging);
	}
	free(staging);

	free(q->name);
	free(q->manager_preferred_connection);
	free(q->uuid);

	hash_table_clear(q->properties, (void *)free);
	hash_table_delete(q->properties);

	free(q->poll_table);
	free(q->ssl_cert);
	free(q->ssl_key);

	link_close(q->manager_link);
	if (q->perf_logfile) {
		fclose(q->perf_logfile);
	}

	rmsummary_delete(q->measured_local_resources);
	rmsummary_delete(q->current_max_worker);
	rmsummary_delete(q->max_task_resources_requested);

	if (q->txn_logfile) {
		vine_txn_log_write_manager(q, "END");

		if (fclose(q->txn_logfile) != 0) {
			debug(D_VINE, "unable to write transactions log: %s\n", strerror(errno));
		}
	}

	if (q->graph_logfile) {
		vine_taskgraph_log_write_footer(q);
		fclose(q->graph_logfile);
	}

	free(q->runtime_directory);
	free(q->stats);
	free(q->stats_measure);

	vine_counters_debug();

	debug(D_VINE, "manager end\n");

	debug_close();

	free(q);
}

static void update_resource_report(struct vine_manager *q)
{
	// Only measure every few seconds.
	if ((time(0) - q->resources_last_update_time) < q->resource_management_interval)
		return;

	rmonitor_measure_process_update_to_peak(q->measured_local_resources, getpid(), /* do not include disk */ 0);

	q->resources_last_update_time = time(0);
}

void vine_disable_monitoring(struct vine_manager *q)
{
	if (q->monitor_mode == VINE_MON_DISABLED)
		return;

	q->monitor_mode = VINE_MON_DISABLED;

	// to do: delete vine file of monitor_exe
}

void vine_monitor_add_files(struct vine_manager *q, struct vine_task *t)
{
	vine_task_add_input(t, q->monitor_exe, RESOURCE_MONITOR_REMOTE_NAME, VINE_RETRACT_ON_RESET);

	char *summary = monitor_file_name(q, t, ".summary", 0);
	vine_task_add_output(t, vine_declare_file(q, summary, VINE_CACHE_LEVEL_TASK, 0), RESOURCE_MONITOR_REMOTE_NAME ".summary", VINE_RETRACT_ON_RESET);
	free(summary);

	if (q->monitor_mode & VINE_MON_FULL) {
		char *debug = monitor_file_name(q, t, ".debug", 1);
		char *series = monitor_file_name(q, t, ".series", 1);

		vine_task_add_output(t, vine_declare_file(q, debug, VINE_CACHE_LEVEL_TASK, 0), RESOURCE_MONITOR_REMOTE_NAME ".debug", VINE_RETRACT_ON_RESET);
		vine_task_add_output(t, vine_declare_file(q, series, VINE_CACHE_LEVEL_TASK, 0), RESOURCE_MONITOR_REMOTE_NAME ".series", VINE_RETRACT_ON_RESET);

		free(debug);
		free(series);
	}
}

char *vine_monitor_wrap(struct vine_manager *q, struct vine_worker_info *w, struct vine_task *t, struct rmsummary *limits)
{
	buffer_t b;
	buffer_init(&b);

	buffer_printf(&b, "-V 'task_id: %d'", t->task_id);

	if (t->category) {
		buffer_printf(&b, " -V 'category: %s'", t->category);
	}

	if (t->monitor_snapshot_file) {
		buffer_printf(&b, " --snapshot-events %s", RESOURCE_MONITOR_REMOTE_NAME_EVENTS);
	}

	if (!(q->monitor_mode & VINE_MON_WATCHDOG)) {
		buffer_printf(&b, " --measure-only");
	}

	if (q->monitor_interval > 0) {
		buffer_printf(&b, " --interval %d", q->monitor_interval);
	}

	/* disable disk as it is measured throught the sandbox, otherwise we end up measuring twice. */
	buffer_printf(&b, " --without-disk-footprint");

	int extra_files = (q->monitor_mode & VINE_MON_FULL);

	char *monitor_cmd = resource_monitor_write_command("./" RESOURCE_MONITOR_REMOTE_NAME,
			RESOURCE_MONITOR_REMOTE_NAME,
			limits,
			/* extra options */ buffer_tostring(&b),
			/* debug */ extra_files,
			/* series */ extra_files,
			/* inotify */ 0,
			/* measure_dir */ NULL);
	char *wrap_cmd = string_wrap_command(t->command_line, monitor_cmd);

	buffer_free(&b);
	free(monitor_cmd);

	return wrap_cmd;
}

/* Put a given task on the ready list, taking into account the task priority and the manager schedule. */

static void push_task_to_ready_tasks(struct vine_manager *q, struct vine_task *t)
{
	if (t->result == VINE_RESULT_RESOURCE_EXHAUSTION) {
		/* when a task is resubmitted given resource exhaustion, we
		 * increment its priority by 1, so it gets to run as soon
		 * as possible among those with the same priority. This avoids
		 * the issue in which all 'big' tasks fail because the first
		 * allocation is too small. */
		priority_queue_push(q->ready_tasks, t, t->priority + 1);
	} else {
		priority_queue_push(q->ready_tasks, t, t->priority);
	}

	/* If the task has been used before, clear out accumulated state. */
	vine_task_clean(t);
}

/*
Changes task to a target state, and performs the associated
accounting needed to log the event and put the task into the
new data structure.

Note that this function should only be called if the task has
already been removed from its prior data structure as a part
of scheduling, task completion, etc.
*/

static vine_task_state_t change_task_state(struct vine_manager *q, struct vine_task *t, vine_task_state_t new_state)
{
	vine_task_state_t old_state = t->state;

	t->state = new_state;

	debug(D_VINE, "Task %d state change: %s (%d) to %s (%d)\n", t->task_id, vine_task_state_to_string(old_state), old_state, vine_task_state_to_string(new_state), new_state);

	struct category *c = vine_category_lookup_or_create(q, t->category);

	/* XXX: update manager task count in the same way */
	switch (old_state) {
	case VINE_TASK_INITIAL:
		break;
	case VINE_TASK_READY:
		c->vine_stats->tasks_waiting--;
		break;
	case VINE_TASK_RUNNING:
		c->vine_stats->tasks_running--;
		break;
	case VINE_TASK_WAITING_RETRIEVAL:
		c->vine_stats->tasks_with_results--;
		break;
	case VINE_TASK_RETRIEVED:
		break;
	case VINE_TASK_DONE:
		break;
	}

	c->vine_stats->tasks_on_workers = c->vine_stats->tasks_running + c->vine_stats->tasks_with_results;
	c->vine_stats->tasks_submitted = c->total_tasks + c->vine_stats->tasks_waiting + c->vine_stats->tasks_on_workers;

	switch (new_state) {
	case VINE_TASK_INITIAL:
		/* should not happen, do nothing */
		break;
	case VINE_TASK_READY:
		vine_task_set_result(t, VINE_RESULT_UNKNOWN);
		push_task_to_ready_tasks(q, t);
		c->vine_stats->tasks_waiting++;
		break;
	case VINE_TASK_RUNNING:
		itable_insert(q->running_table, t->task_id, t);
		c->vine_stats->tasks_running++;
		break;
	case VINE_TASK_WAITING_RETRIEVAL:
		list_push_head(q->waiting_retrieval_list, t);
		c->vine_stats->tasks_with_results++;
		break;
	case VINE_TASK_RETRIEVED:
		/* Library task can be set to RETRIEVED when it failed or was removed intentionally */
		if (t->type == VINE_TASK_TYPE_LIBRARY_INSTANCE) {
			vine_task_set_result(t, VINE_RESULT_LIBRARY_EXIT);
		}
		list_push_head(q->retrieved_list, t);
		break;
	case VINE_TASK_DONE:
		/* Task was added a reference when entered into our own table, so delete a reference on removal. */
		if (t->has_fixed_locations) {
			q->fixed_location_in_queue--;
		}
		vine_taskgraph_log_write_task(q, t);
		itable_remove(q->tasks, t->task_id);
		vine_task_delete(t);
		break;
	}

	vine_perf_log_write_update(q, 0);
	vine_txn_log_write_task(q, t);

	return old_state;
}

const char *vine_result_string(vine_result_t result)
{
	const char *str = NULL;

	switch (result) {
	case VINE_RESULT_SUCCESS:
		str = "SUCCESS";
		break;
	case VINE_RESULT_INPUT_MISSING:
		str = "INPUT_MISSING";
		break;
	case VINE_RESULT_OUTPUT_MISSING:
		str = "OUTPUT_MISSING";
		break;
	case VINE_RESULT_STDOUT_MISSING:
		str = "STDOUT_MISSING";
		break;
	case VINE_RESULT_SIGNAL:
		str = "SIGNAL";
		break;
	case VINE_RESULT_RESOURCE_EXHAUSTION:
		str = "RESOURCE_EXHAUSTION";
		break;
	case VINE_RESULT_MAX_END_TIME:
		str = "MAX_END_TIME";
		break;
	case VINE_RESULT_UNKNOWN:
		str = "UNKNOWN";
		break;
	case VINE_RESULT_FORSAKEN:
		str = "FORSAKEN";
		break;
	case VINE_RESULT_MAX_RETRIES:
		str = "MAX_RETRIES";
		break;
	case VINE_RESULT_MAX_WALL_TIME:
		str = "MAX_WALL_TIME";
		break;
	case VINE_RESULT_RMONITOR_ERROR:
		str = "MONITOR_ERROR";
		break;
	case VINE_RESULT_OUTPUT_TRANSFER_ERROR:
		str = "OUTPUT_TRANSFER_ERROR";
		break;
	case VINE_RESULT_FIXED_LOCATION_MISSING:
		str = "FIXED_LOCATION_MISSING";
		break;
	case VINE_RESULT_CANCELLED:
		str = "CANCELLED";
		break;
	case VINE_RESULT_LIBRARY_EXIT:
		str = "LIBRARY_EXIT";
		break;
	case VINE_RESULT_SANDBOX_EXHAUSTION:
		str = "SANDBOX_EXHAUSTION";
		break;
	}

	return str;
}

static int task_request_count(struct vine_manager *q, const char *category, category_allocation_t request)
{
	struct vine_task *t;
	uint64_t task_id;

	int count = 0;

	ITABLE_ITERATE(q->tasks, task_id, t)
	{
		if (t->resource_request == request) {
			if (!category || strcmp(category, t->category) == 0) {
				count++;
			}
		}
	}

	return count;
}

int vine_submit(struct vine_manager *q, struct vine_task *t)
{
	if (t->state != VINE_TASK_INITIAL) {
		notice(D_VINE, "vine_submit: you cannot submit the same task (%d) (%s) twice!", t->task_id, t->command_line);
		return 0;
	}

	/* Assign a unique ID to each task only when submitted. */
	t->task_id = q->next_task_id++;

	/* Issue warnings if the files are set up strangely. */
	vine_task_check_consistency(t);

	if (t->has_fixed_locations) {
		q->fixed_location_in_queue++;
		vine_task_set_scheduler(t, VINE_SCHEDULE_FILES);
	}

	/* If the task produces temporary files, create recovery tasks for those. */
	vine_manager_create_recovery_tasks(q, t);

	/* If the task produces watched output files, truncate them. */
	vine_task_truncate_watched_outputs(t);

	/* Add reference to task when adding it to primary table. */
	itable_insert(q->tasks, t->task_id, vine_task_addref(t));

	/* Ensure category structure is created. */
	vine_category_lookup_or_create(q, t->category);

	change_task_state(q, t, VINE_TASK_READY);

	t->time_when_submitted = timestamp_get();
	q->stats->tasks_submitted++;

	if (q->monitor_mode != VINE_MON_DISABLED)
		vine_monitor_add_files(q, t);

	rmsummary_merge_max(q->max_task_resources_requested, t->resources_requested);

	return (t->task_id);
}

/* Send a given library by name to the target worker.
 * This involves duplicating the prototype task in q->library_templates
 * and then sending the copy as a (mostly) normal task.
 * @param q The manager structure.
 * @param w The worker info structure.
 * @param name The name of the library to be sent.
 * @param result A pointer to a result reflecting the reason for any failure.
 * @return pointer to the library task if the operation succeeds, 0 otherwise.
 */
struct vine_task *send_library_to_worker(struct vine_manager *q, struct vine_worker_info *w, const char *name, vine_result_code_t *result)
{
	/* Find the original prototype library task by name, if it exists. */
	struct vine_task *original = hash_table_lookup(q->library_templates, name);
	if (!original) {
		return 0;
	}

	/*
	If this template had been failed for over a specific count,
	then remove it and notify the user that this template might be broken
	*/
	if (original->library_failed_count > q->max_library_retries) {
		vine_manager_remove_library(q, name);
		debug(D_VINE, "library %s has reached the maximum failure count %d, it has been removed", name, q->max_library_retries);
		printf("library %s has reached the maximum failure count %d, it has been removed\n", name, q->max_library_retries);
		return 0;
	}

	/*
	If an instance of this library has recently failed,
	don't send another right away.  This is a rather late (and inefficient)
	point at which to notice this, but we don't know that we are starting
	a new library until the moment of scheduling a function to that worker.
	*/
	timestamp_t lastfail = original->time_when_last_failure;
	timestamp_t current = timestamp_get();
	if (current < lastfail + q->transient_error_interval) {
		return 0;
	}

	/* Check if this library task can fit in this worker. */
	/* check_worker_against_task does not, and should not, modify the task */
	if (!check_worker_against_task(q, w, original)) {
		return 0;
	}
	/* Track the number of duplicated libraries */
	q->duplicated_libraries += 1;

	/* Duplicate the original task */
	struct vine_task *t = vine_task_copy(original);
	t->type = VINE_TASK_TYPE_LIBRARY_INSTANCE;

	/* Give it a unique taskid if library fits the worker. */
	t->task_id = q->next_task_id++;

	/* If watch-library-logfiles is tuned, watch the output file of every duplicated library instance */
	if (q->watch_library_logfiles) {
		char *remote_stdout_filename = string_format(".taskvine.stdout");
		char *local_library_log_filename = string_format("library-%d.debug.log", q->duplicated_libraries);
		t->library_log_path = vine_get_path_library_log(q, local_library_log_filename);

		struct vine_file *library_local_stdout_file = vine_declare_file(q, t->library_log_path, VINE_CACHE_LEVEL_TASK, 0);
		vine_task_add_output(t, library_local_stdout_file, remote_stdout_filename, VINE_WATCH);

		free(remote_stdout_filename);
		free(local_library_log_filename);
	}

	/* Add reference to task when adding it to primary table. */
	itable_insert(q->tasks, t->task_id, vine_task_addref(t));

	/* Send the task to the worker in the usual way. */
	/* Careful: If this failed, then the worker object or task object may no longer be valid! */
	*result = commit_task_to_worker(q, w, t);

	/* Careful again: If commit_task_to_worker failed the worker object or task object may no longer be valid! */
	if (*result == VINE_SUCCESS) {
		vine_txn_log_write_library_update(q, w, t->task_id, VINE_LIBRARY_SENT);
		return t;
	} else {
		/* if failure, tasks was handled by handle_failure(...) according to result. */
		return 0;
	}
}

void vine_manager_install_library(struct vine_manager *q, struct vine_task *t, const char *name)
{
	t->type = VINE_TASK_TYPE_LIBRARY_TEMPLATE;
	t->library_failed_count = 0;
	t->task_id = -1;
	vine_task_set_library_provided(t, name);
	hash_table_insert(q->library_templates, name, t);
	t->time_when_submitted = timestamp_get();
}

void vine_manager_remove_library(struct vine_manager *q, const char *name)
{
	char *worker_key;
	struct vine_worker_info *w;

	HASH_TABLE_ITERATE(q->worker_table, worker_key, w)
	{
		/* A worker might contain multiple library instances */
		struct vine_task *library = vine_schedule_find_library(q, w, name);
		while (library) {
			vine_cancel_by_task_id(q, library->task_id);
			library = vine_schedule_find_library(q, w, name);
		}
		hash_table_remove(q->library_templates, name);

		debug(D_VINE, "All instances and the template for library %s have been removed", name);
	}
}

struct vine_task *vine_manager_find_library_template(struct vine_manager *q, const char *library_name)
{
	return hash_table_lookup(q->library_templates, library_name);
}

static void handle_library_update(struct vine_manager *q, struct vine_worker_info *w, const char *line)
{
	int library_id = 0;
	vine_library_state_t state;

	int n = sscanf(line, "%d %d", &library_id, (int *)&state);
	if (n != 2) {
		debug(D_VINE, "Library %d update message is corrupt.", library_id);
		return;
	}

	vine_txn_log_write_library_update(q, w, library_id, state);
}

void vine_block_host_with_timeout(struct vine_manager *q, const char *hostname, time_t timeout)
{
	return vine_blocklist_block(q, hostname, timeout);
}

void vine_block_host(struct vine_manager *q, const char *hostname)
{
	vine_blocklist_block(q, hostname, -1);
}

void vine_unblock_host(struct vine_manager *q, const char *hostname)
{
	vine_blocklist_unblock(q, hostname);
}

void vine_unblock_all(struct vine_manager *q)
{
	vine_blocklist_unblock_all_by_time(q, -1);
}

static void print_password_warning(struct vine_manager *q)
{
	static int did_password_warning = 0;

	if (did_password_warning) {
		return;
	}

	if (!q->password && q->name) {
		debug(D_DEBUG, "warning: this taskvine manager is visible to the public.\n");
		debug(D_DEBUG, "warning: you should set a password with the --password option.\n");
	}

	if (!q->ssl_enabled) {
		debug(D_DEBUG, "warning: using plain-text when communicating with workers.\n");
		debug(D_DEBUG, "warning: use encryption with a key and cert when creating the manager.\n");
	}

	did_password_warning = 1;
}

#define BEGIN_ACCUM_TIME(q, stat) \
	{ \
		if (q->stats_measure->stat != 0) { \
			fatal("Double-counting stat %s. This should not happen, and it is a taskvine bug."); \
		} else { \
			q->stats_measure->stat = timestamp_get(); \
		} \
	}

#define END_ACCUM_TIME(q, stat) \
	{ \
		q->stats->stat += timestamp_get() - q->stats_measure->stat; \
		q->stats_measure->stat = 0; \
	}

struct vine_task *vine_wait(struct vine_manager *q, int timeout)
{
	return vine_wait_for_tag(q, NULL, timeout);
}

struct vine_task *vine_wait_for_tag(struct vine_manager *q, const char *tag, int timeout)
{
	return vine_wait_internal(q, timeout, tag, -1);
}

struct vine_task *vine_wait_for_task_id(struct vine_manager *q, int task_id, int timeout)
{
	return vine_wait_internal(q, timeout, NULL, task_id);
}

/* return number of workers that failed */
static int poll_active_workers(struct vine_manager *q, int stoptime)
{
	BEGIN_ACCUM_TIME(q, time_polling);

	int n = build_poll_table(q);

	// We poll in at most small time segments (of a second). This lets
	// promptly dispatch tasks, while avoiding busy waiting.
	int msec = q->busy_waiting_flag ? 1000 : 0;
	if (stoptime) {
		msec = MIN(msec, (stoptime - time(0)) * 1000);
	}

	END_ACCUM_TIME(q, time_polling);

	if (msec < 0) {
		return 0;
	}

	BEGIN_ACCUM_TIME(q, time_polling);

	// Poll all links for activity.
	link_poll(q->poll_table, n, msec);
	q->link_poll_end = timestamp_get();

	END_ACCUM_TIME(q, time_polling);

	BEGIN_ACCUM_TIME(q, time_status_msgs);

	int i, j = 1;
	int workers_failed = 0;
	// Then consider all existing active workers
	for (i = j; i < n; i++) {
		if (q->poll_table[i].revents) {
			if (handle_worker(q, q->poll_table[i].link) == VINE_WORKER_FAILURE) {
				workers_failed++;
			}
		}
	}

	END_ACCUM_TIME(q, time_status_msgs);

	return workers_failed;
}

static int connect_new_workers(struct vine_manager *q, int stoptime, int max_new_workers)
{
	int new_workers = 0;

	// If the manager link was awake, then accept at most max_new_workers.
	// Note we are using the information gathered in poll_active_workers, which
	// is a little ugly.
	if (q->poll_table[0].revents) {
		do {
			add_worker(q);
			new_workers++;
		} while (link_usleep(q->manager_link, 0, 1, 0) && (stoptime >= time(0) && (max_new_workers > new_workers)));
	}

	return new_workers;
}

struct vine_task *find_task_to_return(struct vine_manager *q, const char *tag, int task_id)
{
	while (1) {
		struct vine_task *t = NULL;

		if (tag) {
			struct vine_task *temp = NULL;
			int tasks_to_consider = list_size(q->retrieved_list);
			while (tasks_to_consider > 0) {
				tasks_to_consider--;
				temp = list_peek_head(q->retrieved_list);
				// a small hack, if task is not standard we accepted it so it can be deleted below.
				if (temp->type != VINE_TASK_TYPE_STANDARD || task_tag_comparator(temp, tag)) {
					// temp points to head of list
					t = list_pop_head(q->retrieved_list);
					break;
				} else {
					list_rotate(q->retrieved_list);
				}
			}
		} else if (task_id >= 0) {
			// XXX: library tasks are never removed!
			struct vine_task *temp = itable_lookup(q->tasks, task_id);
			if (!temp || temp->state != VINE_TASK_RETRIEVED) {
				break;
			}
			t = temp;
			list_remove(q->retrieved_list, t);
		} else if (list_size(q->retrieved_list) > 0) {
			t = list_pop_head(q->retrieved_list);
		}

		if (!t) {
			/* didn't find a retrieved task to return */
			return NULL;
		}

		change_task_state(q, t, VINE_TASK_DONE);
		if (t->result != VINE_RESULT_SUCCESS) {
			q->stats->tasks_failed++;
		}

		switch (t->type) {
		case VINE_TASK_TYPE_STANDARD:
			/* if this is a standard task type, then break and return it to the user. */
			return t;
			break;
		case VINE_TASK_TYPE_RECOVERY:
			/* do nothing and let vine_manager_consider_recovery_task do its job */
			break;
		case VINE_TASK_TYPE_LIBRARY_INSTANCE:
			/* silently delete it */
			vine_task_delete(t); // delete as manager created this task
			break;
		case VINE_TASK_TYPE_LIBRARY_TEMPLATE:
			/* A template shouldn't be scheduled. It's deleted when template table is deleted.*/
			break;
		}
	}

	return NULL;
}

static struct vine_task *vine_wait_internal(struct vine_manager *q, int timeout, const char *tag, int task_id)
{
	/*
	   - compute stoptime
	   S time left?                                               No:  break
	   - update catalog if appropriate
	   - task completed or (prefer-dispatch and would busy wait)? Yes: return completed task to user
	   - retrieve workers status messages
	   - retrieve available results for watched files
	   - workers with complete tasks?                             Yes: retrieve max_retrievals tasks from worker
	   - tasks waiting to be retrieved?                           Yes: retrieve max_retrievals
	   - tasks expired?                                           Yes: mark as retrieved
	   - tasks lost fixed location files?                         Yes: mark as retrieved
	   - tasks mark as retrieved and not prefer-dispatch          Yes: go to S
	   - tasks waiting to be dispatched?                          Yes: dispatch one task and go to S.
	   - send keepalives to appropriate workers
	   - disconnect slow workers
	   - drain workers from factories
	   - new workers?                                             Yes: connect max_new_workers and to to S
	   - send all libraries to all workers
	   - replicate temp files
	   - manager empty?                                           Yes: break
	   - mark as busy-waiting and go to S
	*/

	// account for time we spend outside vine_wait
	if (q->time_last_wait > 0) {
		q->stats->time_application += timestamp_get() - q->time_last_wait;
	} else {
		q->stats->time_application += timestamp_get() - q->stats->time_when_started;
	}

	if (timeout == 0) {
		// if timeout is 0, just return any completed task if one available.
		return vine_manager_no_wait(q, tag, task_id);
	}

	if (timeout != VINE_WAIT_FOREVER && timeout < 0) {
		debug(D_NOTICE | D_VINE, "Invalid wait timeout value '%d'. Waiting for 5 seconds.", timeout);
		timeout = 5;
	}

	int events = 0;
	print_password_warning(q);

	// compute stoptime
	time_t stoptime = (timeout == VINE_WAIT_FOREVER) ? 0 : time(0) + timeout;

	int result;
	struct vine_task *t = NULL;

	// used for q->prefer_dispatch. If 0 and there is a task retrieved, then return task to app.
	int sent_in_previous_cycle = 1;

	// time left?
	while ((stoptime == 0) || (time(0) < stoptime)) {

		BEGIN_ACCUM_TIME(q, time_internal);
		// update catalog if appropriate
		if (q->name) {
			update_catalog(q, 0);
		}

		if (q->monitor_mode) {
			update_resource_report(q);
		}
		END_ACCUM_TIME(q, time_internal);

		// break loop if there is a task to be returned to the user; or if prefering dispatching, if there
		// are no tasks to be dispatched; or if we already looped once and no events were triggered.
		if (list_size(q->retrieved_list) > 0) {
			if (!t) {
				BEGIN_ACCUM_TIME(q, time_internal);
				t = find_task_to_return(q, tag, task_id);
				END_ACCUM_TIME(q, time_internal);
			}

			if (t && (!q->prefer_dispatch || priority_queue_size(q->ready_tasks) == 0 || !sent_in_previous_cycle)) {
				break;
			}
		}

		// retrieve worker status messages
		if (poll_active_workers(q, stoptime) > 0) {
			// at least one worker was removed.
			events++;
			// note we keep going, and we do not restart the loop as we do in
			// further events. This is because we give top priority to
			// returning and retrieving tasks.
		}

		// get updates for watched files.
		if (hash_table_size(q->workers_with_watched_file_updates)) {

			struct vine_worker_info *w;
			char *key;
			HASH_TABLE_ITERATE(q->worker_table, key, w)
			{
				get_available_results(q, w);
				hash_table_remove(q->workers_with_watched_file_updates, w->hashkey);
			}
		}

		q->busy_waiting_flag = 0;

		// retrieve results from workers
		// if worker_retrievals, then all the tasks from a worker
		// are retrieved. (this is the default)
		// otherwise, retrieve at most q->max_retrievals (default is 1)
		int retrieved_this_cycle = 0;
		BEGIN_ACCUM_TIME(q, time_receive);
		do {
			char *key;
			struct vine_worker_info *w;

			// consider one worker at a time

			hash_table_firstkey(q->workers_with_complete_tasks);
			if (hash_table_nextkey(q->workers_with_complete_tasks, &key, (void **)&w)) {
				int retrieved_from_worker = receive_tasks_from_worker(q, w, retrieved_this_cycle);
				retrieved_this_cycle += retrieved_from_worker;
				events += retrieved_from_worker;
			} else if (receive_one_task(q)) {
				// retrieved at least one task
				retrieved_this_cycle++;
				events++;
			} else {
				// didn't received a task this cycle, thus there are no
				// task to be received
				break;
			}
		} while (q->max_retrievals < 0 || retrieved_this_cycle < q->max_retrievals || !priority_queue_size(q->ready_tasks));
		END_ACCUM_TIME(q, time_receive);

		// expired tasks
		BEGIN_ACCUM_TIME(q, time_internal);
		result = expire_waiting_tasks(q);
		END_ACCUM_TIME(q, time_internal);
		if (result > 0) {
			retrieved_this_cycle += result;
			events++;
		}

		// only check for fixed location if any are present (high overhead)
		if (q->fixed_location_in_queue) {

			BEGIN_ACCUM_TIME(q, time_internal);
			result = enforce_waiting_fixed_locations(q);
			END_ACCUM_TIME(q, time_internal);
			if (result > 0) {
				retrieved_this_cycle += result;
				events++;
			}
		}

		if (retrieved_this_cycle) {
			// reset the rotate cursor on task retrieval
			priority_queue_rotate_reset(q->ready_tasks);
			if (!q->prefer_dispatch) {
				continue;
			}
		}

		sent_in_previous_cycle = 0;
		if (q->wait_for_workers <= hash_table_size(q->worker_table)) {
			if (q->wait_for_workers > 0) {
				debug(D_VINE, "Target number of workers reached (%d).", q->wait_for_workers);
				q->wait_for_workers = 0;
			}
			// tasks waiting to be dispatched?
			BEGIN_ACCUM_TIME(q, time_send);
			result = send_one_task(q);
			END_ACCUM_TIME(q, time_send);
			if (result) {
				// sent at least one task
				events++;
				sent_in_previous_cycle = 1;
				continue;
			}
		}

		// send keepalives to appropriate workers
		BEGIN_ACCUM_TIME(q, time_status_msgs);
		ask_for_workers_updates(q);
		END_ACCUM_TIME(q, time_status_msgs);

		// Kill off slow/drained workers.
		BEGIN_ACCUM_TIME(q, time_internal);
		result = disconnect_slow_workers(q);
		result += shutdown_drained_workers(q);
		vine_blocklist_unblock_all_by_time(q, time(0));
		END_ACCUM_TIME(q, time_internal);

		// if new workers, connect n of them
		BEGIN_ACCUM_TIME(q, time_status_msgs);
		result = connect_new_workers(q, stoptime, MAX(q->wait_for_workers, q->max_new_workers));
		END_ACCUM_TIME(q, time_status_msgs);
		if (result) {
			// accepted at least one worker
			// reset the rotate cursor on worker connection
			priority_queue_rotate_reset(q->ready_tasks);
			events++;
			continue;
		}

		// Check if any temp files need replication and start replicating
		BEGIN_ACCUM_TIME(q, time_internal);
		result = recover_temp_files(q);
		END_ACCUM_TIME(q, time_internal);
		if (result) {
			// recovered at least one temp file
			events++;
			continue;
		}

		if (q->process_pending_check) {
			BEGIN_ACCUM_TIME(q, time_internal);
			int pending = process_pending();
			END_ACCUM_TIME(q, time_internal);

			if (pending) {
				events++;
				break;
			}
		}

		// return if manager is empty and something interesting already happened
		// in this wait.
		if (events > 0) {
			BEGIN_ACCUM_TIME(q, time_internal);
			int done = !priority_queue_size(q->ready_tasks) && !list_size(q->waiting_retrieval_list) && !itable_size(q->running_table);
			END_ACCUM_TIME(q, time_internal);

			if (done) {
				if (retrieved_this_cycle > 0) {
					continue; // we only get here with prefer-dispatch, continue to find a task to
						  // return
				} else {
					break;
				}
			}
		}

		timestamp_t current_time = timestamp_get();
		if (current_time - q->time_last_large_tasks_check >= q->large_task_check_interval) {
			q->time_last_large_tasks_check = current_time;
			find_max_worker(q);
			vine_schedule_check_for_large_tasks(q);
		}

		// if we got here, no events were triggered this time around.
		// we set the busy_waiting flag so that link_poll waits for some time
		// the next time around, or return retrieved tasks if there some available.
		q->busy_waiting_flag = 1;
	}

	if (events > 0) {
		vine_perf_log_write_update(q, 1);
	}

	q->time_last_wait = timestamp_get();

	return t;
}

struct vine_task *vine_manager_no_wait(struct vine_manager *q, const char *tag, int task_id)
{
	BEGIN_ACCUM_TIME(q, time_internal);
	struct vine_task *t = find_task_to_return(q, tag, task_id);
	if (t) {
		vine_perf_log_write_update(q, 1);
	}
	END_ACCUM_TIME(q, time_internal);

	q->time_last_wait = timestamp_get();

	return t;
}

// check if workers' resources are available to execute more tasks queue should
// have at least MAX(hungry_minimum, hungry_minimum_factor * number of workers) ready tasks
// Usually not called directly, but by vine_hungry.
//@param: 	struct vine_manager* - pointer to manager
//@return: 	approximate number of additional tasks if hungry, 0 otherwise
int vine_hungry_computation(struct vine_manager *q)
{
	struct vine_stats qstats;
	vine_get_stats(q, &qstats);

	// set min tasks running to 1. if it was 0, then committed resource would be 0 anyway so average works out to 0.
	int64_t tasks_running = MAX(qstats.tasks_running, 1);
	int64_t tasks_waiting = qstats.tasks_waiting;

	/* queue is hungry according to the number of workers available (assume each worker can run at least one task) */
	int hungry_minimum = MAX(q->hungry_minimum, qstats.workers_connected * q->hungry_minimum_factor);

	if (tasks_running < 1 && tasks_waiting < 1) {
		return hungry_minimum;
	}

	/* assume a task uses at least one core, otherwise if no resource is specified, the queue is infinitely hungry */
	int64_t avg_commited_tasks_cores = MAX(1, DIV_INT_ROUND_UP(qstats.committed_cores, tasks_running));
	int64_t avg_commited_tasks_memory = DIV_INT_ROUND_UP(qstats.committed_memory, tasks_running);
	int64_t avg_commited_tasks_disk = DIV_INT_ROUND_UP(qstats.committed_disk, tasks_running);
	int64_t avg_commited_tasks_gpus = DIV_INT_ROUND_UP(qstats.committed_gpus, tasks_running);

	// get total available resources consumption (cores, memory, disk, gpus) of all workers of this manager
	// available = factor*total (all) - committed (actual in use)
	int64_t workers_total_avail_cores = q->hungry_minimum_factor * qstats.total_cores - qstats.committed_cores;
	int64_t workers_total_avail_memory = q->hungry_minimum_factor * qstats.total_memory - qstats.committed_memory;
	int64_t workers_total_avail_disk = q->hungry_minimum_factor * qstats.total_disk - qstats.committed_disk;
	int64_t workers_total_avail_gpus = q->hungry_minimum_factor * qstats.total_gpus - qstats.committed_gpus;

	int64_t tasks_needed = 0;
	if (tasks_waiting < 1) {
		tasks_needed = DIV_INT_ROUND_UP(workers_total_avail_cores, avg_commited_tasks_cores);
		if (avg_commited_tasks_memory > 0) {
			tasks_needed = MIN(tasks_needed, DIV_INT_ROUND_UP(workers_total_avail_memory, avg_commited_tasks_memory));
		}

		if (avg_commited_tasks_disk > 0) {
			tasks_needed = MIN(tasks_needed, DIV_INT_ROUND_UP(workers_total_avail_disk, avg_commited_tasks_disk));
		}

		if (avg_commited_tasks_gpus > 0) {
			tasks_needed = MIN(tasks_needed, DIV_INT_ROUND_UP(workers_total_avail_gpus, avg_commited_tasks_gpus));
		}

		return MAX(tasks_needed, hungry_minimum);
	}

	// from here on we can assume that tasks_waiting > 0.

	// get required resources (cores, memory, disk, gpus) of one (all?) waiting tasks
	// seems to iterate through all tasks counted in the queue.
	int64_t ready_task_cores = 0;
	int64_t ready_task_memory = 0;
	int64_t ready_task_disk = 0;
	int64_t ready_task_gpus = 0;

	int t_idx;
	struct vine_task *t;
	int iter_depth = MIN(q->attempt_schedule_depth, tasks_waiting);
	int sampled_tasks_waiting = 0;
	PRIORITY_QUEUE_BASE_ITERATE(q->ready_tasks, t_idx, t, sampled_tasks_waiting, iter_depth)
	{
		/* unset resources are marked with -1, so we added what we know about currently running tasks */
		ready_task_cores += t->resources_requested->cores > 0 ? t->resources_requested->cores : avg_commited_tasks_cores;
		ready_task_memory += t->resources_requested->memory > 0 ? t->resources_requested->memory : avg_commited_tasks_memory;
		ready_task_disk += t->resources_requested->disk > 0 ? t->resources_requested->disk : avg_commited_tasks_disk;
		ready_task_gpus += t->resources_requested->gpus > 0 ? t->resources_requested->gpus : avg_commited_tasks_gpus;
	}

	int64_t avg_ready_tasks_cores = DIV_INT_ROUND_UP(ready_task_cores, sampled_tasks_waiting);
	int64_t avg_ready_tasks_memory = DIV_INT_ROUND_UP(ready_task_memory, sampled_tasks_waiting);
	int64_t avg_ready_tasks_disk = DIV_INT_ROUND_UP(ready_task_disk, sampled_tasks_waiting);
	int64_t avg_ready_tasks_gpus = DIV_INT_ROUND_UP(ready_task_gpus, sampled_tasks_waiting);

	// since sampled_tasks_waiting > 0 and avg_commited_tasks_cores > 0, then ready_task_cores > 0 and avg_ready_tasks_cores > 0
	tasks_needed = DIV_INT_ROUND_UP(workers_total_avail_cores, avg_ready_tasks_cores);

	if (avg_ready_tasks_memory > 0) {
		tasks_needed = MIN(tasks_needed, DIV_INT_ROUND_UP(workers_total_avail_memory, avg_ready_tasks_memory));
	}

	if (avg_ready_tasks_disk > 0) {
		tasks_needed = MIN(tasks_needed, DIV_INT_ROUND_UP(workers_total_avail_disk, avg_ready_tasks_disk));
	}

	if (avg_ready_tasks_gpus > 0) {
		tasks_needed = MIN(tasks_needed, DIV_INT_ROUND_UP(workers_total_avail_gpus, avg_ready_tasks_gpus));
	}

	tasks_needed = MAX(0, MAX(tasks_needed, hungry_minimum) - tasks_waiting);

	return tasks_needed;
}

/*
 * Finding out the number of tasks needed when the manager is hungry is a potentially
 * expensive operation if there are many workers connected or there already many waiting
 * tasks. However, the number of tasks needed only changes significantly when the number
 * of connected workers changes, and this does not happen very often. Thus we only call
 * the expensive computation every few seconds, and in between these calls we just
 * keep track how many tasks have been added/removed to the ready queue since last
 * time we really checked.
 * */
int vine_hungry(struct vine_manager *q)
{
	if (!q) {
		return 0;
	}

	timestamp_t current_time = timestamp_get();

	if (current_time - q->time_last_hungry + q->hungry_check_interval > 0) {
		q->time_last_hungry = current_time;
		q->tasks_waiting_last_hungry = priority_queue_size(q->ready_tasks);
		q->tasks_to_sate_hungry = vine_hungry_computation(q);
	}

	int change = q->tasks_waiting_last_hungry - priority_queue_size(q->ready_tasks);

	return MAX(0, q->tasks_to_sate_hungry - change);
}

int vine_workers_shutdown(struct vine_manager *q, int n)
{
	struct vine_worker_info *w;
	char *key;
	int i = 0;

	/* by default, remove all workers. */
	if (n < 1)
		n = hash_table_size(q->worker_table);

	if (!q)
		return -1;

	// send worker the "exit" msg
	HASH_TABLE_ITERATE(q->worker_table, key, w)
	{
		if (i >= n)
			break;
		if (itable_size(w->current_tasks) == 0) {
			vine_manager_shut_down_worker(q, w);

			/* vine_manager_shut_down_worker alters the table, so we reset it here. */
			hash_table_firstkey(q->worker_table);
			i++;
		}
	}

	return i;
}

int vine_set_draining_by_hostname(struct vine_manager *q, const char *hostname, int drain_flag)
{
	char *worker_hashkey = NULL;
	struct vine_worker_info *w = NULL;

	drain_flag = !!(drain_flag);

	int workers_updated = 0;

	HASH_TABLE_ITERATE(q->worker_table, worker_hashkey, w)
	{
		if (!strcmp(w->hostname, hostname)) {
			w->draining = drain_flag;
			workers_updated++;
		}
	}

	return workers_updated;
}

int vine_cancel_by_task_id(struct vine_manager *q, int task_id)
{
	struct vine_task *task = itable_lookup(q->tasks, task_id);
	if (!task) {
		debug(D_VINE, "Task with id %d is not found in manager.", task_id);
		return 0;
	}

	reset_task_to_state(q, task, VINE_TASK_RETRIEVED);

	task->result = VINE_RESULT_CANCELLED;
	q->stats->tasks_cancelled++;

	return 1;
}

int vine_cancel_by_task_tag(struct vine_manager *q, const char *task_tag)
{
	if (!task_tag)
		return 0;

	struct vine_task *task = find_task_by_tag(q, task_tag);
	if (task) {
		return vine_cancel_by_task_id(q, task->task_id);
	} else {
		debug(D_VINE, "Task with tag %s is not found in manager.", task_tag);
		return 0;
	}
}

int vine_cancel_all(struct vine_manager *q)
{
	int count = 0;

	struct vine_task *t;
	uint64_t task_id;

	ITABLE_ITERATE(q->tasks, task_id, t)
	{
		vine_cancel_by_task_id(q, task_id);
		count++;
	}

	return count;
}

static void release_all_workers(struct vine_manager *q)
{
	struct vine_worker_info *w;
	char *key;

	if (!q)
		return;

	HASH_TABLE_ITERATE(q->worker_table, key, w)
	{
		release_worker(q, w);
		hash_table_firstkey(q->worker_table);
	}
}

/*
If there are any standard tasks (those submitted by the user)
known to the manager, then the system is not empty, and the caller
should wait some more.
XXX This is a linear-time operation, perhaps there is a more efficient way to do it.
*/

int vine_empty(struct vine_manager *q)
{
	struct vine_task *t;
	uint64_t task_id;

	ITABLE_ITERATE(q->tasks, task_id, t)
	{
		if (t->type == VINE_TASK_TYPE_STANDARD)
			return 0;
	}

	return 1;
}

void vine_set_keepalive_interval(struct vine_manager *q, int interval)
{
	q->keepalive_interval = interval;
}

void vine_set_keepalive_timeout(struct vine_manager *q, int timeout)
{
	q->keepalive_timeout = timeout;
}

void vine_set_manager_preferred_connection(struct vine_manager *q, const char *preferred_connection)
{
	free(q->manager_preferred_connection);
	assert(preferred_connection);

	if (strcmp(preferred_connection, "by_ip") && strcmp(preferred_connection, "by_hostname") && strcmp(preferred_connection, "by_apparent_ip")) {
		fatal("manager_preferred_connection should be one of: by_ip, by_hostname, by_apparent_ip");
	}

	q->manager_preferred_connection = xxstrdup(preferred_connection);
}

int vine_tune(struct vine_manager *q, const char *name, double value)
{

	if (!strcmp(name, "attempt-schedule-depth")) {
		q->attempt_schedule_depth = MAX(1, (int)value);

	} else if (!strcmp(name, "category-steady-n-tasks")) {
		category_tune_bucket_size("category-steady-n-tasks", (int)value);

	} else if (!strcmp(name, "default-transfer-rate")) {
		q->default_transfer_rate = value;

	} else if (!strcmp(name, "disconnect-slow-worker-factor")) {
		vine_enable_disconnect_slow_workers(q, value);

	} else if (!strcmp(name, "hungry-minimum")) {
		q->hungry_minimum = MAX(1, (int)value);

	} else if (!strcmp(name, "hungry-minimum-factor")) {
		q->hungry_minimum_factor = MAX(1, (int)value);

	} else if (!strcmp(name, "immediate-recovery")) {
		q->immediate_recovery = !!((int)value);

	} else if (!strcmp(name, "keepalive-interval")) {
		q->keepalive_interval = MAX(0, (int)value);

	} else if (!strcmp(name, "keepalive-timeout")) {
		q->keepalive_timeout = MAX(0, (int)value);

	} else if (!strcmp(name, "long-timeout")) {
		q->long_timeout = MAX(1, (int)value);

	} else if (!strcmp(name, "max-retrievals")) {
		q->max_retrievals = MAX(-1, (int)value);

	} else if (!strcmp(name, "min-transfer-timeout")) {
		q->minimum_transfer_timeout = (int)value;

	} else if (!strcmp(name, "monitor-interval")) {
		/* 0 means use monitor's default */
		q->monitor_interval = MAX(0, (int)value);

	} else if (!strcmp(name, "prefer-dispatch")) {
		q->prefer_dispatch = !!((int)value);

	} else if (!strcmp(name, "force-proportional-resources") || !strcmp(name, "proportional-resources")) {
		if (value > 0) {
			vine_enable_proportional_resources(q);
		} else {
			vine_disable_proportional_resources(q);
		}

	} else if (!strcmp(name, "force-proportional-resources-whole-tasks") || !strcmp(name, "proportional-whole-tasks")) {
		q->proportional_whole_tasks = MAX(0, (int)value);

	} else if (!strcmp(name, "ramp-down-heuristic")) {
		q->ramp_down_heuristic = MAX(0, (int)value);

	} else if (!strcmp(name, "resource-submit-multiplier") || !strcmp(name, "asynchrony-multiplier")) {
		q->resource_submit_multiplier = MAX(value, 1.0);

	} else if (!strcmp(name, "short-timeout")) {
		q->short_timeout = MAX(1, (int)value);

	} else if (!strcmp(name, "temp-replica-count")) {
		q->temp_replica_count = MAX(1, (int)value);

	} else if (!strcmp(name, "transfer-outlier-factor")) {
		q->transfer_outlier_factor = value;

	} else if (!strcmp(name, "transfer-replica-per-cycle")) {
		q->transfer_replica_per_cycle = MAX(1, (int)value);

	} else if (!strcmp(name, "transfer-temps-recovery")) {
		q->transfer_temps_recovery = !!((int)value);

	} else if (!strcmp(name, "transient-error-interval")) {
		if (value < 1) {
			q->transient_error_interval = VINE_DEFAULT_TRANSIENT_ERROR_INTERVAL;
		} else {
			q->transient_error_interval = value * ONE_SECOND;
		}

	} else if (!strcmp(name, "wait-for-workers")) {
		q->wait_for_workers = MAX(0, (int)value);

	} else if (!strcmp(name, "worker-retrievals")) {
		q->worker_retrievals = MAX(0, (int)value);

	} else if (!strcmp(name, "file-source-max-transfers")) {
		q->file_source_max_transfers = MAX(1, (int)value);

	} else if (!strcmp(name, "worker-source-max-transfers")) {
		q->worker_source_max_transfers = MAX(1, (int)value);

	} else if (!strcmp(name, "load-from-shared-filesystem")) {
		q->load_from_shared_fs_enabled = !!((int)value);

	} else if (!strcmp(name, "perf-log-interval")) {
		q->perf_log_interval = MAX(1, (int)value);

	} else if (!strcmp(name, "update-interval")) {
		q->update_interval = MAX(1, (int)value);

	} else if (!strcmp(name, "resource-management-interval")) {
		q->resource_management_interval = MAX(1, (int)value);

	} else if (!strcmp(name, "max-task-stdout-storage")) {
		q->max_task_stdout_storage = MAX(1, (int)value);

	} else if (!strcmp(name, "max-new-workers")) {
		q->max_new_workers = MAX(0, (int)value); /*todo: confirm 0 or 1*/

	} else if (!strcmp(name, "large-task-check-interval")) {
		q->large_task_check_interval = MAX(1, (timestamp_t)value);

	} else if (!strcmp(name, "option-blocklist-slow-workers-timeout")) {
		q->option_blocklist_slow_workers_timeout = MAX(0, value); /*todo: confirm 0 or 1*/

	} else if (!strcmp(name, "watch-library-logfiles")) {
		q->watch_library_logfiles = !!((int)value);

	} else if (!strcmp(name, "sandbox-grow-factor")) {
		q->sandbox_grow_factor = MAX(1.1, value);

	} else {
		debug(D_NOTICE | D_VINE, "Warning: tuning parameter \"%s\" not recognized\n", name);
		return -1;
	}

	return 0;
}

void vine_manager_enable_process_shortcut(struct vine_manager *q)
{
	q->process_pending_check = 1;
}

struct rmsummary **vine_summarize_workers(struct vine_manager *q)
{
	return vine_manager_summarize_workers(q);
}

void vine_set_bandwidth_limit(struct vine_manager *q, const char *bandwidth)
{
	q->bandwidth_limit = string_metric_parse(bandwidth);
}

double vine_get_effective_bandwidth(struct vine_manager *q)
{
	double manager_bandwidth = get_manager_transfer_rate(q, NULL) / MEGABYTE; // return in MB per second
	return manager_bandwidth;
}

void vine_get_stats(struct vine_manager *q, struct vine_stats *s)
{
	struct vine_stats *qs;
	qs = q->stats;

	memcpy(s, qs, sizeof(*s));

	// info about workers
	s->workers_connected = count_workers(q, VINE_WORKER_TYPE_WORKER);
	s->workers_init = count_workers(q, VINE_WORKER_TYPE_UNKNOWN);
	s->workers_busy = workers_with_tasks(q);
	s->workers_idle = s->workers_connected - s->workers_busy;
	// s->workers_able computed below.

	// info about tasks
	s->tasks_waiting = priority_queue_size(q->ready_tasks);
	s->tasks_with_results = list_size(q->waiting_retrieval_list);
	s->tasks_running = itable_size(q->running_table);
	s->tasks_on_workers = s->tasks_with_results + s->tasks_running;

	vine_task_info_compute_capacity(q, s);

	// info about resources
	s->bandwidth = vine_get_effective_bandwidth(q);
	struct vine_resources rtotal, rmin, rmax;
	int64_t inuse_cache = 0;
	aggregate_workers_resources(q, &rtotal, &rmin, &rmax, &inuse_cache, NULL);

	s->total_cores = rtotal.cores.total;
	s->total_memory = rtotal.memory.total;
	s->total_disk = rtotal.disk.total;
	s->total_gpus = rtotal.gpus.total;

	s->committed_cores = rtotal.cores.inuse;
	s->committed_memory = rtotal.memory.inuse;
	s->committed_disk = rtotal.disk.inuse;
	s->committed_gpus = rtotal.gpus.inuse;

	s->inuse_cache = inuse_cache;

	s->min_cores = rmin.cores.total;
	s->max_cores = rmax.cores.total;
	s->min_memory = rmin.memory.total;
	s->max_memory = rmax.memory.total;
	s->min_disk = rmin.disk.total;
	s->max_disk = rmax.disk.total;
	s->min_gpus = rmin.gpus.total;
	s->max_gpus = rmax.gpus.total;

	s->workers_able = count_workers_for_waiting_tasks(q, largest_seen_resources(q, NULL));
}

void vine_get_stats_category(struct vine_manager *q, const char *category, struct vine_stats *s)
{
	struct category *c = vine_category_lookup_or_create(q, category);
	struct vine_stats *cs = c->vine_stats;
	memcpy(s, cs, sizeof(*s));

	s->workers_able = count_workers_for_waiting_tasks(q, largest_seen_resources(q, c->name));
}

char *vine_get_status(struct vine_manager *q, const char *request)
{
	struct jx *a = construct_status_message(q, request);

	if (!a) {
		return "[]";
	}

	char *result = jx_print_string(a);

	jx_delete(a);

	return result;
}

/*
Sum up all of the resources available at each worker in total,
as well as the minimum and maximum in rmin and rmax respectively.
Used to summarize queue state for vine_get_stats().
*/

static void aggregate_workers_resources(
		struct vine_manager *q, struct vine_resources *total, struct vine_resources *rmin, struct vine_resources *rmax, int64_t *inuse_cache, struct hash_table *features)
{
	struct vine_worker_info *w;
	char *key;
	int first = 1;

	bzero(total, sizeof(*total));
	bzero(rmin, sizeof(*rmin));
	bzero(rmax, sizeof(*rmax));
	*inuse_cache = 0;

	if (hash_table_size(q->worker_table) == 0) {
		return;
	}

	if (features) {
		hash_table_clear(features, 0);
	}

	HASH_TABLE_ITERATE(q->worker_table, key, w)
	{
		struct vine_resources *r = w->resources;

		/* If tag <0 then no resource updates have been received, skip it. */
		if (r->tag < 0)
			continue;

		/* Sum up the total and inuse values in total. */
		vine_resources_add(total, r);

		*inuse_cache += w->inuse_cache;

		/* Add all available features to the features table */
		if (features) {
			if (w->features) {
				char *key;
				void *dummy;
				HASH_TABLE_ITERATE(w->features, key, dummy)
				{
					hash_table_insert(features, key, (void **)1);
				}
			}
		}

		/*
		On the first time through, the min and max get the value of the first worker.
		After that, compute min and max for each value.
		*/

		if (first) {
			*rmin = *r;
			*rmax = *r;
			first = 0;
		} else {
			vine_resources_min(rmin, r);
			vine_resources_max(rmax, r);
		}
	}

	// vine_stats wants MB
	*inuse_cache = (int64_t)ceil(*inuse_cache / (1.0 * MEGA));
}

/* This simple wrapper function allows us to hide the debug.h interface from the end user. */
int vine_enable_debug_log(const char *logfile)
{
	debug_config("vine_manager");
	debug_config_file(logfile);
	debug_flags_set("all");
	return 1;
}

int vine_enable_perf_log(struct vine_manager *q, const char *filename)
{
	char *logpath = vine_get_path_log(q, filename);
	q->perf_logfile = fopen(logpath, "w");
	free(logpath);

	if (q->perf_logfile) {
		vine_perf_log_write_header(q);
		vine_perf_log_write_update(q, 1);
		debug(D_VINE, "log enabled and is being written to %s\n", filename);
		return 1;
	} else {
		debug(D_NOTICE | D_VINE, "couldn't open logfile %s: %s\n", filename, strerror(errno));
		return 0;
	}
}

int vine_enable_transactions_log(struct vine_manager *q, const char *filename)
{
	char *logpath = vine_get_path_log(q, filename);
	q->txn_logfile = fopen(logpath, "w");
	free(logpath);

	if (q->txn_logfile) {
		debug(D_VINE, "transactions log enabled and is being written to %s\n", filename);
		vine_txn_log_write_header(q);
		vine_txn_log_write_manager(q, "START");
		return 1;
	} else {
		debug(D_NOTICE | D_VINE, "couldn't open transactions logfile %s: %s\n", filename, strerror(errno));
		return 0;
	}
}

int vine_enable_taskgraph_log(struct vine_manager *q, const char *filename)
{
	char *logpath = vine_get_path_log(q, filename);
	q->graph_logfile = fopen(logpath, "w");
	free(logpath);

	if (q->graph_logfile) {
		debug(D_VINE, "graph log enabled and is being written to %s\n", filename);
		vine_taskgraph_log_write_header(q);
		return 1;
	} else {
		debug(D_NOTICE | D_VINE, "couldn't open graph logfile %s: %s\n", filename, strerror(errno));
		return 0;
	}
}

void vine_accumulate_task(struct vine_manager *q, struct vine_task *t)
{
	const char *name = t->category ? t->category : "default";
	struct category *c = vine_category_lookup_or_create(q, name);

	struct vine_stats *s = c->vine_stats;

	s->bytes_sent += t->bytes_sent;
	s->bytes_received += t->bytes_received;

	s->time_workers_execute += t->time_workers_execute_last;

	s->time_send += t->time_when_commit_end - t->time_when_commit_start;
	s->time_receive += t->time_when_done - t->time_when_retrieval;

	s->bandwidth = (1.0 * MEGABYTE * (s->bytes_sent + s->bytes_received)) / (s->time_send + s->time_receive + 1);

	q->stats->tasks_done++;

	if (t->result == VINE_RESULT_SUCCESS) {
		q->stats->time_workers_execute_good += t->time_workers_execute_last;
		q->stats->time_send_good += t->time_when_commit_end - t->time_when_commit_end;
		q->stats->time_receive_good += t->time_when_done - t->time_when_retrieval;

		s->tasks_done++;
		s->time_workers_execute_good += t->time_workers_execute_last;
		s->time_send_good += t->time_when_commit_end - t->time_when_commit_end;
		s->time_receive_good += t->time_when_done - t->time_when_retrieval;
	} else {
		s->tasks_failed++;

		if (t->result == VINE_RESULT_RESOURCE_EXHAUSTION) {
			s->time_workers_execute_exhaustion += t->time_workers_execute_last;

			q->stats->time_workers_execute_exhaustion += t->time_workers_execute_last;
			q->stats->tasks_exhausted_attempts++;

			t->time_workers_execute_exhaustion += t->time_workers_execute_last;
			t->exhausted_attempts++;
		}
	}

	/* accumulate resource summary to category only if task result makes it meaningful. */
	switch (t->result) {
	case VINE_RESULT_SUCCESS:
	case VINE_RESULT_SIGNAL:
	case VINE_RESULT_RESOURCE_EXHAUSTION:
	case VINE_RESULT_MAX_WALL_TIME:
	case VINE_RESULT_OUTPUT_TRANSFER_ERROR:
	case VINE_RESULT_SANDBOX_EXHAUSTION:
		if (category_accumulate_summary(c, t->resources_measured, q->current_max_worker)) {
			vine_txn_log_write_category(q, c);
		}

		// if in bucketing mode, add resources measured to bucketing manager
		if (category_in_bucketing_mode(c)) {
			int success; // 1 if success, 0 if resource exhaustion, -1 otherwise
			if (t->result == VINE_RESULT_SUCCESS)
				success = 1;
			else if (t->result == VINE_RESULT_RESOURCE_EXHAUSTION)
				success = 0;
			else
				success = -1;
			if (success != -1)
				bucketing_manager_add_resource_report(c->bucketing_manager, t->task_id, t->resources_measured, success);
		}
		break;
	case VINE_RESULT_INPUT_MISSING:
	case VINE_RESULT_OUTPUT_MISSING:
	case VINE_RESULT_FIXED_LOCATION_MISSING:
	case VINE_RESULT_CANCELLED:
	case VINE_RESULT_RMONITOR_ERROR:
	case VINE_RESULT_STDOUT_MISSING:
	case VINE_RESULT_MAX_END_TIME:
	case VINE_RESULT_UNKNOWN:
	case VINE_RESULT_FORSAKEN:
	case VINE_RESULT_MAX_RETRIES:
	case VINE_RESULT_LIBRARY_EXIT:
		break;
	}
}

void vine_initialize_categories(struct vine_manager *q, struct rmsummary *max, const char *summaries_file)
{
	categories_initialize(q->categories, max, summaries_file);
}

void vine_set_resources_max(struct vine_manager *q, const struct rmsummary *rm)
{
	vine_set_category_resources_max(q, "default", rm);
}

void vine_set_resources_min(struct vine_manager *q, const struct rmsummary *rm)
{
	vine_set_category_resources_min(q, "default", rm);
}

void vine_set_category_resources_max(struct vine_manager *q, const char *category, const struct rmsummary *rm)
{
	struct category *c = vine_category_lookup_or_create(q, category);
	category_specify_max_allocation(c, rm);
}

void vine_set_category_resources_min(struct vine_manager *q, const char *category, const struct rmsummary *rm)
{
	struct category *c = vine_category_lookup_or_create(q, category);
	category_specify_min_allocation(c, rm);
}

void vine_set_category_first_allocation_guess(struct vine_manager *q, const char *category, const struct rmsummary *rm)
{
	struct category *c = vine_category_lookup_or_create(q, category);
	category_specify_first_allocation_guess(c, rm);
}

int vine_set_category_mode(struct vine_manager *q, const char *category, vine_category_mode_t mode)
{

	switch (mode) {
	case CATEGORY_ALLOCATION_MODE_FIXED:
	case CATEGORY_ALLOCATION_MODE_MAX:
	case CATEGORY_ALLOCATION_MODE_MIN_WASTE:
	case CATEGORY_ALLOCATION_MODE_MAX_THROUGHPUT:
	case CATEGORY_ALLOCATION_MODE_GREEDY_BUCKETING:
	case CATEGORY_ALLOCATION_MODE_EXHAUSTIVE_BUCKETING:
		break;
	default:
		notice(D_VINE, "Unknown category mode specified.");
		return 0;
		break;
	}

	if (!category) {
		q->allocation_default_mode = mode;
	} else {
		struct category *c = vine_category_lookup_or_create(q, category);
		category_specify_allocation_mode(c, (category_mode_t)mode);
		vine_txn_log_write_category(q, c);
	}

	return 1;
}

void vine_set_category_max_concurrent(struct vine_manager *m, const char *category, int max_concurrent)
{
	struct category *c = vine_category_lookup_or_create(m, category);

	c->max_concurrent = MAX(-1, max_concurrent);
}

int vine_enable_category_resource(struct vine_manager *q, const char *category, const char *resource, int autolabel)
{

	struct category *c = vine_category_lookup_or_create(q, category);

	return category_enable_auto_resource(c, resource, autolabel);
}

const struct rmsummary *vine_manager_task_resources_max(struct vine_manager *q, struct vine_task *t)
{
	struct category *c = vine_category_lookup_or_create(q, t->category);

	return category_task_max_resources(c, t->resources_requested, t->resource_request, t->task_id);
}

const struct rmsummary *vine_manager_task_resources_min(struct vine_manager *q, struct vine_task *t)
{
	struct category *c = vine_category_lookup_or_create(q, t->category);

	const struct rmsummary *s = category_task_min_resources(c, t->resources_requested, t->resource_request, t->task_id);

	if (t->resource_request != CATEGORY_ALLOCATION_FIRST || !q->current_max_worker) {
		return s;
	}

	// If this task is being tried for the first time, we take the minimum as
	// the minimum between what we have observed and the largest worker. This
	// is to eliminate observed outliers that would prevent new tasks to run.
	if ((q->current_max_worker->cores > 0 && q->current_max_worker->cores < s->cores) || (q->current_max_worker->memory > 0 && q->current_max_worker->memory < s->memory) ||
			(q->current_max_worker->disk > 0 && q->current_max_worker->disk < s->disk) || (q->current_max_worker->gpus > 0 && q->current_max_worker->gpus < s->gpus)) {

		struct rmsummary *r = rmsummary_create(-1);

		rmsummary_merge_override_basic(r, q->current_max_worker);
		rmsummary_merge_override_basic(r, t->resources_requested);

		s = category_task_min_resources(c, r, t->resource_request, t->task_id);
		rmsummary_delete(r);
	}

	return s;
}

struct category *vine_category_lookup_or_create(struct vine_manager *q, const char *name)
{
	struct category *c = category_lookup_or_create(q->categories, name);

	if (!c->vine_stats) {
		c->vine_stats = calloc(1, sizeof(struct vine_stats));
		category_specify_allocation_mode(c, (category_mode_t)q->allocation_default_mode);
	}

	return c;
}

int vine_set_task_id_min(struct vine_manager *q, int minid)
{

	if (minid > q->next_task_id) {
		q->next_task_id = minid;
	}

	return q->next_task_id;
}

/* File functions */

/*
Remove all replicas of a special file across the compute cluster.

While invoking outside, it is primarily used to remove replicas
from workers when the file is no longer needed by the manager.
*/
void vine_prune_file(struct vine_manager *m, struct vine_file *f)
{
	if (!f) {
		return;
	}

	if (!m) {
		return;
	}

	const char *filename = f->cached_name;
	/*
	If this is not a file that should be cached forever,
	delete all of the replicas present at remote workers.
	*/
	if (f->cache_level < VINE_CACHE_LEVEL_FOREVER) {
		char *key;
		struct vine_worker_info *w;
		HASH_TABLE_ITERATE(m->worker_table, key, w)
		{
			if (vine_file_replica_table_lookup(w, filename)) {
				delete_worker_file(m, w, filename, 0, 0);
			}
		}
	}

	/*
	Pruned files do not need to be scheduled for replication anymore.
	*/
	hash_table_remove(m->temp_files_to_replicate, f->cached_name);
}

/*
Careful: The semantics of undeclare_file are a little subtle.
The user calls this function to indicate that they are done
using a particular file, and there will be no more tasks
that can consume it.

This causes the file to be removed from the manager's table,
the replicas in the cluster to be deleted.
There should be no running tasks that require the file after this.

However, there may be *returned* tasks that still hold
references to the vine_file object, and so it will not be
fully garbage collected until those also call vine_file_delete
to bring the reference count to zero.  At that point, if
the UNLINK_WHEN_DONE flag is on, the local state will also be deleted.
*/

void vine_undeclare_file(struct vine_manager *m, struct vine_file *f)
{
	if (!f) {
		return;
	}

	/*
	Special case: If the manager has already been gc'ed
	(e.g. by python exiting), do nothing. Any memory or unlink_when_done files were gc'ed by vine_delete.
	*/
	if (!m) {
		return;
	}

	/* First prune the file on all workers */
	vine_prune_file(m, f);

	/* Then, remove the object from our table and delete a reference. */
	if (hash_table_lookup(m->file_table, f->cached_name)) {
		hash_table_remove(m->file_table, f->cached_name);
		vine_file_delete(f);
	}

	/*
	Note that the file object may still exist if the user
	still holds pointers to inactive tasks that refer to
	this file. But the object is no longer the manager's
	responsibility.
	*/
}

struct vine_file *vine_manager_lookup_file(struct vine_manager *m, const char *cached_name)
{
	return hash_table_lookup(m->file_table, cached_name);
}

struct vine_file *vine_manager_declare_file(struct vine_manager *m, struct vine_file *f)
{
	if (!f) {
		return NULL;
	}
	assert(f->cached_name);
	struct vine_file *previous = vine_manager_lookup_file(m, f->cached_name);

	if (previous) {
		/* If declared before, use the previous instance. */
		vine_file_delete(f);
		f = vine_file_addref(previous);
	} else {
		/* Otherwise add it to the table. */
		hash_table_insert(m->file_table, f->cached_name, f);
	}

	vine_taskgraph_log_write_file(m, f);

	return f;
}

struct vine_file *vine_declare_file(struct vine_manager *m, const char *source, vine_cache_level_t cache, vine_file_flags_t flags)
{
	struct vine_file *f;

	if (m->load_from_shared_fs_enabled) {
		char *file_url = vine_file_make_file_url(source);
		f = vine_file_url(file_url, cache, flags);
		free(file_url);

	} else {
		f = vine_file_local(source, cache, flags);
	}

	return vine_manager_declare_file(m, f);
}

struct vine_file *vine_declare_url(struct vine_manager *m, const char *source, vine_cache_level_t cache, vine_file_flags_t flags)
{
	struct vine_file *f = vine_file_url(source, cache, flags);
	return vine_manager_declare_file(m, f);
}

struct vine_file *vine_declare_temp(struct vine_manager *m)
{
	if (m->peer_transfers_enabled) {
		struct vine_file *f = vine_file_temp();
		return vine_manager_declare_file(m, f);
	} else {
		struct vine_file *f = vine_file_temp_no_peers();
		return vine_manager_declare_file(m, f);
	}
}

struct vine_file *vine_declare_buffer(struct vine_manager *m, const char *buffer, size_t size, vine_cache_level_t cache, vine_file_flags_t flags)
{
	struct vine_file *f = vine_file_buffer(buffer, size, cache, flags);
	return vine_manager_declare_file(m, f);
}

struct vine_file *vine_declare_mini_task(struct vine_manager *m, struct vine_task *t, const char *name, vine_cache_level_t cache, vine_file_flags_t flags)
{
	struct vine_file *f = vine_file_mini_task(t, name, cache, flags);
	return vine_manager_declare_file(m, f);
}

struct vine_file *vine_declare_untar(struct vine_manager *m, struct vine_file *f, vine_cache_level_t cache, vine_file_flags_t flags)
{
	struct vine_file *t = vine_file_untar(f, cache, flags);
	return vine_manager_declare_file(m, t);
}

struct vine_file *vine_declare_poncho(struct vine_manager *m, struct vine_file *f, vine_cache_level_t cache, vine_file_flags_t flags)
{
	struct vine_file *t = vine_file_poncho(f, cache, flags);
	return vine_manager_declare_file(m, t);
}

struct vine_file *vine_declare_starch(struct vine_manager *m, struct vine_file *f, vine_cache_level_t cache, vine_file_flags_t flags)
{
	struct vine_file *t = vine_file_starch(f, cache, flags);
	return vine_manager_declare_file(m, t);
}

struct vine_file *vine_declare_xrootd(struct vine_manager *m, const char *source, struct vine_file *proxy, struct vine_file *env, vine_cache_level_t cache, vine_file_flags_t flags)
{
	struct vine_file *t = vine_file_xrootd(source, proxy, env, cache, flags);
	return vine_manager_declare_file(m, t);
}

struct vine_file *vine_declare_chirp(
		struct vine_manager *m, const char *server, const char *source, struct vine_file *ticket, struct vine_file *env, vine_cache_level_t cache, vine_file_flags_t flags)
{
	struct vine_file *t = vine_file_chirp(server, source, ticket, env, cache, flags);
	return vine_manager_declare_file(m, t);
}

const char *vine_fetch_file(struct vine_manager *m, struct vine_file *f)
{
	/* If the data has already been loaded, just return it. */
	if (f->data)
		return f->data;

	switch (f->type) {
	case VINE_FILE:
		/* If it is on the local filesystem, load it. */
		{
			size_t length;
			if (copy_file_to_buffer(f->source, &f->data, &length)) {
				return f->data;
			} else {
				return 0;
			}
		}
		break;
	case VINE_BUFFER:
		/* Buffer files will already have their contents in memory, if available. */
		return f->data;
		break;
	case VINE_TEMP:
	case VINE_URL:
	case VINE_MINI_TASK:
		/* If the file has been materialized remotely, go get it from a worker. */
		{
			struct vine_worker_info *w = vine_file_replica_table_find_worker(m, f->cached_name);
			if (w)
				vine_manager_get_single_file(m, w, f);
			/* If that succeeded, then f->data is now set, null otherwise. */
			return f->data;
		}
		break;
	}

	return 0;
}

void vine_log_debug_app(struct vine_manager *m, const char *entry)
{
	debug(D_VINE, "APPLICATION %s", entry);
}

void vine_log_txn_app(struct vine_manager *m, const char *entry)
{
	vine_txn_log_write_app_entry(m, entry);
}

char *vine_version_string()
{
	return cctools_version_string();
}

/* vim: set noexpandtab tabstop=8: */
