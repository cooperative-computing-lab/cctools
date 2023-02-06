/*
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "work_queue.h"
#include "work_queue_protocol.h"
#include "work_queue_internal.h"
#include "work_queue_resources.h"

#include "cctools.h"
#include "int_sizes.h"
#include "link.h"
#include "link_auth.h"
#include "debug.h"
#include "stringtools.h"
#include "catalog_query.h"
#include "datagram.h"
#include "domain_name_cache.h"
#include "hash_table.h"
#include "interfaces_address.h"
#include "itable.h"
#include "list.h"
#include "macros.h"
#include "username.h"
#include "create_dir.h"
#include "xxmalloc.h"
#include "load_average.h"
#include "buffer.h"
#include "rmonitor.h"
#include "rmonitor_types.h"
#include "rmonitor_poll.h"
#include "category_internal.h"
#include "copy_stream.h"
#include "random.h"
#include "process.h"
#include "path.h"
#include "md5.h"
#include "url_encode.h"
#include "jx_print.h"
#include "jx_parse.h"
#include "shell.h"
#include "pattern.h"
#include "tlq_config.h"
#include "host_disk_info.h"

#include <unistd.h>
#include <dirent.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <assert.h>
#include <errno.h>
#include <limits.h>
#include <math.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

// The default tasks capacity reported before information is available.
// Default capacity also implies 1 core, 1024 MB of disk and 512 memory per task.
#define WORK_QUEUE_DEFAULT_CAPACITY_TASKS 10

// The minimum number of task reports to keep
#define WORK_QUEUE_TASK_REPORT_MIN_SIZE 50

// Seconds between updates to the catalog
#define WORK_QUEUE_UPDATE_INTERVAL 60

// Seconds between measurement of manager local resources
#define WORK_QUEUE_RESOURCE_MEASUREMENT_INTERVAL 30

#define WORKER_ADDRPORT_MAX 64
#define WORKER_HASHKEY_MAX 32

#define RESOURCE_MONITOR_TASK_LOCAL_NAME "wq-%d-task-%d"
#define RESOURCE_MONITOR_REMOTE_NAME "cctools-monitor"
#define RESOURCE_MONITOR_REMOTE_NAME_EVENTS RESOURCE_MONITOR_REMOTE_NAME "events.json"

#define MAX_TASK_STDOUT_STORAGE (1*GIGABYTE)

#define MAX_NEW_WORKERS 10

// Result codes for signaling the completion of operations in WQ
typedef enum {
	WQ_SUCCESS = 0,
	WQ_WORKER_FAILURE,
	WQ_APP_FAILURE,
	WQ_MGR_FAILURE
} work_queue_result_code_t;

typedef enum {
	MSG_PROCESSED = 0,        /* Message was processed and connection is still good. */
	MSG_PROCESSED_DISCONNECT, /* Message was processed and disconnect now expected. */
	MSG_NOT_PROCESSED,        /* Message was not processed, waiting to be consumed. */
	MSG_FAILURE               /* Message not received, connection failure. */
} work_queue_msg_code_t;

typedef enum {
	MON_DISABLED = 0,
	MON_SUMMARY  = 1,   /* generate only summary. */
	MON_FULL     = 2,   /* generate summary, series and monitoring debug output. */
	MON_WATCHDOG = 4    /* kill tasks that exhaust resources */
} work_queue_monitoring_mode;

typedef enum {
	WORKER_DISCONNECT_UNKNOWN  = 0,
	WORKER_DISCONNECT_EXPLICIT,
	WORKER_DISCONNECT_STATUS_WORKER,
	WORKER_DISCONNECT_IDLE_OUT,
	WORKER_DISCONNECT_FAST_ABORT,
	WORKER_DISCONNECT_FAILURE
} worker_disconnect_reason;

typedef enum {
	WORKER_TYPE_UNKNOWN = 1,
	WORKER_TYPE_WORKER  = 2,
	WORKER_TYPE_STATUS  = 4,
	WORKER_TYPE_FOREMAN = 8
} worker_type;


typedef enum {
	CORES_BIT = (1 << 0),
	MEMORY_BIT = (1 << 1),
	DISK_BIT = (1 << 2),
	GPUS_BIT = (1 << 3),
} resource_bitmask;

// Threshold for available disk space (MB) beyond which files are not received from worker.
static uint64_t disk_avail_threshold = 100;

int wq_option_scheduler = WORK_QUEUE_SCHEDULE_TIME;

/* default timeout for slow workers to come back to the pool */
double wq_option_blocklist_slow_workers_timeout = 900;

/* Internal use: when the worker uses the client library, do not recompute cached names. */
int wq_hack_do_not_compute_cached_name = 0;

/* time threshold to check when tasks are larger than connected workers */
static timestamp_t interval_check_for_large_tasks = 180000000; // 3 minutes in usecs

struct work_queue {
	char *name;
	int port;
	int priority;
	int num_tasks_left;

	int next_taskid;

	char workingdir[PATH_MAX];

	struct link      *manager_link;   // incoming tcp connection for workers.
	struct link_info *poll_table;
	int poll_table_size;

	struct itable *tasks;           // taskid -> task
	struct itable *task_state_map;  // taskid -> state
	struct list   *ready_list;      // ready to be sent to a worker

	struct hash_table *worker_table;
	struct hash_table *worker_blocklist;
	struct itable  *worker_task_map;

	struct hash_table *factory_table;

	struct hash_table *categories;

	struct hash_table *workers_with_available_results;

	struct work_queue_stats *stats;
	struct work_queue_stats *stats_measure;
	struct work_queue_stats *stats_disconnected_workers;
	timestamp_t time_last_wait;
	timestamp_t time_last_log_stats;
	timestamp_t time_last_large_tasks_check;
	int worker_selection_algorithm;
	int task_ordering;
	int process_pending_check;

	int short_timeout;		// timeout to send/recv a brief message from worker
	int long_timeout;		// timeout to send/recv a brief message from a foreman

	struct list *task_reports;	      /* list of last N work_queue_task_reports. */

	double resource_submit_multiplier; /* Times the resource value, but disk */

	int minimum_transfer_timeout;
	int foreman_transfer_timeout;
	int transfer_outlier_factor;
	int default_transfer_rate;

	char *catalog_hosts;

	time_t catalog_last_update_time;
	time_t resources_last_update_time;
	int    busy_waiting_flag;

	int hungry_minimum;               /* minimum number of waiting tasks to consider queue not hungry. */;

	int wait_for_workers;             /* wait for these many workers before dispatching tasks at start of execution. */

	work_queue_category_mode_t allocation_default_mode;

	FILE *logfile;
	FILE *transactions_logfile;
	int keepalive_interval;
	int keepalive_timeout;
	timestamp_t link_poll_end;	//tracks when we poll link; used to timeout unacknowledged keepalive checks

    char *manager_preferred_connection;

	int monitor_mode;
	FILE *monitor_file;

	char *monitor_output_directory;
	char *monitor_summary_filename;

	char *monitor_exe;
	struct rmsummary *measured_local_resources;
	struct rmsummary *current_max_worker;
	struct rmsummary *max_task_resources_requested;

	char *password;
	char *ssl_key;
	char *ssl_cert;
	int ssl_enabled;

	double bandwidth;

	char *debug_path;
	int tlq_port;
	char *tlq_url;

	int fetch_factory;

	int wait_retrieve_many;
	int proportional_resources;
	int proportional_whole_tasks;
};

struct work_queue_worker {
	char *hostname;
	char *os;
	char *arch;
	char *version;
	char *factory_name;
	char addrport[WORKER_ADDRPORT_MAX];
	char hashkey[WORKER_HASHKEY_MAX];

	worker_type type;                         // unknown, regular worker, status worker, foreman

	int  draining;                            // if 1, worker does not accept anymore tasks. It is shutdown if no task running.

	int  fast_abort_alarm;                    // if 1, no task has finished since a task triggered fast abort.
	                                          // 0 otherwise. A 2nd task triggering fast abort will cause the worker to disconnect

	struct work_queue_stats     *stats;
	struct work_queue_resources *resources;
	struct work_queue_resources *coprocess_resources;
	struct hash_table           *features;

	char *workerid;

	struct hash_table *current_files;
	struct link *link;
	struct itable *current_tasks;
	struct itable *current_tasks_boxes;
	int finished_tasks;
	int64_t total_tasks_complete;
	int64_t total_bytes_transferred;
	timestamp_t total_task_time;
	timestamp_t total_transfer_time;
	timestamp_t start_time;
	timestamp_t last_msg_recv_time;
	timestamp_t last_update_msg_time;
	int64_t end_time;                   // epoch time (in seconds) at which the worker terminates
										// If -1, means the worker has not reported in. If 0, means no limit.
};

struct work_queue_factory_info {
	char *name;
	int   connected_workers;
	int   max_workers;
	int   seen_at_catalog;
};

struct work_queue_task_report {
	timestamp_t transfer_time;
	timestamp_t exec_time;
	timestamp_t manager_time;

	struct rmsummary *resources;
};

struct blocklist_host_info {
	int    blocked;
	int    times_blocked;
	time_t release_at;
};

struct remote_file_info {
	work_queue_file_t type;
	int64_t           size;
	time_t            mtime;
	timestamp_t       transfer_time;
};

struct remote_file_info * remote_file_info_create( work_queue_file_t type, int64_t size, time_t mtime )
{
	struct remote_file_info *rinfo = malloc(sizeof(*rinfo));
	rinfo->type = type;
	rinfo->size = size;
	rinfo->mtime = mtime;
	rinfo->transfer_time = 0;
	return rinfo;
}

void remote_file_info_delete( struct remote_file_info *rinfo )
{
	free(rinfo);
}

static void handle_failure(struct work_queue *q, struct work_queue_worker *w, struct work_queue_task *t, work_queue_result_code_t fail_type);
static void handle_worker_failure(struct work_queue *q, struct work_queue_worker *w);
static void handle_app_failure(struct work_queue *q, struct work_queue_worker *w, struct work_queue_task *t);
static void remove_worker(struct work_queue *q, struct work_queue_worker *w, worker_disconnect_reason reason);
static int shut_down_worker(struct work_queue *q, struct work_queue_worker *w);

static void add_task_report(struct work_queue *q, struct work_queue_task *t );

static void commit_task_to_worker(struct work_queue *q, struct work_queue_worker *w, struct work_queue_task *t);
static void reap_task_from_worker(struct work_queue *q, struct work_queue_worker *w, struct work_queue_task *t, work_queue_task_state_t new_state);
static int cancel_task_on_worker(struct work_queue *q, struct work_queue_task *t, work_queue_task_state_t new_state);
static void count_worker_resources(struct work_queue *q, struct work_queue_worker *w);

static void find_max_worker(struct work_queue *q);
static void update_max_worker(struct work_queue *q, struct work_queue_worker *w);

static void push_task_to_ready_list( struct work_queue *q, struct work_queue_task *t );

/* returns old state */
static work_queue_task_state_t change_task_state( struct work_queue *q, struct work_queue_task *t, work_queue_task_state_t new_state);

const char *task_state_str(work_queue_task_state_t state);

/* 1, 0 whether t is in state */
static int task_state_is( struct work_queue *q, uint64_t taskid, work_queue_task_state_t state);
/* pointer to first task found with state. NULL if no such task */
static struct work_queue_task *task_state_any(struct work_queue *q, work_queue_task_state_t state);
/* number of tasks with state */
static int task_state_count( struct work_queue *q, const char *category, work_queue_task_state_t state);
/* number of tasks with the resource allocation request */
static int task_request_count( struct work_queue *q, const char *category, category_allocation_t request);

static work_queue_result_code_t get_result(struct work_queue *q, struct work_queue_worker *w, const char *line);
static work_queue_result_code_t get_available_results(struct work_queue *q, struct work_queue_worker *w);

static int update_task_result(struct work_queue_task *t, work_queue_result_t new_result);

static void process_data_index( struct work_queue *q, struct work_queue_worker *w, time_t stoptime );
static work_queue_msg_code_t process_http_request( struct work_queue *q, struct work_queue_worker *w, const char *path, time_t stoptime );
static work_queue_msg_code_t process_workqueue(struct work_queue *q, struct work_queue_worker *w, const char *line);
static work_queue_msg_code_t process_queue_status(struct work_queue *q, struct work_queue_worker *w, const char *line, time_t stoptime);
static work_queue_msg_code_t process_resource(struct work_queue *q, struct work_queue_worker *w, const char *line);
static work_queue_msg_code_t process_feature(struct work_queue *q, struct work_queue_worker *w, const char *line);

static struct jx * queue_to_jx( struct work_queue *q, struct link *foreman_uplink );
static struct jx * queue_lean_to_jx( struct work_queue *q, struct link *foreman_uplink );

char *work_queue_monitor_wrap(struct work_queue *q, struct work_queue_worker *w, struct work_queue_task *t, struct rmsummary *limits);

const struct rmsummary *task_max_resources(struct work_queue *q, struct work_queue_task *t);
const struct rmsummary *task_min_resources(struct work_queue *q, struct work_queue_task *t);
static struct rmsummary *task_worker_box_size(struct work_queue *q, struct work_queue_worker *w, struct work_queue_task *t);

void work_queue_accumulate_task(struct work_queue *q, struct work_queue_task *t);
struct category *work_queue_category_lookup_or_create(struct work_queue *q, const char *name);

static void write_transaction(struct work_queue *q, const char *str);
static void write_transaction_task(struct work_queue *q, struct work_queue_task *t);
static void write_transaction_category(struct work_queue *q, struct category *c);
static void write_transaction_worker(struct work_queue *q, struct work_queue_worker *w, int leaving, worker_disconnect_reason reason_leaving);
static void write_transaction_transfer(struct work_queue *q, struct work_queue_worker *w, struct work_queue_task *t, struct work_queue_file *f, size_t size_in_bytes, int time_in_usecs, work_queue_file_type_t type);
static void write_transaction_worker_resources(struct work_queue *q, struct work_queue_worker *w);

static void delete_feature(struct work_queue_task *t, const char *name);

static struct work_queue_factory_info *create_factory_info(struct work_queue *q, const char *name);
static void remove_factory_info(struct work_queue *q, const char *name);

static void fill_deprecated_queue_stats(struct work_queue *q, struct work_queue_stats *s);
static void fill_deprecated_tasks_stats(struct work_queue_task *t);

/** Clone a @ref work_queue_file
This performs a deep copy of the file struct.
@param file The file to clone.
@return A newly allocated file.
*/
static struct work_queue_file *work_queue_file_clone(const struct work_queue_file *file);

/** Clone a list of @ref work_queue_file structs
This performs a deep copy of the file list.
@param list The list to clone.
@return A newly allocated list of files.
*/
static struct list *work_queue_task_file_list_clone(struct list *list);

/** Write manager's resources to resource summary file and close the file **/
void work_queue_disable_monitoring(struct work_queue *q);

/******************************************************/
/********** work_queue internal functions *************/
/******************************************************/

static int64_t overcommitted_resource_total(struct work_queue *q, int64_t total) {
	int64_t r = 0;
	if(total != 0)
	{
		r = ceil(total * q->resource_submit_multiplier);
	}

	return r;
}

//Returns count of workers according to type
static int count_workers(struct work_queue *q, int type) {
	struct work_queue_worker *w;
	char* id;

	int count = 0;

	hash_table_firstkey(q->worker_table);
	while(hash_table_nextkey(q->worker_table, &id, (void**)&w)) {
		if(w->type & type) {
			count++;
		}
	}

	return count;
}

//Returns count of workers that are available to run tasks.
static int available_workers(struct work_queue *q) {
	struct work_queue_worker *w;
	char* id;
	int available_workers = 0;

	hash_table_firstkey(q->worker_table);
	while(hash_table_nextkey(q->worker_table, &id, (void**)&w)) {
		if(strcmp(w->hostname, "unknown") != 0) {
			if(overcommitted_resource_total(q, w->resources->cores.total) > w->resources->cores.inuse || w->resources->disk.total > w->resources->disk.inuse || overcommitted_resource_total(q, w->resources->memory.total) > w->resources->memory.inuse){
				available_workers++;
			}
		}
	}

	return available_workers;
}

//Returns count of workers that are running at least 1 task.
static int workers_with_tasks(struct work_queue *q) {
	struct work_queue_worker *w;
	char* id;
	int workers_with_tasks = 0;

	hash_table_firstkey(q->worker_table);
	while(hash_table_nextkey(q->worker_table, &id, (void**)&w)) {
		if(strcmp(w->hostname, "unknown")){
			if(itable_size(w->current_tasks)){
				workers_with_tasks++;
			}
		}
	}

	return workers_with_tasks;
}

static void log_queue_stats(struct work_queue *q, int force)
{
	struct work_queue_stats s;

	timestamp_t now = timestamp_get();
	if(!force && (now - q->time_last_log_stats < ONE_SECOND)) {
		return;
	}

	work_queue_get_stats(q, &s);
	debug(D_WQ, "workers connections -- known: %d, connecting: %d, available: %d.",
			s.workers_connected,
			s.workers_init,
			available_workers(q));

	q->time_last_log_stats = now;
	if(!q->logfile) {
		return;
	}

	buffer_t B;
	buffer_init(&B);

	buffer_printf(&B, "%" PRIu64, timestamp_get());

	/* Stats for the current state of workers: */
	buffer_printf(&B, " %d", s.workers_connected);
	buffer_printf(&B, " %d", s.workers_init);
	buffer_printf(&B, " %d", s.workers_idle);
	buffer_printf(&B, " %d", s.workers_busy);
	buffer_printf(&B, " %d", s.workers_able);

	/* Cumulative stats for workers: */
	buffer_printf(&B, " %d", s.workers_joined);
	buffer_printf(&B, " %d", s.workers_removed);
	buffer_printf(&B, " %d", s.workers_released);
	buffer_printf(&B, " %d", s.workers_idled_out);
	buffer_printf(&B, " %d", s.workers_blocked);
	buffer_printf(&B, " %d", s.workers_fast_aborted);
	buffer_printf(&B, " %d", s.workers_lost);

	/* Stats for the current state of tasks: */
	buffer_printf(&B, " %d", s.tasks_waiting);
	buffer_printf(&B, " %d", s.tasks_on_workers);
	buffer_printf(&B, " %d", s.tasks_running);
	buffer_printf(&B, " %d", s.tasks_with_results);

	/* Cumulative stats for tasks: */
	buffer_printf(&B, " %d", s.tasks_submitted);
	buffer_printf(&B, " %d", s.tasks_dispatched);
	buffer_printf(&B, " %d", s.tasks_done);
	buffer_printf(&B, " %d", s.tasks_failed);
	buffer_printf(&B, " %d", s.tasks_cancelled);
	buffer_printf(&B, " %d", s.tasks_exhausted_attempts);

	/* Master time statistics: */
	buffer_printf(&B, " %" PRId64, s.time_send);
	buffer_printf(&B, " %" PRId64, s.time_receive);
	buffer_printf(&B, " %" PRId64, s.time_send_good);
	buffer_printf(&B, " %" PRId64, s.time_receive_good);
	buffer_printf(&B, " %" PRId64, s.time_status_msgs);
	buffer_printf(&B, " %" PRId64, s.time_internal);
	buffer_printf(&B, " %" PRId64, s.time_polling);
	buffer_printf(&B, " %" PRId64, s.time_application);

	/* Workers time statistics: */
	buffer_printf(&B, " %" PRId64, s.time_workers_execute);
	buffer_printf(&B, " %" PRId64, s.time_workers_execute_good);
	buffer_printf(&B, " %" PRId64, s.time_workers_execute_exhaustion);

	/* BW statistics */
	buffer_printf(&B, " %" PRId64, s.bytes_sent);
	buffer_printf(&B, " %" PRId64, s.bytes_received);
	buffer_printf(&B, " %f", s.bandwidth);

	/* resources statistics */
	buffer_printf(&B, " %d", s.capacity_tasks);
	buffer_printf(&B, " %d", s.capacity_cores);
	buffer_printf(&B, " %d", s.capacity_memory);
	buffer_printf(&B, " %d", s.capacity_disk);
	buffer_printf(&B, " %d", s.capacity_instantaneous);
	buffer_printf(&B, " %d", s.capacity_weighted);
	buffer_printf(&B, " %f", s.manager_load);

	buffer_printf(&B, " %" PRId64, s.total_cores);
	buffer_printf(&B, " %" PRId64, s.total_memory);
	buffer_printf(&B, " %" PRId64, s.total_disk);

	buffer_printf(&B, " %" PRId64, s.committed_cores);
	buffer_printf(&B, " %" PRId64, s.committed_memory);
	buffer_printf(&B, " %" PRId64, s.committed_disk);

	buffer_printf(&B, " %" PRId64, s.max_cores);
	buffer_printf(&B, " %" PRId64, s.max_memory);
	buffer_printf(&B, " %" PRId64, s.max_disk);

	buffer_printf(&B, " %" PRId64, s.min_cores);
	buffer_printf(&B, " %" PRId64, s.min_memory);
	buffer_printf(&B, " %" PRId64, s.min_disk);

	fprintf(q->logfile, "%s\n", buffer_tostring(&B));

	buffer_free(&B);
}

static void link_to_hash_key(struct link *link, char *key)
{
	sprintf(key, "0x%p", link);
}

/**
 * This function sends a message to the worker and records the time the message is
 * successfully sent. This timestamp is used to determine when to send keepalive checks.
 */
__attribute__ (( format(printf,3,4) ))
static int send_worker_msg( struct work_queue *q, struct work_queue_worker *w, const char *fmt, ... )
{
	va_list va;
	time_t stoptime;
	buffer_t B[1];
	buffer_init(B);
	buffer_abortonfailure(B, 1);
	buffer_max(B, WORK_QUEUE_LINE_MAX);

	va_start(va, fmt);
	buffer_putvfstring(B, fmt, va);
	va_end(va);

	debug(D_WQ, "tx to %s (%s): %s", w->hostname, w->addrport, buffer_tostring(B));

	//If foreman, then we wait until foreman gives the manager some attention.
	if(w->type == WORKER_TYPE_FOREMAN)
		stoptime = time(0) + q->long_timeout;
	else
		stoptime = time(0) + q->short_timeout;

	int result = link_putlstring(w->link, buffer_tostring(B), buffer_pos(B), stoptime);

	buffer_free(B);

	return result;
}

void work_queue_broadcast_message(struct work_queue *q, const char *msg) {
	if(!q)
		return;

	struct work_queue_worker *w;
	char* id;

	hash_table_firstkey(q->worker_table);
	while(hash_table_nextkey(q->worker_table, &id, (void**)&w)) {
		send_worker_msg(q, w, "%s", msg);
	}
}

work_queue_msg_code_t process_name(struct work_queue *q, struct work_queue_worker *w, char *line)
{
	debug(D_WQ, "Sending project name to worker (%s)", w->addrport);

	//send project name (q->name) if there is one. otherwise send blank line
	send_worker_msg(q, w, "%s\n", q->name ? q->name : "");

	return MSG_PROCESSED;
}

work_queue_msg_code_t advertise_tlq_url(struct work_queue *q, struct work_queue_worker *w, char *line)
{
	//attempt to find local TLQ server to retrieve manager URL
	if(q->tlq_port && q->debug_path && !q->tlq_url) {
		debug(D_TLQ, "looking up manager TLQ URL");
		time_t config_stoptime = time(0) + 10;
		q->tlq_url = tlq_config_url(q->tlq_port, q->debug_path, config_stoptime);
		if(q->tlq_url) debug(D_TLQ, "set manager TLQ URL: %s", q->tlq_url);
		else debug(D_TLQ, "error setting manager TLQ URL");
	}
	else if(q->tlq_port && !q->debug_path && !q->tlq_url) debug(D_TLQ, "cannot get manager TLQ URL: no debug log path set");

	char worker_url[WORK_QUEUE_LINE_MAX];
	int n = sscanf(line, "tlq %s", worker_url);
	if(n != 1) debug(D_TLQ, "empty TLQ URL received from worker (%s)", w->addrport);
	else debug(D_TLQ, "received worker (%s) TLQ URL %s", w->addrport, worker_url);

	//send manager TLQ URL if there is one
	if(q->tlq_url) {
		debug(D_TLQ, "sending manager TLQ URL to worker (%s)", w->addrport);
		send_worker_msg(q, w, "tlq %s\n", q->tlq_url);
	}
	return MSG_PROCESSED;
}

work_queue_msg_code_t process_info(struct work_queue *q, struct work_queue_worker *w, char *line)
{
	char field[WORK_QUEUE_LINE_MAX];
	char value[WORK_QUEUE_LINE_MAX];

	int n = sscanf(line,"info %s %[^\n]", field, value);

	if(n != 2)
		return MSG_FAILURE;

	if(string_prefix_is(field, "workers_joined")) {
		w->stats->workers_joined = atoll(value);
	} else if(string_prefix_is(field, "workers_removed")) {
		w->stats->workers_removed = atoll(value);
	} else if(string_prefix_is(field, "time_send")) {
		w->stats->time_send = atoll(value);
	} else if(string_prefix_is(field, "time_receive")) {
		w->stats->time_receive = atoll(value);
	} else if(string_prefix_is(field, "time_execute")) {
		w->stats->time_workers_execute = atoll(value);
	} else if(string_prefix_is(field, "bytes_sent")) {
		w->stats->bytes_sent = atoll(value);
	} else if(string_prefix_is(field, "bytes_received")) {
		w->stats->bytes_received = atoll(value);
	} else if(string_prefix_is(field, "tasks_waiting")) {
		w->stats->tasks_waiting = atoll(value);
	} else if(string_prefix_is(field, "tasks_running")) {
		w->stats->tasks_running = atoll(value);
	} else if(string_prefix_is(field, "idle-disconnecting")) {
		remove_worker(q, w, WORKER_DISCONNECT_IDLE_OUT);
		q->stats->workers_idled_out++;
	} else if(string_prefix_is(field, "end_of_resource_update")) {
		count_worker_resources(q, w);
		write_transaction_worker_resources(q, w);
	} else if(string_prefix_is(field, "worker-id")) {
		free(w->workerid);
		w->workerid = xxstrdup(value);
		write_transaction_worker(q, w, 0, 0);
	} else if(string_prefix_is(field, "worker-end-time")) {
		w->end_time = MAX(0, atoll(value));
	} else if(string_prefix_is(field, "from-factory")) {
		q->fetch_factory = 1;
		w->factory_name = xxstrdup(value);
		struct work_queue_factory_info *f;
		if ( (f = hash_table_lookup(q->factory_table, w->factory_name)) ) {
			if (f->connected_workers + 1 > f->max_workers) {
				shut_down_worker(q, w);
			} else {
				f->connected_workers++;
			}
		} else {
			f = create_factory_info(q, w->factory_name);
			f->connected_workers++;
		}
	}

	//Note we always mark info messages as processed, as they are optional.
	return MSG_PROCESSED;
}

/*
A cache-update message coming from the worker means that a requested
remote transfer or command was successful, and know we know the size
of the file for the purposes of cache storage management.
*/

int process_cache_update( struct work_queue *q, struct work_queue_worker *w, const char *line )
{
	char cachename[WORK_QUEUE_LINE_MAX];
	long long size;
	long long transfer_time;
	
	if(sscanf(line,"cache-update %s %lld %lld",cachename,&size,&transfer_time)==3) {
		struct remote_file_info *remote_info = hash_table_lookup(w->current_files,cachename);
		if(remote_info) {
			remote_info->size = size;
			remote_info->transfer_time = transfer_time;
		}
	}
	
	return MSG_PROCESSED;
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

int process_cache_invalid( struct work_queue *q, struct work_queue_worker *w, const char *line )
{
	char cachename[WORK_QUEUE_LINE_MAX];
	int length;
	if(sscanf(line,"cache-invalid %s %d",cachename,&length)==2) {

		char *message = malloc(length+1);
		time_t stoptime = time(0) + q->long_timeout;
		
		int actual = link_read(w->link,message,length,stoptime);
		if(actual!=length) {
			free(message);
			return MSG_FAILURE;
		}
		
		message[length] = 0;
		debug(D_WQ,"%s (%s) invalidated %s with error: %s",w->hostname,w->addrport,cachename,message);
		free(message);
		
		struct remote_file_info *remote_info = hash_table_remove(w->current_files,cachename);
		if(remote_info) remote_file_info_delete(remote_info);
	}
	return MSG_PROCESSED;
}

/**
 * This function receives a message from worker and records the time a message is successfully
 * received. This timestamp is used in keepalive timeout computations.
 */
static work_queue_msg_code_t recv_worker_msg(struct work_queue *q, struct work_queue_worker *w, char *line, size_t length )
{
	time_t stoptime;
	//If foreman, then we wait until foreman gives the manager some attention.
	if(w->type == WORKER_TYPE_FOREMAN)
		stoptime = time(0) + q->long_timeout;
	else
		stoptime = time(0) + q->short_timeout;

	int result = link_readline(w->link, line, length, stoptime);

	if (result <= 0) {
		return MSG_FAILURE;
	}

	w->last_msg_recv_time = timestamp_get();

	debug(D_WQ, "rx from %s (%s): %s", w->hostname, w->addrport, line);

	char path[length];

	// Check for status updates that can be consumed here.
	if(string_prefix_is(line, "alive")) {
		result = MSG_PROCESSED;
	} else if(string_prefix_is(line, "workqueue")) {
		result = process_workqueue(q, w, line);
	} else if (string_prefix_is(line,"queue_status") || string_prefix_is(line, "worker_status") || string_prefix_is(line, "task_status") || string_prefix_is(line, "wable_status") || string_prefix_is(line, "resources_status")) {
		result = process_queue_status(q, w, line, stoptime);
	} else if (string_prefix_is(line, "available_results")) {
		hash_table_insert(q->workers_with_available_results, w->hashkey, w);
		result = MSG_PROCESSED;
	} else if (string_prefix_is(line, "resource")) {
		result = process_resource(q, w, line);
	} else if (string_prefix_is(line, "feature")) {
		result = process_feature(q, w, line);
	} else if (string_prefix_is(line, "auth")) {
		debug(D_WQ|D_NOTICE,"worker (%s) is attempting to use a password, but I do not have one.",w->addrport);
		result = MSG_FAILURE;
	} else if (string_prefix_is(line,"ready")) {
		debug(D_WQ|D_NOTICE,"worker (%s) is an older worker that is not compatible with this manager.",w->addrport);
		result = MSG_FAILURE;
	} else if (string_prefix_is(line, "name")) {
		result = process_name(q, w, line);
	} else if (string_prefix_is(line, "info")) {
		result = process_info(q, w, line);
	} else if (string_prefix_is(line, "tlq")) {
		result = advertise_tlq_url(q, w, line);
	} else if (string_prefix_is(line, "cache-update")) {
		result = process_cache_update(q, w, line);
	} else if (string_prefix_is(line, "cache-invalid")) {
		result = process_cache_invalid(q, w, line);
	} else if( sscanf(line,"GET %s HTTP/%*d.%*d",path)==1) {
	        result = process_http_request(q,w,path,stoptime);
	} else {
		// Message is not a status update: return it to the user.
		result = MSG_NOT_PROCESSED;
	}

	return result;
}


/*
Call recv_worker_msg and silently retry if the result indicates
an asynchronous update message like 'keepalive' or 'resource'.
*/

work_queue_msg_code_t recv_worker_msg_retry( struct work_queue *q, struct work_queue_worker *w, char *line, int length )
{
	work_queue_msg_code_t result = MSG_PROCESSED;

	do {
		result = recv_worker_msg(q, w,line,length);
	} while(result == MSG_PROCESSED);

	return result;
}

static double get_queue_transfer_rate(struct work_queue *q, char **data_source)
{
	double queue_transfer_rate; // bytes per second
	int64_t     q_total_bytes_transferred = q->stats->bytes_sent + q->stats->bytes_received;
	timestamp_t q_total_transfer_time     = q->stats->time_send  + q->stats->time_receive;

	// Note q_total_transfer_time is timestamp_t with units of microseconds.
	if(q_total_transfer_time>1000000) {
		queue_transfer_rate = 1000000.0 * q_total_bytes_transferred / q_total_transfer_time;
		if (data_source) {
			*data_source = xxstrdup("overall queue");
		}
	} else {
		queue_transfer_rate = q->default_transfer_rate;
		if (data_source) {
			*data_source = xxstrdup("conservative default");
		}
	}

	return queue_transfer_rate;
}

/*
Select an appropriate timeout value for the transfer of a certain number of bytes.
We do not know in advance how fast the system will perform.

So do this by starting with an assumption of bandwidth taken from the worker,
from the queue, or from a (slow) default number, depending on what information is available.
The timeout is chosen to be a multiple of the expected transfer time from the assumed bandwidth.

The overall effect is to reject transfers that are 10x slower than what has been seen before.

Two exceptions are made:
- The transfer time cannot be below a configurable minimum time.
- A foreman must have a high minimum, because its attention is divided
  between the manager and the workers that it serves.
*/

static int get_transfer_wait_time(struct work_queue *q, struct work_queue_worker *w, struct work_queue_task *t, int64_t length)
{
	double avg_transfer_rate; // bytes per second
	char *data_source;

	if(w->total_transfer_time>1000000) {
		// Note w->total_transfer_time is timestamp_t with units of microseconds.
		avg_transfer_rate = 1000000 * w->total_bytes_transferred / w->total_transfer_time;
		data_source = xxstrdup("worker's observed");
	} else {
		avg_transfer_rate = get_queue_transfer_rate(q, &data_source);
	}

	double tolerable_transfer_rate = avg_transfer_rate / q->transfer_outlier_factor; // bytes per second

	int timeout = length / tolerable_transfer_rate;

	if(w->type == WORKER_TYPE_FOREMAN) {
		// A foreman must have a much larger minimum timeout, b/c it does not respond immediately to the manager.
		timeout = MAX(q->foreman_transfer_timeout,timeout);
	} else {
		// An ordinary manager has a lower minimum timeout b/c it responds immediately to the manager.
		timeout = MAX(q->minimum_transfer_timeout,timeout);
	}

	/* Don't bother printing anything for transfers of less than 1MB, to avoid excessive output. */

	if( length >= 1048576 ) {
		debug(D_WQ,"%s (%s) using %s average transfer rate of %.2lf MB/s\n", w->hostname, w->addrport, data_source, avg_transfer_rate/MEGABYTE);

		debug(D_WQ, "%s (%s) will try up to %d seconds to transfer this %.2lf MB file.", w->hostname, w->addrport, timeout, length/1000000.0);
	}

	free(data_source);
	return timeout;
}

static int factory_trim_workers(struct work_queue *q, struct work_queue_factory_info *f)
{
	if (!f) return 0;
	assert(f->name);

	// Iterate through all workers and shut idle ones down
	struct work_queue_worker *w;
	char *key;
	int trimmed_workers = 0;

	struct hash_table *idle_workers = hash_table_create(0, 0);
	hash_table_firstkey(q->worker_table);
	while ( f->connected_workers - trimmed_workers > f->max_workers &&
			hash_table_nextkey(q->worker_table, &key, (void **) &w) ) {
		if ( w->factory_name &&
				!strcmp(f->name, w->factory_name) &&
				itable_size(w->current_tasks) < 1 ) {
			hash_table_insert(idle_workers, key, w);
			trimmed_workers++;
		}
	}

	hash_table_firstkey(idle_workers);
	while (hash_table_nextkey(idle_workers, &key, (void **) &w)) {
		hash_table_remove(idle_workers, key);
		hash_table_firstkey(idle_workers);
		shut_down_worker(q, w);
	}
	hash_table_delete(idle_workers);

	debug(D_WQ, "Trimmed %d workers from %s", trimmed_workers, f->name);
	return trimmed_workers;
}

static struct work_queue_factory_info *create_factory_info(struct work_queue *q, const char *name)
{
	struct work_queue_factory_info *f;
	if ( (f = hash_table_lookup(q->factory_table, name)) ) return f;

	f = malloc(sizeof(*f));
	f->name = xxstrdup(name);
	f->connected_workers = 0;
	f->max_workers = INT_MAX;
	f->seen_at_catalog = 0;
	hash_table_insert(q->factory_table, name, f);
	return f;
}

static void remove_factory_info(struct work_queue *q, const char *name)
{
	struct work_queue_factory_info *f = hash_table_lookup(q->factory_table, name);
	if (f) {
		free(f->name);
		free(f);
		hash_table_remove(q->factory_table, name);
	} else {
		debug(D_WQ, "Failed to remove unrecorded factory %s", name);
	}
}

static void update_factory(struct work_queue *q, struct jx *j)
{
	const char *name = jx_lookup_string(j, "factory_name");
	if (!name) return;
	struct work_queue_factory_info *f = hash_table_lookup(q->factory_table, name);
	if (!f) {
		debug(D_WQ, "factory %s not recorded", name);
		return;
	}

	f->seen_at_catalog = 1;
	int found = 0;
	struct jx *m = jx_lookup_guard(j, "max_workers", &found);
	if (found) {
		int old_max_workers = f->max_workers;
		f->max_workers = m->u.integer_value;
		// Trim workers if max_workers reduced.
		if (f->max_workers < old_max_workers) {
			factory_trim_workers(q, f);
		}
	}
}

void update_read_catalog_factory(struct work_queue *q, time_t stoptime) {
	struct catalog_query *cq;
	struct jx *jexpr = NULL;
	struct jx *j;

	// Iterate through factory_table to create a query filter.
	int first_name = 1;
	buffer_t filter;
	buffer_init(&filter);
	char *factory_name = NULL;
	struct work_queue_factory_info *f = NULL;
	buffer_putfstring(&filter, "type == \"wq_factory\" && (");

	hash_table_firstkey(q->factory_table);
	while ( hash_table_nextkey(q->factory_table, &factory_name, (void **)&f) ) {
		buffer_putfstring(&filter, "%sfactory_name == \"%s\"", first_name ? "" : " || ", factory_name);
		first_name = 0;
		f->seen_at_catalog = 0;
	}
	buffer_putfstring(&filter, ")");
	jexpr = jx_parse_string(buffer_tolstring(&filter, NULL));
	buffer_free(&filter);

	// Query the catalog server
	debug(D_WQ, "Retrieving factory info from catalog server(s) at %s ...", q->catalog_hosts);
	if ( (cq = catalog_query_create(q->catalog_hosts, jexpr, stoptime)) ) {
		// Update the table
		while((j = catalog_query_read(cq, stoptime))) {
			update_factory(q, j);
			jx_delete(j);
		}
		catalog_query_delete(cq);
	} else {
		debug(D_WQ, "Failed to retrieve factory info from catalog server(s) at %s.", q->catalog_hosts);
	}

	// Remove outdated factories
	struct list *outdated_factories = list_create();
	hash_table_firstkey(q->factory_table);
	while ( hash_table_nextkey(q->factory_table, &factory_name, (void **) &f) ) {
		if ( !f->seen_at_catalog && f->connected_workers < 1 ) {
			list_push_tail(outdated_factories, f);
		}
	}
	while ( list_size(outdated_factories) ) {
		f = list_pop_head(outdated_factories);
		remove_factory_info(q, f->name);
	}
	list_delete(outdated_factories);
}

void update_write_catalog(struct work_queue *q, struct link *foreman_uplink)
{
	// Only write if we have a name.
	if (!q->name) return; 

	// Generate the manager status in an jx, and print it to a buffer.
	struct jx *j = queue_to_jx(q,foreman_uplink);
	char *str = jx_print_string(j);

	// Send the buffer.
	debug(D_WQ, "Advertising manager status to the catalog server(s) at %s ...", q->catalog_hosts);
	if(!catalog_query_send_update_conditional(q->catalog_hosts, str)) {

		// If the send failed b/c the buffer is too big, send the lean version instead.
		struct jx *lj = queue_lean_to_jx(q,foreman_uplink);
		char *lstr = jx_print_string(lj);
		catalog_query_send_update(q->catalog_hosts,lstr);
		free(lstr);
		jx_delete(lj);
	}

	// Clean up.
	free(str);
	jx_delete(j);
}

void update_read_catalog(struct work_queue *q)
{
	time_t stoptime = time(0) + 5; // Short timeout for query

	if (q->fetch_factory) {
		update_read_catalog_factory(q, stoptime);
	}
}

void update_catalog(struct work_queue *q, struct link *foreman_uplink, int force_update )
{
	// Only update every last_update_time seconds.
	if(!force_update && (time(0) - q->catalog_last_update_time) < WORK_QUEUE_UPDATE_INTERVAL)
		return;

	// If host and port are not set, pick defaults.
	if(!q->catalog_hosts) q->catalog_hosts = xxstrdup(CATALOG_HOST);

	// Update the catalog.
	update_write_catalog(q, foreman_uplink);
	update_read_catalog(q);

	q->catalog_last_update_time = time(0);
}

static void clean_task_state(struct work_queue_task *t, int full_clean) {

		t->time_when_commit_start = 0;
		t->time_when_commit_end   = 0;
		t->time_when_retrieval    = 0;

		t->time_workers_execute_last = 0;

		t->bytes_sent = 0;
		t->bytes_received = 0;
		t->bytes_transferred = 0;

		t->disk_allocation_exhausted = 0;

		free(t->output);
		t->output = NULL;

		free(t->hostname);
		t->hostname = NULL;

		free(t->host);
		t->host = NULL;

		t->return_status = -1;
		t->result = WORK_QUEUE_RESULT_UNKNOWN;

		if(full_clean) {
			t->resource_request = CATEGORY_ALLOCATION_FIRST;
			t->try_count = 0;
			t->exhausted_attempts = 0;
			t->fast_abort_count = 0;

			t->time_workers_execute_all = 0;
			t->time_workers_execute_exhaustion = 0;
			t->time_workers_execute_failure = 0;

			rmsummary_delete(t->resources_measured);
			rmsummary_delete(t->resources_allocated);
			t->resources_measured  = rmsummary_create(-1);
			t->resources_allocated = rmsummary_create(-1);
		}

		fill_deprecated_tasks_stats(t);

		/* If result is never updated, then it is mark as a failure. */
		t->result = WORK_QUEUE_RESULT_UNKNOWN;
}

static void cleanup_worker(struct work_queue *q, struct work_queue_worker *w)
{
	char *key, *value;
	struct work_queue_task *t;
	struct rmsummary *r;
	uint64_t taskid;

	if(!q || !w) return;

	hash_table_firstkey(w->current_files);
	while(hash_table_nextkey(w->current_files, &key, (void **) &value)) {
		hash_table_remove(w->current_files, key);
		free(value);
		hash_table_firstkey(w->current_files);
	}

	itable_firstkey(w->current_tasks);
	while(itable_nextkey(w->current_tasks, &taskid, (void **)&t)) {
		if (t->time_when_commit_end >= t->time_when_commit_start) {
			timestamp_t delta_time = timestamp_get() - t->time_when_commit_end;
			t->time_workers_execute_failure += delta_time;
			t->time_workers_execute_all     += delta_time;
		}

		clean_task_state(t, 0);
		reap_task_from_worker(q, w, t, WORK_QUEUE_TASK_READY);

		itable_firstkey(w->current_tasks);
	}

	itable_firstkey(w->current_tasks_boxes);
	while(itable_nextkey(w->current_tasks_boxes, &taskid, (void **) &r)) {
		rmsummary_delete(r);
	}

	itable_clear(w->current_tasks);
	itable_clear(w->current_tasks_boxes);
	w->finished_tasks = 0;
}

#define accumulate_stat(qs, ws, field) (qs)->field += (ws)->field

static void record_removed_worker_stats(struct work_queue *q, struct work_queue_worker *w)
{
	struct work_queue_stats *qs = q->stats_disconnected_workers;
	struct work_queue_stats *ws = w->stats;

	accumulate_stat(qs, ws, workers_joined);
	accumulate_stat(qs, ws, workers_removed);
	accumulate_stat(qs, ws, workers_released);
	accumulate_stat(qs, ws, workers_idled_out);
	accumulate_stat(qs, ws, workers_fast_aborted);
	accumulate_stat(qs, ws, workers_blocked);
	accumulate_stat(qs, ws, workers_lost);

	accumulate_stat(qs, ws, time_send);
	accumulate_stat(qs, ws, time_receive);
	accumulate_stat(qs, ws, time_workers_execute);

	accumulate_stat(qs, ws, bytes_sent);
	accumulate_stat(qs, ws, bytes_received);

	//Count all the workers joined as removed.
	qs->workers_removed = ws->workers_joined;
}

static void remove_worker(struct work_queue *q, struct work_queue_worker *w, worker_disconnect_reason reason)
{
	if(!q || !w) return;

	debug(D_WQ, "worker %s (%s) removed", w->hostname, w->addrport);

	if(w->type == WORKER_TYPE_WORKER || w->type == WORKER_TYPE_FOREMAN) {
		q->stats->workers_removed++;
	}

	write_transaction_worker(q, w, 1, reason);

	cleanup_worker(q, w);

	hash_table_remove(q->worker_table, w->hashkey);
	hash_table_remove(q->workers_with_available_results, w->hashkey);

	record_removed_worker_stats(q, w);

	if(w->link)
		link_close(w->link);

	itable_delete(w->current_tasks);
	itable_delete(w->current_tasks_boxes);
	hash_table_delete(w->current_files);
	work_queue_resources_delete(w->resources);
	work_queue_resources_delete(w->coprocess_resources);
	free(w->workerid);

	if(w->features)
		hash_table_delete(w->features);

	if (w->factory_name) {
		struct work_queue_factory_info *f;
		if ( (f = hash_table_lookup(q->factory_table, w->factory_name)) ) {
			f->connected_workers--;
		}
	}

	free(w->stats);
	free(w->hostname);
	free(w->os);
	free(w->arch);
	free(w->version);
	free(w->factory_name);
	free(w);

	/* update the largest worker seen */
	find_max_worker(q);

	debug(D_WQ, "%d workers connected in total now", count_workers(q, WORKER_TYPE_WORKER | WORKER_TYPE_FOREMAN));
}

static int release_worker(struct work_queue *q, struct work_queue_worker *w)
{
	if(!w) return 0;


	send_worker_msg(q,w,"release\n");

	remove_worker(q, w, WORKER_DISCONNECT_EXPLICIT);

	q->stats->workers_released++;

	return 1;
}

static void add_worker(struct work_queue *q)
{
	struct link *link;
	struct work_queue_worker *w;
	char addr[LINK_ADDRESS_MAX];
	int port;

	link = link_accept(q->manager_link, time(0) + q->short_timeout);
	if(!link) {
		return;
	}

	link_keepalive(link, 1);
	link_tune(link, LINK_TUNE_INTERACTIVE);

	if(!link_address_remote(link, addr, &port)) {
		link_close(link);
		return;
	}

	debug(D_WQ,"worker %s:%d connected",addr,port);

	if(q->ssl_enabled) {
		if(link_ssl_wrap_accept(link,q->ssl_key,q->ssl_cert)) {
			debug(D_WQ,"worker %s:%d completed ssl connection",addr,port);
		} else {
			debug(D_WQ,"worker %s:%d failed ssl connection",addr,port);
			link_close(link);
			return;
		}
	} else {
		/* nothing to do */
	}

	if(q->password) {
		debug(D_WQ,"worker %s:%d authenticating",addr,port);
		if(!link_auth_password(link,q->password,time(0)+q->short_timeout)) {
			debug(D_WQ|D_NOTICE,"worker %s:%d presented the wrong password",addr,port);
			link_close(link);
			return;
		}
	}

	w = malloc(sizeof(*w));
	if(!w) {
		debug(D_NOTICE, "Cannot allocate memory for worker %s:%d.", addr, port);
		link_close(link);
		return;
	}

	memset(w, 0, sizeof(*w));
	w->hostname = strdup("unknown");
	w->os = strdup("unknown");
	w->arch = strdup("unknown");
	w->version = strdup("unknown");
	w->type = WORKER_TYPE_UNKNOWN;
	w->draining = 0;
	w->link = link;
	w->current_files = hash_table_create(0, 0);
	w->current_tasks = itable_create(0);
	w->current_tasks_boxes = itable_create(0);
	w->finished_tasks = 0;
	w->start_time = timestamp_get();
	w->end_time = -1;

	w->last_update_msg_time = w->start_time;

	w->resources = work_queue_resources_create();
	w->coprocess_resources = work_queue_resources_create();

	w->workerid = NULL;

	w->stats     = calloc(1, sizeof(struct work_queue_stats));
	link_to_hash_key(link, w->hashkey);
	sprintf(w->addrport, "%s:%d", addr, port);
	hash_table_insert(q->worker_table, w->hashkey, w);

	return;
}

/*
Get a single file from a remote worker.
*/
static work_queue_result_code_t get_file( struct work_queue *q, struct work_queue_worker *w, struct work_queue_task *t, const char *local_name, int64_t length, int64_t * total_bytes)
{
	// If a bandwidth limit is in effect, choose the effective stoptime.
	timestamp_t effective_stoptime = 0;
	if(q->bandwidth) {
		effective_stoptime = (length/q->bandwidth)*1000000 + timestamp_get();
	}

	// Choose the actual stoptime.
	time_t stoptime = time(0) + get_transfer_wait_time(q, w, t, length);

	// If necessary, create parent directories of the file.
	char dirname[WORK_QUEUE_LINE_MAX];
	path_dirname(local_name,dirname);
	if(strchr(local_name,'/')) {
		if(!create_dir(dirname, 0777)) {
			debug(D_WQ, "Could not create directory - %s (%s)", dirname, strerror(errno));
			link_soak(w->link, length, stoptime);
			return WQ_MGR_FAILURE;
		}
	}

	// Create the local file.
	debug(D_WQ, "Receiving file %s (size: %"PRId64" bytes) from %s (%s) ...", local_name, length, w->addrport, w->hostname);
	// Check if there is space for incoming file at manager
	if(!check_disk_space_for_filesize(dirname, length, disk_avail_threshold)) {
		debug(D_WQ, "Could not receive file %s, not enough disk space (%"PRId64" bytes needed)\n", local_name, length);
		return WQ_MGR_FAILURE;
	}

	int fd = open(local_name, O_WRONLY | O_TRUNC | O_CREAT, 0777);
	if(fd < 0) {
		debug(D_NOTICE, "Cannot open file %s for writing: %s", local_name, strerror(errno));
		link_soak(w->link, length, stoptime);
		return WQ_MGR_FAILURE;
	}

	// Write the data on the link to file.
	int64_t actual = link_stream_to_fd(w->link, fd, length, stoptime);

	if(close(fd) < 0) {
		warn(D_WQ, "Could not write file %s: %s\n", local_name, strerror(errno));
		unlink(local_name);
		return WQ_MGR_FAILURE;
	}

	if(actual != length) {
		debug(D_WQ, "Received item size (%"PRId64") does not match the expected size - %"PRId64" bytes.", actual, length);
		unlink(local_name);
		return WQ_WORKER_FAILURE;
	}

	*total_bytes += length;

	// If the transfer was too fast, slow things down.
	timestamp_t current_time = timestamp_get();
	if(effective_stoptime && effective_stoptime > current_time) {
		usleep(effective_stoptime - current_time);
	}

	return WQ_SUCCESS;
}

/*
This function implements the recursive get protocol.
The manager sends a single get message, then the worker
responds with a continuous stream of dir and file message
that indicate the entire contents of the directory.
This makes it efficient to move deep directory hierarchies with
high throughput and low latency.
*/
static work_queue_result_code_t get_file_or_directory( struct work_queue *q, struct work_queue_worker *w, struct work_queue_task *t, const char *remote_name, const char *local_name, int64_t * total_bytes)
{
	// Remember the length of the specified remote path so it can be chopped from the result.
	int remote_name_len = strlen(remote_name);

	// Send the name of the file/dir name to fetch
	debug(D_WQ, "%s (%s) sending back %s to %s", w->hostname, w->addrport, remote_name, local_name);
	send_worker_msg(q,w, "get %s 1\n",remote_name);

	work_queue_result_code_t result = WQ_SUCCESS; //return success unless something fails below

	char *tmp_remote_path = NULL;
	char *length_str      = NULL;
	char *errnum_str      = NULL;

	// Process the recursive file/dir responses as they are sent.
	while(1) {
		char line[WORK_QUEUE_LINE_MAX];

		free(tmp_remote_path);
		free(length_str);

		tmp_remote_path = NULL;
		length_str      = NULL;

		work_queue_msg_code_t mcode;
		mcode = recv_worker_msg_retry(q, w, line, sizeof(line));
		if(mcode!=MSG_NOT_PROCESSED) {
			result = WQ_WORKER_FAILURE;
			break;
		}

		if(pattern_match(line, "^dir (%S+) (%d+)$", &tmp_remote_path, &length_str) >= 0) {
			char *tmp_local_name = string_format("%s%s",local_name, (tmp_remote_path + remote_name_len));
			int result_dir = create_dir(tmp_local_name,0777);
			if(!result_dir) {
				debug(D_WQ, "Could not create directory - %s (%s)", tmp_local_name, strerror(errno));
				result = WQ_APP_FAILURE;
				free(tmp_local_name);
				break;
			}
			free(tmp_local_name);
		} else if(pattern_match(line, "^file (.+) (%d+)$", &tmp_remote_path, &length_str) >= 0) {
			int64_t length = strtoll(length_str, NULL, 10);
			char *tmp_local_name = string_format("%s%s",local_name, (tmp_remote_path + remote_name_len));
			result = get_file(q,w,t,tmp_local_name,length,total_bytes);
			free(tmp_local_name);
			//Return if worker failure. Else wait for end message from worker.
			if((result == WQ_WORKER_FAILURE) || (result == WQ_MGR_FAILURE)) {
				break;
			}
		} else if(pattern_match(line, "^missing (.+) (%d+)$", &tmp_remote_path, &errnum_str) >= 0) {
			// If the output file is missing, we make a note of that in the task result,
			// but we continue and consider the transfer a 'success' so that other
			// outputs are transferred and the task is given back to the caller.
			int errnum = atoi(errnum_str);
			debug(D_WQ, "%s (%s): could not access requested file %s (%s)",w->hostname,w->addrport,remote_name,strerror(errnum));
			update_task_result(t, WORK_QUEUE_RESULT_OUTPUT_MISSING);
		} else if(!strcmp(line,"end")) {
			// We have to return on receiving an end message.
			if (result == WQ_SUCCESS) {
				return result;
			} else {
				break;
			}
		} else {
			debug(D_WQ, "%s (%s): sent invalid response to get: %s",w->hostname,w->addrport,line);
			result = WQ_WORKER_FAILURE; //signal sys-level failure
			break;
		}
	}

	free(tmp_remote_path);
	free(length_str);

	// If we failed to *transfer* the output file, then that is a hard
	// failure which causes this function to return failure and the task
	// to be returned to the queue to be attempted elsewhere.
	debug(D_WQ, "%s (%s) failed to return output %s to %s", w->addrport, w->hostname, remote_name, local_name);
	if(result == WQ_APP_FAILURE) {
		update_task_result(t, WORK_QUEUE_RESULT_OUTPUT_MISSING);
	} else if(result == WQ_MGR_FAILURE) {
		update_task_result(t, WORK_QUEUE_RESULT_OUTPUT_TRANSFER_ERROR);
	}

	return result;
}

/*
For a given task and file, generate the name under which the file
should be stored in the remote cache directory.

The basic strategy is to construct a name that is unique to the
namespace from where the file is drawn, so that tasks sharing
the same input file can share the same copy.

In the common case of files, the cached name is based on the
hash of the local path, with the basename of the local path
included simply to assist with debugging.

In each of the other file types, a similar approach is taken,
including a hash and a name where one is known, or another
unique identifier where no name is available.
*/

char *make_cached_name( const struct work_queue_file *f )
{
	static unsigned int file_count = 0;
	file_count++;

	/* Default of payload is remote name (needed only for directories) */
	char *payload = f->payload ? f->payload : f->remote_name;

	unsigned char digest[MD5_DIGEST_LENGTH];
	char payload_enc[PATH_MAX];

	if(f->type == WORK_QUEUE_BUFFER) {
		//dummy digest for buffers
		md5_buffer("buffer", 6, digest);
	} else {
		md5_buffer(payload,strlen(payload),digest);
		url_encode(path_basename(payload), payload_enc, PATH_MAX);
	}

	/* 0 for cache files, file_count for non-cache files. With this, non-cache
	 * files cannot be shared among tasks, and can be safely deleted once a
	 * task finishes. */
	unsigned int cache_file_id = 0;
	if(!(f->flags & WORK_QUEUE_CACHE)) {
		cache_file_id = file_count;
	}

	switch(f->type) {
		case WORK_QUEUE_FILE:
		case WORK_QUEUE_DIRECTORY:
			return string_format("file-%d-%s-%s", cache_file_id, md5_string(digest), payload_enc);
			break;
		case WORK_QUEUE_FILE_PIECE:
			return string_format("piece-%d-%s-%s-%lld-%lld",cache_file_id, md5_string(digest),payload_enc,(long long)f->offset,(long long)f->piece_length);
			break;
		case WORK_QUEUE_REMOTECMD:
			return string_format("cmd-%d-%s", cache_file_id, md5_string(digest));
			break;
		case WORK_QUEUE_URL:
			return string_format("url-%d-%s", cache_file_id, md5_string(digest));
			break;
		case WORK_QUEUE_BUFFER:
		default:
			return string_format("buffer-%d-%s", cache_file_id, md5_string(digest));
			break;
	}
}

/*
Get a single output file, located at the worker under 'cached_name'.
*/
static work_queue_result_code_t get_output_file( struct work_queue *q, struct work_queue_worker *w, struct work_queue_task *t, struct work_queue_file *f )
{
	int64_t total_bytes = 0;
	work_queue_result_code_t result = WQ_SUCCESS; //return success unless something fails below.

	timestamp_t open_time = timestamp_get();

	result = get_file_or_directory(q, w, t, f->cached_name, f->payload, &total_bytes);

	timestamp_t close_time = timestamp_get();
	timestamp_t sum_time = close_time - open_time;

	if(total_bytes>0) {
		q->stats->bytes_received += total_bytes;

		t->bytes_received    += total_bytes;
		t->bytes_transferred += total_bytes;

		w->total_bytes_transferred += total_bytes;
		w->total_transfer_time += sum_time;

		debug(D_WQ, "%s (%s) sent %.2lf MB in %.02lfs (%.02lfs MB/s) average %.02lfs MB/s", w->hostname, w->addrport, total_bytes / 1000000.0, sum_time / 1000000.0, (double) total_bytes / sum_time, (double) w->total_bytes_transferred / w->total_transfer_time);

        write_transaction_transfer(q, w, t, f, total_bytes, sum_time, WORK_QUEUE_OUTPUT);
	}

	// If the transfer was successful, make a record of it in the cache.
	if(result == WQ_SUCCESS && f->flags & WORK_QUEUE_CACHE) {
		struct stat local_info;
		if (stat(f->payload,&local_info) == 0) {
			struct remote_file_info *remote_info = remote_file_info_create(f->type,local_info.st_size,local_info.st_mtime);
			hash_table_insert(w->current_files, f->cached_name, remote_info);
		} else {
			debug(D_NOTICE, "Cannot stat file %s: %s", f->payload, strerror(errno));
		}
	}

	return result;
}

static work_queue_result_code_t get_output_files( struct work_queue *q, struct work_queue_worker *w, struct work_queue_task *t )
{
	struct work_queue_file *f;
	work_queue_result_code_t result = WQ_SUCCESS;

	if(t->output_files) {
		list_first_item(t->output_files);
		while((f = list_next_item(t->output_files))) {
			// non-file objects are handled by the worker.
			if(f->type!=WORK_QUEUE_FILE) continue;
		     
			int task_succeeded = (t->result==WORK_QUEUE_RESULT_SUCCESS && t->return_status==0);

			// skip failure-only files on success 
			if(f->flags&WORK_QUEUE_FAILURE_ONLY && task_succeeded) continue;

 			// skip success-only files on failure
			if(f->flags&WORK_QUEUE_SUCCESS_ONLY && !task_succeeded) continue;

			// otherwise, get the file.
			result = get_output_file(q,w,t,f);

			//if success or app-level failure, continue to get other files.
			//if worker failure, return.
			if(result == WQ_WORKER_FAILURE) {
				break;
			}
		}
	}

	// tell the worker you no longer need that task's output directory.
	send_worker_msg(q,w, "kill %d\n",t->taskid);

	return result;
}

static work_queue_result_code_t get_monitor_output_file( struct work_queue *q, struct work_queue_worker *w, struct work_queue_task *t )
{
	struct work_queue_file *f;
	work_queue_result_code_t result = WQ_SUCCESS;

	const char *summary_name = RESOURCE_MONITOR_REMOTE_NAME ".summary";

	if(t->output_files) {
		list_first_item(t->output_files);
		while((f = list_next_item(t->output_files))) {
			if(!strcmp(summary_name, f->remote_name)) {
				result = get_output_file(q,w,t,f);
				break;
			}
		}
	}

	// tell the worker you no longer need that task's output directory.
	send_worker_msg(q,w, "kill %d\n",t->taskid);

	return result;
}

static void delete_worker_file( struct work_queue *q, struct work_queue_worker *w, const char *filename, int flags, int except_flags ) {
	if(!(flags & except_flags)) {
		send_worker_msg(q,w, "unlink %s\n", filename);
		hash_table_remove(w->current_files, filename);
	}
}

// Sends "unlink file" for every file in the list except those that match one or more of the "except_flags"
static void delete_worker_files( struct work_queue *q, struct work_queue_worker *w, struct list *files, int except_flags ) {
	struct work_queue_file *tf;

	if(!files) return;

	list_first_item(files);
	while((tf = list_next_item(files))) {
		delete_worker_file(q, w, tf->cached_name, tf->flags, except_flags);
	}
}

static void delete_task_output_files(struct work_queue *q, struct work_queue_worker *w, struct work_queue_task *t)
{
	delete_worker_files(q, w, t->output_files, 0);
}

static void delete_uncacheable_files( struct work_queue *q, struct work_queue_worker *w, struct work_queue_task *t )
{
	delete_worker_files(q, w, t->input_files, WORK_QUEUE_CACHE | WORK_QUEUE_PREEXIST);
	delete_worker_files(q, w, t->output_files, WORK_QUEUE_CACHE | WORK_QUEUE_PREEXIST);
}

char *monitor_file_name(struct work_queue *q, struct work_queue_task *t, const char *ext) {
	char *dir;
	
	if(t->monitor_output_directory) {
		dir = t->monitor_output_directory;
	} else if(q->monitor_output_directory) {
		dir = q->monitor_output_directory;
	} else {
		dir = "./";
	}

	return string_format("%s/" RESOURCE_MONITOR_TASK_LOCAL_NAME "%s",
			dir, getpid(), t->taskid, ext ? ext : "");
}

void read_measured_resources(struct work_queue *q, struct work_queue_task *t) {

	char *summary = monitor_file_name(q, t, ".summary");

	if(t->resources_measured) {
		rmsummary_delete(t->resources_measured);
	}

	t->resources_measured = rmsummary_parse_file_single(summary);

	if(t->resources_measured) {
		t->resources_measured->category = xxstrdup(t->category);
		t->return_status = t->resources_measured->exit_status;

		/* cleanup noise in cores value, otherwise small fluctuations trigger new
		* maximums */
		if(t->resources_measured->cores > 0) {
			t->resources_measured->cores = MIN(t->resources_measured->cores, ceil(t->resources_measured->cores - 0.1));
		}

	} else {
		/* if no resources were measured, then we don't overwrite the return
		 * status, and mark the task as with error from monitoring. */
		t->resources_measured = rmsummary_create(-1);
		update_task_result(t, WORK_QUEUE_RESULT_RMONITOR_ERROR);
	}

	free(summary);
}

void resource_monitor_append_report(struct work_queue *q, struct work_queue_task *t)
{
	if(q->monitor_mode == MON_DISABLED)
		return;

	char *summary = monitor_file_name(q, t, ".summary");

	if(q->monitor_output_directory) {
		int monitor_fd = fileno(q->monitor_file);

		struct flock lock;
		lock.l_type   = F_WRLCK;
		lock.l_start  = 0;
		lock.l_whence = SEEK_SET;
		lock.l_len    = 0;

		fcntl(monitor_fd, F_SETLKW, &lock);

		if(!t->resources_measured)
		{
			fprintf(q->monitor_file, "# Summary for task %d was not available.\n", t->taskid);
		}

		FILE *fs = fopen(summary, "r");
		if(fs) {
			copy_stream_to_stream(fs, q->monitor_file);
			fclose(fs);
		}

		fprintf(q->monitor_file, "\n");

		lock.l_type   = F_ULOCK;
		fcntl(monitor_fd, F_SETLK, &lock);
	}

	/* Remove individual summary file unless it is named specifically. */
	int keep = 0;
	if(t->monitor_output_directory)
		keep = 1;

	if(q->monitor_mode & MON_FULL && q->monitor_output_directory)
		keep = 1;

	if(!keep)
		unlink(summary);

	free(summary);
}

void resource_monitor_compress_logs(struct work_queue *q, struct work_queue_task *t) {
	char *series    = monitor_file_name(q, t, ".series");
	char *debug_log = monitor_file_name(q, t, ".debug");

	char *command = string_format("gzip -9 -q %s %s", series, debug_log);

	int status;
	int rc = shellcode(command, NULL, NULL, 0, NULL, NULL, &status);

	if(rc) {
		debug(D_NOTICE, "Could no successfully compress '%s', and '%s'\n", series, debug_log);
	}

	free(series);
	free(debug_log);
	free(command);
}

static void fetch_output_from_worker(struct work_queue *q, struct work_queue_worker *w, int taskid)
{
	struct work_queue_task *t;
	work_queue_result_code_t result = WQ_SUCCESS;

	t = itable_lookup(w->current_tasks, taskid);
	if(!t) {
		debug(D_WQ, "Failed to find task %d at worker %s (%s).", taskid, w->hostname, w->addrport);
		handle_failure(q, w, t, WQ_WORKER_FAILURE);
		return;
	}

	// Start receiving output...
	t->time_when_retrieval = timestamp_get();

	if(t->result == WORK_QUEUE_RESULT_RESOURCE_EXHAUSTION) {
		result = get_monitor_output_file(q,w,t);
	} else {
		result = get_output_files(q,w,t);
	}

	if(result != WQ_SUCCESS) {
		debug(D_WQ, "Failed to receive output from worker %s (%s).", w->hostname, w->addrport);
		handle_failure(q, w, t, result);
	}

	if(result == WQ_WORKER_FAILURE) {
		// Finish receiving output:
		t->time_when_done = timestamp_get();

		return;
	}

	delete_uncacheable_files(q,w,t);

	/* if q is monitoring, append the task summary to the single
	 * queue summary, update t->resources_used, and delete the task summary. */
	if(q->monitor_mode) {
		read_measured_resources(q, t);

		/* Further, if we got debug and series files, gzip them. */
		if(q->monitor_mode & MON_FULL)
			resource_monitor_compress_logs(q, t);
	}

	// Finish receiving output.
	t->time_when_done = timestamp_get();

	work_queue_accumulate_task(q, t);

	// At this point, a task is completed.
	reap_task_from_worker(q, w, t, WORK_QUEUE_TASK_RETRIEVED);

	w->finished_tasks--;
	w->total_tasks_complete++;

	// At least one task has finished without triggering fast abort, thus we
	// now have evidence that worker is not slow (e.g., it was probably the
	// previous task that was slow).
	w->fast_abort_alarm = 0;

	if(t->result == WORK_QUEUE_RESULT_RESOURCE_EXHAUSTION) {
		if(t->resources_measured && t->resources_measured->limits_exceeded) {
			struct jx *j = rmsummary_to_json(t->resources_measured->limits_exceeded, 1);
			if(j) {
				char *str = jx_print_string(j);
				debug(D_WQ, "Task %d exhausted resources on %s (%s): %s\n",
						t->taskid,
						w->hostname,
						w->addrport,
						str);
				free(str);
				jx_delete(j);
			}
		} else {
				debug(D_WQ, "Task %d exhausted resources on %s (%s), but not resource usage was available.\n",
						t->taskid,
						w->hostname,
						w->addrport);
		}

		struct category *c = work_queue_category_lookup_or_create(q, t->category);
		category_allocation_t next = category_next_label(c, t->resource_request, /* resource overflow */ 1, t->resources_requested, t->resources_measured);

		if(next == CATEGORY_ALLOCATION_ERROR) {
			debug(D_WQ, "Task %d failed given max resource exhaustion.\n", t->taskid);
		}
		else {
			debug(D_WQ, "Task %d resubmitted using new resource allocation.\n", t->taskid);
			t->resource_request = next;
			change_task_state(q, t, WORK_QUEUE_TASK_READY);
			return;
		}
	}

	/* print warnings if the task ran for a very short time (1s) and exited with common non-zero status */
	if(t->result == WORK_QUEUE_RESULT_SUCCESS && t->time_workers_execute_last < 1000000) {
		switch(t->return_status) {
			case(126):
				warn(D_WQ, "Task %d ran for a very short time and exited with code %d.\n", t->taskid, t->return_status);
				warn(D_WQ, "This usually means that the task's command is not an executable,\n");
				warn(D_WQ, "or that the worker's scratch directory is on a no-exec partition.\n");
				break;
			case(127):
				warn(D_WQ, "Task %d ran for a very short time and exited with code %d.\n", t->taskid, t->return_status);
				warn(D_WQ, "This usually means that the task's command could not be found, or that\n");
				warn(D_WQ, "it uses a shared library not available at the worker, or that\n");
				warn(D_WQ, "it uses a version of the glibc different than the one at the worker.\n");
				break;
			case(139):
				warn(D_WQ, "Task %d ran for a very short time and exited with code %d.\n", t->taskid, t->return_status);
				warn(D_WQ, "This usually means that the task's command had a segmentation fault,\n");
				warn(D_WQ, "either because it has a memory access error (segfault), or because\n");
				warn(D_WQ, "it uses a version of a shared library different from the one at the worker.\n");
				break;
			default:
				break;
		}
	}

	add_task_report(q, t);
	debug(D_WQ, "%s (%s) done in %.02lfs total tasks %lld average %.02lfs",
			w->hostname,
			w->addrport,
			(t->time_when_done - t->time_when_commit_start) / 1000000.0,
			(long long) w->total_tasks_complete,
			w->total_task_time / w->total_tasks_complete / 1000000.0);

	return;
}

static int expire_waiting_tasks(struct work_queue *q)
{
	struct work_queue_task *t;
	int expired = 0;
	int count;

	double current_time = timestamp_get() / ONE_SECOND;
	count = task_state_count(q, NULL, WORK_QUEUE_TASK_READY);

	while(count > 0)
	{
		count--;

		t = list_pop_head(q->ready_list);

		if(t->resources_requested->end > 0 && t->resources_requested->end <= current_time)
		{
			update_task_result(t, WORK_QUEUE_RESULT_TASK_TIMEOUT);
			change_task_state(q, t, WORK_QUEUE_TASK_RETRIEVED);
			expired++;
		} else if(t->max_retries > 0 && t->try_count > t->max_retries) {
			update_task_result(t, WORK_QUEUE_RESULT_MAX_RETRIES);
			change_task_state(q, t, WORK_QUEUE_TASK_RETRIEVED);
			expired++;
		} else {
			list_push_tail(q->ready_list, t);
		}
	}

	return expired;
}


/*
This function handles app-level failures. It remove the task from WQ and marks
the task as complete so it is returned to the application.
*/
static void handle_app_failure(struct work_queue *q, struct work_queue_worker *w, struct work_queue_task *t)
{
	//remove the task from tables that track dispatched tasks.
	//and add the task to complete list so it is given back to the application.
	reap_task_from_worker(q, w, t, WORK_QUEUE_TASK_RETRIEVED);

	/*If the failure happened after a task execution, we remove all the output
	files specified for that task from the worker's cache.  This is because the
	application may resubmit the task and the resubmitted task may produce
	different outputs. */
	if(t) {
		if(t->time_when_commit_end > 0) {
			delete_task_output_files(q,w,t);
		}
	}

	return;
}

static void handle_worker_failure(struct work_queue *q, struct work_queue_worker *w)
{
	//WQ failures happen in the manager-worker interactions. In this case, we
	//remove the worker and retry the tasks dispatched to it elsewhere.
	remove_worker(q, w, WORKER_DISCONNECT_FAILURE);
	return;
}

static void handle_failure(struct work_queue *q, struct work_queue_worker *w, struct work_queue_task *t, work_queue_result_code_t fail_type)
{
	if(fail_type == WQ_APP_FAILURE) {
		handle_app_failure(q, w, t);
	} else {
		handle_worker_failure(q, w);
	}
	return;
}

static work_queue_msg_code_t process_workqueue(struct work_queue *q, struct work_queue_worker *w, const char *line)
{
	char items[4][WORK_QUEUE_LINE_MAX];
	int worker_protocol;

	int n = sscanf(line,"workqueue %d %s %s %s %s",&worker_protocol,items[0],items[1],items[2],items[3]);
	if(n != 5)
		return MSG_FAILURE;

	if(worker_protocol!=WORK_QUEUE_PROTOCOL_VERSION) {
		debug(D_WQ|D_NOTICE,"rejecting worker (%s) as it uses protocol %d. The manager is using protocol %d.",w->addrport,worker_protocol,WORK_QUEUE_PROTOCOL_VERSION);
		work_queue_block_host(q, w->hostname);
		return MSG_FAILURE;
	}

	if(w->hostname) free(w->hostname);
	if(w->os)       free(w->os);
	if(w->arch)     free(w->arch);
	if(w->version)  free(w->version);

	w->hostname = strdup(items[0]);
	w->os       = strdup(items[1]);
	w->arch     = strdup(items[2]);
	w->version  = strdup(items[3]);

	if(!strcmp(w->os, "foreman"))
	{
		w->type = WORKER_TYPE_FOREMAN;
	} else {
		w->type = WORKER_TYPE_WORKER;
	}

	q->stats->workers_joined++;
	debug(D_WQ, "%d workers are connected in total now", count_workers(q, WORKER_TYPE_WORKER | WORKER_TYPE_FOREMAN));


	debug(D_WQ, "%s (%s) running CCTools version %s on %s (operating system) with architecture %s is ready", w->hostname, w->addrport, w->version, w->os, w->arch);

	if(cctools_version_cmp(CCTOOLS_VERSION, w->version) != 0) {
		debug(D_DEBUG, "Warning: potential worker version mismatch: worker %s (%s) is version %s, and manager is version %s", w->hostname, w->addrport, w->version, CCTOOLS_VERSION);
	}


	return MSG_PROCESSED;
}

/*
If the manager has requested that a file be watched with WORK_QUEUE_WATCH,
the worker will periodically send back update messages indicating that
the file has been written to.  There are a variety of ways in which the
message could be stale (e.g. task was cancelled) so if the message does
not line up with an expected task and file, then we discard it and keep
going.
*/

static work_queue_result_code_t get_update( struct work_queue *q, struct work_queue_worker *w, const char *line )
{
	int64_t taskid;
	char path[WORK_QUEUE_LINE_MAX];
	int64_t offset;
	int64_t length;

	int n = sscanf(line,"update %"PRId64" %s %"PRId64" %"PRId64,&taskid,path,&offset,&length);
	if(n!=4) {
		debug(D_WQ,"Invalid message from worker %s (%s): %s", w->hostname, w->addrport, line );
		return WQ_WORKER_FAILURE;
	}

	struct work_queue_task *t = itable_lookup(w->current_tasks,taskid);
	if(!t) {
		debug(D_WQ,"worker %s (%s) sent output for unassigned task %"PRId64, w->hostname, w->addrport, taskid);
		link_soak(w->link,length,time(0)+get_transfer_wait_time(q,w,0,length));
		return WQ_SUCCESS;
	}


	time_t stoptime = time(0) + get_transfer_wait_time(q,w,t,length);

	struct work_queue_file *f;
	const char *local_name = 0;

	list_first_item(t->output_files);
	while((f=list_next_item(t->output_files))) {
		if(!strcmp(path,f->remote_name)) {
			local_name = f->payload;
			break;
		}
	}

	if(!local_name) {
		debug(D_WQ,"worker %s (%s) sent output for unwatched file %s",w->hostname,w->addrport,path);
		link_soak(w->link,length,stoptime);
		return WQ_SUCCESS;
	}

	int fd = open(local_name,O_WRONLY|O_CREAT,0777);
	if(fd<0) {
		debug(D_WQ,"unable to update watched file %s: %s",local_name,strerror(errno));
		link_soak(w->link,length,stoptime);
		return WQ_SUCCESS;
	}

	lseek(fd,offset,SEEK_SET);
	link_stream_to_fd(w->link,fd,length,stoptime);
	ftruncate(fd,offset+length);

	if(close(fd) < 0) {
		debug(D_WQ, "unable to update watched file %s: %s\n", local_name, strerror(errno));
		return WQ_SUCCESS;
	}

	return WQ_SUCCESS;
}

/*
Failure to store result is treated as success so we continue to retrieve the
output files of the task.
*/
static work_queue_result_code_t get_result(struct work_queue *q, struct work_queue_worker *w, const char *line) {

	if(!q || !w || !line) 
		return WQ_WORKER_FAILURE;

	struct work_queue_task *t;

	int task_status, exit_status;
	uint64_t taskid;
	int64_t output_length, retrieved_output_length;
	timestamp_t execution_time;

	int64_t actual;

	timestamp_t observed_execution_time;
	timestamp_t effective_stoptime = 0;
	time_t stoptime;

	//Format: task completion status, exit status (exit code or signal), output length, execution time, taskid
	char items[5][WORK_QUEUE_PROTOCOL_FIELD_MAX];
	int n = sscanf(line, "result %s %s %s %s %" SCNd64"", items[0], items[1], items[2], items[3], &taskid);

	if(n < 5) {
		debug(D_WQ, "Invalid message from worker %s (%s): %s", w->hostname, w->addrport, line);
		return WQ_WORKER_FAILURE;
	}

	task_status = atoi(items[0]);
	exit_status   = atoi(items[1]);
	output_length = atoll(items[2]);

	t = itable_lookup(w->current_tasks, taskid);
	if(!t) {
		debug(D_WQ, "Unknown task result from worker %s (%s): no task %" PRId64" assigned to worker.  Ignoring result.", w->hostname, w->addrport, taskid);
		stoptime = time(0) + get_transfer_wait_time(q, w, 0, output_length);
		link_soak(w->link, output_length, stoptime);
		return WQ_SUCCESS;
	}

	if(task_status == WORK_QUEUE_RESULT_FORSAKEN) {
		// Delete any input files that are not to be cached.
		delete_worker_files(q, w, t->input_files, WORK_QUEUE_CACHE | WORK_QUEUE_PREEXIST);

		/* task will be resubmitted, so we do not update any of the execution stats */
		reap_task_from_worker(q, w, t, WORK_QUEUE_TASK_READY);

		return WQ_SUCCESS;
	}

	observed_execution_time = timestamp_get() - t->time_when_commit_end;

	execution_time = atoll(items[3]);
	t->time_workers_execute_last = observed_execution_time > execution_time ? execution_time : observed_execution_time;

	t->time_workers_execute_all += t->time_workers_execute_last;

	if(task_status == WORK_QUEUE_RESULT_DISK_ALLOC_FULL) {
		t->disk_allocation_exhausted = 1;
	}
	else {
		t->disk_allocation_exhausted = 0;
	}

	if(q->bandwidth) {
		effective_stoptime = (output_length/q->bandwidth)*1000000 + timestamp_get();
	}

	if(output_length <= MAX_TASK_STDOUT_STORAGE) {
		retrieved_output_length = output_length;
	} else {
		retrieved_output_length = MAX_TASK_STDOUT_STORAGE;
		fprintf(stderr, "warning: stdout of task %"PRId64" requires %2.2lf GB of storage. This exceeds maximum supported size of %d GB. Only %d GB will be retrieved.\n", taskid, ((double) output_length)/MAX_TASK_STDOUT_STORAGE, MAX_TASK_STDOUT_STORAGE/GIGABYTE, MAX_TASK_STDOUT_STORAGE/GIGABYTE);
		update_task_result(t, WORK_QUEUE_RESULT_STDOUT_MISSING);
	}

	t->output = malloc(retrieved_output_length+1);
	if(t->output == NULL) {
		fprintf(stderr, "error: allocating memory of size %"PRId64" bytes failed for storing stdout of task %"PRId64".\n", retrieved_output_length, taskid);
		//drop the entire length of stdout on the link
		stoptime = time(0) + get_transfer_wait_time(q, w, t, output_length);
		link_soak(w->link, output_length, stoptime);
		retrieved_output_length = 0;
		update_task_result(t, WORK_QUEUE_RESULT_STDOUT_MISSING);
	}

	if(retrieved_output_length > 0) {
		debug(D_WQ, "Receiving stdout of task %"PRId64" (size: %"PRId64" bytes) from %s (%s) ...", taskid, retrieved_output_length, w->addrport, w->hostname);

		//First read the bytes we keep.
		stoptime = time(0) + get_transfer_wait_time(q, w, t, retrieved_output_length);
		actual = link_read(w->link, t->output, retrieved_output_length, stoptime);
		if(actual != retrieved_output_length) {
			debug(D_WQ, "Failure: actual received stdout size (%"PRId64" bytes) is different from expected (%"PRId64" bytes).", actual, retrieved_output_length);
			t->output[actual] = '\0';
			return WQ_WORKER_FAILURE;
		}
		debug(D_WQ, "Retrieved %"PRId64" bytes from %s (%s)", actual, w->hostname, w->addrport);

		//Then read the bytes we need to throw away.
		if(output_length > retrieved_output_length) {
			debug(D_WQ, "Dropping the remaining %"PRId64" bytes of the stdout of task %"PRId64" since stdout length is limited to %d bytes.\n", (output_length-MAX_TASK_STDOUT_STORAGE), taskid, MAX_TASK_STDOUT_STORAGE);
			stoptime = time(0) + get_transfer_wait_time(q, w, t, (output_length-retrieved_output_length));
			link_soak(w->link, (output_length-retrieved_output_length), stoptime);

			//overwrite the last few bytes of buffer to signal truncated stdout.
			char *truncate_msg = string_format("\n>>>>>> WORK QUEUE HAS TRUNCATED THE STDOUT AFTER THIS POINT.\n>>>>>> MAXIMUM OF %d BYTES REACHED, %" PRId64 " BYTES TRUNCATED.", MAX_TASK_STDOUT_STORAGE, output_length - retrieved_output_length);
			memcpy(t->output + MAX_TASK_STDOUT_STORAGE - strlen(truncate_msg) - 1, truncate_msg, strlen(truncate_msg));
			*(t->output + MAX_TASK_STDOUT_STORAGE - 1) = '\0';
			free(truncate_msg);
		}

		timestamp_t current_time = timestamp_get();
		if(effective_stoptime && effective_stoptime > current_time) {
			usleep(effective_stoptime - current_time);
		}
	} else {
		actual = 0;
	}

	if(t->output)
		t->output[actual] = 0;

	t->result        = task_status;
	t->return_status = exit_status;

	q->stats->time_workers_execute += t->time_workers_execute_last;

	w->finished_tasks++;

	// Convert resource_monitor status into work queue status if needed.
	if(q->monitor_mode) {
		if(t->return_status == RM_OVERFLOW) {
			update_task_result(t, WORK_QUEUE_RESULT_RESOURCE_EXHAUSTION);
		} else if(t->return_status == RM_TIME_EXPIRE) {
			update_task_result(t, WORK_QUEUE_RESULT_TASK_TIMEOUT);
		}
	}

	change_task_state(q, t, WORK_QUEUE_TASK_WAITING_RETRIEVAL);

	return WQ_SUCCESS;
}

static work_queue_result_code_t get_available_results(struct work_queue *q, struct work_queue_worker *w)
{

	//max_count == -1, tells the worker to send all available results.
	send_worker_msg(q, w, "send_results %d\n", -1);
	debug(D_WQ, "Reading result(s) from %s (%s)", w->hostname, w->addrport);

	char line[WORK_QUEUE_LINE_MAX];
	int i = 0;

	work_queue_result_code_t result = WQ_SUCCESS; //return success unless something fails below.

	while(1) {
		work_queue_msg_code_t mcode;
		mcode = recv_worker_msg_retry(q, w, line, sizeof(line));
		if(mcode!=MSG_NOT_PROCESSED) {
			result = WQ_WORKER_FAILURE;
			break;
		}

		if(string_prefix_is(line,"result")) {
			result = get_result(q, w, line);
			if(result != WQ_SUCCESS) break;
			i++;
		} else if(string_prefix_is(line,"update")) {
			result = get_update(q,w,line);
			if(result != WQ_SUCCESS) break;
		} else if(!strcmp(line,"end")) {
			//Only return success if last message is end.
			break;
		} else {
			debug(D_WQ, "%s (%s): sent invalid response to send_results: %s",w->hostname,w->addrport,line);
			result = WQ_WORKER_FAILURE;
			break;
		}
	}

	if(result != WQ_SUCCESS) {
		handle_worker_failure(q, w);
	}

	return result;
}

static int update_task_result(struct work_queue_task *t, work_queue_result_t new_result) {

	if(new_result & ~(0x7)) {
		/* Upper bits are set, so this is not related to old-style result for
		 * inputs, outputs, or stdout, so we simply make an update. */
		t->result = new_result;
	} else if(t->result != WORK_QUEUE_RESULT_UNKNOWN && t->result & ~(0x7)) {
		/* Ignore new result, since we only update for input, output, or
		 * stdout missing when no other result exists. This is because
		 * missing inputs/outputs are anyway expected with other kind of
		 * errors. */
	} else if(new_result == WORK_QUEUE_RESULT_INPUT_MISSING) {
		/* input missing always appears by itself, so yet again we simply make an update. */
		t->result = new_result;
	} else if(new_result == WORK_QUEUE_RESULT_OUTPUT_MISSING) {
		/* output missing clobbers stdout missing. */
		t->result = new_result;
	} else {
		/* we only get here for stdout missing. */
		t->result = new_result;
	}

	return t->result;
}

static struct jx *blocked_to_json( struct work_queue  *q ) {
	if(hash_table_size(q->worker_blocklist) < 1) {
		return NULL;
	}

	struct jx *j = jx_array(0);

	char *hostname;
	struct blocklist_host_info *info;

	hash_table_firstkey(q->worker_blocklist);
	while(hash_table_nextkey(q->worker_blocklist, &hostname, (void *) &info)) {
		if(info->blocked) {
			jx_array_insert(j, jx_string(hostname));
		}
	}

	return j;
}

static struct rmsummary  *total_resources_needed(struct work_queue *q) {

	struct work_queue_task *t;

	struct rmsummary *total = rmsummary_create(0);

	/* for waiting tasks, we use what they would request if dispatched right now. */
	list_first_item(q->ready_list);
	while((t = list_next_item(q->ready_list))) {
		const struct rmsummary *s = task_min_resources(q, t);
		rmsummary_add(total, s);
	}

	/* for running tasks, we use what they have been allocated already. */
	char *key;
	struct work_queue_worker *w;
	hash_table_firstkey(q->worker_table);

	while(hash_table_nextkey(q->worker_table, &key, (void **) &w)) {
		if(w->resources->tag < 0) {
			continue;
		}

		total->cores  += w->resources->cores.inuse;
		total->memory += w->resources->memory.inuse;
		total->disk   += w->resources->disk.inuse;
		total->gpus   += w->resources->gpus.inuse;
	}

	return total;
}

static const struct rmsummary *largest_seen_resources(struct work_queue *q, const char *category) {
	char *key;
	struct category *c;

	if(category) {
		c = work_queue_category_lookup_or_create(q, category);
		return c->max_allocation;
	} else {
		hash_table_firstkey(q->categories);
		while(hash_table_nextkey(q->categories, &key, (void **) &c)) {
			rmsummary_merge_max(q->max_task_resources_requested, c->max_allocation);
		}
		return q->max_task_resources_requested;
	}
}

static int check_worker_fit(struct work_queue_worker *w, const struct rmsummary *s) {

	if(w->resources->workers.total < 1)
		return 0;

	if(!s)
		return w->resources->workers.total;

	if(s->cores > w->resources->cores.largest)
		return 0;
	if(s->memory > w->resources->memory.largest)
		return 0;
	if(s->disk > w->resources->disk.largest)
		return 0;
	if(s->gpus > w->resources->gpus.largest)
		return 0;

	return w->resources->workers.total;
}

static int count_workers_for_waiting_tasks(struct work_queue *q, const struct rmsummary *s) {

	int count = 0;

	char *key;
	struct work_queue_worker *w;
	hash_table_firstkey(q->worker_table);
	while(hash_table_nextkey(q->worker_table, &key, (void**)&w)) {
		count += check_worker_fit(w, s);
	}

	return count;
}

/* category_to_jx creates a jx expression with category statistics that can be
sent to the catalog.
*/

void category_jx_insert_max(struct jx *j, struct category *c, const char *field, const struct rmsummary *largest) {

	double l = rmsummary_get(largest, field);
	double m = -1;
	double e = -1;

	if(c) {
		m = rmsummary_get(c->max_resources_seen, field);
		if(c->max_resources_seen->limits_exceeded) {
			e = rmsummary_get(c->max_resources_seen->limits_exceeded, field);
		}
	}

	char *field_str = string_format("max_%s", field);

	if(l > -1){
		char *max_str = string_format("%s", rmsummary_resource_to_str(field, l, 0));
		jx_insert_string(j, field_str, max_str);
		free(max_str);
	} else if(c && !category_in_steady_state(c) && e > -1) {
		char *max_str = string_format(">%s", rmsummary_resource_to_str(field, m - 1, 0));
		jx_insert_string(j, field_str, max_str);
		free(max_str);
	} else if(c && m > -1) {
		char *max_str = string_format("~%s", rmsummary_resource_to_str(field, m, 0));
		jx_insert_string(j, field_str, max_str);
		free(max_str);
	} else {
		jx_insert_string(j, field_str, "na");
	}

	free(field_str);
}

/* create dummy task to obtain first allocation that category would get if using largest worker */
static struct rmsummary *category_alloc_info(struct work_queue *q, struct category *c, category_allocation_t request) {
	struct work_queue_task *t = work_queue_task_create("nop");
	work_queue_task_specify_category(t, c->name);
	t->resource_request = request;

	struct work_queue_worker *w = malloc(sizeof(*w));
	w->resources = work_queue_resources_create();
	w->resources->cores.largest = q->current_max_worker->cores;
	w->resources->memory.largest = q->current_max_worker->memory;
	w->resources->disk.largest = q->current_max_worker->disk;
	w->resources->gpus.largest = q->current_max_worker->gpus;

	struct rmsummary *allocation = task_worker_box_size(q, w, t);

	work_queue_task_delete(t);
	work_queue_resources_delete(w->resources);
	free(w);

	return allocation;
}

static struct jx * alloc_to_jx(struct work_queue *q, struct category *c, struct rmsummary *resources) {
	struct jx *j = jx_object(0);

	jx_insert_double(j, "cores", resources->cores);
	jx_insert_integer(j, "memory", resources->memory);
	jx_insert_integer(j, "disk", resources->disk);
	jx_insert_integer(j, "gpus", resources->gpus);

	return j;
}

static struct jx * category_to_jx(struct work_queue *q, const char *category) {
	struct work_queue_stats s;
	struct category *c = NULL;
	const struct rmsummary *largest = largest_seen_resources(q, category);

	c = work_queue_category_lookup_or_create(q, category);
	work_queue_get_stats_category(q, category, &s);

	if(s.tasks_waiting + s.tasks_on_workers + s.tasks_done < 1) {
		return NULL;
	}

	struct jx *j = jx_object(0);

	jx_insert_string(j,  "category",         category);
	jx_insert_integer(j, "tasks_waiting",    s.tasks_waiting);
	jx_insert_integer(j, "tasks_running",    s.tasks_running);
	jx_insert_integer(j, "tasks_on_workers", s.tasks_on_workers);
	jx_insert_integer(j, "tasks_dispatched", s.tasks_dispatched);
	jx_insert_integer(j, "tasks_done",       s.tasks_done);
	jx_insert_integer(j, "tasks_failed",     s.tasks_failed);
	jx_insert_integer(j, "tasks_cancelled",  s.tasks_cancelled);
	jx_insert_integer(j, "workers_able",	 s.workers_able);

	category_jx_insert_max(j, c, "cores",  largest);
	category_jx_insert_max(j, c, "memory", largest);
	category_jx_insert_max(j, c, "disk",   largest);
	category_jx_insert_max(j, c, "gpus",   largest);

	struct rmsummary *first_allocation = category_alloc_info(q, c, CATEGORY_ALLOCATION_FIRST);
	struct jx *jr = alloc_to_jx(q, c, first_allocation);
	rmsummary_delete(first_allocation);
	jx_insert(j, jx_string("first_allocation"), jr);

	struct rmsummary *max_allocation = category_alloc_info(q, c, CATEGORY_ALLOCATION_MAX);
	jr = alloc_to_jx(q, c, max_allocation);
	rmsummary_delete(max_allocation);
	jx_insert(j, jx_string("max_allocation"), jr);

	if(q->monitor_mode) {
		jr = alloc_to_jx(q, c, c->max_resources_seen);
		jx_insert(j, jx_string("max_seen"), jr);
	}

	jx_insert_integer(j, "first_allocation_count", task_request_count(q, c->name, CATEGORY_ALLOCATION_FIRST));
	jx_insert_integer(j, "max_allocation_count",   task_request_count(q, c->name, CATEGORY_ALLOCATION_MAX));

	return j;
}

static struct jx *categories_to_jx(struct work_queue *q) {
	struct jx *a = jx_array(0);

	struct category *c;
	char *category_name;
	hash_table_firstkey(q->categories);
	while(hash_table_nextkey(q->categories, &category_name, (void **) &c)) {
		struct jx *j = category_to_jx(q, category_name);
		if(j) {
			jx_array_insert(a, j);
		}
	}

	return a;
}

/*
queue_to_jx examines the overall queue status and creates
an jx expression which can be sent directly to the
user that connects via work_queue_status.
*/

static struct jx * queue_to_jx( struct work_queue *q, struct link *foreman_uplink )
{
	struct jx *j = jx_object(0);
	if(!j) return 0;

	// Insert all properties from work_queue_stats

	struct work_queue_stats info;
	work_queue_get_stats(q,&info);

	// Add special properties expected by the catalog server
	char owner[USERNAME_MAX];
	username_get(owner);

	jx_insert_string(j,"type","wq_master");
	if(q->name) jx_insert_string(j,"project",q->name);
	jx_insert_integer(j,"starttime",(q->stats->time_when_started/1000000)); // catalog expects time_t not timestamp_t
	jx_insert_string(j,"working_dir",q->workingdir);
	jx_insert_string(j,"owner",owner);
	jx_insert_string(j,"version",CCTOOLS_VERSION);
	jx_insert_integer(j,"port",work_queue_port(q));
	jx_insert_integer(j,"priority",info.priority);
	jx_insert_string(j,"manager_preferred_connection",q->manager_preferred_connection);

	int use_ssl = 0;
#ifdef HAS_OPENSSL
	if(q->ssl_enabled) {
		use_ssl = 1;
	}
#endif
	jx_insert_boolean(j,"ssl",use_ssl);

	struct jx *interfaces = interfaces_of_host();
	if(interfaces) {
		jx_insert(j,jx_string("network_interfaces"),interfaces);
	}

	//send info on workers
	jx_insert_integer(j,"workers",info.workers_connected);
	jx_insert_integer(j,"workers_connected",info.workers_connected);
	jx_insert_integer(j,"workers_init",info.workers_init);
	jx_insert_integer(j,"workers_idle",info.workers_idle);
	jx_insert_integer(j,"workers_busy",info.workers_busy);
	jx_insert_integer(j,"workers_able",info.workers_able);

	jx_insert_integer(j,"workers_joined",info.workers_joined);
	jx_insert_integer(j,"workers_removed",info.workers_removed);
	jx_insert_integer(j,"workers_released",info.workers_released);
	jx_insert_integer(j,"workers_idled_out",info.workers_idled_out);
	jx_insert_integer(j,"workers_fast_aborted",info.workers_fast_aborted);
	jx_insert_integer(j,"workers_lost",info.workers_lost);

	//workers_blocked adds host names, not a count
	struct jx *blocklist = blocked_to_json(q);
	if(blocklist) {
		jx_insert(j,jx_string("workers_blocked"), blocklist);
	}


	//send info on tasks
	jx_insert_integer(j,"tasks_waiting",info.tasks_waiting);
	jx_insert_integer(j,"tasks_on_workers",info.tasks_on_workers);
	jx_insert_integer(j,"tasks_running",info.tasks_running);
	jx_insert_integer(j,"tasks_with_results",info.tasks_with_results);
	jx_insert_integer(j,"tasks_left",q->num_tasks_left);

	jx_insert_integer(j,"tasks_submitted",info.tasks_submitted);
	jx_insert_integer(j,"tasks_dispatched",info.tasks_dispatched);
	jx_insert_integer(j,"tasks_done",info.tasks_done);
	jx_insert_integer(j,"tasks_failed",info.tasks_failed);
	jx_insert_integer(j,"tasks_cancelled",info.tasks_cancelled);
	jx_insert_integer(j,"tasks_exhausted_attempts",info.tasks_exhausted_attempts);

	// tasks_complete is deprecated, but the old work_queue_status expects it.
	jx_insert_integer(j,"tasks_complete",info.tasks_done);

	//send info on queue
	jx_insert_integer(j,"time_when_started",info.time_when_started);
	jx_insert_integer(j,"time_send",info.time_send);
	jx_insert_integer(j,"time_receive",info.time_receive);
	jx_insert_integer(j,"time_send_good",info.time_send_good);
	jx_insert_integer(j,"time_receive_good",info.time_receive_good);
	jx_insert_integer(j,"time_status_msgs",info.time_status_msgs);
	jx_insert_integer(j,"time_internal",info.time_internal);
	jx_insert_integer(j,"time_polling",info.time_polling);
	jx_insert_integer(j,"time_application",info.time_application);

	jx_insert_integer(j,"time_workers_execute",info.time_workers_execute);
	jx_insert_integer(j,"time_workers_execute_good",info.time_workers_execute_good);
	jx_insert_integer(j,"time_workers_execute_exhaustion",info.time_workers_execute_exhaustion);

	jx_insert_integer(j,"bytes_sent",info.bytes_sent);
	jx_insert_integer(j,"bytes_received",info.bytes_received);

	jx_insert_integer(j,"capacity_tasks",info.capacity_tasks);
	jx_insert_integer(j,"capacity_cores",info.capacity_cores);
	jx_insert_integer(j,"capacity_memory",info.capacity_memory);
	jx_insert_integer(j,"capacity_disk",info.capacity_disk);
	jx_insert_integer(j,"capacity_gpus",info.capacity_gpus);
	jx_insert_integer(j,"capacity_instantaneous",info.capacity_instantaneous);
	jx_insert_integer(j,"capacity_weighted",info.capacity_weighted);
	jx_insert_integer(j,"manager_load",info.manager_load);

	if(q->tlq_url) jx_insert_string(j,"tlq_url",q->tlq_url);

	// Add the resources computed from tributary workers.
	struct work_queue_resources r;
	aggregate_workers_resources(q,&r,NULL);
	work_queue_resources_add_to_jx(&r,j);

	// If this is a foreman, add the manager address and the disk resources
	if(foreman_uplink) {
		int port;
		char address[LINK_ADDRESS_MAX];
		char addrport[WORK_QUEUE_LINE_MAX];

		link_address_remote(foreman_uplink,address,&port);
		sprintf(addrport,"%s:%d",address,port);
		jx_insert_string(j,"my_manager",addrport);

		// get foreman local resources and overwrite disk usage
		struct work_queue_resources local_resources;
		work_queue_resources_measure_locally(&local_resources,q->workingdir);
		r.disk.total = local_resources.disk.total;
		r.disk.inuse = local_resources.disk.inuse;
		work_queue_resources_add_to_jx(&r,j);
	}

	//add the stats per category
	jx_insert(j, jx_string("categories"), categories_to_jx(q));

	//add total resources used/needed by the queue
	struct rmsummary *total = total_resources_needed(q);
	jx_insert_integer(j,"tasks_total_cores",total->cores);
	jx_insert_integer(j,"tasks_total_memory",total->memory);
	jx_insert_integer(j,"tasks_total_disk",total->disk);
	jx_insert_integer(j,"tasks_total_gpus",total->gpus);
	rmsummary_delete(total);

	return j;
}

/*
queue_lean_to_jx examines the overall queue status and creates
an jx expression which can be sent to the catalog.
It different from queue_to_jx in that only the minimum information that
workers, work_queue_status and the work_queue_factory need.
*/

static struct jx * queue_lean_to_jx( struct work_queue *q, struct link *foreman_uplink )
{
	struct jx *j = jx_object(0);
	if(!j) return 0;

	// Insert all properties from work_queue_stats

	struct work_queue_stats info;
	work_queue_get_stats(q,&info);

	//information regarding how to contact the manager
	jx_insert_string(j,"version",CCTOOLS_VERSION);
	jx_insert_string(j,"type","wq_master");
	jx_insert_integer(j,"port",work_queue_port(q));

	int use_ssl = 0;
#ifdef HAS_OPENSSL
	if(q->ssl_enabled) {
		use_ssl = 1;
	}
#endif
	jx_insert_boolean(j,"ssl",use_ssl);

	char owner[USERNAME_MAX];
	username_get(owner);
	jx_insert_string(j,"owner",owner);

	if(q->name) jx_insert_string(j,"project",q->name);
	jx_insert_integer(j,"starttime",(q->stats->time_when_started/1000000)); // catalog expects time_t not timestamp_t
	jx_insert_string(j,"manager_preferred_connection",q->manager_preferred_connection);



	struct jx *interfaces = interfaces_of_host();
	if(interfaces) {
		jx_insert(j,jx_string("network_interfaces"),interfaces);
	}

	//task information for general work_queue_status report
	jx_insert_integer(j,"tasks_waiting",info.tasks_waiting);
	jx_insert_integer(j,"tasks_running",info.tasks_running);
	jx_insert_integer(j,"tasks_complete",info.tasks_done);    // tasks_complete is deprecated, but the old work_queue_status expects it.

	//additional task information for work_queue_factory
	jx_insert_integer(j,"tasks_on_workers",info.tasks_on_workers);
	jx_insert_integer(j,"tasks_left",q->num_tasks_left);

	//capacity information the factory needs
	jx_insert_integer(j,"capacity_tasks",info.capacity_tasks);
	jx_insert_integer(j,"capacity_cores",info.capacity_cores);
	jx_insert_integer(j,"capacity_memory",info.capacity_memory);
	jx_insert_integer(j,"capacity_disk",info.capacity_disk);
	jx_insert_integer(j,"capacity_gpus",info.capacity_gpus);
	jx_insert_integer(j,"capacity_weighted",info.capacity_weighted);
	jx_insert_double(j,"manager_load",info.manager_load);

	//resources information the factory needs
	struct rmsummary *total = total_resources_needed(q);
	jx_insert_integer(j,"tasks_total_cores",total->cores);
	jx_insert_integer(j,"tasks_total_memory",total->memory);
	jx_insert_integer(j,"tasks_total_disk",total->disk);
	jx_insert_integer(j,"tasks_total_gpus",total->gpus);

	//worker information for general work_queue_status report
	jx_insert_integer(j,"workers",info.workers_connected);
	jx_insert_integer(j,"workers_connected",info.workers_connected);


	//additional worker information the factory needs
	struct jx *blocklist = blocked_to_json(q);
	if(blocklist) {
		jx_insert(j,jx_string("workers_blocked"), blocklist);   //danger! unbounded field
	}

	// Add information about the foreman
	if(foreman_uplink) {
		int port;
		char address[LINK_ADDRESS_MAX];
		char addrport[WORK_QUEUE_LINE_MAX];

		link_address_remote(foreman_uplink,address,&port);
		sprintf(addrport,"%s:%d",address,port);
		jx_insert_string(j,"my_manager",addrport);
	}

	return j;
}



void current_tasks_to_jx( struct jx *j, struct work_queue_worker *w )
{
	struct work_queue_task *t;
	uint64_t taskid;
	int n = 0;

	itable_firstkey(w->current_tasks);
	while(itable_nextkey(w->current_tasks, &taskid, (void**)&t)) {
		char task_string[WORK_QUEUE_LINE_MAX];

		sprintf(task_string, "current_task_%03d_id", n);
		jx_insert_integer(j,task_string,t->taskid);

		sprintf(task_string, "current_task_%03d_command", n);
		jx_insert_string(j,task_string,t->command_line);
		n++;
	}
}

struct jx * worker_to_jx( struct work_queue *q, struct work_queue_worker *w )
{
	struct jx *j = jx_object(0);
	if(!j) return 0;

	if(strcmp(w->hostname, "QUEUE_STATUS") == 0){
		return 0;
	}
	jx_insert_string(j,"hostname",w->hostname);
	jx_insert_string(j,"os",w->os);
	jx_insert_string(j,"arch",w->arch);
	jx_insert_string(j,"address_port",w->addrport);
	jx_insert_integer(j,"ncpus",w->resources->cores.total);
	jx_insert_integer(j,"total_tasks_complete",w->total_tasks_complete);
	jx_insert_integer(j,"total_tasks_running",itable_size(w->current_tasks));
	jx_insert_integer(j,"total_bytes_transferred",w->total_bytes_transferred);
	jx_insert_integer(j,"total_transfer_time",w->total_transfer_time);
	jx_insert_integer(j,"start_time",w->start_time);
	jx_insert_integer(j,"current_time",timestamp_get());


	work_queue_resources_add_to_jx(w->resources,j);

	current_tasks_to_jx(j, w);

	return j;
}

static void priority_add_to_jx(struct jx *j, double priority)
{
	int decimals = 2;
	int factor   = pow(10, decimals);

	int dpart = ((int) (priority * factor)) - ((int) priority) * factor;

	char *str;

	if(dpart == 0)
		str = string_format("%d", (int) priority);
	else
		str = string_format("%.2g", priority);

	jx_insert_string(j, "priority", str);

	free(str);
}


struct jx * task_to_jx( struct work_queue *q, struct work_queue_task *t, const char *state, const char *host )
{
	struct jx *j = jx_object(0);

	jx_insert_integer(j,"taskid",t->taskid);
	jx_insert_string(j,"state",state);
	if(t->tag) jx_insert_string(j,"tag",t->tag);
	if(t->category) jx_insert_string(j,"category",t->category);
	jx_insert_string(j,"command",t->command_line);
	if(t->coprocess) jx_insert_string(j,"coprocess",t->coprocess);
	if(host) jx_insert_string(j,"host",host);

	if(host) {
		jx_insert_integer(j,"cores",t->resources_allocated->cores);
		jx_insert_integer(j,"gpus",t->resources_allocated->gpus);
		jx_insert_integer(j,"memory",t->resources_allocated->memory);
		jx_insert_integer(j,"disk",t->resources_allocated->disk);
	} else {
		const struct rmsummary *min = task_min_resources(q, t);
		const struct rmsummary *max = task_max_resources(q, t);
		struct rmsummary *limits = rmsummary_create(-1);

		rmsummary_merge_override(limits, max);
		rmsummary_merge_max(limits, min);

		jx_insert_integer(j,"cores",limits->cores);
		jx_insert_integer(j,"gpus",limits->gpus);
		jx_insert_integer(j,"memory",limits->memory);
		jx_insert_integer(j,"disk",limits->disk);

		rmsummary_delete(limits);
	}

	priority_add_to_jx(j, t->priority);


	return j;
}

/*
Send a brief human-readable index listing the data
types that can be queried via this API.
*/

static void process_data_index( struct work_queue *q, struct work_queue_worker *w, time_t stoptime )
{
	buffer_t buf;
	buffer_init(&buf);

	buffer_printf(&buf,"<h1>Work Queue Data API</h1>");
        buffer_printf(&buf,"<ul>\n");
	buffer_printf(&buf,"<li> <a href=\"/queue_status\">Queue Status</a>\n");
	buffer_printf(&buf,"<li> <a href=\"/task_status\">Task Status</a>\n");
	buffer_printf(&buf,"<li> <a href=\"/worker_status\">Worker Status</a>\n");
	buffer_printf(&buf,"<li> <a href=\"/resources_status\">Resources Status</a>\n");
        buffer_printf(&buf,"</ul>\n");

	send_worker_msg(q,w,buffer_tostring(&buf),buffer_pos(&buf),stoptime);

	buffer_free(&buf);

}

/*
Process an HTTP request that comes in via a worker port.
This represents a web browser that connected directly
to the manager to fetch status data.
*/

static work_queue_msg_code_t process_http_request( struct work_queue *q, struct work_queue_worker *w, const char *path, time_t stoptime )
{
	char line[WORK_QUEUE_LINE_MAX];

	// Consume (and ignore) the remainder of the headers.
	while(link_readline(w->link,line,WORK_QUEUE_LINE_MAX,stoptime)) {
		if(line[0]==0) break;
	}

	send_worker_msg(q,w,"HTTP/1.1 200 OK\nConnection: close\n");
	if(!strcmp(path,"/")) {
	        // Requests to root get a simple human readable index.
		send_worker_msg(q,w,"Content-type: text/html\n\n");
		process_data_index(q, w, stoptime );
	} else {
	        // Other requests get raw JSON data.
		send_worker_msg(q,w,"Access-Control-Allow-Origin: *\n");
		send_worker_msg(q,w,"Content-type: text/plain\n\n");
		process_queue_status(q, w, &path[1], stoptime );
	}

	// Return success but require a disconnect now.
	return MSG_PROCESSED_DISCONNECT;
}

static struct jx *construct_status_message( struct work_queue *q, const char *request ) {
	struct jx *a = jx_array(NULL);

	if(!strcmp(request, "queue_status") || !strcmp(request, "queue") || !strcmp(request, "resources_status")) {
		struct jx *j = queue_to_jx( q, 0 );
		if(j) {
			jx_array_insert(a, j);
		}
	} else if(!strcmp(request, "task_status") || !strcmp(request, "tasks")) {
		struct work_queue_task *t;
		struct work_queue_worker *w;
		struct jx *j;
		uint64_t taskid;

		itable_firstkey(q->tasks);
		while(itable_nextkey(q->tasks,&taskid,(void**)&t)) {
			w = itable_lookup(q->worker_task_map, taskid);
			work_queue_task_state_t state = (uintptr_t) itable_lookup(q->task_state_map, taskid);
			if(w) {
				j = task_to_jx(q,t,task_state_str(state),w->hostname);
				if(j) {
					// Include detailed information on where the task is running:
					// address and port, workspace
					jx_insert_string(j, "address_port", w->addrport);

					// Timestamps on running task related events
					jx_insert_integer(j, "time_when_submitted", t->time_when_submitted);
					jx_insert_integer(j, "time_when_commit_start", t->time_when_commit_start);
					jx_insert_integer(j, "time_when_commit_end", t->time_when_commit_end);
					jx_insert_integer(j, "current_time", timestamp_get());

					jx_array_insert(a, j);
				}
			} else {
				j = task_to_jx(q,t,task_state_str(state),0);
				if(j) {
					jx_array_insert(a, j);
				}
			}
		}
	} else if(!strcmp(request, "worker_status") || !strcmp(request, "workers")) {
		struct work_queue_worker *w;
		struct jx *j;
		char *key;

		hash_table_firstkey(q->worker_table);
		while(hash_table_nextkey(q->worker_table,&key,(void**)&w)) {
			// If the worker has not been initialized, ignore it.
			if(!strcmp(w->hostname, "unknown")) continue;
			j = worker_to_jx(q, w);
			if(j) {
				jx_array_insert(a, j);
			}
		}
	} else if(!strcmp(request, "wable_status") || !strcmp(request, "categories")) {
		jx_delete(a);
		a = categories_to_jx(q);
	} else {
		debug(D_WQ, "Unknown status request: '%s'", request);
		jx_delete(a);
		a = NULL;
	}

	return a;
}


static work_queue_msg_code_t process_queue_status( struct work_queue *q, struct work_queue_worker *target, const char *line, time_t stoptime )
{
	struct link *l = target->link;

	struct jx *a = construct_status_message(q, line);
	target->type = WORKER_TYPE_STATUS;

	free(target->hostname);
	target->hostname = xxstrdup("QUEUE_STATUS");

	if(!a) {
		debug(D_WQ, "Unknown status request: '%s'", line);
		return MSG_FAILURE;
	}

	jx_print_link(a,l,stoptime);
	jx_delete(a);

	return MSG_PROCESSED_DISCONNECT;
}

static work_queue_msg_code_t process_resource( struct work_queue *q, struct work_queue_worker *w, const char *line )
{
	char resource_name[WORK_QUEUE_LINE_MAX];
	struct work_queue_resource r;

	int n = sscanf(line, "resource %s %"PRId64" %"PRId64" %"PRId64, resource_name, &r.total, &r.smallest, &r.largest);

	if(n == 2 && !strcmp(resource_name,"tag"))
	{
		/* Shortcut, total has the tag, as "resources tag" only sends one value */
		w->resources->tag = r.total;
	} else if(n == 4) {

		/* inuse is computed by the manager, so we save it here */
		int64_t inuse;

		if(!strcmp(resource_name,"cores")) {
			inuse = w->resources->cores.inuse;
			w->resources->cores = r;
			w->resources->cores.inuse = inuse;
		} else if(!strcmp(resource_name,"memory")) {
			inuse = w->resources->memory.inuse;
			w->resources->memory = r;
			w->resources->memory.inuse = inuse;
		} else if(!strcmp(resource_name,"disk")) {
			inuse = w->resources->disk.inuse;
			w->resources->disk = r;
			w->resources->disk.inuse = inuse;
		} else if(!strcmp(resource_name,"gpus")) {
			inuse = w->resources->gpus.inuse;
			w->resources->gpus = r;
			w->resources->gpus.inuse = inuse;
		} else if(!strcmp(resource_name,"workers")) {
			inuse = w->resources->workers.inuse;
			w->resources->workers = r;
			w->resources->workers.inuse = inuse;
		}
		else if(string_prefix_is(resource_name, "coprocess_cores")) {
			w->coprocess_resources->cores = r;
		} else if(string_prefix_is(resource_name, "coprocess_memory")) {
			w->coprocess_resources->memory = r;
		} else if(string_prefix_is(resource_name, "coprocess_disk")) {
			w->coprocess_resources->disk = r;
		} else if(string_prefix_is(resource_name, "coprocess_gpus")) {
			w->coprocess_resources->gpus = r;
		}
	} else {
		return MSG_FAILURE;
	}

	return MSG_PROCESSED;
}

static work_queue_msg_code_t process_feature( struct work_queue *q, struct work_queue_worker *w, const char *line )
{
	char feature[WORK_QUEUE_LINE_MAX];
	char fdec[WORK_QUEUE_LINE_MAX];

	int n = sscanf(line, "feature %s", feature);

	if(n != 1) {
		return MSG_FAILURE;
	}

	if(!w->features)
		w->features = hash_table_create(4,0);

	url_decode(feature, fdec, WORK_QUEUE_LINE_MAX);

	debug(D_WQ, "Feature found: %s\n", fdec);

	hash_table_insert(w->features, fdec, (void **) 1);

	return MSG_PROCESSED;
}

static work_queue_result_code_t handle_worker(struct work_queue *q, struct link *l)
{
	char line[WORK_QUEUE_LINE_MAX];
	char key[WORK_QUEUE_LINE_MAX];
	struct work_queue_worker *w;

	link_to_hash_key(l, key);
	w = hash_table_lookup(q->worker_table, key);

	work_queue_msg_code_t mcode;
	mcode = recv_worker_msg(q, w, line, sizeof(line));

	// We only expect asynchronous status queries and updates here.

	switch(mcode) {
		case MSG_PROCESSED:
			// A status message was received and processed.
			return WQ_SUCCESS;
			break;

		case MSG_PROCESSED_DISCONNECT:
			// A status query was received and processed, so disconnect.
			remove_worker(q, w, WORKER_DISCONNECT_STATUS_WORKER);
			return WQ_SUCCESS;

		case MSG_NOT_PROCESSED:
			debug(D_WQ, "Invalid message from worker %s (%s): %s", w->hostname, w->addrport, line);
			q->stats->workers_lost++;
			remove_worker(q, w, WORKER_DISCONNECT_FAILURE);
			return WQ_WORKER_FAILURE;
			break;

		case MSG_FAILURE:
			debug(D_WQ, "Failed to read from worker %s (%s)", w->hostname, w->addrport);
			q->stats->workers_lost++;
			remove_worker(q, w, WORKER_DISCONNECT_FAILURE);
			return WQ_WORKER_FAILURE;
	}

	return WQ_SUCCESS;
}

static int build_poll_table(struct work_queue *q, struct link *manager)
{
	int n = 0;
	char *key;
	struct work_queue_worker *w;

	// Allocate a small table, if it hasn't been done yet.
	if(!q->poll_table) {
		q->poll_table = malloc(sizeof(*q->poll_table) * q->poll_table_size);
		if(!q->poll_table) {
			//if we can't allocate a poll table, we can't do anything else.
			fatal("allocating memory for poll table failed.");
		}
	}

	// The first item in the poll table is the manager link, which accepts new connections.
	q->poll_table[0].link = q->manager_link;
	q->poll_table[0].events = LINK_READ;
	q->poll_table[0].revents = 0;
	n = 1;

	if(manager) {
		/* foreman uplink */
		q->poll_table[1].link = manager;
		q->poll_table[1].events = LINK_READ;
		q->poll_table[1].revents = 0;
		n++;
	}

	// For every worker in the hash table, add an item to the poll table
	hash_table_firstkey(q->worker_table);
	while(hash_table_nextkey(q->worker_table, &key, (void **) &w)) {
		// If poll table is not large enough, reallocate it
		if(n >= q->poll_table_size) {
			q->poll_table_size *= 2;
			q->poll_table = realloc(q->poll_table, sizeof(*q->poll_table) * q->poll_table_size);
			if(q->poll_table == NULL) {
				//if we can't allocate a poll table, we can't do anything else.
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

/*
Send a symbolic link to the remote worker.
Note that the target of the link is sent
as the "body" of the link, following the
message header.
*/

static int send_symlink( struct work_queue *q, struct work_queue_worker *w, struct work_queue_task *t, const char *localname, const char *remotename, int64_t *total_bytes )
{
	char target[WORK_QUEUE_LINE_MAX];

	int length = readlink(localname,target,sizeof(target));
	if(length<0) return WQ_APP_FAILURE;

	char remotename_encoded[WORK_QUEUE_LINE_MAX];
	url_encode(remotename,remotename_encoded,sizeof(remotename_encoded));

	send_worker_msg(q,w,"symlink %s %d\n",remotename_encoded,length);

	link_write(w->link,target,length,time(0)+q->long_timeout);

	*total_bytes += length;

	return WQ_SUCCESS;
}

/*
Send a single file (or a piece of a file) to the remote worker.
The transfer time is controlled by the size of the file.
If the transfer takes too long, then abort.
*/

static int send_file( struct work_queue *q, struct work_queue_worker *w, struct work_queue_task *t, const char *localname, const char *remotename, off_t offset, int64_t length, struct stat info, int64_t *total_bytes )
{
	time_t stoptime;
	timestamp_t effective_stoptime = 0;
	int64_t actual = 0;

	/* normalize the mode so as not to set up invalid permissions */
	int mode = ( info.st_mode | 0x600 ) & 0777;

	if(!length) {
		length = info.st_size;
	}

	int fd = open(localname, O_RDONLY, 0);
	if(fd < 0) {
		debug(D_NOTICE, "Cannot open file %s: %s", localname, strerror(errno));
		return WQ_APP_FAILURE;
	}

	/* If we are sending only a piece of the file, seek there first. */

	if (offset >= 0 && (offset+length) <= info.st_size) {
		if(lseek(fd, offset, SEEK_SET) == -1) {
			debug(D_NOTICE, "Cannot seek file %s to offset %lld: %s", localname, (long long) offset, strerror(errno));
			close(fd);
			return WQ_APP_FAILURE;
		}
	} else {
		debug(D_NOTICE, "File specification %s (%lld:%lld) is invalid", localname, (long long) offset, (long long) offset+length);
		close(fd);
		return WQ_APP_FAILURE;
	}

	if(q->bandwidth) {
		effective_stoptime = (length/q->bandwidth)*1000000 + timestamp_get();
	}

	/* filenames are url-encoded to avoid problems with spaces, etc */
	char remotename_encoded[WORK_QUEUE_LINE_MAX];
	url_encode(remotename,remotename_encoded,sizeof(remotename_encoded));

	stoptime = time(0) + get_transfer_wait_time(q, w, t, length);
	send_worker_msg(q,w, "put %s %"PRId64" 0%o\n",remotename_encoded, length, mode );
	actual = link_stream_from_fd(w->link, fd, length, stoptime);
	close(fd);

	*total_bytes += actual;

	if(actual != length) return WQ_WORKER_FAILURE;

	timestamp_t current_time = timestamp_get();
	if(effective_stoptime && effective_stoptime > current_time) {
		usleep(effective_stoptime - current_time);
	}

	return WQ_SUCCESS;
}

/* Need prototype here to address mutually recursive code. */

static work_queue_result_code_t send_item( struct work_queue *q, struct work_queue_worker *w, struct work_queue_task *t, const char *name, const char *remotename, int64_t offset, int64_t length, int64_t * total_bytes, int follow_links );

/*
Send a directory and all of its contents using the new streaming protocol.
Do this by sending a "dir" prefix, then all of the directory contents,
and then an "end" marker.
*/

static work_queue_result_code_t send_directory( struct work_queue *q, struct work_queue_worker *w, struct work_queue_task *t, const char *localname, const char *remotename, int64_t * total_bytes )
{
	DIR *dir = opendir(localname);
	if(!dir) {
		debug(D_NOTICE, "Cannot open dir %s: %s", localname, strerror(errno));
		return WQ_APP_FAILURE;
	}

	work_queue_result_code_t result = WQ_SUCCESS;

	char remotename_encoded[WORK_QUEUE_LINE_MAX];
	url_encode(remotename,remotename_encoded,sizeof(remotename_encoded));

	send_worker_msg(q,w,"dir %s\n",remotename_encoded);

	struct dirent *d;
	while((d = readdir(dir))) {
		if(!strcmp(d->d_name, ".") || !strcmp(d->d_name, "..")) continue;

		char *localpath = string_format("%s/%s",localname,d->d_name);

		result = send_item( q, w, t, localpath, d->d_name, 0, 0, total_bytes, 0 );

		free(localpath);

		if(result != WQ_SUCCESS) break;
	}

	send_worker_msg(q,w,"end\n");

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


static work_queue_result_code_t send_item( struct work_queue *q, struct work_queue_worker *w, struct work_queue_task *t, const char *localpath, const char *remotepath, int64_t offset, int64_t length, int64_t * total_bytes, int follow_links )
{
	struct stat info;
	int result = WQ_SUCCESS;

	if(follow_links) {
		result = stat(localpath,&info);
	} else {
		result = lstat(localpath,&info);
	}

	if(result>=0) {
		if(S_ISDIR(info.st_mode))  {
			result = send_directory( q, w, t, localpath, remotepath, total_bytes );
		} else if(S_ISLNK(info.st_mode)) {
			result = send_symlink( q, w, t, localpath, remotepath, total_bytes );
		} else if(S_ISREG(info.st_mode)) {
			result = send_file( q, w, t, localpath, remotepath, offset, length, info, total_bytes );
		} else {
			debug(D_NOTICE,"skipping unusual file: %s",strerror(errno));
		}
	} else {
		debug(D_NOTICE, "cannot stat file %s: %s", localpath, strerror(errno));
		result = WQ_APP_FAILURE;
	}

	return result;
}

/*
Send an item to a remote worker, if it is not already cached.
The local file name should already have been expanded by the caller.
If it is in the worker, but a new version is available, warn and return.
We do not want to rewrite the file while some other task may be using it.
Otherwise, send it to the worker.
*/

static work_queue_result_code_t send_item_if_not_cached( struct work_queue *q, struct work_queue_worker *w, struct work_queue_task *t, struct work_queue_file *tf, const char *expanded_local_name, int64_t * total_bytes)
{
	struct stat local_info;
	if(lstat(expanded_local_name, &local_info) < 0) {
		debug(D_NOTICE, "Cannot stat file %s: %s", expanded_local_name, strerror(errno));
		return WQ_APP_FAILURE;
	}

	struct remote_file_info *remote_info = hash_table_lookup(w->current_files, tf->cached_name);

	if(remote_info && (remote_info->mtime != local_info.st_mtime || remote_info->size != local_info.st_size)) {
		debug(D_NOTICE|D_WQ, "File %s changed locally. Task %d will be executed with an older version.", expanded_local_name, t->taskid);
		return WQ_SUCCESS;
	} else if(!remote_info) {

		if(tf->offset==0 && tf->length==0) {
			debug(D_WQ, "%s (%s) needs file %s as '%s'", w->hostname, w->addrport, expanded_local_name, tf->cached_name);
		} else {
			debug(D_WQ, "%s (%s) needs file %s (offset %lld length %lld) as '%s'", w->hostname, w->addrport, expanded_local_name, (long long) tf->offset, (long long) tf->length, tf->cached_name );
		}

		work_queue_result_code_t result;
		result = send_item(q, w, t, expanded_local_name, tf->cached_name, tf->offset, tf->piece_length, total_bytes, 1 );

		if(result == WQ_SUCCESS && tf->flags & WORK_QUEUE_CACHE) {
			remote_info = remote_file_info_create(tf->type,local_info.st_size,local_info.st_mtime);
			hash_table_insert(w->current_files, tf->cached_name, remote_info);
		}

		return result;
	} else {
		/* Up-to-date file on the worker, we do nothing. */
		return WQ_SUCCESS;
	}
}

/**
 *	This function expands Work Queue environment variables such as
 * 	$OS, $ARCH, that are specified in the definition of Work Queue
 * 	input files. It expands these variables based on the info reported
 *	by each connected worker.
 *	Will always return a non-empty string. That is if no match is found
 *	for any of the environment variables, it will return the input string
 *	as is.
 * 	*/
static char *expand_envnames(struct work_queue_worker *w, const char *payload)
{
	char *expanded_name;
	char *str, *curr_pos;
	char *delimtr = "$";
	char *token;

	// Shortcut: If no dollars anywhere, duplicate the whole string.
	if(!strchr(payload,'$')) return strdup(payload);

	str = xxstrdup(payload);

	expanded_name = (char *) malloc(strlen(payload) + (50 * sizeof(char)));
	if(expanded_name == NULL) {
		debug(D_NOTICE, "Cannot allocate memory for filename %s.\n", payload);
		return NULL;
	} else {
		//Initialize to null byte so it works correctly with strcat.
		*expanded_name = '\0';
	}

	token = strtok(str, delimtr);
	while(token) {
		if((curr_pos = strstr(token, "ARCH"))) {
			if((curr_pos - token) == 0) {
				strcat(expanded_name, w->arch);
				strcat(expanded_name, token + 4);
			} else {
				//No match. So put back '$' and rest of the string.
				strcat(expanded_name, "$");
				strcat(expanded_name, token);
			}
		} else if((curr_pos = strstr(token, "OS"))) {
			if((curr_pos - token) == 0) {
				//Cygwin oddly reports OS name in all caps and includes version info.
				if(strstr(w->os, "CYGWIN")) {
					strcat(expanded_name, "Cygwin");
				} else {
					strcat(expanded_name, w->os);
				}
				strcat(expanded_name, token + 2);
			} else {
				strcat(expanded_name, "$");
				strcat(expanded_name, token);
			}
		} else {
			//If token and str don't point to same location, then $ sign was before token and needs to be put back.
			if((token - str) > 0) {
				strcat(expanded_name, "$");
			}
			strcat(expanded_name, token);
		}
		token = strtok(NULL, delimtr);
	}

	free(str);

	debug(D_WQ, "File name %s expanded to %s for %s (%s).", payload, expanded_name, w->hostname, w->addrport);

	return expanded_name;
}

/*
Send a url or remote command used to generate a cached file,
if it has not already been cached there.  Note that the length
may be an estimate at this point and will be updated by return
message once the object is actually loaded into the cache.
*/

static work_queue_result_code_t send_special_if_not_cached( struct work_queue *q, struct work_queue_worker *w, struct work_queue_task *t, struct work_queue_file *tf, const char *typestring )
{
	if(hash_table_lookup(w->current_files,tf->cached_name)) return WQ_SUCCESS;

	char source_encoded[WORK_QUEUE_LINE_MAX];
	char cached_name_encoded[WORK_QUEUE_LINE_MAX];

	url_encode(tf->payload,source_encoded,sizeof(source_encoded));
	url_encode(tf->cached_name,cached_name_encoded,sizeof(cached_name_encoded));

	send_worker_msg(q,w,"%s %s %s %d %o\n",typestring, source_encoded, cached_name_encoded, tf->length, 0777);

	if(tf->flags & WORK_QUEUE_CACHE) {
		struct remote_file_info *remote_info = remote_file_info_create(tf->type,tf->length,time(0));
		hash_table_insert(w->current_files,tf->cached_name,remote_info);
	}

	return WQ_SUCCESS;
}

static work_queue_result_code_t send_input_file(struct work_queue *q, struct work_queue_worker *w, struct work_queue_task *t, struct work_queue_file *f)
{

	int64_t total_bytes = 0;
	int64_t actual = 0;
	work_queue_result_code_t result = WQ_SUCCESS; //return success unless something fails below

	timestamp_t open_time = timestamp_get();

	switch (f->type) {

	case WORK_QUEUE_BUFFER:
		debug(D_WQ, "%s (%s) needs literal as %s", w->hostname, w->addrport, f->remote_name);
		time_t stoptime = time(0) + get_transfer_wait_time(q, w, t, f->length);
		send_worker_msg(q,w, "put %s %d %o\n",f->cached_name, f->length, 0777 );
		actual = link_putlstring(w->link, f->payload, f->length, stoptime);
		if(actual!=f->length) {
			result = WQ_WORKER_FAILURE;
		}
		total_bytes = actual;
		break;

	case WORK_QUEUE_REMOTECMD:
		debug(D_WQ, "%s (%s) will get %s via remote command \"%s\"", w->hostname, w->addrport, f->remote_name, f->payload);
		result = send_special_if_not_cached(q,w,t,f,"putcmd");
		break;

	case WORK_QUEUE_URL:
		debug(D_WQ, "%s (%s) will get %s from url %s", w->hostname, w->addrport, f->remote_name, f->payload);
		result = send_special_if_not_cached(q,w,t,f,"puturl");
		break;

	case WORK_QUEUE_DIRECTORY:
		debug(D_WQ, "%s (%s) will create directory %s", w->hostname, w->addrport, f->remote_name);
  		// Do nothing.  Empty directories are handled by the task specification, while recursive directories are implemented as WORK_QUEUE_FILEs
		break;

	case WORK_QUEUE_FILE:
	case WORK_QUEUE_FILE_PIECE: {
		char *expanded_payload = expand_envnames(w, f->payload);
		if(expanded_payload) {
			result = send_item_if_not_cached(q,w,t,f,expanded_payload,&total_bytes);
			free(expanded_payload);
		} else {
			result = WQ_APP_FAILURE; //signal app-level failure.
		}
		break;
		}
	}

	if(result == WQ_SUCCESS) {
		timestamp_t close_time = timestamp_get();
		timestamp_t elapsed_time = close_time-open_time;

		t->bytes_sent        += total_bytes;
		t->bytes_transferred += total_bytes;

		w->total_bytes_transferred += total_bytes;
		w->total_transfer_time     += elapsed_time;

		q->stats->bytes_sent += total_bytes;

		// Write to the transaction log.
		write_transaction_transfer(q, w, t, f, total_bytes, elapsed_time, WORK_QUEUE_INPUT);

		// Avoid division by zero below.
		if(elapsed_time==0) elapsed_time = 1;

		if(total_bytes > 0) {
			debug(D_WQ, "%s (%s) received %.2lf MB in %.02lfs (%.02lfs MB/s) average %.02lfs MB/s",
				w->hostname,
				w->addrport,
				total_bytes / 1000000.0,
				elapsed_time / 1000000.0,
				(double) total_bytes / elapsed_time,
				(double) w->total_bytes_transferred / w->total_transfer_time
			);
		}
	} else {
		debug(D_WQ, "%s (%s) failed to send %s (%" PRId64 " bytes sent).",
			w->hostname,
			w->addrport,
			f->type == WORK_QUEUE_BUFFER ? "literal data" : f->payload,
			total_bytes);

		if(result == WQ_APP_FAILURE) {
			update_task_result(t, WORK_QUEUE_RESULT_INPUT_MISSING);
		}
	}

	return result;
}

static work_queue_result_code_t send_input_files( struct work_queue *q, struct work_queue_worker *w, struct work_queue_task *t )
{
	struct work_queue_file *f;
	struct stat s;

	// Check for existence of each input file first.
	// If any one fails to exist, set the failure condition and return failure.
	if(t->input_files) {
		list_first_item(t->input_files);
		while((f = list_next_item(t->input_files))) {
			if(f->type == WORK_QUEUE_FILE || f->type == WORK_QUEUE_FILE_PIECE) {
				char * expanded_payload = expand_envnames(w, f->payload);
				if(!expanded_payload) {
					update_task_result(t, WORK_QUEUE_RESULT_INPUT_MISSING);
					return WQ_APP_FAILURE;
				}
				if(stat(expanded_payload, &s) != 0) {
					debug(D_WQ,"Could not stat %s: %s\n", expanded_payload, strerror(errno));
					free(expanded_payload);
					update_task_result(t, WORK_QUEUE_RESULT_INPUT_MISSING);
					return WQ_APP_FAILURE;
				}
				free(expanded_payload);
			}
		}
	}

	// Send each of the input files.
	// If any one fails to be sent, return failure.
	if(t->input_files) {
		list_first_item(t->input_files);
		while((f = list_next_item(t->input_files))) {
			work_queue_result_code_t result = send_input_file(q,w,t,f);
			if(result != WQ_SUCCESS) {
				return result;
			}
		}
	}

	return WQ_SUCCESS;
}

static struct rmsummary *task_worker_box_size(struct work_queue *q, struct work_queue_worker *w, struct work_queue_task *t) {

	const struct rmsummary *min = task_min_resources(q, t);
	const struct rmsummary *max = task_max_resources(q, t);

	struct rmsummary *limits = rmsummary_create(-1);

	rmsummary_merge_override(limits, max);

	int use_whole_worker = 1;
	if(q->proportional_resources) {
		double max_proportion = -1;
		if(w->resources->cores.largest > 0) {
			max_proportion = MAX(max_proportion, limits->cores / w->resources->cores.largest);
		}

		if(w->resources->memory.largest > 0) {
			max_proportion = MAX(max_proportion, limits->memory / w->resources->memory.largest);
		}

		if(w->resources->disk.largest > 0) {
			max_proportion = MAX(max_proportion, limits->disk / w->resources->disk.largest);
		}

		if(w->resources->gpus.largest > 0) {
			max_proportion = MAX(max_proportion, limits->gpus / w->resources->gpus.largest);
		}

		//if max_proportion > 1, then the task does not fit the worker for the
		//specified resources. For the unspecified resources we use the whole
		//worker as not to trigger a warning when checking for tasks that can't
		//run on any available worker.
		if (max_proportion > 1){
			use_whole_worker = 1;
		}
		else if(max_proportion > 0) {
			use_whole_worker = 0;

			// adjust max_proportion so that an integer number of tasks fit the
			// worker.
			if(q->proportional_whole_tasks) {
				max_proportion = 1.0/(floor(1.0/max_proportion));
			}

			/* when cores are unspecified, they are set to 0 if gpus are specified.
			 * Otherwise they get a proportion according to specified
			 * resources. Tasks will get at least one core. */
			if(limits->cores < 0) {
				if(limits->gpus > 0) {
					limits->cores = 0;
				} else {
					limits->cores = MAX(1, floor(w->resources->cores.largest * max_proportion));
				}
			}

			if(limits->gpus < 0) {
				/* unspecified gpus are always 0 */
				limits->gpus = 0;
			}

			if(limits->memory < 0) {
				limits->memory = MAX(1, floor(w->resources->memory.largest * max_proportion));
			}

			if(limits->disk < 0) {
				limits->disk = MAX(1, floor(w->resources->disk.largest * max_proportion));
			}
		}
	}

	if(limits->cores < 1 && limits->gpus < 1 && limits->memory < 1 && limits->disk < 1) {
		/* no resource was specified, using whole worker */
		use_whole_worker = 1;
	}

	if((limits->cores > 0 && limits->cores >= w->resources->cores.largest) ||
			(limits->gpus > 0 && limits->gpus >= w->resources->gpus.largest) ||
			(limits->memory > 0 && limits->memory >= w->resources->memory.largest) ||
			(limits->disk > 0 && limits->disk >= w->resources->disk.largest)) {
		/* at least one specified resource would use the whole worker, thus
		 * using whole worker for all unspecified resources. */
		use_whole_worker = 1;
	}

	if(use_whole_worker) {
		/* default cores for tasks that define gpus is 0 */
		if(limits->cores <= 0) {
			limits->cores = limits->gpus > 0 ? 0 : w->resources->cores.largest;
		}

		/* default gpus is 0 */
		if(limits->gpus <= 0) {
			limits->gpus = 0;
		}

		if(limits->memory <= 0) {
			limits->memory = w->resources->memory.largest;
		}

		if(limits->disk <= 0) {
			limits->disk = w->resources->disk.largest;
		}
	}

	/* never go below specified min resources. */
	rmsummary_merge_max(limits, min);

	return limits;
}

static work_queue_result_code_t start_one_task(struct work_queue *q, struct work_queue_worker *w, struct work_queue_task *t)
{
	/* wrap command at the last minute, so that we have the updated information
	 * about resources. */
	struct rmsummary *limits = task_worker_box_size(q, w, t);

	char *command_line;
	if(q->monitor_mode && !t->coprocess) {
		command_line = work_queue_monitor_wrap(q, w, t, limits);
	} else {
		command_line = xxstrdup(t->command_line);
	}

	work_queue_result_code_t result = send_input_files(q, w, t);

	if (result != WQ_SUCCESS) {
		free(command_line);
		return result;
	}

	send_worker_msg(q,w, "task %lld\n",  (long long) t->taskid);

	long long cmd_len = strlen(command_line);
	send_worker_msg(q,w, "cmd %lld\n", (long long) cmd_len);
	link_putlstring(w->link, command_line, cmd_len, /* stoptime */ time(0) + (w->type == WORKER_TYPE_FOREMAN ? q->long_timeout : q->short_timeout));
	debug(D_WQ, "%s\n", command_line);
	free(command_line);


	if(t->coprocess) {
		cmd_len = strlen(t->coprocess);
		send_worker_msg(q,w, "coprocess %lld\n", (long long) cmd_len);
		link_putlstring(w->link, t->coprocess, cmd_len, /* stoptime */ time(0) + (w->type == WORKER_TYPE_FOREMAN ? q->long_timeout : q->short_timeout));
	}

	send_worker_msg(q,w, "category %s\n", t->category);

	send_worker_msg(q,w, "cores %s\n",  rmsummary_resource_to_str("cores", limits->cores, 0));
	send_worker_msg(q,w, "gpus %s\n",   rmsummary_resource_to_str("gpus", limits->gpus, 0));
	send_worker_msg(q,w, "memory %s\n", rmsummary_resource_to_str("memory", limits->memory, 0));
	send_worker_msg(q,w, "disk %s\n",   rmsummary_resource_to_str("disk", limits->disk, 0));

	/* Do not specify end, wall_time if running the resource monitor. We let the monitor police these resources. */
	if(q->monitor_mode == MON_DISABLED) {
		if(limits->end > 0) {
			send_worker_msg(q,w, "end_time %s\n",  rmsummary_resource_to_str("end", limits->end, 0));
		}
		if(limits->wall_time > 0) {
			send_worker_msg(q,w, "wall_time %s\n", rmsummary_resource_to_str("wall_time", limits->wall_time, 0));
		}
	}

	itable_insert(w->current_tasks_boxes, t->taskid, limits);
	rmsummary_merge_override(t->resources_allocated, limits);

	/* Note that even when environment variables after resources, values for
	 * CORES, MEMORY, etc. will be set at the worker to the values of
	 * specify_*, if used. */
	char *var;
	list_first_item(t->env_list);
	while((var=list_next_item(t->env_list))) {
		send_worker_msg(q, w,"env %zu\n%s\n", strlen(var), var);
	}

	if(t->input_files) {
		struct work_queue_file *tf;
		list_first_item(t->input_files);
		while((tf = list_next_item(t->input_files))) {
			if(tf->type == WORK_QUEUE_DIRECTORY) {
				send_worker_msg(q,w, "dir %s\n", tf->remote_name);
			} else {
				char remote_name_encoded[PATH_MAX];
				url_encode(tf->remote_name, remote_name_encoded, PATH_MAX);
				send_worker_msg(q,w, "infile %s %s %d\n", tf->cached_name, remote_name_encoded, tf->flags);
			}
		}
	}

	if(t->output_files) {
		struct work_queue_file *tf;
		list_first_item(t->output_files);
		while((tf = list_next_item(t->output_files))) {
			char remote_name_encoded[PATH_MAX];
			url_encode(tf->remote_name, remote_name_encoded, PATH_MAX);
			send_worker_msg(q,w, "outfile %s %s %d\n", tf->cached_name, remote_name_encoded, tf->flags);
		}
	}

	// send_worker_msg returns the number of bytes sent, or a number less than
	// zero to indicate errors. We are lazy here, we only check the last
	// message we sent to the worker (other messages may have failed above).
	int result_msg = send_worker_msg(q,w,"end\n");

	if(result_msg > -1)
	{
		debug(D_WQ, "%s (%s) busy on '%s'", w->hostname, w->addrport, t->command_line);
		return WQ_SUCCESS;
	}
	else
	{
		return WQ_WORKER_FAILURE;
	}
}

/*
Store a report summarizing the performance of a completed task.
Keep a list of reports equal to the number of workers connected.
Used for computing queue capacity below.
*/

static void task_report_delete(struct work_queue_task_report *tr) {
	rmsummary_delete(tr->resources);
	free(tr);
}

static void add_task_report(struct work_queue *q, struct work_queue_task *t)
{
	struct work_queue_task_report *tr;
	struct work_queue_stats s;
	work_queue_get_stats(q, &s);

	if(!t->resources_allocated) {
		return;
	}

	// Create a new report object and add it to the list.
	tr = calloc(1, sizeof(struct work_queue_task_report));

	tr->transfer_time = (t->time_when_commit_end - t->time_when_commit_start) + (t->time_when_done - t->time_when_retrieval);
	tr->exec_time     = t->time_workers_execute_last;
	tr->manager_time  = (((t->time_when_done - t->time_when_commit_start) - tr->transfer_time) - tr->exec_time);
	tr->resources     = rmsummary_copy(t->resources_allocated, 0);

	list_push_tail(q->task_reports, tr);

	// Trim the list, but never below its previous size.
	static int count = WORK_QUEUE_TASK_REPORT_MIN_SIZE;
	count = MAX(count, 2*q->stats->tasks_on_workers);

	while(list_size(q->task_reports) >= count) {
	  tr = list_pop_head(q->task_reports);
	  task_report_delete(tr);
	}

	resource_monitor_append_report(q, t);
}

/*
Compute queue capacity based on stored task reports
and the summary of manager activity.
*/

static void compute_capacity(const struct work_queue *q, struct work_queue_stats *s)
{
	struct work_queue_task_report *capacity = calloc(1, sizeof(*capacity));
	capacity->resources = rmsummary_create(0);

	struct work_queue_task_report *tr;
	double alpha = 0.05;
	int count = list_size(q->task_reports);
	int capacity_instantaneous = 0;

	// Compute the average task properties.
	if(count < 1) {
		capacity->resources->cores  = 1;
		capacity->resources->memory = 512;
		capacity->resources->disk   = 1024;
		capacity->resources->gpus   = 0;

		capacity->exec_time     = WORK_QUEUE_DEFAULT_CAPACITY_TASKS;
		capacity->transfer_time = 1;

		q->stats->capacity_weighted = WORK_QUEUE_DEFAULT_CAPACITY_TASKS;
		capacity_instantaneous = WORK_QUEUE_DEFAULT_CAPACITY_TASKS;

		count = 1;
	} else {
		// Sum up the task reports available.
		list_first_item(q->task_reports);
		while((tr = list_next_item(q->task_reports))) {
			capacity->transfer_time += tr->transfer_time;
			capacity->exec_time     += tr->exec_time;
			capacity->manager_time   += tr->manager_time;

			if(tr->resources) {
				capacity->resources->cores  += tr->resources ? tr->resources->cores  : 1;
				capacity->resources->memory += tr->resources ? tr->resources->memory : 512;
				capacity->resources->disk   += tr->resources ? tr->resources->disk   : 1024;
				capacity->resources->gpus   += tr->resources ? tr->resources->gpus   : 0;
			}
		}

		tr = list_peek_tail(q->task_reports);
		if(tr->transfer_time > 0) {
			capacity_instantaneous = DIV_INT_ROUND_UP(tr->exec_time, (tr->transfer_time + tr->manager_time));
			q->stats->capacity_weighted = (int) ceil((alpha * (float) capacity_instantaneous) + ((1.0 - alpha) * q->stats->capacity_weighted));
			time_t ts;
			time(&ts);
		}
	}

	capacity->transfer_time = MAX(1, capacity->transfer_time);
	capacity->exec_time     = MAX(1, capacity->exec_time);
	capacity->manager_time   = MAX(1, capacity->manager_time);

	// Never go below the default capacity
	int64_t ratio = MAX(WORK_QUEUE_DEFAULT_CAPACITY_TASKS, DIV_INT_ROUND_UP(capacity->exec_time, (capacity->transfer_time + capacity->manager_time)));

	q->stats->capacity_tasks  = ratio;
	q->stats->capacity_cores  = DIV_INT_ROUND_UP(capacity->resources->cores  * ratio, count);
	q->stats->capacity_memory = DIV_INT_ROUND_UP(capacity->resources->memory * ratio, count);
	q->stats->capacity_disk   = DIV_INT_ROUND_UP(capacity->resources->disk   * ratio, count);
	q->stats->capacity_gpus   = DIV_INT_ROUND_UP(capacity->resources->gpus   * ratio, count);
	q->stats->capacity_instantaneous = DIV_INT_ROUND_UP(capacity_instantaneous, 1);

	task_report_delete(capacity);
}

void compute_manager_load(struct work_queue *q, int task_activity) {

	double alpha = 0.05;

	double load = q->stats->manager_load;

	if(task_activity) {
		load = load * (1 - alpha) + 1 * alpha;
	} else {
		load = load * (1 - alpha) + 0 * alpha;
	}

	q->stats->manager_load = load;
}

static int check_hand_against_task(struct work_queue *q, struct work_queue_worker *w, struct work_queue_task *t) {

	/* worker has not reported any resources yet */
	if(w->resources->tag < 0)
		return 0;

	if(w->resources->workers.total < 1) {
		return 0;
	}

	if(w->draining) {
		return 0;
	}

	if ( w->factory_name ) {
		struct work_queue_factory_info *f;
		f = hash_table_lookup(q->factory_table, w->factory_name);
		if ( f && f->connected_workers > f->max_workers ) return 0;
	}

	if(w->type != WORKER_TYPE_FOREMAN) {
		struct blocklist_host_info *info = hash_table_lookup(q->worker_blocklist, w->hostname);
		if (info && info->blocked) {
			return 0;
		}
	}

	struct rmsummary *l = task_worker_box_size(q, w, t);
	struct work_queue_resources *r = NULL;
	if (t->coprocess == NULL) {
		r = w->resources;
	} else {
		r = w->coprocess_resources;
	}

	

	int ok = 1;

	if(r->disk.inuse + l->disk > r->disk.total) { /* No overcommit disk */
		ok = 0;
	}

	if((l->cores > r->cores.total) || (r->cores.inuse + l->cores > overcommitted_resource_total(q, r->cores.total))) {
		ok = 0;
	}

	if((l->memory > r->memory.total) || (r->memory.inuse + l->memory > overcommitted_resource_total(q, r->memory.total))) {
		ok = 0;
	}

	if((l->gpus > r->gpus.total) || (r->gpus.inuse + l->gpus > overcommitted_resource_total(q, r->gpus.total))) {
		ok = 0;
	}


	//if worker's end time has not been received
	if(w->end_time < 0){
		ok = 0;
	}

	//if wall time for worker is specified and there's not enough time for task, then not ok
	if(w->end_time > 0){
		double current_time = timestamp_get() / ONE_SECOND;
		if(t->resources_requested->end > 0 && w->end_time < t->resources_requested->end) {
			ok = 0;
		}
		if(t->min_running_time > 0 && w->end_time - current_time < t->min_running_time){
			ok = 0;
		}
	}

	rmsummary_delete(l);

	if(t->features) {
		if(!w->features)
			return 0;

		char *feature;
		list_first_item(t->features);
		while((feature = list_next_item(t->features))) {
			if(!hash_table_lookup(w->features, feature))
				return 0;
		}
	}

	return ok;
}

static struct work_queue_worker *find_worker_by_files(struct work_queue *q, struct work_queue_task *t)
{
	char *key;
	struct work_queue_worker *w;
	struct work_queue_worker *best_worker = 0;
	int64_t most_task_cached_bytes = 0;
	int64_t task_cached_bytes;
	struct remote_file_info *remote_info;
	struct work_queue_file *tf;

	hash_table_firstkey(q->worker_table);
	while(hash_table_nextkey(q->worker_table, &key, (void **) &w)) {
		if( check_hand_against_task(q, w, t) ) {
			task_cached_bytes = 0;
			list_first_item(t->input_files);
			while((tf = list_next_item(t->input_files))) {
				if((tf->type == WORK_QUEUE_FILE || tf->type == WORK_QUEUE_FILE_PIECE) && (tf->flags & WORK_QUEUE_CACHE)) {
					remote_info = hash_table_lookup(w->current_files, tf->cached_name);
					if(remote_info)
						task_cached_bytes += remote_info->size;
				}
			}

			if(!best_worker || task_cached_bytes > most_task_cached_bytes) {
				best_worker = w;
				most_task_cached_bytes = task_cached_bytes;
			}
		}
	}

	return best_worker;
}

static struct work_queue_worker *find_worker_by_fcfs(struct work_queue *q, struct work_queue_task *t)
{
	char *key;
	struct work_queue_worker *w;
	hash_table_firstkey(q->worker_table);
	while(hash_table_nextkey(q->worker_table, &key, (void**)&w)) {
		if( check_hand_against_task(q, w, t) ) {
			return w;
		}
	}
	return NULL;
}

static struct work_queue_worker *find_worker_by_random(struct work_queue *q, struct work_queue_task *t)
{
	char *key;
	struct work_queue_worker *w = NULL;
	int random_worker;
	struct list *valid_workers = list_create();

	hash_table_firstkey(q->worker_table);
	while(hash_table_nextkey(q->worker_table, &key, (void**)&w)) {
		if(check_hand_against_task(q, w, t)) {
			list_push_tail(valid_workers, w);
		}
	}

	w = NULL;
	if(list_size(valid_workers) > 0) {
		random_worker = (rand() % list_size(valid_workers)) + 1;

		while(random_worker && list_size(valid_workers)) {
			w = list_pop_head(valid_workers);
			random_worker--;
		}
	}

	list_delete(valid_workers);
	return w;
}

// 1 if a < b, 0 if a >= b
static int compare_worst_fit(struct work_queue_resources *a, struct work_queue_resources *b)
{
	//Total worker order: free cores > free memory > free disk > free gpus
	if((a->cores.total < b->cores.total))
		return 1;

	if((a->cores.total > b->cores.total))
		return 0;

	//Same number of free cores...
	if((a->memory.total < b->memory.total))
		return 1;

	if((a->memory.total > b->memory.total))
		return 0;

	//Same number of free memory...
	if((a->disk.total < b->disk.total))
		return 1;

	if((a->disk.total > b->disk.total))
		return 0;

	//Same number of free disk...
	if((a->gpus.total < b->gpus.total))
		return 1;

	if((a->gpus.total > b->gpus.total))
		return 0;

	//Number of free resources are the same.
	return 0;
}

static struct work_queue_worker *find_worker_by_worst_fit(struct work_queue *q, struct work_queue_task *t)
{
	char *key;
	struct work_queue_worker *w;
	struct work_queue_worker *best_worker = NULL;

	struct work_queue_resources bres;
	struct work_queue_resources wres;

	memset(&bres, 0, sizeof(struct work_queue_resources));
	memset(&wres, 0, sizeof(struct work_queue_resources));

	hash_table_firstkey(q->worker_table);
	while(hash_table_nextkey(q->worker_table, &key, (void **) &w)) {
		if( check_hand_against_task(q, w, t) ) {

			//Use total field on bres, wres to indicate free resources.
			wres.cores.total   = w->resources->cores.total   - w->resources->cores.inuse;
			wres.memory.total  = w->resources->memory.total  - w->resources->memory.inuse;
			wres.disk.total    = w->resources->disk.total    - w->resources->disk.inuse;
			wres.gpus.total    = w->resources->gpus.total    - w->resources->gpus.inuse;

			if(!best_worker || compare_worst_fit(&bres, &wres))
			{
				best_worker = w;
				memcpy(&bres, &wres, sizeof(struct work_queue_resources));
			}
		}
	}

	return best_worker;
}

static struct work_queue_worker *find_worker_by_time(struct work_queue *q, struct work_queue_task *t)
{
	char *key;
	struct work_queue_worker *w;
	struct work_queue_worker *best_worker = 0;
	double best_time = HUGE_VAL;

	hash_table_firstkey(q->worker_table);
	while(hash_table_nextkey(q->worker_table, &key, (void **) &w)) {
		if(check_hand_against_task(q, w, t)) {
			if(w->total_tasks_complete > 0) {
				double t = (w->total_task_time + w->total_transfer_time) / w->total_tasks_complete;
				if(!best_worker || t < best_time) {
					best_worker = w;
					best_time = t;
				}
			}
		}
	}

	if(best_worker) {
		return best_worker;
	} else {
		return find_worker_by_fcfs(q, t);
	}
}


// compares the resources needed by a task to a given worker
// returns a bitmask that indicates which resource of the task, if any, cannot
// be met by the worker. If the task fits in the worker, it returns 0.
static int is_task_larger_than_worker(struct work_queue *q, struct work_queue_task *t, struct work_queue_worker *w)
{
	if(w->resources->tag < 0) {
		/* quickly return if worker has not sent its resources yet */
		return 0;
	}

	int set = 0;
	struct rmsummary *l = task_worker_box_size(q,w,t);

	// baseline resurce comparison of worker total resources and a task requested resorces

	if((double)w->resources->cores.total < l->cores ) {
		set = set | CORES_BIT;
	}

	if((double)w->resources->memory.total < l->memory ) {
		set = set | MEMORY_BIT;
	}

	if((double)w->resources->disk.total < l->disk ) {
		set = set | DISK_BIT;
	}

	if((double)w->resources->gpus.total < l->gpus ) {
		set = set | GPUS_BIT;
	}
	rmsummary_delete(l);

	return set;
}

// compares the resources needed by a task to all connected workers.
// returns 0 if there is worker than can fit the task. Otherwise it returns a bitmask
// that indicates that there was at least one worker that could not fit that task resource.
static int is_task_larger_than_connected_workers(struct work_queue *q, struct work_queue_task *t)
{
	char *key;
	struct work_queue_worker *w;
	hash_table_firstkey(q->worker_table);

	int bit_set = 0;
	while(hash_table_nextkey(q->worker_table, &key, (void**)&w))
	{
		int new_set = is_task_larger_than_worker(q, t, w);
		if (new_set == 0){
			// Task could run on a currently connected worker, immediately
			// return
			return 0;
		}

		// Inherit the unfit criteria for this task
		bit_set = bit_set | new_set;
	}

	return bit_set;
}

// use task-specific algorithm if set, otherwise default to the queue's setting.
static struct work_queue_worker *find_best_worker(struct work_queue *q, struct work_queue_task *t)
{
	int a = t->worker_selection_algorithm;

	if(a == WORK_QUEUE_SCHEDULE_UNSET) {
		a = q->worker_selection_algorithm;
	}

	switch (a) {
	case WORK_QUEUE_SCHEDULE_FILES:
		return find_worker_by_files(q, t);
	case WORK_QUEUE_SCHEDULE_TIME:
		return find_worker_by_time(q, t);
	case WORK_QUEUE_SCHEDULE_WORST:
		return find_worker_by_worst_fit(q, t);
	case WORK_QUEUE_SCHEDULE_FCFS:
		return find_worker_by_fcfs(q, t);
	case WORK_QUEUE_SCHEDULE_RAND:
	default:
		return find_worker_by_random(q, t);
	}
}

static void count_worker_resources(struct work_queue *q, struct work_queue_worker *w)
{
	struct rmsummary *box;
	uint64_t taskid;

	w->resources->cores.inuse  = 0;
	w->resources->memory.inuse = 0;
	w->resources->disk.inuse   = 0;
	w->resources->gpus.inuse   = 0;

	w->coprocess_resources->cores.inuse  = 0;
	w->coprocess_resources->memory.inuse = 0;
	w->coprocess_resources->disk.inuse   = 0;
	w->coprocess_resources->gpus.inuse   = 0;

	update_max_worker(q, w);

	if(w->resources->workers.total < 1)
	{
		return;
	}

	itable_firstkey(w->current_tasks_boxes);
	while(itable_nextkey(w->current_tasks_boxes, &taskid, (void **)& box)) {
		struct work_queue_task *t = itable_lookup(w->current_tasks, taskid);
		if (t->coprocess) {
			w->coprocess_resources->cores.inuse     += box->cores;
			w->coprocess_resources->memory.inuse    += box->memory;
			w->coprocess_resources->disk.inuse      += box->disk;
			w->coprocess_resources->gpus.inuse      += box->gpus;
		}
		else {
			w->resources->cores.inuse     += box->cores;
			w->resources->memory.inuse    += box->memory;
			w->resources->disk.inuse      += box->disk;
			w->resources->gpus.inuse      += box->gpus;
		}
	}
}

static void update_max_worker(struct work_queue *q, struct work_queue_worker *w) {
	if(!w)
		return;

	if(w->resources->workers.total < 1) {
		return;
	}

	if(q->current_max_worker->cores < w->resources->cores.largest) {
		q->current_max_worker->cores = w->resources->cores.largest;
	}

	if(q->current_max_worker->memory < w->resources->memory.largest) {
		q->current_max_worker->memory = w->resources->memory.largest;
	}

	if(q->current_max_worker->disk < w->resources->disk.largest) {
		q->current_max_worker->disk = w->resources->disk.largest;
	}

	if(q->current_max_worker->gpus < w->resources->gpus.largest) {
		q->current_max_worker->gpus = w->resources->gpus.largest;
	}
}

/* we call this function when a worker is disconnected. For efficiency, we use
 * update_max_worker when a worker sends resource updates. */
static void find_max_worker(struct work_queue *q) {
	q->current_max_worker->cores  = 0;
	q->current_max_worker->memory = 0;
	q->current_max_worker->disk   = 0;
	q->current_max_worker->gpus   = 0;

	char *key;
	struct work_queue_worker *w;
	hash_table_firstkey(q->worker_table);
	while(hash_table_nextkey(q->worker_table, &key, (void **) &w)) {
		if(w->resources->workers.total > 0)
		{
			update_max_worker(q, w);
		}
	}
}

static void commit_task_to_worker(struct work_queue *q, struct work_queue_worker *w, struct work_queue_task *t)
{
	t->hostname = xxstrdup(w->hostname);
	t->host = xxstrdup(w->addrport);

	t->time_when_commit_start = timestamp_get();
	work_queue_result_code_t result = start_one_task(q, w, t);
	t->time_when_commit_end = timestamp_get();

	itable_insert(w->current_tasks, t->taskid, t);
	itable_insert(q->worker_task_map, t->taskid, w); //add worker as execution site for t.

	change_task_state(q, t, WORK_QUEUE_TASK_RUNNING);

	t->try_count += 1;
	q->stats->tasks_dispatched += 1;

	count_worker_resources(q, w);

	if(result != WQ_SUCCESS) {
		debug(D_WQ, "Failed to send task %d to worker %s (%s).", t->taskid, w->hostname, w->addrport);
		handle_failure(q, w, t, result);
	}
}

static void reap_task_from_worker(struct work_queue *q, struct work_queue_worker *w, struct work_queue_task *t, work_queue_task_state_t new_state)
{
	struct work_queue_worker *wr = itable_lookup(q->worker_task_map, t->taskid);

	if(wr != w)
	{
		debug(D_WQ, "Cannot reap task %d from worker. It is not being run by %s (%s)\n", t->taskid, w->hostname, w->addrport);
	} else {
		w->total_task_time += t->time_workers_execute_last;
	}

	//update tables.
	struct rmsummary *task_box = itable_lookup(w->current_tasks_boxes, t->taskid);
	if(task_box)
		rmsummary_delete(task_box);

	itable_remove(w->current_tasks_boxes, t->taskid);
	itable_remove(w->current_tasks, t->taskid);
	itable_remove(q->worker_task_map, t->taskid);
	change_task_state(q, t, new_state);

	count_worker_resources(q, w);
}

static int send_one_task( struct work_queue *q )
{
	struct work_queue_task *t;
	struct work_queue_worker *w;

	timestamp_t now = timestamp_get();

	// Consider each task in the order of priority:
	list_first_item(q->ready_list);
	while( (t = list_next_item(q->ready_list))) {
		// Skip task if min requested start time not met.
		if(t->resources_requested->start > now) continue;

		// Find the best worker for the task at the head of the list
		w = find_best_worker(q,t);

		// If there is no suitable worker, consider the next task.
		if(!w) continue;
		// Otherwise, remove it from the ready list and start it:
		commit_task_to_worker(q,w,t);

		return 1;
	}

	return 0;
}


static void print_large_tasks_warning(struct work_queue *q)
{
	timestamp_t current_time = timestamp_get();
	if(current_time - q->time_last_large_tasks_check < interval_check_for_large_tasks) {
		return;
	}

	q->time_last_large_tasks_check = current_time;

	struct work_queue_task *t;
	int unfit_core = 0;
	int unfit_mem  = 0;
	int unfit_disk = 0;
	int unfit_gpu  = 0;

	struct rmsummary *largest_unfit_task = rmsummary_create(-1);

	list_first_item(q->ready_list);
	while( (t = list_next_item(q->ready_list))){
		// check each task against the queue of connected workers
		int bit_set = is_task_larger_than_connected_workers(q, t);
		if(bit_set) {
			rmsummary_merge_max(largest_unfit_task, task_max_resources(q, t));
			rmsummary_merge_max(largest_unfit_task, task_min_resources(q, t));
		}
		if (bit_set & CORES_BIT) {
			unfit_core++;
		}
		if (bit_set & MEMORY_BIT) {
			unfit_mem++;
		}
		if (bit_set & DISK_BIT) {
			unfit_disk++;
		}
		if (bit_set & GPUS_BIT) {
			unfit_gpu++;
		}
	}

	if(unfit_core || unfit_mem || unfit_disk || unfit_gpu){
		notice(D_WQ,"There are tasks that cannot fit any currently connected worker:\n");
	}

	if(unfit_core) {
		notice(D_WQ,"    %d waiting task(s) need more than %s", unfit_core, rmsummary_resource_to_str("cores", largest_unfit_task->cores, 1));
	}

	if(unfit_mem) {
		notice(D_WQ,"    %d waiting task(s) need more than %s of memory", unfit_mem, rmsummary_resource_to_str("memory", largest_unfit_task->memory, 1));
	}

	if(unfit_disk) {
		notice(D_WQ,"    %d waiting task(s) need more than %s of disk", unfit_disk, rmsummary_resource_to_str("disk", largest_unfit_task->disk, 1));
	}

	if(unfit_gpu) {
		notice(D_WQ,"    %d waiting task(s) need more than %s", unfit_gpu, rmsummary_resource_to_str("gpus", largest_unfit_task->gpus, 1));
	}

	rmsummary_delete(largest_unfit_task);
}

static int receive_one_task( struct work_queue *q )
{
	struct work_queue_task *t;

	struct work_queue_worker *w;
	uint64_t taskid;

	itable_firstkey(q->tasks);
	while( itable_nextkey(q->tasks, &taskid, (void **) &t) ) {
		if( task_state_is(q, taskid, WORK_QUEUE_TASK_WAITING_RETRIEVAL) ) {
			w = itable_lookup(q->worker_task_map, taskid);
			fetch_output_from_worker(q, w, taskid);
			// Shutdown worker if appropriate.
			if ( w->factory_name ) {
				struct work_queue_factory_info *f;
				f = hash_table_lookup(q->factory_table, w->factory_name);
				if ( f && f->connected_workers > f->max_workers &&
						itable_size(w->current_tasks) < 1 ) {
					debug(D_WQ, "Final task received from worker %s, shutting down.", w->hostname);
					shut_down_worker(q, w);
				}
			}
			return 1;
		}
	}

	return 0;
}

//Sends keepalives to check if connected workers are responsive, and ask for updates If not, removes those workers.
static void ask_for_workers_updates(struct work_queue *q) {
	struct work_queue_worker *w;
	char *key;
	timestamp_t current_time = timestamp_get();

	hash_table_firstkey(q->worker_table);
	while(hash_table_nextkey(q->worker_table, &key, (void **) &w)) {
		if(q->keepalive_interval > 0) {

			/* we have not received workqueue message from worker yet, so we
			 * simply check again its start_time. */
			if(!strcmp(w->hostname, "unknown")){
				if ((int)((current_time - w->start_time)/1000000) >= q->keepalive_timeout) {
					debug(D_WQ, "Removing worker %s (%s): hasn't sent its initialization in more than %d s", w->hostname, w->addrport, q->keepalive_timeout);
					handle_worker_failure(q, w);
				}
				continue;
			}


			// send new keepalive check only (1) if we received a response since last keepalive check AND
			// (2) we are past keepalive interval
			if(w->last_msg_recv_time > w->last_update_msg_time) {
				int64_t last_update_elapsed_time = (int64_t)(current_time - w->last_update_msg_time)/1000000;
				if(last_update_elapsed_time >= q->keepalive_interval) {
					if(send_worker_msg(q,w, "check\n")<0) {
						debug(D_WQ, "Failed to send keepalive check to worker %s (%s).", w->hostname, w->addrport);
						handle_worker_failure(q, w);
					} else {
						debug(D_WQ, "Sent keepalive check to worker %s (%s)", w->hostname, w->addrport);
						w->last_update_msg_time = current_time;
					}
				}
			} else {
				// we haven't received a message from worker since its last keepalive check. Check if time
				// since we last polled link for responses has exceeded keepalive timeout. If so, remove worker.
				if (q->link_poll_end > w->last_update_msg_time) {
					if ((int)((q->link_poll_end - w->last_update_msg_time)/1000000) >= q->keepalive_timeout) {
						debug(D_WQ, "Removing worker %s (%s): hasn't responded to keepalive check for more than %d s", w->hostname, w->addrport, q->keepalive_timeout);
						handle_worker_failure(q, w);
					}
				}
			}
		}
	}
}

static int abort_slow_workers(struct work_queue *q)
{
	struct category *c;
	char *category_name;

	struct work_queue_worker *w;
	struct work_queue_task *t;
	uint64_t taskid;

	int removed = 0;

	/* optimization. If no category has a fast abort multiplier, simply return. */
	int fast_abort_flag = 0;

	hash_table_firstkey(q->categories);
	while(hash_table_nextkey(q->categories, &category_name, (void **) &c)) {
		struct work_queue_stats *stats = c->wq_stats;
		if(!stats) {
			/* no stats have been computed yet */
			continue;
		}

		if(stats->tasks_done < 10) {
			c->average_task_time = 0;
			continue;
		}

		c->average_task_time = (stats->time_workers_execute_good + stats->time_send_good + stats->time_receive_good) / stats->tasks_done;

		if(c->fast_abort > 0)
			fast_abort_flag = 1;
	}

	if(!fast_abort_flag)
		return 0;

	struct category *c_def = work_queue_category_lookup_or_create(q, "default");

	timestamp_t current = timestamp_get();

	itable_firstkey(q->tasks);
	while(itable_nextkey(q->tasks, &taskid, (void **) &t)) {
		c = work_queue_category_lookup_or_create(q, t->category);
		/* Fast abort deactivated for this category */
		if(c->fast_abort == 0)
			continue;

		timestamp_t runtime = current - t->time_when_commit_start;
		timestamp_t average_task_time = c->average_task_time;

		/* Not enough samples, skip the task. */
		if(average_task_time < 1)
			continue;

		double multiplier;
		if(c->fast_abort > 0) {
			multiplier = c->fast_abort;
		}
		else if(c_def->fast_abort > 0) {
			/* This category uses the default fast abort. (< 0 use default, 0 deactivate). */
			multiplier = c_def->fast_abort;
		}
		else {
			/* Fast abort also deactivated for the default category. */
			continue;
		}

		if(runtime >= (average_task_time * (multiplier + t->fast_abort_count))) {
			w = itable_lookup(q->worker_task_map, t->taskid);
			if(w && (w->type == WORKER_TYPE_WORKER))
			{
				debug(D_WQ, "Task %d is taking too long. Removing from worker.", t->taskid);
				cancel_task_on_worker(q, t, WORK_QUEUE_TASK_READY);
				t->fast_abort_count++;

				/* a task cannot mark two different workers as suspect */
				if(t->fast_abort_count > 1) {
					continue;
				}

				if(w->fast_abort_alarm > 0) {
					/* this is the second task in a row that triggered fast
					 * abort, therefore we have evidence that this a slow
					 * worker (rather than a task) */

					debug(D_WQ, "Removing worker %s (%s): takes too long to execute the current task - %.02lf s (average task execution time by other workers is %.02lf s)", w->hostname, w->addrport, runtime / 1000000.0, average_task_time / 1000000.0);
					work_queue_block_host_with_timeout(q, w->hostname, wq_option_blocklist_slow_workers_timeout);
					remove_worker(q, w, WORKER_DISCONNECT_FAST_ABORT);

					q->stats->workers_fast_aborted++;
					removed++;
				}

				w->fast_abort_alarm = 1;
			}
		}
	}

	return removed;
}

static int shut_down_worker(struct work_queue *q, struct work_queue_worker *w)
{
	if(!w) return 0;

	send_worker_msg(q,w,"exit\n");
	remove_worker(q, w, WORKER_DISCONNECT_EXPLICIT);
	q->stats->workers_released++;

	return 1;
}

static int abort_drained_workers(struct work_queue *q) {
	char *worker_hashkey = NULL;
	struct work_queue_worker *w = NULL;

	int removed = 0;

	hash_table_firstkey(q->worker_table);
	while(hash_table_nextkey(q->worker_table, &worker_hashkey, (void **) &w)) {
		if(w->draining && itable_size(w->current_tasks) == 0) {
			removed++;
			shut_down_worker(q, w);
		}
	}

	return removed;
}


//comparator function for checking if a task matches a given tag.
static int tasktag_comparator(void *t, const void *r) {

	struct work_queue_task *task_in_queue = t;
	const char *tasktag = r;

	if(!task_in_queue->tag && !tasktag) {
		return 1;
	}

	if(!task_in_queue->tag || !tasktag) {
		return 0;
	}

	return !strcmp(task_in_queue->tag, tasktag);
}


static int cancel_task_on_worker(struct work_queue *q, struct work_queue_task *t, work_queue_task_state_t new_state) {

	struct work_queue_worker *w = itable_lookup(q->worker_task_map, t->taskid);

	if (w) {
		//send message to worker asking to kill its task.
		send_worker_msg(q,w, "kill %d\n",t->taskid);
		debug(D_WQ, "Task with id %d is aborted at worker %s (%s) and removed.", t->taskid, w->hostname, w->addrport);

		//Delete any input files that are not to be cached.
		delete_worker_files(q, w, t->input_files, WORK_QUEUE_CACHE | WORK_QUEUE_PREEXIST);

		//Delete all output files since they are not needed as the task was aborted.
		delete_worker_files(q, w, t->output_files, 0);

		//update tables.
		reap_task_from_worker(q, w, t, new_state);

		return 1;
	} else {
		change_task_state(q, t, new_state);
		return 0;
	}
}

static struct work_queue_task *find_task_by_tag(struct work_queue *q, const char *tasktag) {
	struct work_queue_task *t;
	uint64_t taskid;

	itable_firstkey(q->tasks);
	while(itable_nextkey(q->tasks, &taskid, (void**)&t)) {
		if( tasktag_comparator(t, tasktag) ) {
			return t;
		}
	}

	return NULL;
}


static struct work_queue_file *work_queue_file_clone(const struct work_queue_file *file) {
  const int file_t_size = sizeof(struct work_queue_file);
  struct work_queue_file *new = xxmalloc(file_t_size);

  memcpy(new, file, file_t_size);
  //allocate new memory for strings so we don't segfault when the original
  //memory is freed.
  new->payload     = xxstrdup(file->payload);
  new->remote_name = xxstrdup(file->remote_name);

  if(file->cached_name)
	  new->cached_name = xxstrdup(file->cached_name);

  return new;
}


static struct list *work_queue_task_file_list_clone(struct list *list) {
  struct list *new = list_create();
  struct work_queue_file *old_file, *new_file;

  list_first_item(list);
  while ((old_file = list_next_item(list))) {
	new_file = work_queue_file_clone(old_file);
	list_push_tail(new, new_file);
  }
  return new;
}

static struct list *work_queue_task_env_list_clone(struct list *env_list) {
	struct list *new = list_create();
	char *var;
	list_first_item(env_list);
	while((var=list_next_item(env_list))) {
		list_push_tail(new, xxstrdup(var));
	}

	return new;
}


/******************************************************/
/********** work_queue_task public functions **********/
/******************************************************/

struct work_queue_task *work_queue_task_create(const char *command_line)
{
	struct work_queue_task *t = malloc(sizeof(*t));
	if(!t) {
		fprintf(stderr, "Error: failed to allocate memory for task.\n");
		return NULL;
	}
	memset(t, 0, sizeof(*t));

	/* REMEMBER: Any memory allocation done in this function should have a
	 * corresponding copy in work_queue_task_clone. Otherwise we get
	 * double-free segfaults. */

	if(command_line) t->command_line = xxstrdup(command_line);

	t->worker_selection_algorithm = WORK_QUEUE_SCHEDULE_UNSET;
	t->input_files = list_create();
	t->output_files = list_create();
	t->env_list = list_create();
	t->return_status = -1;

	t->result = WORK_QUEUE_RESULT_UNKNOWN;

	t->resource_request   = CATEGORY_ALLOCATION_FIRST;

	/* In the absence of additional information, a task consumes an entire worker. */
	t->resources_requested = rmsummary_create(-1);
	t->resources_measured  = rmsummary_create(-1);
	t->resources_allocated = rmsummary_create(-1);

	t->category = xxstrdup("default");

	return t;
}


struct work_queue_task *work_queue_task_clone(const struct work_queue_task *task)
{
	struct work_queue_task *new = work_queue_task_create(task->command_line);

	if(task->tag) {
		work_queue_task_specify_tag(new, task->tag);
	}

	if(task->category) {
		work_queue_task_specify_category(new, task->category);
	}

	work_queue_task_specify_algorithm(new, task->worker_selection_algorithm);
	work_queue_task_specify_priority(new, task->priority);
	work_queue_task_specify_max_retries(new, task->max_retries);
	work_queue_task_specify_running_time_min(new, task->min_running_time);


	if(task->monitor_output_directory) {
		work_queue_task_specify_monitor_output(new, task->monitor_output_directory);
	}

	if(task->monitor_snapshot_file) {
		work_queue_specify_snapshot_file(new, task->monitor_snapshot_file);
	}

	new->input_files  = work_queue_task_file_list_clone(task->input_files);
	new->output_files = work_queue_task_file_list_clone(task->output_files);
	new->env_list     = work_queue_task_env_list_clone(task->env_list);

	if(task->features) {
		new->features = list_create();
		char *req;
		list_first_item(task->features);
		while((req = list_next_item(task->features))) {
			list_push_tail(new->features, xxstrdup(req));
		}
	}

	if(task->resources_requested) {
		new->resources_requested = rmsummary_copy(task->resources_requested, 0);
	}


	return new;
}


void work_queue_task_specify_command( struct work_queue_task *t, const char *cmd )
{
	if(t->command_line) free(t->command_line);
	t->command_line = xxstrdup(cmd);
}

void work_queue_task_specify_coprocess( struct work_queue_task *t, const char *coprocess )
{
	if(t->coprocess) {
		delete_feature(t, t->coprocess);
		free(t->coprocess);
		t->coprocess = NULL;
	}
	if(coprocess) {
		t->coprocess = string_format("wq_worker_coprocess:%s", coprocess);
		work_queue_task_specify_feature(t, t->coprocess);
	}
}

void work_queue_task_specify_environment_variable( struct work_queue_task *t, const char *name, const char *value )
{
	if(value) {
		list_push_tail(t->env_list,string_format("%s=%s",name,value));
	} else {
		/* Specifications without = indicate variables to me unset. */
		list_push_tail(t->env_list,string_format("%s",name));
	}
}

/* same as above, but with a typo. can't remove as it is part of already published api. */
void work_queue_task_specify_enviroment_variable( struct work_queue_task *t, const char *name, const char *value ) {
	work_queue_task_specify_environment_variable(t, name, value);
}

void work_queue_task_specify_max_retries( struct work_queue_task *t, int64_t max_retries ) {
	if(max_retries < 1) {
		t->max_retries = 0;
	}
	else {
		t->max_retries = max_retries;
	}
}

void work_queue_task_specify_memory( struct work_queue_task *t, int64_t memory )
{
	if(memory < 0)
	{
		t->resources_requested->memory = -1;
	}
	else
	{
		t->resources_requested->memory = memory;
	}
}

void work_queue_task_specify_disk( struct work_queue_task *t, int64_t disk )
{
	if(disk < 0)
	{
		t->resources_requested->disk = -1;
	}
	else
	{
		t->resources_requested->disk = disk;
	}
}

void work_queue_task_specify_cores( struct work_queue_task *t, int cores )
{
	if(cores < 0)
	{
		t->resources_requested->cores = -1;
	}
	else
	{
		t->resources_requested->cores = cores;
	}
}

void work_queue_task_specify_gpus( struct work_queue_task *t, int gpus )
{
	if(gpus < 0)
	{
		t->resources_requested->gpus = -1;
	}
	else
	{
		t->resources_requested->gpus = gpus;
	}
}

void work_queue_task_specify_end_time( struct work_queue_task *t, int64_t useconds )
{
	if(useconds < 1)
	{
		t->resources_requested->end = -1;
	}
	else
	{
		t->resources_requested->end = DIV_INT_ROUND_UP(useconds, ONE_SECOND);
	}
}

void work_queue_task_specify_start_time_min( struct work_queue_task *t, int64_t useconds )
{
	if(useconds < 1)
	{
		t->resources_requested->start = -1;
	}
	else
	{
		t->resources_requested->start = DIV_INT_ROUND_UP(useconds, ONE_SECOND);
	}
}

void work_queue_task_specify_running_time( struct work_queue_task *t, int64_t useconds )
{
	if(useconds < 1)
	{
		t->resources_requested->wall_time = -1;
	}
	else
	{
		t->resources_requested->wall_time = DIV_INT_ROUND_UP(useconds, ONE_SECOND);
	}
}

void work_queue_task_specify_running_time_max( struct work_queue_task *t, int64_t seconds )
{
	work_queue_task_specify_running_time(t, seconds);
}

void work_queue_task_specify_running_time_min( struct work_queue_task *t, int64_t seconds )
{
	if(seconds < 1)
	{
		t->min_running_time = -1;
	}
	else
	{
		t->min_running_time = seconds;
	}
}

void work_queue_task_specify_resources(struct work_queue_task *t, const struct rmsummary *rm) {
	if(!rm)
		return;

	work_queue_task_specify_cores(t,        rm->cores);
	work_queue_task_specify_memory(t,       rm->memory);
	work_queue_task_specify_disk(t,         rm->disk);
	work_queue_task_specify_gpus(t,         rm->gpus);
	work_queue_task_specify_running_time(t, rm->wall_time);
	work_queue_task_specify_running_time_max(t, rm->wall_time);
	work_queue_task_specify_running_time_min(t, t->min_running_time);
	work_queue_task_specify_end_time(t,     rm->end);
}

void work_queue_task_specify_tag(struct work_queue_task *t, const char *tag)
{
	if(t->tag)
		free(t->tag);
	t->tag = xxstrdup(tag);
}

void work_queue_task_specify_category(struct work_queue_task *t, const char *category)
{
	if(t->category)
		free(t->category);

	t->category = xxstrdup(category ? category : "default");
}

void work_queue_task_specify_feature(struct work_queue_task *t, const char *name)
{
	if(!name) {
		return;
	}

	if(!t->features) {
		t->features = list_create();
	}

	list_push_tail(t->features, xxstrdup(name));
}

static void delete_feature(struct work_queue_task *t, const char *name)
{
	if(!t->features) {
		return;
	}

	struct list_cursor *c = list_cursor_create(t->features);

	char *feature;
	for(list_seek(c, 0); list_get(c, (void **) &feature); list_next(c)) {
		if(name && feature && (strcmp(name, feature) == 0)) {
			list_drop(c);
		}
	}

	list_cursor_destroy(c);
}

struct work_queue_file *work_queue_file_create(const char *payload, const char *remote_name, work_queue_file_t type, work_queue_file_flags_t flags)
{
	struct work_queue_file *f;

	f = malloc(sizeof(*f));
	if(!f) {
		debug(D_NOTICE, "Cannot allocate memory for file %s.\n", remote_name);
		return NULL;
	}

	memset(f, 0, sizeof(*f));

	f->remote_name = xxstrdup(remote_name);
	f->type = type;
	f->flags = flags;

	/* WORK_QUEUE_BUFFER needs to set these after the current function returns */
	if(payload) {
		f->payload = xxstrdup(payload);
		f->length  = strlen(payload);
	}

	if(wq_hack_do_not_compute_cached_name) {
  		f->cached_name = xxstrdup(f->payload);
	} else {
		f->cached_name = make_cached_name(f);
	}
	
	return f;
}

int work_queue_task_specify_url(struct work_queue_task *t, const char *file_url, const char *remote_name, work_queue_file_type_t type, work_queue_file_flags_t flags)
{
	struct list *files;
	struct work_queue_file *tf;

	if(!t || !file_url || !remote_name) {
		fprintf(stderr, "Error: Null arguments for task, url, and remote name not allowed in specify_url.\n");
		return 0;
	}
	if(remote_name[0] == '/') {
		fatal("Error: Remote name %s is an absolute path.\n", remote_name);
	}

	if(type == WORK_QUEUE_INPUT) {
		files = t->input_files;

		//check if two different urls map to the same remote name for inputs.
		list_first_item(t->input_files);
		while((tf = (struct work_queue_file*)list_next_item(files))) {
			if(!strcmp(remote_name, tf->remote_name) && strcmp(file_url, tf->payload)) {
				fprintf(stderr, "Error: input url %s conflicts with another input pointing to same remote name (%s).\n", file_url, remote_name);
				return 0;
			}
		}
		//check if there is an output file with the same remote name.
		list_first_item(t->output_files);
		while((tf = (struct work_queue_file*)list_next_item(t->input_files))) {
			if(!strcmp(remote_name, tf->remote_name)){
				fprintf(stderr, "Error: input url %s conflicts with an output pointing to same remote name (%s).\n", file_url, remote_name);
				return 0;
			}
		}
	} else {
		fprintf(stderr, "Error: work_queue_specify_url does not yet support output files.\n");
	  	return 0;
	}

	tf = work_queue_file_create(file_url, remote_name, WORK_QUEUE_URL, flags);
	if(!tf) return 0;

	// length of source data is not known yet.
	tf->length = 0;

	list_push_tail(files, tf);

	return 1;
}

int work_queue_task_specify_file(struct work_queue_task *t, const char *local_name, const char *remote_name, work_queue_file_type_t type, work_queue_file_flags_t flags)
{
	struct list *files;
	struct work_queue_file *tf;

	if(!t || !local_name || !remote_name) {
		fprintf(stderr, "Error: Null arguments for task, local name, and remote name not allowed in specify_file.\n");
		return 0;
	}

	// @param remote_name is the path of the file as on the worker machine. In
	// the Work Queue framework, workers are prohibited from writing to paths
	// outside of their workspaces. When a task is specified, the workspace of
	// the worker(the worker on which the task will be executed) is unlikely to
	// be known. Thus @param remote_name should not be an absolute path.
	if(remote_name[0] == '/') {
		fatal("Error: Remote name %s is an absolute path.\n", remote_name);
	}


	if(type == WORK_QUEUE_INPUT) {
		files = t->input_files;

		//check if two different local names map to the same remote name for inputs.
		list_first_item(t->input_files);
		while((tf = (struct work_queue_file*)list_next_item(t->input_files))) {
			if(!strcmp(remote_name, tf->remote_name) && strcmp(local_name, tf->payload)){
				fprintf(stderr, "Error: input file %s conflicts with another input pointing to same remote name (%s).\n", local_name, remote_name);
				return 0;
			}
		}

		//check if there is an output file with the same remote name.
		list_first_item(t->output_files);
		while((tf = (struct work_queue_file*)list_next_item(t->input_files))) {
			if(!strcmp(remote_name, tf->remote_name)){
				fprintf(stderr, "Error: input file %s conflicts with an output pointing to same remote name (%s).\n", local_name, remote_name);
				return 0;
			}
		}
	} else {
		files = t->output_files;

		//check if two different different remote names map to the same local name for outputs.
		list_first_item(files);
		while((tf = (struct work_queue_file*)list_next_item(files))) {
			if(!strcmp(local_name, tf->payload) && strcmp(remote_name, tf->remote_name)) {
				fprintf(stderr, "Error: output file %s conflicts with another output pointing to same remote name (%s).\n", local_name, remote_name);
				return 0;
			}
		}

		//check if there is an input file with the same remote name.
		list_first_item(t->input_files);
		while((tf = (struct work_queue_file*)list_next_item(t->input_files))) {
			if(!strcmp(remote_name, tf->remote_name)){
				fprintf(stderr, "Error: output file %s conflicts with an input pointing to same remote name (%s).\n", local_name, remote_name);
				return 0;
			}
		}
	}

	tf = work_queue_file_create(local_name, remote_name, WORK_QUEUE_FILE, flags);
	if(!tf) return 0;

	list_push_tail(files, tf);
	return 1;
}

int work_queue_task_specify_directory(struct work_queue_task *t, const char *local_name, const char *remote_name, work_queue_file_type_t type, work_queue_file_flags_t flags, int recursive) {
	struct list *files;
	struct work_queue_file *tf;

	if(!t || !remote_name) {
		fprintf(stderr, "Error: Null arguments for task and remote name not allowed in specify_directory.\n");
		return 0;
	}

	// @param remote_name is the path of the file as on the worker machine. In
	// the Work Queue framework, workers are prohibited from writing to paths
	// outside of their workspaces. When a task is specified, the workspace of
	// the worker(the worker on which the task will be executed) is unlikely to
	// be known. Thus @param remote_name should not be an absolute path.
	if(remote_name[0] == '/') {
		fatal("Error: Remote name %s is an absolute path.\n", remote_name);
	}

	if(type == WORK_QUEUE_OUTPUT || recursive) {
		return work_queue_task_specify_file(t, local_name, remote_name, type, flags);
	}

	files = t->input_files;

	list_first_item(files);
	while((tf = (struct work_queue_file*)list_next_item(files))) {
		if(!strcmp(remote_name, tf->remote_name))
		{	return 0;	}
	}

	//KNOWN HACK: Every file passes through make_cached_name() which expects the
	//payload field to be set. So we simply set the payload to remote name if
	//local name is null. This doesn't affect the behavior of the file transfers.
	const char *payload = local_name ? local_name : remote_name;

	tf = work_queue_file_create(payload, remote_name, WORK_QUEUE_DIRECTORY, flags);
	if(!tf) return 0;

	list_push_tail(files, tf);
	return 1;

}

int work_queue_task_specify_file_piece(struct work_queue_task *t, const char *local_name, const char *remote_name, off_t start_byte, off_t end_byte, work_queue_file_type_t type, work_queue_file_flags_t flags)
{
	struct list *files;
	struct work_queue_file *tf;
	if(!t || !local_name || !remote_name) {
		fprintf(stderr, "Error: Null arguments for task, local name, and remote name not allowed in specify_file_piece.\n");
		return 0;
	}

	// @param remote_name should not be an absolute path. @see
	// work_queue_task_specify_file
	if(remote_name[0] == '/') {
		fatal("Error: Remote name %s is an absolute path.\n", remote_name);
	}

	if(end_byte < start_byte) {
		fprintf(stderr, "Error: End byte lower than start byte for %s.\n", remote_name);
		return 0;
	}

	if(type == WORK_QUEUE_INPUT) {
		files = t->input_files;

		//check if two different local names map to the same remote name for inputs.
		list_first_item(t->input_files);
		while((tf = (struct work_queue_file*)list_next_item(t->input_files))) {
			if(!strcmp(remote_name, tf->remote_name) && strcmp(local_name, tf->payload)){
				fprintf(stderr, "Error: piece of input file %s conflicts with another input pointing to same remote name (%s).\n", local_name, remote_name);
				return 0;
			}
		}

		//check if there is an output file with the same remote name.
		list_first_item(t->output_files);
		while((tf = (struct work_queue_file*)list_next_item(t->input_files))) {
			if(!strcmp(remote_name, tf->remote_name)){
				fprintf(stderr, "Error: piece of input file %s conflicts with an output pointing to same remote name (%s).\n", local_name, remote_name);
				return 0;
			}
		}
	} else {
		files = t->output_files;

		//check if two different different remote names map to the same local name for outputs.
		list_first_item(files);
		while((tf = (struct work_queue_file*)list_next_item(files))) {
			if(!strcmp(local_name, tf->payload) && strcmp(remote_name, tf->remote_name)) {
				fprintf(stderr, "Error: piece of output file %s conflicts with another output pointing to same remote name (%s).\n", local_name, remote_name);
				return 0;
			}
		}

		//check if there is an input file with the same remote name.
		list_first_item(t->input_files);
		while((tf = (struct work_queue_file*)list_next_item(t->input_files))) {
			if(!strcmp(remote_name, tf->remote_name)){
				fprintf(stderr, "Error: piece of output file %s conflicts with an input pointing to same remote name (%s).\n", local_name, remote_name);
				return 0;
			}
		}
	}

	tf = work_queue_file_create(local_name, remote_name, WORK_QUEUE_FILE_PIECE, flags);
	if(!tf) return 0;

	tf->offset = start_byte;
	tf->piece_length = end_byte - start_byte + 1;

	list_push_tail(files, tf);
	return 1;
}

int work_queue_task_specify_buffer(struct work_queue_task *t, const char *data, int length, const char *remote_name, work_queue_file_flags_t flags)
{
	struct work_queue_file *tf;
	if(!t || !remote_name) {
		fprintf(stderr, "Error: Null arguments for task and remote name not allowed in specify_buffer.\n");
		return 0;
	}

	// @param remote_name should not be an absolute path. @see
	// work_queue_task_specify_file
	if(remote_name[0] == '/') {
		fatal("Error: Remote name %s is an absolute path.\n", remote_name);
	}

	list_first_item(t->input_files);
	while((tf = (struct work_queue_file*)list_next_item(t->input_files))) {
		if(!strcmp(remote_name, tf->remote_name)) {
			fprintf(stderr, "Error: buffer conflicts with another input pointing to same remote name (%s).\n", remote_name);
			return 0;
		}
	}

	list_first_item(t->output_files);
	while((tf = (struct work_queue_file*)list_next_item(t->input_files))) {
		if(!strcmp(remote_name, tf->remote_name)) {
			fprintf(stderr, "Error: buffer conflicts with an output pointing to same remote name (%s).\n", remote_name);
			return 0;
		}
	}

	tf = work_queue_file_create(NULL, remote_name, WORK_QUEUE_BUFFER, flags);
	if(!tf) return 0;

	tf->payload = malloc(length);
	if(!tf->payload) {
		fprintf(stderr, "Error: failed to allocate memory for buffer with remote name %s and length %d bytes.\n", remote_name, length);
		return 0;
	}

	tf->length  = length;

	memcpy(tf->payload, data, length);
	list_push_tail(t->input_files, tf);

	return 1;
}

int work_queue_task_specify_file_command(struct work_queue_task *t, const char *cmd, const char *remote_name, work_queue_file_type_t type, work_queue_file_flags_t flags)
{
	struct list *files;
	struct work_queue_file *tf;
	if(!t || !remote_name || !cmd) {
		fprintf(stderr, "Error: Null arguments for task, remote name, and command not allowed in specify_file_command.\n");
		return 0;
	}

	// @param remote_name should not be an absolute path. @see
	// work_queue_task_specify_file
	if(remote_name[0] == '/') {
		fatal("Error: Remote name %s is an absolute path.\n", remote_name);
	}

	if(type == WORK_QUEUE_INPUT) {
		files = t->input_files;

		//check if two different local names map to the same remote name for inputs.
		list_first_item(t->input_files);
		while((tf = (struct work_queue_file*)list_next_item(t->input_files))) {
			if(!strcmp(remote_name, tf->remote_name) && strcmp(cmd, tf->payload)){
				fprintf(stderr, "Error: input file command %s conflicts with another input pointing to same remote name (%s).\n", cmd, remote_name);
				return 0;
			}
		}

		//check if there is an output file with the same remote name.
		list_first_item(t->output_files);
		while((tf = (struct work_queue_file*)list_next_item(t->input_files))) {
			if(!strcmp(remote_name, tf->remote_name)) {
				fprintf(stderr, "Error: input file command %s conflicts with an output pointing to same remote name (%s).\n", cmd, remote_name);
				return 0;
			}
		}
	} else {
		fprintf(stderr, "Error: work_queue_specify_file_command does not yet support output files.\n");
	  	return 0;
	}

	if(strstr(cmd, "%%") == NULL) {
		fatal("command to transfer file does not contain %%%% specifier: %s", cmd);
	}

	tf = work_queue_file_create(cmd, remote_name, WORK_QUEUE_REMOTECMD, flags);
	if(!tf) return 0;

	// length of source data is not known yet.
	tf->length = 0;

	list_push_tail(files, tf);

	return 1;
}

int work_queue_specify_snapshot_file(struct work_queue_task *t, const char *monitor_snapshot_file) {

	assert(monitor_snapshot_file);

	free(t->monitor_snapshot_file);
	t->monitor_snapshot_file = xxstrdup(monitor_snapshot_file);

	return work_queue_task_specify_file(t, monitor_snapshot_file, RESOURCE_MONITOR_REMOTE_NAME_EVENTS, WORK_QUEUE_INPUT, WORK_QUEUE_CACHE);

}

void work_queue_task_specify_algorithm(struct work_queue_task *t, work_queue_schedule_t algorithm)
{
	t->worker_selection_algorithm = algorithm;
}

void work_queue_task_specify_priority( struct work_queue_task *t, double priority )
{
	t->priority = priority;
}

void work_queue_task_specify_monitor_output(struct work_queue_task *t, const char *monitor_output_directory) {

	if(!monitor_output_directory) {
		fatal("Error: no monitor_output_file was specified.");
	}

	if(t->monitor_output_directory) {
		free(t->monitor_output_directory);
	}

	t->monitor_output_directory = xxstrdup(monitor_output_directory);
}

void work_queue_file_delete(struct work_queue_file *tf) {
	if(tf->payload)
		free(tf->payload);
	if(tf->remote_name)
		free(tf->remote_name);
	if(tf->cached_name)
		free(tf->cached_name);
	free(tf);
}

void work_queue_invalidate_cached_file(struct work_queue *q, const char *local_name, work_queue_file_t type) {
	struct work_queue_file *f = work_queue_file_create(local_name, local_name, type, WORK_QUEUE_CACHE);

	work_queue_invalidate_cached_file_internal(q, f->cached_name);
	work_queue_file_delete(f);
}

void work_queue_invalidate_cached_file_internal(struct work_queue *q, const char *filename) {
	char *key;
	struct work_queue_worker *w;
	hash_table_firstkey(q->worker_table);
	while(hash_table_nextkey(q->worker_table, &key, (void**)&w)) {
		if(!hash_table_lookup(w->current_files, filename))
			continue;

		if(w->type == WORKER_TYPE_FOREMAN) {
			send_worker_msg(q, w, "invalidate-file %s\n", filename);
		}

		struct work_queue_task *t;
		uint64_t taskid;

		itable_firstkey(w->current_tasks);
		while(itable_nextkey(w->current_tasks, &taskid, (void**)&t)) {
			struct work_queue_file *tf;
			list_first_item(t->input_files);

			while((tf = list_next_item(t->input_files))) {
				if(strcmp(filename, tf->cached_name) == 0) {
					cancel_task_on_worker(q, t, WORK_QUEUE_TASK_READY);
					continue;
				}
			}

			while((tf = list_next_item(t->output_files))) {
				if(strcmp(filename, tf->cached_name) == 0) {
					cancel_task_on_worker(q, t, WORK_QUEUE_TASK_READY);
					continue;
				}
			}
		}

		delete_worker_file(q, w, filename, 0, 0);
	}
}


void work_queue_task_delete(struct work_queue_task *t)
{
	struct work_queue_file *tf;
	if(t) {

		free(t->command_line);
		free(t->coprocess);
		free(t->tag);
		free(t->category);
		free(t->output);

		if(t->input_files) {
			while((tf = list_pop_tail(t->input_files))) {
				work_queue_file_delete(tf);
			}
			list_delete(t->input_files);
		}
		if(t->output_files) {
			while((tf = list_pop_tail(t->output_files))) {
				work_queue_file_delete(tf);
			}
			list_delete(t->output_files);
		}
		if(t->env_list) {
			char *var;
			while((var=list_pop_tail(t->env_list))) {
				free(var);
			}
			list_delete(t->env_list);
		}

		if(t->features) {
			char *feature;
			while((feature=list_pop_tail(t->features))) {
				free(feature);
			}
			list_delete(t->features);
		}

		free(t->hostname);
		free(t->host);

		rmsummary_delete(t->resources_requested);
		rmsummary_delete(t->resources_measured);
		rmsummary_delete(t->resources_allocated);

		free(t->monitor_output_directory);
		free(t->monitor_snapshot_file);
		free(t);
	}
}

/** DEPRECATED FUNCTIONS **/
int work_queue_task_specify_output_file(struct work_queue_task *t, const char *rname, const char *fname)
{
	return work_queue_task_specify_file(t, fname, rname, WORK_QUEUE_OUTPUT, WORK_QUEUE_CACHE);
}

int work_queue_task_specify_output_file_do_not_cache(struct work_queue_task *t, const char *rname, const char *fname)
{
	return work_queue_task_specify_file(t, fname, rname, WORK_QUEUE_OUTPUT, WORK_QUEUE_NOCACHE);
}

int work_queue_task_specify_input_buf(struct work_queue_task *t, const char *buf, int length, const char *rname)
{
	return work_queue_task_specify_buffer(t, buf, length, rname, WORK_QUEUE_NOCACHE);
}

int work_queue_task_specify_input_file(struct work_queue_task *t, const char *fname, const char *rname)
{
	return work_queue_task_specify_file(t, fname, rname, WORK_QUEUE_INPUT, WORK_QUEUE_CACHE);
}

int work_queue_task_specify_input_file_do_not_cache(struct work_queue_task *t, const char *fname, const char *rname)
{
	return work_queue_task_specify_file(t, fname, rname, WORK_QUEUE_INPUT, WORK_QUEUE_NOCACHE);
}



/******************************************************/
/********** work_queue public functions **********/
/******************************************************/

struct work_queue *work_queue_create(int port) {
	return work_queue_ssl_create(port, NULL, NULL);
}

struct work_queue *work_queue_ssl_create(int port, const char *key, const char *cert)
{
	struct work_queue *q = malloc(sizeof(*q));
	if(!q) {
		fprintf(stderr, "Error: failed to allocate memory for queue.\n");
		return 0;
	}
	char *envstring;

	random_init();

	memset(q, 0, sizeof(*q));

	if(port == 0) {
		envstring = getenv("WORK_QUEUE_PORT");
		if(envstring) {
			port = atoi(envstring);
		}
	}

	/* compatibility code */
	if (getenv("WORK_QUEUE_LOW_PORT"))
		setenv("TCP_LOW_PORT", getenv("WORK_QUEUE_LOW_PORT"), 0);
	if (getenv("WORK_QUEUE_HIGH_PORT"))
		setenv("TCP_HIGH_PORT", getenv("WORK_QUEUE_HIGH_PORT"), 0);

	q->manager_link = link_serve(port);
	if(!q->manager_link) {
		debug(D_NOTICE, "Could not create work_queue on port %i.", port);
		free(q);
		return 0;
	} else {
		char address[LINK_ADDRESS_MAX];
		link_address_local(q->manager_link, address, &q->port);
	}

	q->ssl_key = key ? strdup(key) : 0;
	q->ssl_cert = cert ? strdup(cert) : 0;

	if(q->ssl_key || q->ssl_cert) q->ssl_enabled=1;

	getcwd(q->workingdir,PATH_MAX);

	q->next_taskid = 1;

	q->ready_list = list_create();

	q->tasks          = itable_create(0);

	q->task_state_map = itable_create(0);

	q->worker_table = hash_table_create(0, 0);
	q->worker_blocklist = hash_table_create(0, 0);
	q->worker_task_map = itable_create(0);

	q->factory_table = hash_table_create(0, 0);
	q->fetch_factory = 0;

	q->measured_local_resources = rmsummary_create(-1);
	q->current_max_worker       = rmsummary_create(-1);
	q->max_task_resources_requested = rmsummary_create(-1);

	q->stats                      = calloc(1, sizeof(struct work_queue_stats));
	q->stats_disconnected_workers = calloc(1, sizeof(struct work_queue_stats));
	q->stats_measure              = calloc(1, sizeof(struct work_queue_stats));

	q->workers_with_available_results = hash_table_create(0, 0);

	// The poll table is initially null, and will be created
	// (and resized) as needed by build_poll_table.
	q->poll_table_size = 8;

	q->worker_selection_algorithm = wq_option_scheduler;
	q->process_pending_check = 0;

	q->short_timeout = 5;
	q->long_timeout = 3600;

	q->stats->time_when_started = timestamp_get();
	q->time_last_large_tasks_check = timestamp_get();
	q->task_reports = list_create();

	q->time_last_wait = 0;
	q->time_last_log_stats = 0;

	q->catalog_hosts = 0;

	q->keepalive_interval = WORK_QUEUE_DEFAULT_KEEPALIVE_INTERVAL;
	q->keepalive_timeout = WORK_QUEUE_DEFAULT_KEEPALIVE_TIMEOUT;

	q->monitor_mode = MON_DISABLED;

	q->hungry_minimum = 10;

	q->wait_for_workers = 0;

	q->proportional_resources = 1;
	q->proportional_whole_tasks = 1;

	q->allocation_default_mode = WORK_QUEUE_ALLOCATION_MODE_FIXED;
	q->categories = hash_table_create(0, 0);

	// The value -1 indicates that fast abort is inactive by default
	// fast abort depends on categories, thus set after them.
	work_queue_activate_fast_abort(q, -1);

	q->password = 0;

	q->resource_submit_multiplier = 1.0;

	q->minimum_transfer_timeout = 60;
	q->foreman_transfer_timeout = 3600;
	q->transfer_outlier_factor = 10;
	q->default_transfer_rate = 1*MEGABYTE;

	q->manager_preferred_connection = xxstrdup("by_ip");

	if( (envstring  = getenv("WORK_QUEUE_BANDWIDTH")) ) {
		q->bandwidth = string_metric_parse(envstring);
		if(q->bandwidth < 0) {
			q->bandwidth = 0;
		}
	}

	//Deprecated:
	q->task_ordering = WORK_QUEUE_TASK_ORDER_FIFO;
	//

	log_queue_stats(q, 1);

	q->time_last_wait = timestamp_get();

	char hostname[DOMAIN_NAME_MAX];
	if(domain_name_cache_guess(hostname)) {
		debug(D_WQ, "Master advertising as %s:%d", hostname, q->port);
	}
	else {
		debug(D_WQ, "Work Queue is listening on port %d.", q->port);
	}
	return q;
}

int work_queue_enable_monitoring(struct work_queue *q, char *monitor_output_directory, int watchdog)
{
	if(!q)
		return 0;

	q->monitor_mode = MON_DISABLED;
	q->monitor_exe = resource_monitor_locate(NULL);

	if(q->monitor_output_directory) {
		free(q->monitor_output_directory);
		q->monitor_output_directory = NULL;
	}

	if(!q->monitor_exe)
	{
		warn(D_WQ, "Could not find the resource monitor executable. Disabling monitoring.\n");
		return 0;
	}

	if(monitor_output_directory) {
		q->monitor_output_directory = xxstrdup(monitor_output_directory);

		if(!create_dir(q->monitor_output_directory, 0777)) {
			fatal("Could not create monitor output directory - %s (%s)", q->monitor_output_directory, strerror(errno));
		}

		q->monitor_summary_filename = string_format("%s/wq-%d.summaries", q->monitor_output_directory, getpid());
		q->monitor_file             = fopen(q->monitor_summary_filename, "a");

		if(!q->monitor_file)
		{
			fatal("Could not open monitor log file for writing: '%s'\n", q->monitor_summary_filename);
		}

	}

	if(q->measured_local_resources)
		rmsummary_delete(q->measured_local_resources);

	q->measured_local_resources = rmonitor_measure_process(getpid());
	q->monitor_mode = MON_SUMMARY;

	if(watchdog) {
		q->monitor_mode |= MON_WATCHDOG;
	}

	return 1;
}

int work_queue_enable_monitoring_full(struct work_queue *q, char *monitor_output_directory, int watchdog) {
	int status = work_queue_enable_monitoring(q, monitor_output_directory, 1);

	if(status) {
		q->monitor_mode = MON_FULL;

		if(watchdog) {
			q->monitor_mode |= MON_WATCHDOG;
		}
	}

	return status;
}

int work_queue_activate_fast_abort_category(struct work_queue *q, const char *category, double multiplier)
{
	struct category *c = work_queue_category_lookup_or_create(q, category);

	if(multiplier >= 1) {
		debug(D_WQ, "Enabling fast abort multiplier for '%s': %3.3lf\n", category, multiplier);
		c->fast_abort = multiplier;
		return 0;
	} else if(multiplier == 0) {
		debug(D_WQ, "Disabling fast abort multiplier for '%s'.\n", category);
		c->fast_abort = 0;
		return 1;
	} else {
		debug(D_WQ, "Using default fast abort multiplier for '%s'.\n", category);
		c->fast_abort = -1;
		return 0;
	}
}

int work_queue_activate_fast_abort(struct work_queue *q, double multiplier)
{
	return work_queue_activate_fast_abort_category(q, "default", multiplier);
}

int work_queue_port(struct work_queue *q)
{
	char addr[LINK_ADDRESS_MAX];
	int port;

	if(!q) return 0;

	if(link_address_local(q->manager_link, addr, &port)) {
		return port;
	} else {
		return 0;
	}
}

void work_queue_specify_estimate_capacity_on(struct work_queue *q, int value)
{
	// always on
}

void work_queue_specify_algorithm(struct work_queue *q, work_queue_schedule_t algorithm)
{
	q->worker_selection_algorithm = algorithm;
}

void work_queue_specify_task_order(struct work_queue *q, int order)
{
	q->task_ordering = order;
}

void work_queue_specify_name(struct work_queue *q, const char *name)
{
	if(q->name) free(q->name);
	if(name) {
		q->name = xxstrdup(name);
		setenv("WORK_QUEUE_NAME", q->name, 1);
	} else {
		q->name = 0;
	}
}

void work_queue_specify_debug_path(struct work_queue *q, const char *path)
{
	if(q->debug_path) free(q->debug_path);
	if(path) {
		q->debug_path = xxstrdup(path);
		setenv("WORK_QUEUE_DEBUG_PATH", q->debug_path, 1);
	} else {
		q->debug_path = 0;
	}
}

void work_queue_specify_tlq_port(struct work_queue *q, int port)
{
	q->tlq_port = port;
}

const char *work_queue_name(struct work_queue *q)
{
	return q->name;
}

void work_queue_specify_priority(struct work_queue *q, int priority)
{
	q->priority = priority;
}

void work_queue_specify_num_tasks_left(struct work_queue *q, int ntasks)
{
	if(ntasks < 1) {
		q->num_tasks_left = 0;
	}
	else {
		q->num_tasks_left = ntasks;
	}
}

void work_queue_specify_manager_mode(struct work_queue *q, int mode)
{
	// Deprecated: Report to the catalog if a name is given.
}

void work_queue_specify_catalog_server(struct work_queue *q, const char *hostname, int port)
{
	char hostport[DOMAIN_NAME_MAX + 8];
	if(hostname && (port > 0)) {
		sprintf(hostport, "%s:%d", hostname, port);
		work_queue_specify_catalog_servers(q, hostport);
	} else if(hostname) {
		work_queue_specify_catalog_servers(q, hostname);
	} else if (port > 0) {
		sprintf(hostport, "%d", port);
		setenv("CATALOG_PORT", hostport, 1);
	}
}

void work_queue_specify_catalog_servers(struct work_queue *q, const char *hosts)
{
	if(hosts) {
		if(q->catalog_hosts) free(q->catalog_hosts);
		q->catalog_hosts = strdup(hosts);
		setenv("CATALOG_HOST", hosts, 1);
	}
}

void work_queue_specify_password( struct work_queue *q, const char *password )
{
	q->password = xxstrdup(password);
}

int work_queue_specify_password_file( struct work_queue *q, const char *file )
{
	return copy_file_to_buffer(file,&q->password,NULL)>0;
}

void work_queue_delete(struct work_queue *q)
{
	if(q) {
		struct work_queue_worker *w;
		char *key;

		hash_table_firstkey(q->worker_table);
		while(hash_table_nextkey(q->worker_table, &key, (void **) &w)) {
			release_worker(q, w);
			hash_table_firstkey(q->worker_table);
		}

		struct work_queue_factory_info *f;
		hash_table_firstkey(q->factory_table);
		while(hash_table_nextkey(q->factory_table, &key, (void **) &f)) {
			remove_factory_info(q, key);
			hash_table_firstkey(q->factory_table);
		}

		log_queue_stats(q, 1);

		if(q->name) {
			update_catalog(q, NULL, 1);
		}

		/* we call this function here before any of the structures are freed. */
		work_queue_disable_monitoring(q);

		if(q->catalog_hosts) free(q->catalog_hosts);

		hash_table_delete(q->worker_table);
		hash_table_delete(q->factory_table);
		hash_table_delete(q->worker_blocklist);
		itable_delete(q->worker_task_map);

		struct category *c;
		hash_table_firstkey(q->categories);
		while(hash_table_nextkey(q->categories, &key, (void **) &c)) {
			category_delete(q->categories, key);
		}
		hash_table_delete(q->categories);

		list_delete(q->ready_list);

		itable_delete(q->tasks);

		itable_delete(q->task_state_map);

		hash_table_delete(q->workers_with_available_results);

		struct work_queue_task_report *tr;
		list_first_item(q->task_reports);
		while((tr = list_next_item(q->task_reports))) {
			task_report_delete(tr);
		}
		list_delete(q->task_reports);

		free(q->stats);
		free(q->stats_disconnected_workers);
		free(q->stats_measure);

		if(q->name)
			free(q->name);

		if(q->manager_preferred_connection)
			free(q->manager_preferred_connection);

		free(q->poll_table);
		free(q->ssl_cert);
		free(q->ssl_key);

		link_close(q->manager_link);
		if(q->logfile) {
			fclose(q->logfile);
		}

		if(q->transactions_logfile) {
			write_transaction(q, "MANAGER END");

			if(fclose(q->transactions_logfile) != 0) {
				debug(D_WQ, "unable to write transactions log: %s\n", strerror(errno));
			}
		}

		rmsummary_delete(q->measured_local_resources);
		rmsummary_delete(q->current_max_worker);
		rmsummary_delete(q->max_task_resources_requested);

		free(q);
	}
}

void update_resource_report(struct work_queue *q) {
	// Only measure every few seconds.
	if((time(0) - q->resources_last_update_time) < WORK_QUEUE_RESOURCE_MEASUREMENT_INTERVAL)
		return;

	rmonitor_measure_process_update_to_peak(q->measured_local_resources, getpid());

	q->resources_last_update_time = time(0);
}

void work_queue_disable_monitoring(struct work_queue *q) {
	if(q->monitor_mode == MON_DISABLED)
		return;

	rmonitor_measure_process_update_to_peak(q->measured_local_resources, getpid());
	if(!q->measured_local_resources->exit_type)
		q->measured_local_resources->exit_type = xxstrdup("normal");

	if(q->monitor_mode && q->monitor_summary_filename) {
		fclose(q->monitor_file);

		char template[] = "rmonitor-summaries-XXXXXX";
		int final_fd = mkstemp(template);
		int summs_fd = open(q->monitor_summary_filename, O_RDONLY);

		if( final_fd < 0 || summs_fd < 0 ) {
			warn(D_DEBUG, "Could not consolidate resource summaries.");
			return;
		}

		/* set permissions according to user's mask. getumask is not available yet,
		   and the only way to get the value of the current mask is to change
		   it... */
		mode_t old_mask = umask(0);
		umask(old_mask);
		fchmod(final_fd, 0777 & ~old_mask  );

		FILE *final = fdopen(final_fd, "w");

		const char *user_name = getlogin();
		if(!user_name) {
			user_name = "unknown";
		}

		struct jx *extra = jx_object(
				jx_pair(jx_string("type"), jx_string("work_queue"),
					jx_pair(jx_string("user"), jx_string(user_name),
						NULL)));

		if(q->name) {
			jx_insert_string(extra, "manager_name", q->name);
		}

		rmsummary_print(final, q->measured_local_resources, /* pprint */ 0, extra);

		copy_fd_to_stream(summs_fd, final);

		jx_delete(extra);
		close(summs_fd);

		if(fclose(final) != 0) {
			debug(D_WQ, "unable to update monitor report to final destination file: %s\n", strerror(errno));
		}

		if(rename(template, q->monitor_summary_filename) < 0) {
			warn(D_DEBUG, "Could not move monitor report to final destination file.");
		}
	}

	if(q->monitor_exe)
		free(q->monitor_exe);
	if(q->monitor_output_directory)
		free(q->monitor_output_directory);
	if(q->monitor_summary_filename)
		free(q->monitor_summary_filename);
}

void work_queue_monitor_add_files(struct work_queue *q, struct work_queue_task *t) {
	work_queue_task_specify_file(t, q->monitor_exe, RESOURCE_MONITOR_REMOTE_NAME, WORK_QUEUE_INPUT, WORK_QUEUE_CACHE);

	char *summary  = monitor_file_name(q, t, ".summary");
	work_queue_task_specify_file(t, summary, RESOURCE_MONITOR_REMOTE_NAME ".summary", WORK_QUEUE_OUTPUT, WORK_QUEUE_NOCACHE);
	free(summary);

	if(q->monitor_mode & MON_FULL && (q->monitor_output_directory || t->monitor_output_directory)) {
		char *debug  = monitor_file_name(q, t, ".debug");
		char *series = monitor_file_name(q, t, ".series");

		work_queue_task_specify_file(t, debug, RESOURCE_MONITOR_REMOTE_NAME ".debug",   WORK_QUEUE_OUTPUT, WORK_QUEUE_NOCACHE);
		work_queue_task_specify_file(t, series, RESOURCE_MONITOR_REMOTE_NAME ".series", WORK_QUEUE_OUTPUT, WORK_QUEUE_NOCACHE);

		free(debug);
		free(series);
	}
}

char *work_queue_monitor_wrap(struct work_queue *q, struct work_queue_worker *w, struct work_queue_task *t, struct rmsummary *limits)
{
	buffer_t b;
	buffer_init(&b);

	buffer_printf(&b, "-V 'task_id: %d'", t->taskid);

	if(t->category) {
		buffer_printf(&b, " -V 'category: %s'", t->category);
	}

	if(t->monitor_snapshot_file) {
		buffer_printf(&b, " --snapshot-events %s", RESOURCE_MONITOR_REMOTE_NAME_EVENTS);
	}

	if(!(q->monitor_mode & MON_WATCHDOG)) {
		buffer_printf(&b, " --measure-only");
	}

	int extra_files = (q->monitor_mode & MON_FULL);

	char *monitor_cmd = resource_monitor_write_command("./" RESOURCE_MONITOR_REMOTE_NAME, RESOURCE_MONITOR_REMOTE_NAME, limits, /* extra options */ buffer_tostring(&b), /* debug */ extra_files, /* series */ extra_files, /* inotify */ 0, /* measure_dir */ NULL);
	char *wrap_cmd  = string_wrap_command(t->command_line, monitor_cmd);

	buffer_free(&b);
	free(monitor_cmd);

	return wrap_cmd;
}

static double work_queue_task_priority(void *item) {
	assert(item);
	struct work_queue_task *t = item;
	return t->priority;
}

/* Put a given task on the ready list, taking into account the task priority and the queue schedule. */

void push_task_to_ready_list( struct work_queue *q, struct work_queue_task *t )
{
	int by_priority = 1;

	if(t->result == WORK_QUEUE_RESULT_RESOURCE_EXHAUSTION) {
		/* when a task is resubmitted given resource exhaustion, we
		 * push it at the head of the list, so it gets to run as soon
		 * as possible. This avoids the issue in which all 'big' tasks
		 * fail because the first allocation is too small. */
		by_priority = 0;
	}

	if(by_priority) {
		list_push_priority(q->ready_list, work_queue_task_priority, t);
	} else {
		list_push_head(q->ready_list,t);
	}

	/* If the task has been used before, clear out accumulated state. */
	clean_task_state(t, 0);
}


work_queue_task_state_t work_queue_task_state(struct work_queue *q, int taskid) {
	return (int)(uintptr_t)itable_lookup(q->task_state_map, taskid);
}

static void fill_deprecated_tasks_stats(struct work_queue_task *t) {
	t->time_task_submit = t->time_when_submitted;
	t->time_task_finish = t->time_when_done;
	t->time_committed   = t->time_when_commit_start;

	t->time_send_input_start      = t->time_when_commit_start;
	t->time_send_input_finish     = t->time_when_commit_end;
	t->time_receive_result_start  = t->time_when_retrieval;
	t->time_receive_result_finish = t->time_when_done;
	t->time_receive_output_start  = t->time_when_retrieval;
	t->time_receive_output_finish = t->time_when_done;

	t->time_execute_cmd_start  = t->time_when_commit_start;
	t->time_execute_cmd_finish = t->time_when_retrieval;

	t->total_transfer_time = (t->time_when_commit_end - t->time_when_commit_start) + (t->time_when_done - t->time_when_retrieval);

	t->cmd_execution_time = t->time_workers_execute_last;
	t->total_cmd_execution_time = t->time_workers_execute_all;
	t->total_cmd_exhausted_execute_time = t->time_workers_execute_exhaustion;
	t->total_time_until_worker_failure = t->time_workers_execute_failure;

	t->total_bytes_received = t->bytes_received;
	t->total_bytes_sent = t->bytes_sent;
	t->total_bytes_transferred = t->bytes_transferred;
}


/* Changes task state. Returns old state */
/* State of the task. One of WORK_QUEUE_TASK(UNKNOWN|READY|RUNNING|WAITING_RETRIEVAL|RETRIEVED|DONE) */
static work_queue_task_state_t change_task_state( struct work_queue *q, struct work_queue_task *t, work_queue_task_state_t new_state ) {

	work_queue_task_state_t old_state = (uintptr_t) itable_lookup(q->task_state_map, t->taskid);
	itable_insert(q->task_state_map, t->taskid, (void *) new_state);
	// remove from current tables:

	if( old_state == WORK_QUEUE_TASK_READY ) {
		// Treat WORK_QUEUE_TASK_READY specially, as it has the order of the tasks
		list_remove(q->ready_list, t);
	}

	// insert to corresponding table
	debug(D_WQ, "Task %d state change: %s (%d) to %s (%d)\n", t->taskid, task_state_str(old_state), old_state, task_state_str(new_state), new_state);

	switch(new_state) {
		case WORK_QUEUE_TASK_READY:
			update_task_result(t, WORK_QUEUE_RESULT_UNKNOWN);
			push_task_to_ready_list(q, t);
			break;
		case WORK_QUEUE_TASK_DONE:
		case WORK_QUEUE_TASK_CANCELED:
			/* tasks are freed when returned to user, thus we remove them from our local record */
			fill_deprecated_tasks_stats(t);
			itable_remove(q->tasks, t->taskid);
			break;
		default:
			/* do nothing */
			break;
	}

	log_queue_stats(q, 0);
	write_transaction_task(q, t);

	return old_state;
}

const char *task_state_str(work_queue_task_state_t task_state) {
	const char *str;

	switch(task_state) {
		case WORK_QUEUE_TASK_READY:
			str = "WAITING";
			break;
		case WORK_QUEUE_TASK_RUNNING:
			str = "RUNNING";
			break;
		case WORK_QUEUE_TASK_WAITING_RETRIEVAL:
			str = "WAITING_RETRIEVAL";
			break;
		case WORK_QUEUE_TASK_RETRIEVED:
			str = "RETRIEVED";
			break;
		case WORK_QUEUE_TASK_DONE:
			str = "DONE";
			break;
		case WORK_QUEUE_TASK_CANCELED:
			str = "CANCELED";
			break;
		case WORK_QUEUE_TASK_UNKNOWN:
		default:
			str = "UNKNOWN";
			break;
	}

	return str;
}

static int task_in_terminal_state(struct work_queue *q, struct work_queue_task *t) {

	work_queue_task_state_t state = (uintptr_t) itable_lookup(q->task_state_map, t->taskid);

	switch(state) {
		case WORK_QUEUE_TASK_READY:
		case WORK_QUEUE_TASK_RUNNING:
		case WORK_QUEUE_TASK_WAITING_RETRIEVAL:
		case WORK_QUEUE_TASK_RETRIEVED:
			return 0;
			break;
		case WORK_QUEUE_TASK_DONE:
		case WORK_QUEUE_TASK_CANCELED:
		case WORK_QUEUE_TASK_UNKNOWN:
			return 1;
			break;
	}

	return 0;
}

const char *work_queue_result_str(work_queue_result_t result) {
	const char *str = NULL;

	switch(result) {
		case WORK_QUEUE_RESULT_SUCCESS:
			str = "SUCCESS";
			break;
		case WORK_QUEUE_RESULT_INPUT_MISSING:
			str = "INPUT_MISS";
			break;
		case WORK_QUEUE_RESULT_OUTPUT_MISSING:
			str = "OUTPUT_MISS";
			break;
		case WORK_QUEUE_RESULT_STDOUT_MISSING:
			str = "STDOUT_MISS";
			break;
		case WORK_QUEUE_RESULT_SIGNAL:
			str = "SIGNAL";
			break;
		case WORK_QUEUE_RESULT_RESOURCE_EXHAUSTION:
			str = "RESOURCE_EXHAUSTION";
			break;
		case WORK_QUEUE_RESULT_TASK_TIMEOUT:
			str = "END_TIME";
			break;
		case WORK_QUEUE_RESULT_UNKNOWN:
			str = "UNKNOWN";
			break;
		case WORK_QUEUE_RESULT_FORSAKEN:
			str = "FORSAKEN";
			break;
		case WORK_QUEUE_RESULT_MAX_RETRIES:
			str = "MAX_RETRIES";
			break;
		case WORK_QUEUE_RESULT_TASK_MAX_RUN_TIME:
			str = "MAX_WALL_TIME";
			break;
		case WORK_QUEUE_RESULT_DISK_ALLOC_FULL:
			str = "DISK_FULL";
			break;
		case WORK_QUEUE_RESULT_RMONITOR_ERROR:
			str = "MONITOR_ERROR";
			break;
		case WORK_QUEUE_RESULT_OUTPUT_TRANSFER_ERROR:
			str = "OUTPUT_TRANSFER_ERROR";
			break;
	}

	return str;
}

static int task_state_is( struct work_queue *q, uint64_t taskid, work_queue_task_state_t state) {
	return itable_lookup(q->task_state_map, taskid) == (void *) state;
}

static struct work_queue_task *task_state_any(struct work_queue *q, work_queue_task_state_t state) {
	struct work_queue_task *t;
	uint64_t taskid;

	itable_firstkey(q->tasks);
	while( itable_nextkey(q->tasks, &taskid, (void **) &t) ) {
		if( task_state_is(q, taskid, state) ) {
			return t;
		}
	}

	return NULL;
}

static struct work_queue_task *task_state_any_with_tag(struct work_queue *q, work_queue_task_state_t state, const char *tag) {
	struct work_queue_task *t;
	uint64_t taskid;

	itable_firstkey(q->tasks);
	while( itable_nextkey(q->tasks, &taskid, (void **) &t) ) {
		if( task_state_is(q, taskid, state) && tasktag_comparator((void *) t, (void *) tag)) {
			return t;
		}
	}

	return NULL;
}

static int task_state_count(struct work_queue *q, const char *category, work_queue_task_state_t state) {
	struct work_queue_task *t;
	uint64_t taskid;

	int count = 0;

	itable_firstkey(q->tasks);
	while( itable_nextkey(q->tasks, &taskid, (void **) &t) ) {
		if( task_state_is(q, taskid, state) ) {
			if(!category || strcmp(category, t->category) == 0) {
				count++;
			}
		}
	}

	return count;
}

static int task_request_count( struct work_queue *q, const char *category, category_allocation_t request) {
	struct work_queue_task *t;
	uint64_t taskid;

	int count = 0;

	itable_firstkey(q->tasks);
	while( itable_nextkey(q->tasks, &taskid, (void **) &t) ) {
		if(t->resource_request == request) {
			if(!category || strcmp(category, t->category) == 0) {
				count++;
			}
		}
	}

	return count;
}

int work_queue_submit_internal(struct work_queue *q, struct work_queue_task *t)
{
	itable_insert(q->tasks, t->taskid, t);

	/* Ensure category structure is created. */
	work_queue_category_lookup_or_create(q, t->category);

	change_task_state(q, t, WORK_QUEUE_TASK_READY);

	t->time_when_submitted = timestamp_get();
	q->stats->tasks_submitted++;

	if(q->monitor_mode != MON_DISABLED)
		work_queue_monitor_add_files(q, t);

	rmsummary_merge_max(q->max_task_resources_requested, t->resources_requested);

	return (t->taskid);
}

int work_queue_submit(struct work_queue *q, struct work_queue_task *t)
{
	if(t->taskid > 0) {
		if(task_in_terminal_state(q, t)) {
			/* this task struct has been submitted before. We keep all the
			 * definitions, but reset all of the stats. */
			clean_task_state(t, /* full clean */ 1);
		} else {
			fatal("Task %d has been already submitted and is not in any final state.", t->taskid);
		}
	}

	t->taskid = q->next_taskid;

	//Increment taskid. So we get a unique taskid for every submit.
	q->next_taskid++;

	return work_queue_submit_internal(q, t);
}

void work_queue_block_host_with_timeout(struct work_queue *q, const char *hostname, time_t timeout)
{
	struct blocklist_host_info *info = hash_table_lookup(q->worker_blocklist, hostname);

	if(!info) {
		info = malloc(sizeof(struct blocklist_host_info));
		info->times_blocked = 0;
		info->blocked       = 0;
	}

	q->stats->workers_blocked++;

	/* count the times the worker goes from active to blocked. */
	if(!info->blocked)
		info->times_blocked++;

	info->blocked = 1;

	if(timeout > 0) {
		debug(D_WQ, "Blocking host %s by %" PRIu64 " seconds (blocked %d times).\n", hostname, (uint64_t) timeout, info->times_blocked);
		info->release_at = time(0) + timeout;
	} else {
		debug(D_WQ, "Blocking host %s indefinitely.\n", hostname);
		info->release_at = -1;
	}

	hash_table_insert(q->worker_blocklist, hostname, (void *) info);
}

void work_queue_block_host(struct work_queue *q, const char *hostname)
{
	work_queue_block_host_with_timeout(q, hostname, -1);
}

void work_queue_unblock_host(struct work_queue *q, const char *hostname)
{
	struct blocklist_host_info *info = hash_table_remove(q->worker_blocklist, hostname);
	if(info) {
		info->blocked = 0;
		info->release_at  = 0;
	}
}

/* deadline < 1 means release all, regardless of release_at time. */
static void work_queue_unblock_all_by_time(struct work_queue *q, time_t deadline)
{
	char *hostname;
	struct blocklist_host_info *info;

	hash_table_firstkey(q->worker_blocklist);
	while(hash_table_nextkey(q->worker_blocklist, &hostname, (void *) &info)) {
		if(!info->blocked)
			continue;

		/* do not clear if blocked indefinitely, and we are not clearing the whole list. */
		if(info->release_at < 1 && deadline > 0)
			continue;

		/* do not clear if the time for this host has not meet the deadline. */
		if(deadline > 0 && info->release_at > deadline)
			continue;

		debug(D_WQ, "Clearing hostname %s from blocklist.\n", hostname);
		work_queue_unblock_host(q, hostname);
	}
}

void work_queue_unblock_all(struct work_queue *q)
{
	work_queue_unblock_all_by_time(q, -1);
}

static void print_password_warning( struct work_queue *q )
{
	static int did_password_warning = 0;

	if(did_password_warning) {
		return;
	}

	if(!q->password && q->name) {
		fprintf(stdout,"warning: this work queue manager is visible to the public.\n");
		fprintf(stdout,"warning: you should set a password with the --password option.\n");
	}

	if(!q->ssl_enabled) {
		fprintf(stdout,"warning: using plain-text when communicating with workers.\n");
		fprintf(stdout,"warning: use encryption with a key and cert when creating the manager.\n");
	}

	did_password_warning = 1;
}

#define BEGIN_ACCUM_TIME(q, stat) {\
	if(q->stats_measure->stat != 0) {\
		fatal("Double-counting stat %s. This should not happen, and it is Work Queue bug.");\
	} else {\
		q->stats_measure->stat = timestamp_get();\
	}\
}

#define END_ACCUM_TIME(q, stat) {\
	q->stats->stat += timestamp_get() - q->stats_measure->stat;\
	q->stats_measure->stat = 0;\
}

struct work_queue_task *work_queue_wait(struct work_queue *q, int timeout)
{
	return work_queue_wait_for_tag(q, NULL, timeout);
}

struct work_queue_task *work_queue_wait_for_tag(struct work_queue *q, const char *tag, int timeout)
{
	if(timeout == 0) {
		// re-establish old, if unintended behavior, where 0 would wait at
		// least a second. With 0, we would like the loop to be executed at
		// least once, but right now we cannot enforce that. Making it 1, we
		// guarantee that the wait loop is executed once.
		timeout = 1;
	}

	if(timeout != WORK_QUEUE_WAITFORTASK && timeout < 0) {
		debug(D_NOTICE|D_WQ, "Invalid wait timeout value '%d'. Waiting for 5 seconds.", timeout);
		timeout = 5;
	}

	return work_queue_wait_internal(q, timeout, NULL, NULL, tag);
}

/* return number of workers that failed */
static int poll_active_workers(struct work_queue *q, int stoptime, struct link *foreman_uplink, int *foreman_uplink_active)
{
	BEGIN_ACCUM_TIME(q, time_polling);

	int n = build_poll_table(q, foreman_uplink);

	// We poll in at most small time segments (of a second). This lets
	// promptly dispatch tasks, while avoiding busy waiting.
	int msec = q->busy_waiting_flag ? 1000 : 0;
	if(stoptime) {
		msec = MIN(msec, (stoptime - time(0)) * 1000);
	}

	END_ACCUM_TIME(q, time_polling);

	if(msec < 0) {
		return 0;
	}

	BEGIN_ACCUM_TIME(q, time_polling);

	// Poll all links for activity.
	link_poll(q->poll_table, n, msec);
	q->link_poll_end = timestamp_get();

	int i, j = 1;
	// Consider the foreman_uplink passed into the function and disregard if inactive.
	if(foreman_uplink) {
		if(q->poll_table[1].revents) {
			*foreman_uplink_active = 1; //signal that the manager link saw activity
		} else {
			*foreman_uplink_active = 0;
		}
		j++;
	}

	END_ACCUM_TIME(q, time_polling);

	BEGIN_ACCUM_TIME(q, time_status_msgs);

	int workers_failed = 0;
	// Then consider all existing active workers
	for(i = j; i < n; i++) {
		if(q->poll_table[i].revents) {
			if(handle_worker(q, q->poll_table[i].link) == WQ_WORKER_FAILURE) {
				workers_failed++;
			}
		}
	}

	if(hash_table_size(q->workers_with_available_results) > 0) {
		char *key;
		struct work_queue_worker *w;
		hash_table_firstkey(q->workers_with_available_results);
		while(hash_table_nextkey(q->workers_with_available_results,&key,(void**)&w)) {
			get_available_results(q, w);
			hash_table_remove(q->workers_with_available_results, key);
			hash_table_firstkey(q->workers_with_available_results);
		}
	}

	END_ACCUM_TIME(q, time_status_msgs);

	return workers_failed;
}


static int connect_new_workers(struct work_queue *q, int stoptime, int max_new_workers)
{
	int new_workers = 0;

	// If the manager link was awake, then accept at most max_new_workers.
	// Note we are using the information gathered in poll_active_workers, which
	// is a little ugly.
	if(q->poll_table[0].revents) {
		do {
			add_worker(q);
			new_workers++;
		} while(link_usleep(q->manager_link, 0, 1, 0) && (stoptime >= time(0) && (max_new_workers > new_workers)));
	}

	return new_workers;
}


struct work_queue_task *work_queue_wait_internal(struct work_queue *q, int timeout, struct link *foreman_uplink, int *foreman_uplink_active, const char *tag)
{
/*
   - compute stoptime
   S time left?                              No:  return null
   - task completed?                         Yes: return completed task to user
   - update catalog if appropriate
   - retrieve workers status messages
   - tasks waiting to be retrieved?          Yes: retrieve one task and go to S.
   - tasks waiting to be dispatched?         Yes: dispatch one task and go to S.
   - send keepalives to appropriate workers
   - fast-abort workers
   - if new workers, connect n of them
   - expired tasks?                          Yes: mark expired tasks as retrieved and go to S.
   - queue empty?                            Yes: return null
   - go to S
*/
	int events = 0;
	// account for time we spend outside work_queue_wait
	if(q->time_last_wait > 0) {
		q->stats->time_application += timestamp_get() - q->time_last_wait;
	} else {
		q->stats->time_application += timestamp_get() - q->stats->time_when_started;
	}

	print_password_warning(q);

	// compute stoptime
	time_t stoptime = (timeout == WORK_QUEUE_WAITFORTASK) ? 0 : time(0) + timeout;

	int result;
	struct work_queue_task *t = NULL;
	// time left?

	while( (stoptime == 0) || (time(0) < stoptime) ) {

		BEGIN_ACCUM_TIME(q, time_internal);
		// task completed?
		if (t == NULL)
		{
			if(tag) {
				t = task_state_any_with_tag(q, WORK_QUEUE_TASK_RETRIEVED, tag);
			} else {
				t = task_state_any(q, WORK_QUEUE_TASK_RETRIEVED);
			}
			if(t) {
				change_task_state(q, t, WORK_QUEUE_TASK_DONE);

				if( t->result != WORK_QUEUE_RESULT_SUCCESS )
				{
					q->stats->tasks_failed++;
				}

				// return completed task (t) to the user. We do not return right
				// away, and instead break out of the loop to correctly update the
				// queue time statistics.
				events++;
				END_ACCUM_TIME(q, time_internal);

				if(!q->wait_retrieve_many) {
					break;
				}
			}
		}

		// update catalog if appropriate
		if(q->name) {
			update_catalog(q, foreman_uplink, 0);
		}

		if(q->monitor_mode)
			update_resource_report(q);

		END_ACCUM_TIME(q, time_internal);

		// retrieve worker status messages
		if(poll_active_workers(q, stoptime, foreman_uplink, foreman_uplink_active) > 0) {
			//at least one worker was removed.
			events++;
			// note we keep going, and we do not restart the loop as we do in
			// further events. This is because we give top priority to
			// returning and retrieving tasks.
		}


		q->busy_waiting_flag = 0;

		// tasks waiting to be retrieved?
		BEGIN_ACCUM_TIME(q, time_receive);
		result = receive_one_task(q);
		END_ACCUM_TIME(q, time_receive);
		if(result) {
			// retrieved at least one task
			events++;
			compute_manager_load(q, 1);
			continue;
		}

		// expired tasks
		BEGIN_ACCUM_TIME(q, time_internal);
		result = expire_waiting_tasks(q);
		END_ACCUM_TIME(q, time_internal);
		if(result) {
			// expired at least one task
			events++;
			compute_manager_load(q, 1);
			continue;
		}

		// record that there was not task activity for this iteration
		compute_manager_load(q, 0);

		if(q->wait_for_workers <= hash_table_size(q->worker_table)) {
			if(q->wait_for_workers > 0) {
				debug(D_WQ, "Target number of workers reached (%d).", q->wait_for_workers);
				q->wait_for_workers = 0;
			}
			// tasks waiting to be dispatched?
			BEGIN_ACCUM_TIME(q, time_send);
			result = send_one_task(q);
			END_ACCUM_TIME(q, time_send);
			if(result) {
				// sent at least one task
				events++;
				continue;
			}
		}
		//we reach here only if no task was neither sent nor received.
		compute_manager_load(q, 1);

		// send keepalives to appropriate workers
		BEGIN_ACCUM_TIME(q, time_status_msgs);
		ask_for_workers_updates(q);
		END_ACCUM_TIME(q, time_status_msgs);

		// Kill off slow/drained workers.
		BEGIN_ACCUM_TIME(q, time_internal);
		result  = abort_slow_workers(q);
		result += abort_drained_workers(q);
		work_queue_unblock_all_by_time(q, time(0));
		END_ACCUM_TIME(q, time_internal);
		if(result) {
			// removed at least one worker
			events++;
			continue;
		}

		// if new workers, connect n of them
		BEGIN_ACCUM_TIME(q, time_status_msgs);
		result = connect_new_workers(q, stoptime, MAX(q->wait_for_workers, MAX_NEW_WORKERS));
		END_ACCUM_TIME(q, time_status_msgs);
		if(result) {
			// accepted at least one worker
			events++;
			continue;
		}

		if(q->process_pending_check) {

			BEGIN_ACCUM_TIME(q, time_internal);
			int pending = process_pending();
			END_ACCUM_TIME(q, time_internal);

			if(pending) {
				events++;
				break;
			}
		}

		// return if queue is empty and something interesting already happened
		// in this wait.
		if(events > 0) {
			BEGIN_ACCUM_TIME(q, time_internal);
			int done = !task_state_any(q, WORK_QUEUE_TASK_RUNNING) && !task_state_any(q, WORK_QUEUE_TASK_READY) && !task_state_any(q, WORK_QUEUE_TASK_WAITING_RETRIEVAL) && !(foreman_uplink);
			END_ACCUM_TIME(q, time_internal);

			if(done) {
				break;
			}
		}

		print_large_tasks_warning(q);

		// if we got here, no events were triggered.
		// we set the busy_waiting flag so that link_poll waits for some time
		// the next time around.
		q->busy_waiting_flag = 1;

		// If the foreman_uplink is active then break so the caller can handle it.
		if(foreman_uplink) {
			break;
		}
	}

	if(events > 0) {
		log_queue_stats(q, 1);
	}

	q->time_last_wait = timestamp_get();

	return t;
}

//check if workers' resources are available to execute more tasks
//queue should have at least q->hungry_minimum ready tasks
//@param: 	struct work_queue* - pointer to queue
//@return: 	1 if hungry, 0 otherwise
int work_queue_hungry(struct work_queue *q)
{
	//check if queue is initialized
	//return false if not
	if (q == NULL){
		return 0;
	}

	struct work_queue_stats qstats;
	work_queue_get_stats(q, &qstats);

	//if number of ready tasks is less than q->hungry_minimum, then queue is hungry
	if (qstats.tasks_waiting < q->hungry_minimum){
		return 1;
	}

	//get total available resources consumption (cores, memory, disk, gpus) of all workers of this manager
	//available = total (all) - committed (actual in use)
	int64_t workers_total_avail_cores 	= 0;
	int64_t workers_total_avail_memory 	= 0;
	int64_t workers_total_avail_disk 	= 0;
	int64_t workers_total_avail_gpus 	= 0;

	workers_total_avail_cores 	= overcommitted_resource_total(q, q->stats->total_cores) - q->stats->committed_cores;
	workers_total_avail_memory 	= overcommitted_resource_total(q, q->stats->total_memory) - q->stats->committed_memory;
	workers_total_avail_gpus	= overcommitted_resource_total(q, q->stats->total_gpus) - q->stats->committed_gpus;
	workers_total_avail_disk 	= q->stats->total_disk - q->stats->committed_disk; //never overcommit disk

	//get required resources (cores, memory, disk, gpus) of one waiting task
	int64_t ready_task_cores 	= 0;
	int64_t ready_task_memory 	= 0;
	int64_t ready_task_disk 	= 0;
	int64_t ready_task_gpus		= 0;

	struct work_queue_task *t;

	int count = task_state_count(q, NULL, WORK_QUEUE_TASK_READY);

	while(count > 0)
	{
		count--;
		t = list_pop_head(q->ready_list);

		ready_task_cores  += MAX(1,t->resources_requested->cores);
		ready_task_memory += t->resources_requested->memory;
		ready_task_disk   += t->resources_requested->disk;
		ready_task_gpus   += t->resources_requested->gpus;

		list_push_tail(q->ready_list, t);
	}

	//check possible limiting factors
	//return false if required resources exceed available resources
	if (ready_task_cores > workers_total_avail_cores){
		return 0;
	}
	if (ready_task_memory > workers_total_avail_memory){
		return 0;
	}
	if (ready_task_disk > workers_total_avail_disk){
		return 0;
	}
	if (ready_task_gpus > workers_total_avail_gpus){
		return 0;
	}

	return 1;	//all good
}

int work_queue_shut_down_workers(struct work_queue *q, int n)
{
	struct work_queue_worker *w;
	char *key;
	int i = 0;

	/* by default, remove all workers. */
	if(n < 1)
		n = hash_table_size(q->worker_table);

	if(!q)
		return -1;

	// send worker the "exit" msg
	hash_table_firstkey(q->worker_table);
	while(i < n && hash_table_nextkey(q->worker_table, &key, (void **) &w)) {
		if(itable_size(w->current_tasks) == 0) {
			shut_down_worker(q, w);

			/* shut_down_worker alters the table, so we reset it here. */
			hash_table_firstkey(q->worker_table);
			i++;
		}
	}

	return i;
}

int work_queue_specify_draining_by_hostname(struct work_queue *q, const char *hostname, int drain_flag)
{
	char *worker_hashkey = NULL;
	struct work_queue_worker *w = NULL;

	drain_flag = !!(drain_flag);

	int workers_updated = 0;

	hash_table_firstkey(q->worker_table);
	while(hash_table_nextkey(q->worker_table, &worker_hashkey, (void *) w)) {
		if (!strcmp(w->hostname, hostname)) {
			w->draining = drain_flag;
			workers_updated++;
		}
	}

	return workers_updated;
}

/**
 * Cancel submitted task as long as it has not been retrieved through wait().
 * This returns the work_queue_task struct corresponding to specified task and
 * null if the task is not found.
 */
struct work_queue_task *work_queue_cancel_by_taskid(struct work_queue *q, int taskid) {

	struct work_queue_task *matched_task = NULL;

	matched_task = itable_lookup(q->tasks, taskid);

	if(!matched_task) {
		debug(D_WQ, "Task with id %d is not found in queue.", taskid);
		return NULL;
	}

	cancel_task_on_worker(q, matched_task, WORK_QUEUE_TASK_CANCELED);

	/* change state even if task is not running on a worker. */
	change_task_state(q, matched_task, WORK_QUEUE_TASK_CANCELED);

	q->stats->tasks_cancelled++;

	return matched_task;
}

struct work_queue_task *work_queue_cancel_by_tasktag(struct work_queue *q, const char* tasktag) {

	struct work_queue_task *matched_task = NULL;

	if (tasktag){
		matched_task = find_task_by_tag(q, tasktag);

		if(matched_task) {
			return work_queue_cancel_by_taskid(q, matched_task->taskid);
		}

	}

	debug(D_WQ, "Task with tag %s is not found in queue.", tasktag);
	return NULL;
}

struct list * work_queue_cancel_all_tasks(struct work_queue *q) {
	struct list *l = list_create();
	struct work_queue_task *t;
	struct work_queue_worker *w;
	uint64_t taskid;
	char *key;

	itable_firstkey(q->tasks);
	while(itable_nextkey(q->tasks, &taskid, (void**)&t)) {
		list_push_tail(l, t);
		work_queue_cancel_by_taskid(q, taskid);
	}

	hash_table_firstkey(q->workers_with_available_results);
	while(hash_table_nextkey(q->workers_with_available_results, &key, (void **) &w)) {
		hash_table_remove(q->workers_with_available_results, key);
		hash_table_firstkey(q->workers_with_available_results);
	}

	hash_table_firstkey(q->worker_table);
	while(hash_table_nextkey(q->worker_table, &key, (void**)&w)) {

		send_worker_msg(q,w,"kill -1\n");

		itable_firstkey(w->current_tasks);
		while(itable_nextkey(w->current_tasks, &taskid, (void**)&t)) {
			//Delete any input files that are not to be cached.
			delete_worker_files(q, w, t->input_files, WORK_QUEUE_CACHE | WORK_QUEUE_PREEXIST);

			//Delete all output files since they are not needed as the task was aborted.
			delete_worker_files(q, w, t->output_files, 0);
			reap_task_from_worker(q, w, t, WORK_QUEUE_TASK_CANCELED);

			list_push_tail(l, t);
			q->stats->tasks_cancelled++;
			itable_firstkey(w->current_tasks);
		}
	}
	return l;
}

void release_all_workers(struct work_queue *q) {
	struct work_queue_worker *w;
	char *key;

	if(!q) return;

	hash_table_firstkey(q->worker_table);
	while(hash_table_nextkey(q->worker_table,&key,(void**)&w)) {
		release_worker(q, w);
		hash_table_firstkey(q->worker_table);
	}
}

int work_queue_empty(struct work_queue *q)
{
	struct work_queue_task *t;
	uint64_t taskid;

	itable_firstkey(q->tasks);
	while( itable_nextkey(q->tasks, &taskid, (void **) &t) ) {
		int state = work_queue_task_state(q, taskid);

		if( state == WORK_QUEUE_TASK_READY   )           return 0;
		if( state == WORK_QUEUE_TASK_RUNNING )           return 0;
		if( state == WORK_QUEUE_TASK_WAITING_RETRIEVAL ) return 0;
		if( state == WORK_QUEUE_TASK_RETRIEVED )         return 0;
	}

	return 1;
}

void work_queue_specify_keepalive_interval(struct work_queue *q, int interval)
{
	q->keepalive_interval = interval;
}

void work_queue_specify_keepalive_timeout(struct work_queue *q, int timeout)
{
	q->keepalive_timeout = timeout;
}

void work_queue_manager_preferred_connection(struct work_queue *q, const char *preferred_connection)
{
	free(q->manager_preferred_connection);
	assert(preferred_connection);

	if(strcmp(preferred_connection, "by_ip") && strcmp(preferred_connection, "by_hostname") && strcmp(preferred_connection, "by_apparent_ip")) {
		fatal("manager_preferred_connection should be one of: by_ip, by_hostname, by_apparent_ip");
	}

	q->manager_preferred_connection = xxstrdup(preferred_connection);
}

int work_queue_tune(struct work_queue *q, const char *name, double value)
{

	if(!strcmp(name, "resource-submit-multiplier") || !strcmp(name, "asynchrony-multiplier")) {
		q->resource_submit_multiplier = MAX(value, 1.0);

	} else if(!strcmp(name, "min-transfer-timeout")) {
		q->minimum_transfer_timeout = (int)value;

	} else if(!strcmp(name, "foreman-transfer-timeout")) {
		q->foreman_transfer_timeout = (int)value;

	} else if(!strcmp(name, "default-transfer-rate")) {
		q->default_transfer_rate = value;

	} else if(!strcmp(name, "transfer-outlier-factor")) {
		q->transfer_outlier_factor = value;

	} else if(!strcmp(name, "fast-abort-multiplier")) {
		work_queue_activate_fast_abort(q, value);

	} else if(!strcmp(name, "keepalive-interval")) {
		q->keepalive_interval = MAX(0, (int)value);

	} else if(!strcmp(name, "keepalive-timeout")) {
		q->keepalive_timeout = MAX(0, (int)value);

	} else if(!strcmp(name, "short-timeout")) {
		q->short_timeout = MAX(1, (int)value);

	} else if(!strcmp(name, "long-timeout")) {
		q->long_timeout = MAX(1, (int)value);

	} else if(!strcmp(name, "category-steady-n-tasks")) {
		category_tune_bucket_size("category-steady-n-tasks", (int) value);

	} else if(!strcmp(name, "hungry-minimum")) {
		q->hungry_minimum = MAX(1, (int)value);

	} else if(!strcmp(name, "wait-for-workers")) {
		q->wait_for_workers = MAX(0, (int)value);

	} else if(!strcmp(name, "wait-retrieve-many")){
		q->wait_retrieve_many = MAX(0, (int)value);

	} else if(!strcmp(name, "force-proportional-resources") || !strcmp(name, "proportional-resources")) {
		q->proportional_resources = MAX(0, (int)value);

	} else if(!strcmp(name, "force-proportional-resources-whole-tasks") || !strcmp(name, "proportional-whole-tasks")) {
		q->proportional_whole_tasks = MAX(0, (int)value);

	} else {
		debug(D_NOTICE|D_WQ, "Warning: tuning parameter \"%s\" not recognized\n", name);
		return -1;
	}

	return 0;
}

void work_queue_enable_process_module(struct work_queue *q)
{
	q->process_pending_check = 1;
}

char * work_queue_get_worker_summary( struct work_queue *q )
{
	return strdup("n/a");
}

void work_queue_set_bandwidth_limit(struct work_queue *q, const char *bandwidth)
{
	q->bandwidth = string_metric_parse(bandwidth);
}

double work_queue_get_effective_bandwidth(struct work_queue *q)
{
	double queue_bandwidth = get_queue_transfer_rate(q, NULL)/MEGABYTE; //return in MB per second
	return queue_bandwidth;
}

static void fill_deprecated_queue_stats(struct work_queue *q, struct work_queue_stats *s) {
	s->total_workers_connected = s->workers_connected;
	s->total_workers_joined = s->workers_joined;
	s->total_workers_removed = s->workers_removed;
	s->total_workers_lost = s->workers_lost;
	s->total_workers_idled_out = s->workers_idled_out;
	s->total_workers_fast_aborted = s->workers_fast_aborted;

	s->tasks_complete = s->tasks_with_results;

	s->total_tasks_dispatched = s->tasks_dispatched;
	s->total_tasks_complete = s->tasks_done;
	s->total_tasks_failed = s->tasks_failed;
	s->total_tasks_cancelled = s->tasks_cancelled;
	s->total_exhausted_attempts = s->tasks_exhausted_attempts;

	s->start_time = s->time_when_started;
	s->total_send_time = s->time_send;
	s->total_receive_time = s->time_receive;
	s->total_good_transfer_time = s->time_send_good + s->time_receive_good;

	s->total_execute_time = s->time_workers_execute;
	s->total_good_execute_time = s->time_workers_execute_good;
	s->total_exhausted_execute_time = s->time_workers_execute_exhaustion;

	s->total_bytes_sent = s->bytes_sent;
	s-> total_bytes_received = s->bytes_received;

	s->capacity = s->capacity_cores;

	s->port = q->port;
	s->priority = q->priority;
	s->workers_ready = s->workers_idle;
	s->workers_full  = s->workers_busy;
	s->total_worker_slots = s->tasks_dispatched;
	s->avg_capacity = s->capacity_cores;

	timestamp_t wall_clock_time = timestamp_get() - q->stats->time_when_started;
	if(wall_clock_time > 0 && s->workers_connected > 0) {
		s->efficiency = (double) (q->stats->time_workers_execute_good) / (wall_clock_time * s->workers_connected);
	}

	if(wall_clock_time>0) {
		s->idle_percentage = (double) q->stats->time_polling / wall_clock_time;
	}
}

void work_queue_get_stats(struct work_queue *q, struct work_queue_stats *s)
{
	struct work_queue_stats *qs;
	qs = q->stats;

	memcpy(s, qs, sizeof(*s));

	//info about workers
	s->workers_connected = count_workers(q, WORKER_TYPE_WORKER | WORKER_TYPE_FOREMAN);
	s->workers_init      = count_workers(q, WORK_QUEUE_TASK_UNKNOWN);
	s->workers_busy      = workers_with_tasks(q);
	s->workers_idle      = s->workers_connected - s->workers_busy;
	// s->workers_able computed below.

	//info about tasks
	s->tasks_waiting      = task_state_count(q, NULL, WORK_QUEUE_TASK_READY);
	s->tasks_with_results = task_state_count(q, NULL, WORK_QUEUE_TASK_WAITING_RETRIEVAL);
	s->tasks_on_workers   = task_state_count(q, NULL, WORK_QUEUE_TASK_RUNNING) + s->tasks_with_results;

	{
		//accumulate tasks running, from workers:
		char *key;
		struct work_queue_worker *w;
		s->tasks_running = 0;
		hash_table_firstkey(q->worker_table);
		while(hash_table_nextkey(q->worker_table, &key, (void **) &w)) {
			accumulate_stat(s, w->stats, tasks_running);
		}
		/* (see work_queue_get_stats_hierarchy for an explanation on the
		 * following line) */
		s->tasks_running = MIN(s->tasks_running, s->tasks_on_workers);
	}

	compute_capacity(q, s);

	//info about resources
	s->bandwidth = work_queue_get_effective_bandwidth(q);
	struct work_queue_resources r;
	aggregate_workers_resources(q,&r,NULL);

	s->total_cores = r.cores.total;
	s->total_memory = r.memory.total;
	s->total_disk = r.disk.total;
	s->total_gpus = r.gpus.total;

	s->committed_cores = r.cores.inuse;
	s->committed_memory = r.memory.inuse;
	s->committed_disk = r.disk.inuse;
	s->committed_gpus = r.gpus.inuse;

	s->min_cores = r.cores.smallest;
	s->max_cores = r.cores.largest;
	s->min_memory = r.memory.smallest;
	s->max_memory = r.memory.largest;
	s->min_disk = r.disk.smallest;
	s->max_disk = r.disk.largest;
	s->min_gpus = r.gpus.smallest;
	s->max_gpus = r.gpus.largest;

	s->workers_able = count_workers_for_waiting_tasks(q, largest_seen_resources(q, NULL));

	fill_deprecated_queue_stats(q, s);
}

void work_queue_get_stats_hierarchy(struct work_queue *q, struct work_queue_stats *s)
{
	work_queue_get_stats(q, s);

	char *key;
	struct work_queue_worker *w;

	/* Consider running only if reported by some hand. */
	s->tasks_running = 0;
	s->workers_connected = 0;

	hash_table_firstkey(q->worker_table);
	while(hash_table_nextkey(q->worker_table, &key, (void **) &w)) {
		if(w->type == WORKER_TYPE_FOREMAN)
		{
			accumulate_stat(s, w->stats, workers_joined);
			accumulate_stat(s, w->stats, workers_removed);
			accumulate_stat(s, w->stats, workers_idled_out);
			accumulate_stat(s, w->stats, workers_fast_aborted);
			accumulate_stat(s, w->stats, workers_lost);

			accumulate_stat(s, w->stats, time_send);
			accumulate_stat(s, w->stats, time_receive);
			accumulate_stat(s, w->stats, time_send_good);
			accumulate_stat(s, w->stats, time_receive_good);

			accumulate_stat(s, w->stats, time_workers_execute);
			accumulate_stat(s, w->stats, time_workers_execute_good);
			accumulate_stat(s, w->stats, time_workers_execute_exhaustion);

			accumulate_stat(s, w->stats, bytes_sent);
			accumulate_stat(s, w->stats, bytes_received);
		}

		accumulate_stat(s, w->stats, tasks_waiting);
		accumulate_stat(s, w->stats, tasks_running);
	}

	/* we rely on workers messages to update tasks_running. such data are
	 * attached to keepalive messages, thus tasks_running is not always
	 * current. Here we simply enforce that there can be more tasks_running
	 * that tasks_on_workers. */
	s->tasks_running = MIN(s->tasks_running, s->tasks_on_workers);

	/* Account also for workers connected directly to the manager. */
	s->workers_connected = s->workers_joined - s->workers_removed;

	s->workers_joined       += q->stats_disconnected_workers->workers_joined;
	s->workers_removed      += q->stats_disconnected_workers->workers_removed;
	s->workers_idled_out    += q->stats_disconnected_workers->workers_idled_out;
	s->workers_fast_aborted += q->stats_disconnected_workers->workers_fast_aborted;
	s->workers_lost         += q->stats_disconnected_workers->workers_lost;

	s->time_send         += q->stats_disconnected_workers->time_send;
	s->time_receive      += q->stats_disconnected_workers->time_receive;
	s->time_send_good    += q->stats_disconnected_workers->time_send_good;
	s->time_receive_good += q->stats_disconnected_workers->time_receive_good;

	s->time_workers_execute            += q->stats_disconnected_workers->time_workers_execute;
	s->time_workers_execute_good       += q->stats_disconnected_workers->time_workers_execute_good;
	s->time_workers_execute_exhaustion += q->stats_disconnected_workers->time_workers_execute_exhaustion;

	s->bytes_sent      += q->stats_disconnected_workers->bytes_sent;
	s->bytes_received  += q->stats_disconnected_workers->bytes_received;

	fill_deprecated_queue_stats(q, s);
}

void work_queue_get_stats_category(struct work_queue *q, const char *category, struct work_queue_stats *s)
{
	struct category *c = work_queue_category_lookup_or_create(q, category);
	struct work_queue_stats *cs = c->wq_stats;
	memcpy(s, cs, sizeof(*s));

	//info about tasks
	s->tasks_waiting      = task_state_count(q, category, WORK_QUEUE_TASK_READY);
	s->tasks_running      = task_state_count(q, category, WORK_QUEUE_TASK_RUNNING);
	s->tasks_with_results = task_state_count(q, category, WORK_QUEUE_TASK_WAITING_RETRIEVAL);
	s->tasks_on_workers   = s->tasks_running + s->tasks_with_results;
	s->tasks_submitted    = c->total_tasks + s->tasks_waiting + s->tasks_on_workers;

	s->workers_able  = count_workers_for_waiting_tasks(q, largest_seen_resources(q, c->name));
}

char *work_queue_status(struct work_queue *q, const char *request) {
	struct jx *a = construct_status_message(q, request);

	if(!a) {
		return "[]";
	}

	char *result = jx_print_string(a);

	jx_delete(a);

	return result;
}

void aggregate_workers_resources( struct work_queue *q, struct work_queue_resources *total, struct hash_table *features)
{
	struct work_queue_worker *w;
	char *key;

	bzero(total, sizeof(struct work_queue_resources));

	if(hash_table_size(q->worker_table)==0) {
		return;
	}

	if(features) {
		hash_table_clear(features,0);
	}

	hash_table_firstkey(q->worker_table);
	while(hash_table_nextkey(q->worker_table,&key,(void**)&w)) {
		if(w->resources->tag < 0)
			continue;

		work_queue_resources_add(total,w->resources);

		if(features) {
			if(w->features) {
				char *key;
				void *dummy;
				hash_table_firstkey(w->features);
				while(hash_table_nextkey(w->features, &key, &dummy)) {
					hash_table_insert(features, key, (void **) 1);
				}
			}
		}
	}
}

int work_queue_specify_log(struct work_queue *q, const char *logfile)
{
	q->logfile = fopen(logfile, "a");
	if(q->logfile) {
		setvbuf(q->logfile, NULL, _IOLBF, 2048); // line buffered, we don't want incomplete lines
		fprintf(q->logfile,
				// start with a comment
				"#"
			// time:
			" timestamp"
			// workers current:
			" workers_connected workers_init workers_idle workers_busy workers_able"
			// workers cumulative:
			" workers_joined workers_removed workers_released workers_idled_out workers_blocked workers_fast_aborted workers_lost"
			// tasks current:
			" tasks_waiting tasks_on_workers tasks_running tasks_with_results"
			// tasks cumulative
			" tasks_submitted tasks_dispatched tasks_done tasks_failed tasks_cancelled tasks_exhausted_attempts"
			// manager time statistics:
			" time_send time_receive time_send_good time_receive_good time_status_msgs time_internal time_polling time_application"
			// workers time statistics:
			" time_execute time_execute_good time_execute_exhaustion"
			// bandwidth:
			" bytes_sent bytes_received bandwidth"
			// resources:
			" capacity_tasks capacity_cores capacity_memory capacity_disk capacity_instantaneous capacity_weighted manager_load"
			" total_cores total_memory total_disk"
			" committed_cores committed_memory committed_disk"
			" max_cores max_memory max_disk"
			" min_cores min_memory min_disk"
			// end with a newline
			"\n"
			);
		log_queue_stats(q, 1);
		debug(D_WQ, "log enabled and is being written to %s\n", logfile);
		return 1;
	} else {
		debug(D_NOTICE | D_WQ, "couldn't open logfile %s: %s\n", logfile, strerror(errno));
		return 0;
	}
}

static void write_transaction(struct work_queue *q, const char *str) {
	if(!q->transactions_logfile)
		return;

	fprintf(q->transactions_logfile, "%" PRIu64 " %d %s\n", timestamp_get(),getpid(),str);
	fflush(q->transactions_logfile);
}

static void write_transaction_task(struct work_queue *q, struct work_queue_task *t) {
	if(!q->transactions_logfile)
		return;

	struct buffer B;
	buffer_init(&B);

	work_queue_task_state_t state = (uintptr_t) itable_lookup(q->task_state_map, t->taskid);

	buffer_printf(&B, "TASK %d %s", t->taskid, task_state_str(state));

	if(state == WORK_QUEUE_TASK_UNKNOWN) {
			/* do not add any info */
	} else if(state == WORK_QUEUE_TASK_READY) {
		const char *allocation = (t->resource_request == CATEGORY_ALLOCATION_FIRST ? "FIRST_RESOURCES" : "MAX_RESOURCES");
		buffer_printf(&B, " %s %s ", t->category, allocation);
		rmsummary_print_buffer(&B, task_min_resources(q, t), 1);
	} else if(state == WORK_QUEUE_TASK_CANCELED) {
			/* do not add any info */
	} else if(state == WORK_QUEUE_TASK_RETRIEVED || state == WORK_QUEUE_TASK_DONE) {
		buffer_printf(&B, " %s ", work_queue_result_str(t->result));
		buffer_printf(&B, " %d ", t->return_status);

		if(t->resources_measured) {
			if(t->result == WORK_QUEUE_RESULT_RESOURCE_EXHAUSTION) {
				rmsummary_print_buffer(&B, t->resources_measured->limits_exceeded, 1);
				buffer_printf(&B, " ");
			}
			else {
				// no limits broken, thus printing an empty dictionary
				buffer_printf(&B, " {} ");
			}

			struct jx *m = rmsummary_to_json(t->resources_measured, /* only resources */ 1);
			jx_insert(m, jx_string("wq_input_size"), jx_arrayv(jx_double(t->bytes_sent/((double) MEGABYTE)), jx_string("MB"), NULL));
			jx_insert(m, jx_string("wq_output_size"), jx_arrayv(jx_double(t->bytes_received/((double) MEGABYTE)), jx_string("MB"), NULL));
			jx_insert(m, jx_string("wq_input_time"), jx_arrayv(jx_double((t->time_when_commit_end - t->time_when_commit_start)/((double) ONE_SECOND)), jx_string("s"), NULL));
			jx_insert(m, jx_string("wq_output_time"), jx_arrayv(jx_double((t->time_when_done - t->time_when_retrieval)/((double) ONE_SECOND)), jx_string("s"), NULL));
			jx_print_buffer(m, &B);
			jx_delete(m);
		} else {
			// no resources measured, one empty dictionary for limits broken, other for resources.
			buffer_printf(&B, " {} {}");
		}
	} else {
		struct work_queue_worker *w = itable_lookup(q->worker_task_map, t->taskid);
		const char *worker_str = "worker-info-not-available";

		if(w) {
			worker_str = w->addrport;
			buffer_printf(&B, " %s ", worker_str);

			if(state == WORK_QUEUE_TASK_RUNNING) {
				const char *allocation = (t->resource_request == CATEGORY_ALLOCATION_FIRST ? "FIRST_RESOURCES" : "MAX_RESOURCES");
				buffer_printf(&B, " %s ", allocation);
				const struct rmsummary *box = itable_lookup(w->current_tasks_boxes, t->taskid);
				rmsummary_print_buffer(&B, box, 1);
			} else if(state == WORK_QUEUE_TASK_WAITING_RETRIEVAL) {
				/* do not add any info */
			}
		}
	}

	write_transaction(q, buffer_tostring(&B));
	buffer_free(&B);
}

static void write_transaction_category(struct work_queue *q, struct category *c) {

	if(!q->transactions_logfile)
		return;

	if(!c)
		return;

	struct buffer B;
	buffer_init(&B);

	buffer_printf(&B, "CATEGORY %s MAX ", c->name);
	rmsummary_print_buffer(&B, category_bucketing_dynamic_task_max_resources(c, NULL, CATEGORY_ALLOCATION_MAX, -1), 1);
	write_transaction(q, buffer_tostring(&B));
	buffer_rewind(&B, 0);

	buffer_printf(&B, "CATEGORY %s MIN ", c->name);
	rmsummary_print_buffer(&B, category_dynamic_task_min_resources(c, NULL, CATEGORY_ALLOCATION_FIRST), 1);
	write_transaction(q, buffer_tostring(&B));
	buffer_rewind(&B, 0);

	const char *mode;

	switch(c->allocation_mode) {
		case CATEGORY_ALLOCATION_MODE_MAX:
			mode = "MAX";
			break;
		case CATEGORY_ALLOCATION_MODE_MIN_WASTE:
			mode = "MIN_WASTE";
			break;
		case CATEGORY_ALLOCATION_MODE_MAX_THROUGHPUT:
			mode = "MAX_THROUGHPUT";
			break;
		case CATEGORY_ALLOCATION_MODE_FIXED:
		default:
			mode = "FIXED";
			break;
		case CATEGORY_ALLOCATION_MODE_GREEDY_BUCKETING:
			mode = "GREEDY_BUCKETING";
			break;
		case CATEGORY_ALLOCATION_MODE_EXHAUSTIVE_BUCKETING:
			mode = "EXHAUSTIVE_BUCKETING";
			break;
	}

	buffer_printf(&B, "CATEGORY %s FIRST %s ", c->name, mode);
	rmsummary_print_buffer(&B, category_bucketing_dynamic_task_max_resources(c, NULL, CATEGORY_ALLOCATION_FIRST, -1), 1);
	write_transaction(q, buffer_tostring(&B));

	buffer_free(&B);
}

static void write_transaction_worker(struct work_queue *q, struct work_queue_worker *w, int leaving, worker_disconnect_reason reason_leaving) {
	struct buffer B;
	buffer_init(&B);

	buffer_printf(&B, "WORKER %s %s ", w->workerid, w->addrport);

	if(leaving) {
		buffer_printf(&B, " DISCONNECTION");
		switch(reason_leaving) {
			case WORKER_DISCONNECT_IDLE_OUT:
				buffer_printf(&B, " IDLE_OUT");
				break;
			case WORKER_DISCONNECT_FAST_ABORT:
				buffer_printf(&B, " FAST_ABORT");
				break;
			case WORKER_DISCONNECT_FAILURE:
				buffer_printf(&B, " FAILURE");
				break;
			case WORKER_DISCONNECT_STATUS_WORKER:
				buffer_printf(&B, " STATUS_WORKER");
				break;
			case WORKER_DISCONNECT_EXPLICIT:
				buffer_printf(&B, " EXPLICIT");
				break;
			case WORKER_DISCONNECT_UNKNOWN:
			default:
				buffer_printf(&B, " UNKNOWN");
				break;
		}
	} else {
		buffer_printf(&B, " CONNECTION");
	}

	write_transaction(q, buffer_tostring(&B));

	buffer_free(&B);
}

static void write_transaction_worker_resources(struct work_queue *q, struct work_queue_worker *w) {

	struct rmsummary *s = rmsummary_create(-1);

	s->cores  = w->resources->cores.total;
	s->memory = w->resources->memory.total;
	s->disk   = w->resources->disk.total;

	char *rjx = rmsummary_print_string(s, 1);


	struct buffer B;
	buffer_init(&B);

	buffer_printf(&B, "WORKER %s RESOURCES %s", w->workerid, rjx);

	write_transaction(q, buffer_tostring(&B));

	rmsummary_delete(s);
	buffer_free(&B);
	free(rjx);
}


static void write_transaction_transfer(struct work_queue *q, struct work_queue_worker *w, struct work_queue_task *t, struct work_queue_file *f, size_t size_in_bytes, int time_in_usecs, work_queue_file_type_t type) {
	struct buffer B;
	buffer_init(&B);
	buffer_printf(&B, "TRANSFER ");
	buffer_printf(&B, type == WORK_QUEUE_INPUT ? "INPUT":"OUTPUT");
	buffer_printf(&B, " %d", t->taskid);
	buffer_printf(&B, " %d", f->flags & WORK_QUEUE_CACHE);
	buffer_printf(&B, " %f", size_in_bytes / ((double) MEGABYTE));
	buffer_printf(&B, " %f", time_in_usecs / ((double) USECOND));
	buffer_printf(&B, " %s", f->remote_name);

	write_transaction(q, buffer_tostring(&B));
	buffer_free(&B);
}


int work_queue_specify_transactions_log(struct work_queue *q, const char *logfile) {
	q->transactions_logfile =fopen(logfile, "a");
	if(q->transactions_logfile) {
		setvbuf(q->transactions_logfile, NULL, _IOLBF, 1024); // line buffered, we don't want incomplete lines
		debug(D_WQ, "transactions log enabled and is being written to %s\n", logfile);

		fprintf(q->transactions_logfile, "# time manager_pid MANAGER START|END\n");
		fprintf(q->transactions_logfile, "# time manager_pid WORKER worker_id host:port CONNECTION\n");
		fprintf(q->transactions_logfile, "# time manager_pid WORKER worker_id host:port DISCONNECTION (UNKNOWN|IDLE_OUT|FAST_ABORT|FAILURE|STATUS_WORKER|EXPLICIT\n");
		fprintf(q->transactions_logfile, "# time manager_pid WORKER worker_id RESOURCES {resources}\n");
		fprintf(q->transactions_logfile, "# time manager_pid CATEGORY name MAX {resources_max_per_task}\n");
		fprintf(q->transactions_logfile, "# time manager_pid CATEGORY name MIN {resources_min_per_task_per_worker}\n");
		fprintf(q->transactions_logfile, "# time manager_pid CATEGORY name FIRST (FIXED|MAX|MIN_WASTE|MAX_THROUGHPUT) {resources_requested}\n");
		fprintf(q->transactions_logfile, "# time manager_pid TASK taskid WAITING category_name (FIRST_RESOURCES|MAX_RESOURCES) {resources_requested}\n");
		fprintf(q->transactions_logfile, "# time manager_pid TASK taskid RUNNING worker_address (FIRST_RESOURCES|MAX_RESOURCES) {resources_allocated}\n");
		fprintf(q->transactions_logfile, "# time manager_pid TASK taskid WAITING_RETRIEVAL worker_address\n");
		fprintf(q->transactions_logfile, "# time manager_pid TASK taskid (RETRIEVED|DONE) (SUCCESS|SIGNAL|END_TIME|FORSAKEN|MAX_RETRIES|MAX_WALLTIME|UNKNOWN|RESOURCE_EXHAUSTION) exit_code {limits_exceeded} {resources_measured}\n");
		fprintf(q->transactions_logfile, "# time manager_pid TRANSFER (INPUT|OUTPUT) taskid cache_flag sizeinmb walltime filename\n");
		fprintf(q->transactions_logfile, "\n");

		write_transaction(q, "MANAGER START");

		return 1;
	}
	else
	{
		debug(D_NOTICE | D_WQ, "couldn't open transactions logfile %s: %s\n", logfile, strerror(errno));
		return 0;
	}
}

void work_queue_accumulate_task(struct work_queue *q, struct work_queue_task *t) {
	const char *name   = t->category ? t->category : "default";
	struct category *c = work_queue_category_lookup_or_create(q, name);

	struct work_queue_stats *s = c->wq_stats;

	s->bytes_sent     += t->bytes_sent;
	s->bytes_received += t->bytes_received;

	s->time_workers_execute += t->time_workers_execute_last;

	s->time_send    += t->time_when_commit_end - t->time_when_commit_start;
	s->time_receive += t->time_when_done - t->time_when_retrieval;

	s->bandwidth = (1.0*MEGABYTE*(s->bytes_sent + s->bytes_received))/(s->time_send + s->time_receive + 1);

	q->stats->tasks_done++;

	if(t->result == WORK_QUEUE_RESULT_SUCCESS)
	{
		q->stats->time_workers_execute_good += t->time_workers_execute_last;
		q->stats->time_send_good            += t->time_when_commit_end - t->time_when_commit_end;
		q->stats->time_receive_good         += t->time_when_done - t->time_when_retrieval;

		s->tasks_done++;
		s->time_workers_execute_good += t->time_workers_execute_last;
		s->time_send_good            += t->time_when_commit_end - t->time_when_commit_end;
		s->time_receive_good         += t->time_when_done - t->time_when_retrieval;
	} else {
		s->tasks_failed++;

		if(t->result == WORK_QUEUE_RESULT_RESOURCE_EXHAUSTION) {
			s->time_workers_execute_exhaustion += t->time_workers_execute_last;

			q->stats->time_workers_execute_exhaustion += t->time_workers_execute_last;
			q->stats->tasks_exhausted_attempts++;

			t->time_workers_execute_exhaustion += t->time_workers_execute_last;
			t->exhausted_attempts++;
		}
	}

	int success;	//1 if success, 0 if resource exhaustion, -1 if other errors
	/* accumulate resource summary to category only if task result makes it meaningful. */
	switch(t->result) {
		case WORK_QUEUE_RESULT_SUCCESS:
		case WORK_QUEUE_RESULT_SIGNAL:
		case WORK_QUEUE_RESULT_RESOURCE_EXHAUSTION:
		case WORK_QUEUE_RESULT_TASK_MAX_RUN_TIME:
		case WORK_QUEUE_RESULT_DISK_ALLOC_FULL:
		case WORK_QUEUE_RESULT_OUTPUT_TRANSFER_ERROR:
			if (t->result == WORK_QUEUE_RESULT_SUCCESS) {
				success = 1;
			}
			else if (t->result == WORK_QUEUE_RESULT_RESOURCE_EXHAUSTION) {
				success = 0;
			}
			else {
				success = -1;
			}
			if(category_bucketing_accumulate_summary(c, t->resources_measured, q->current_max_worker, t->taskid, success)) {
				write_transaction_category(q, c);
			}
			break;
		case WORK_QUEUE_RESULT_INPUT_MISSING:
		case WORK_QUEUE_RESULT_OUTPUT_MISSING:
		case WORK_QUEUE_RESULT_TASK_TIMEOUT:
		case WORK_QUEUE_RESULT_UNKNOWN:
		case WORK_QUEUE_RESULT_FORSAKEN:
		case WORK_QUEUE_RESULT_MAX_RETRIES:
		default:
			break;
	}
}

void work_queue_initialize_categories(struct work_queue *q, struct rmsummary *max, const char *summaries_file) {
	categories_initialize(q->categories, max, summaries_file);
}

void work_queue_specify_max_resources(struct work_queue *q,  const struct rmsummary *rm) {
	work_queue_specify_category_max_resources(q,  "default", rm);
}

void work_queue_specify_min_resources(struct work_queue *q,  const struct rmsummary *rm) {
	work_queue_specify_category_min_resources(q,  "default", rm);
}

void work_queue_specify_category_max_resources(struct work_queue *q,  const char *category, const struct rmsummary *rm) {
	struct category *c = work_queue_category_lookup_or_create(q, category);
	category_specify_max_allocation(c, rm);
}

void work_queue_specify_category_min_resources(struct work_queue *q,  const char *category, const struct rmsummary *rm) {
	struct category *c = work_queue_category_lookup_or_create(q, category);
	category_specify_min_allocation(c, rm);
}

void work_queue_specify_category_first_allocation_guess(struct work_queue *q,  const char *category, const struct rmsummary *rm) {
	struct category *c = work_queue_category_lookup_or_create(q, category);
	category_specify_first_allocation_guess(c, rm);
}

int work_queue_specify_category_mode(struct work_queue *q, const char *category, work_queue_category_mode_t mode) {

	switch(mode) {
		case WORK_QUEUE_ALLOCATION_MODE_FIXED:
		case WORK_QUEUE_ALLOCATION_MODE_MAX:
		case WORK_QUEUE_ALLOCATION_MODE_MIN_WASTE:
		case WORK_QUEUE_ALLOCATION_MODE_MAX_THROUGHPUT:
		case WORK_QUEUE_ALLOCATION_MODE_GREEDY_BUCKETING:
		case WORK_QUEUE_ALLOCATION_MODE_EXHAUSTIVE_BUCKETING:
			break;
		default:
			notice(D_WQ, "Unknown category mode specified.");
			return 0;
			break;
	}

	if(!category) {
		q->allocation_default_mode = mode;
	}
	else {
		struct category *c = work_queue_category_lookup_or_create(q, category);
		category_specify_allocation_mode(c, (category_mode_t) mode);
		write_transaction_category(q, c);
	}

	return 1;
}

int work_queue_enable_category_resource(struct work_queue *q, const char *category, const char *resource, int autolabel) {

	struct category *c = work_queue_category_lookup_or_create(q, category);

	return category_enable_auto_resource(c, resource, autolabel);
}

const struct rmsummary *task_max_resources(struct work_queue *q, struct work_queue_task *t) {

	struct category *c = work_queue_category_lookup_or_create(q, t->category);

	return category_bucketing_dynamic_task_max_resources(c, t->resources_requested, t->resource_request, t->taskid);
}

const struct rmsummary *task_min_resources(struct work_queue *q, struct work_queue_task *t) {
	struct category *c = work_queue_category_lookup_or_create(q, t->category);

	const struct rmsummary *s = category_dynamic_task_min_resources(c, t->resources_requested, t->resource_request);

	if(t->resource_request != CATEGORY_ALLOCATION_FIRST || !q->current_max_worker) {
		return s;
	}

	// If this task is being tried for the first time, we take the minimum as
	// the minimum between what we have observed and the largest worker. This
	// is to eliminate observed outliers that would prevent new tasks to run.
	if((q->current_max_worker->cores > 0 && q->current_max_worker->cores < s->cores)
			|| (q->current_max_worker->memory > 0 && q->current_max_worker->memory < s->memory)
			|| (q->current_max_worker->disk > 0 && q->current_max_worker->disk < s->disk)
			|| (q->current_max_worker->gpus > 0 && q->current_max_worker->gpus < s->gpus)) {

		struct rmsummary *r = rmsummary_create(-1);

		rmsummary_merge_override(r, q->current_max_worker);
		rmsummary_merge_override(r, t->resources_requested);

		s = category_dynamic_task_min_resources(c, r, t->resource_request);
		rmsummary_delete(r);
	}

	return s;
}

struct category *work_queue_category_lookup_or_create(struct work_queue *q, const char *name) {
	struct category *c = category_lookup_or_create(q->categories, name);

	if(!c->wq_stats) {
		c->wq_stats = calloc(1, sizeof(struct work_queue_stats));
		category_specify_allocation_mode(c, (category_mode_t) q->allocation_default_mode);
	}

	return c;
}

char *work_queue_generate_disk_alloc_full_filename(char *pwd, int taskid) {

	path_remove_trailing_slashes(pwd);
	if(!taskid) {
		return string_format("%s/cctools_disk_allocation_exhausted.log", pwd);
	}
	return string_format("%s/cctools_disk_allocation_exhausted.%d.log", pwd, taskid);
}

int work_queue_specify_min_taskid(struct work_queue *q, int minid) {

	if(minid > q->next_taskid) {
		q->next_taskid = minid;
	}

	return q->next_taskid;
}

//the functions below are used by qsort in order to sort the workers summary data
size_t sort_work_queue_worker_summary_offset = 0;
int sort_work_queue_worker_cmp(const void *a, const void *b)
{
	const struct rmsummary *x = *((const struct rmsummary **) a);
	const struct rmsummary *y = *((const struct rmsummary **) b);

	double count_x = x->workers;
	double count_y = y->workers;

	double res_x = rmsummary_get_by_offset(x, sort_work_queue_worker_summary_offset);
	double res_y = rmsummary_get_by_offset(y, sort_work_queue_worker_summary_offset);


	if(res_x == res_y) {
		return count_y - count_x;
	}
	else {
		return res_y - res_x;
	}
}


// function used by other functions
static void sort_work_queue_worker_summary(struct rmsummary **worker_data, int count, const char *sortby)
{
	if(!strcmp(sortby, "cores")) {
		sort_work_queue_worker_summary_offset = offsetof(struct rmsummary, cores);
	} else if(!strcmp(sortby, "memory")) {
		sort_work_queue_worker_summary_offset = offsetof(struct rmsummary, memory);
	} else if(!strcmp(sortby, "disk")) {
		sort_work_queue_worker_summary_offset = offsetof(struct rmsummary, disk);
	} else if(!strcmp(sortby, "gpus")) {
		sort_work_queue_worker_summary_offset = offsetof(struct rmsummary, gpus);
	} else if(!strcmp(sortby, "workers")) {
		sort_work_queue_worker_summary_offset = offsetof(struct rmsummary, workers);
	} else {
		debug(D_NOTICE, "Invalid field to sort worker summaries. Valid fields are: cores, memory, disk, gpus, and workers.");
		sort_work_queue_worker_summary_offset = offsetof(struct rmsummary, memory);
	}

	qsort(&worker_data[0], count, sizeof(struct rmsummary *), sort_work_queue_worker_cmp);
}


// round to powers of two log scale with 1/n divisions
static double round_to_nice_power_of_2(double value, int n) {
	double exp_org = log2(value);
	double below = pow(2, floor(exp_org));

	double rest = value - below;
	double fact = below/n;

	double rounded = below + floor(rest/fact) * fact;

	return rounded;
}


struct rmsummary **work_queue_workers_summary(struct work_queue *q) {
	struct work_queue_worker *w;
	struct rmsummary *s;
	char *id;
	char *resources_key;


	struct hash_table *workers_count = hash_table_create(0, 0);

	hash_table_firstkey(q->worker_table);
	while(hash_table_nextkey(q->worker_table, &id, (void**) &w)) {
		if (w->resources->tag < 0) {
			// worker has not yet declared resources
			continue;
		}

		int cores = w->resources->cores.total;
		int memory = round_to_nice_power_of_2(w->resources->memory.total, 8);
		int disk = round_to_nice_power_of_2(w->resources->disk.total, 8);
		int gpus = w->resources->gpus.total;

		char *resources_key = string_format("%d_%d_%d_%d", cores, memory, disk, gpus);

		struct rmsummary *s = hash_table_lookup(workers_count, resources_key);
		if(!s) {
			s = rmsummary_create(-1);
			s->cores = cores;
			s->memory = memory;
			s->disk = disk;
			s->gpus = gpus;
			s->workers = 0;

			hash_table_insert(workers_count, resources_key, (void *) s);
		}
		free(resources_key);

		s->workers++;
	}

	int count = 0;
	struct rmsummary **worker_data = (struct rmsummary **) malloc((hash_table_size(workers_count) + 1) * sizeof(struct rmsummary *));

	hash_table_firstkey(workers_count);
	while(hash_table_nextkey(workers_count, &resources_key, (void**) &s)) {
		worker_data[count] = s;
		count++;
	}

	worker_data[count] = NULL;

	hash_table_delete(workers_count);

	sort_work_queue_worker_summary(worker_data, count, "disk");
	sort_work_queue_worker_summary(worker_data, count, "memory");
	sort_work_queue_worker_summary(worker_data, count, "gpus");
	sort_work_queue_worker_summary(worker_data, count, "cores");
	sort_work_queue_worker_summary(worker_data, count, "workers");

	return worker_data;
}

/* vim: set noexpandtab tabstop=4: */
