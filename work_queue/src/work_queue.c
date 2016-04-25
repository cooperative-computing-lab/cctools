/*
Copyright (C) 2014- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

/*
The following major problems must be fixed:
- The capacity code assumes one task per worker.
- The log specification need to be updated.
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
#include "category.h"
#include "copy_stream.h"
#include "random.h"
#include "process.h"
#include "path.h"
#include "md5.h"
#include "url_encode.h"
#include "jx_print.h"
#include "shell.h"

#include "host_disk_info.h"

#include <unistd.h>
#include <dirent.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/wait.h>

#include <errno.h>
#include <limits.h>
#include <math.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#ifdef CCTOOLS_OPSYS_SUNOS
extern int setenv(const char *name, const char *value, int overwrite);
#endif

// The default capacity reported before information is available.
#define WORK_QUEUE_DEFAULT_CAPACITY 10

// The minimum number of task reports to keep
#define WORK_QUEUE_TASK_REPORT_MIN_SIZE 20

// Seconds between updates to the catalog
#define WORK_QUEUE_UPDATE_INTERVAL 60

// Seconds between measurement of master local resources
#define WORK_QUEUE_RESOURCE_MEASUREMENT_INTERVAL 30

#define WORKER_ADDRPORT_MAX 32
#define WORKER_HASHKEY_MAX 32

#define RESOURCE_MONITOR_TASK_LOCAL_NAME "wq-%d-task-%d"
#define RESOURCE_MONITOR_REMOTE_NAME "cctools-monitor"

#define MAX_TASK_STDOUT_STORAGE (1*GIGABYTE)

// Result codes for signaling the completion of operations in WQ
typedef enum {
	SUCCESS = 0,
	WORKER_FAILURE, 
	APP_FAILURE
} work_queue_result_code_t;


typedef enum {
	MSG_PROCESSED = 0,
	MSG_NOT_PROCESSED,
	MSG_FAILURE
} work_queue_msg_code_t;

typedef enum {
	MON_DISABLED = 0,
	MON_SUMMARY  = 1,   /* generate only summary. */
	MON_FULL     = 2    /* generate summary, series and monitoring debug output. */
} work_queue_monitoring_mode;

// Threshold for available disk space (MB) beyond which files are not received from worker.
static uint64_t disk_avail_threshold = 100;

/* Default: When there is a choice, send a task rather than receive 1 out of 2 times.
 * Classical WQ:   1.0 (always prefer to send)
 * Evictionphobic: 0.0 (always prefer to receive completed tasks) */
double wq_option_send_receive_ratio    = 0.5;

int wq_option_scheduler = WORK_QUEUE_SCHEDULE_TIME;

int first_allocation_every_n_tasks   = 25;
int first_allocation_every_n_seconds = 300;

/* default timeout for slow workers to come back to the pool */
double wq_option_blacklist_slow_workers_timeout = 900;

struct work_queue {
	char *name;
	int port;
	int priority;
	int num_tasks_left;

	char workingdir[PATH_MAX];

	struct datagram  *update_port;   // outgoing udp connection to catalog
	struct link      *master_link;   // incoming tcp connection for workers.
	struct link_info *poll_table;
	int poll_table_size;

	struct itable *tasks;           // taskid -> task
	struct itable *task_state_map;  // taskid -> state
	struct list   *ready_list;      // ready to be sent to a worker

	struct hash_table *worker_table;
	struct hash_table *worker_blacklist;
	struct itable  *worker_task_map;

	struct hash_table *categories;

	struct hash_table *workers_with_available_results;

	struct work_queue_stats *stats;
	struct work_queue_stats *stats_disconnected_workers;

	double  send_receive_ratio;

	int worker_selection_algorithm;
	int task_ordering;
	int process_pending_check;
	int workers_to_wait;

	int short_timeout;		// timeout to send/recv a brief message from worker
	int long_timeout;		// timeout to send/recv a brief message from a foreman

	struct list * task_reports;	// list of last N work_queue_task_reports
	timestamp_t total_idle_time;	// sum of time spent waiting for workers
	timestamp_t total_app_time;	// sum of time spend above work_queue_wait

	double asynchrony_multiplier;     /* Times the resource value, but disk */
	int    asynchrony_modifier;       /* Plus this many cores or unlabeled tasks */

	int minimum_transfer_timeout;
	int foreman_transfer_timeout;
	int transfer_outlier_factor;
	int default_transfer_rate;

	char *catalog_host;
	int catalog_port;

	category_mode_t allocation_default_mode;

	FILE *logfile;
	FILE *transactions_logfile;
	int keepalive_interval;
	int keepalive_timeout;
	timestamp_t link_poll_end;	//tracks when we poll link; used to timeout unacknowledged keepalive checks

    char *master_preferred_connection; 

	int monitor_mode;
	FILE *monitor_file;

	char *monitor_output_directory;
	char *monitor_summary_filename;

	char *monitor_exe;
	struct rmsummary *measured_local_resources;
	struct rmsummary *current_max_worker;

	char *password;
	double bandwidth;
};

struct work_queue_worker {
	char *hostname;
	char *os;
	char *arch;
	char *version;
	char addrport[WORKER_ADDRPORT_MAX];
	char hashkey[WORKER_HASHKEY_MAX];
	int  foreman;                             // 0 if regular worker, 1 if foreman
	struct work_queue_stats     *stats;
	struct work_queue_resources *resources;

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
};

struct work_queue_task_report {
	timestamp_t transfer_time;
	timestamp_t exec_time;
};

static void handle_failure(struct work_queue *q, struct work_queue_worker *w, struct work_queue_task *t, work_queue_result_code_t fail_type);

struct blacklist_host_info {
	int    blacklisted;
	int    times_blacklisted;
	time_t release_at;
};

static void handle_worker_failure(struct work_queue *q, struct work_queue_worker *w);
static void handle_app_failure(struct work_queue *q, struct work_queue_worker *w, struct work_queue_task *t);

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
const char *task_result_str(work_queue_result_t result);

/* 1, 0 whether t is in state */
static int task_state_is( struct work_queue *q, uint64_t taskid, work_queue_task_state_t state);
/* pointer to first task found with state. NULL if no such task */
static struct work_queue_task *task_state_any(struct work_queue *q, work_queue_task_state_t state);
/* number of tasks with state */
static int task_state_count( struct work_queue *q, const char *category, work_queue_task_state_t state);

static work_queue_result_code_t get_result(struct work_queue *q, struct work_queue_worker *w, const char *line);
static work_queue_result_code_t get_available_results(struct work_queue *q, struct work_queue_worker *w);

static int update_task_result(struct work_queue_task *t, work_queue_result_t new_result);

static work_queue_msg_code_t process_workqueue(struct work_queue *q, struct work_queue_worker *w, const char *line);
static work_queue_msg_code_t process_queue_status(struct work_queue *q, struct work_queue_worker *w, const char *line, time_t stoptime);
static work_queue_msg_code_t process_resource(struct work_queue *q, struct work_queue_worker *w, const char *line);

static struct jx * queue_to_jx( struct work_queue *q, struct link *foreman_uplink );

char *work_queue_monitor_wrap(struct work_queue *q, struct work_queue_worker *w, struct work_queue_task *t, struct rmsummary *limits);

const struct rmsummary *task_max_declared_resources(struct work_queue *q, struct work_queue_task *t);
const struct rmsummary *task_max_resources(struct work_queue *q, struct work_queue_task *t);
const struct rmsummary *task_min_resources(struct work_queue *q, struct work_queue_task *t);

void work_queue_category_accumulate_task(struct work_queue *q, struct work_queue_task *t);
struct category *work_queue_category_lookup_or_create(struct work_queue *q, const char *name);

static void write_transaction(struct work_queue *q, const char *str);
static void write_transaction_task(struct work_queue *q, struct work_queue_task *t);
static void write_transaction_category(struct work_queue *q, struct category *c);

/** Clone a @ref work_queue_file
This performs a deep copy of the file struct.
@param file The file to clone.
@return A newly allocated file.
*/
static struct work_queue_file *work_queue_file_clone(const struct work_queue_file *file);

/** Clone a list of @ref work_queue_file structs
Thie performs a deep copy of the file list.
@param list The list to clone.
@return A newly allocated list of files.
*/
static struct list *work_queue_task_file_list_clone(struct list *list);

/** Write master's resources to resource summary file and close the file **/
void work_queue_disable_monitoring(struct work_queue *q);

/******************************************************/
/********** work_queue internal functions *************/
/******************************************************/

static int64_t overcommitted_resource_total(struct work_queue *q, int64_t total, int cores_flag) {
	int64_t r = 0;
	if(total != 0)
	{
		r = ceil(total * q->asynchrony_multiplier);

		if(cores_flag)
		{
			r += q->asynchrony_modifier;
		}
	}

	return r;
}

//Returns count of workers that have identified themselves.
static int known_workers(struct work_queue *q) {
	struct work_queue_worker *w;
	char* id;
	int known_workers = 0;

	hash_table_firstkey(q->worker_table);
	while(hash_table_nextkey(q->worker_table, &id, (void**)&w)) {
		if(strcmp(w->hostname, "unknown")){
			known_workers++;
		}
	}

	return known_workers;
}

//Returns count of workers that are available to run tasks.
static int available_workers(struct work_queue *q) {
	struct work_queue_worker *w;
	char* id;
	int available_workers = 0;

	hash_table_firstkey(q->worker_table);
	while(hash_table_nextkey(q->worker_table, &id, (void**)&w)) {
		if(strcmp(w->hostname, "unknown") != 0) {
			if(overcommitted_resource_total(q, w->resources->cores.total, 1) > w->resources->cores.inuse || w->resources->disk.total > w->resources->disk.inuse || overcommitted_resource_total(q, w->resources->memory.total, 0) > w->resources->memory.inuse){
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

static void log_worker_stats(struct work_queue *q)
{
	struct work_queue_stats s;

	debug(D_WQ, "workers status -- total: %d, active: %d, available: %d.",
		hash_table_size(q->worker_table),
		known_workers(q),
		available_workers(q));

	if(!q->logfile) return;

	work_queue_get_stats(q, &s);

	//print in the order described by the headers in specify_log().
	fprintf(q->logfile, "%" PRIu64 " ", timestamp_get());
	fprintf(q->logfile, "%d %d %d %d %d %d ", s.total_workers_connected, s.workers_init, s.workers_idle, s.workers_busy, s.total_workers_joined, s.total_workers_removed);
	fprintf(q->logfile, "%d %d %d %d %d %d ", s.tasks_waiting, s.tasks_running, s.tasks_complete, s.total_tasks_dispatched, s.total_tasks_complete, s.total_tasks_cancelled);
	fprintf(q->logfile, "%" PRIu64 " %" PRIu64 " %" PRIu64 " %" PRIu64 " %" PRIu64 " %f %f %d ", s.start_time, s.total_send_time, s.total_receive_time, s.total_bytes_sent, s.total_bytes_received, s.efficiency, s.idle_percentage, s.capacity);
	fprintf(q->logfile, "%f %" PRIu64 " %" PRIu64 " %" PRIu64 " %" PRIu64 " ", s.bandwidth, s.total_cores, s.total_memory, s.total_disk, s.total_gpus);
	fprintf(q->logfile, "%" PRIu64 " %" PRIu64 " %" PRIu64 " %" PRIu64 " %" PRIu64 " %" PRIu64 " %" PRIu64 " %" PRIu64 " ", s.min_cores, s.max_cores, s.min_memory, s.max_memory, s.min_disk, s.max_disk, s.min_gpus, s.max_gpus);
	fprintf(q->logfile, "%" PRIu64 " %" PRIu64 " ", s.total_execute_time, s.total_good_execute_time);
	fprintf(q->logfile, "\n");
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

	//If foreman, then we wait until foreman gives the master some attention.
	if(w->foreman)
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

work_queue_msg_code_t process_info(struct work_queue *q, struct work_queue_worker *w, char *line)
{
	char field[WORK_QUEUE_LINE_MAX];
	char value[WORK_QUEUE_LINE_MAX];

	int n = sscanf(line,"info %s %[^\n]", field, value);
	debug(D_WQ, "Receiving %s info from worker (%s)", field, w->addrport);

	if(n != 2)
		return MSG_FAILURE;

	if(string_prefix_is(field, "total_workers_joined")) {
		w->stats->total_workers_joined = atoll(value);
	} else if(string_prefix_is(field, "total_workers_removed")) {
		w->stats->total_workers_removed = atoll(value);
	} else if(string_prefix_is(field, "total_send_time")) {
		w->stats->total_send_time = atoll(value);
	} else if(string_prefix_is(field, "total_receive_time")) {
		w->stats->total_receive_time = atoll(value);
	} else if(string_prefix_is(field, "total_execute_time")) {
		w->stats->total_execute_time = atoll(value);
	} else if(string_prefix_is(field, "total_bytes_sent")) {
		w->stats->total_bytes_sent = atoll(value);
	} else if(string_prefix_is(field, "total_bytes_received")) {
		w->stats->total_bytes_received = atoll(value);
	} else if(string_prefix_is(field, "tasks_waiting")) {
		w->stats->tasks_waiting = atoll(value);
	} else if(string_prefix_is(field, "tasks_running")) {
		w->stats->tasks_running = atoll(value);
	} else if(string_prefix_is(field, "idle-disconnecting")) {
		q->stats->total_workers_idled_out++;
	} else if(string_prefix_is(field, "end_of_resource_update")) {
		count_worker_resources(q, w);
	}

	//Note we always mark info messages as processed, as they are optional.
	return MSG_PROCESSED;
}


/**
 * This function receives a message from worker and records the time a message is successfully
 * received. This timestamp is used in keepalive timeout computations.
 */
static work_queue_msg_code_t recv_worker_msg(struct work_queue *q, struct work_queue_worker *w, char *line, size_t length )
{
	time_t stoptime;

	//If foreman, then we wait until foreman gives the master some attention.
	if(w->foreman)
		stoptime = time(0) + q->long_timeout;
	else
		stoptime = time(0) + q->short_timeout;

	int result = link_readline(w->link, line, length, stoptime);

	if (result <= 0) {
		return MSG_FAILURE;
	}

	w->last_msg_recv_time = timestamp_get();

	debug(D_WQ, "rx from %s (%s): %s", w->hostname, w->addrport, line);

	// Check for status updates that can be consumed here.
	if(string_prefix_is(line, "alive")) {
		result = MSG_PROCESSED;
	} else if(string_prefix_is(line, "workqueue")) {
		result = process_workqueue(q, w, line);
	} else if (string_prefix_is(line,"queue_status") || string_prefix_is(line, "worker_status") || string_prefix_is(line, "task_status") || string_prefix_is(line, "wable_status")) {
		result = process_queue_status(q, w, line, stoptime);
	} else if (string_prefix_is(line, "available_results")) {
		hash_table_insert(q->workers_with_available_results, w->hashkey, w);
		result = MSG_PROCESSED;
	} else if (string_prefix_is(line, "resource")) {
		result = process_resource(q, w, line);
	} else if (string_prefix_is(line, "auth")) {
		debug(D_WQ|D_NOTICE,"worker (%s) is attempting to use a password, but I do not have one.",w->addrport);
		result = MSG_FAILURE;
	} else if (string_prefix_is(line,"ready")) {
		debug(D_WQ|D_NOTICE,"worker (%s) is an older worker that is not compatible with this master.",w->addrport);
		result = MSG_FAILURE;
	} else if (string_prefix_is(line, "name")) {
		result = process_name(q, w, line);
	} else if (string_prefix_is(line, "info")) {
		result = process_info(q, w, line);
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
	int64_t     q_total_bytes_transferred = q->stats->total_bytes_sent + q->stats->total_bytes_received;
	timestamp_t q_total_transfer_time     = q->stats->total_send_time  + q->stats->total_receive_time;

	// Note q_total_transfer_time is timestamp_t with units of milliseconds.
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
  between the master and the workers that it serves.
*/

static int get_transfer_wait_time(struct work_queue *q, struct work_queue_worker *w, struct work_queue_task *t, int64_t length)
{
	double avg_transfer_rate; // bytes per second
	char *data_source;

	if(w->total_transfer_time>1000000) {
		// Note w->total_transfer_time is timestamp_t with units of milliseconds.
		avg_transfer_rate = 1000000 * w->total_bytes_transferred / w->total_transfer_time;
		data_source = xxstrdup("worker's observed");
	} else {
		avg_transfer_rate = get_queue_transfer_rate(q, &data_source);
	}

	debug(D_WQ,"%s (%s) using %s average transfer rate of %.2lf MB/s\n", w->hostname, w->addrport, data_source, avg_transfer_rate/MEGABYTE);

	double tolerable_transfer_rate = avg_transfer_rate / q->transfer_outlier_factor; // bytes per second

	int timeout = length / tolerable_transfer_rate;

	if(w->foreman) {
		// A foreman must have a much larger minimum timeout, b/c it does not respond immediately to the master.
		timeout = MAX(q->foreman_transfer_timeout,timeout);
	} else {
		// An ordinary master has a lower minimum timeout b/c it responds immediately to the master.
		timeout = MAX(q->minimum_transfer_timeout,timeout);
	}

	debug(D_WQ, "%s (%s) will try up to %d seconds to transfer this %.2lf MB file.", w->hostname, w->addrport, timeout, length/1000000.0);

	free(data_source);
	return timeout;
}

void update_catalog(struct work_queue *q, struct link *foreman_uplink, int force_update )
{
	static time_t last_update_time = 0;
	char address[LINK_ADDRESS_MAX];

	// Only advertise if we have a name.
	if(!q->name) return;

	// Only advertise every last_update_time seconds.
	if(!force_update && (time(0) - last_update_time) < WORK_QUEUE_UPDATE_INTERVAL)
		return;

	// If host and port are not set, pick defaults.
	if(!q->catalog_host) q->catalog_host = strdup(CATALOG_HOST);
	if(!q->catalog_port) q->catalog_port = CATALOG_PORT;
	if(!q->update_port)  q->update_port = datagram_create(DATAGRAM_PORT_ANY);

	if(!domain_name_cache_lookup(q->catalog_host, address)) {
		debug(D_WQ,"could not resolve address of catalog server %s!",q->catalog_host);
		// don't try again until the next update period
		last_update_time = time(0);
		return;
	}

	// Generate the master status in an jx, and print it to a buffer.
	struct jx *j = queue_to_jx(q,foreman_uplink);
	char *str = jx_print_string(j);

	// Send the buffer.
	debug(D_WQ, "Advertising master status to the catalog server at %s:%d ...", q->catalog_host, q->catalog_port);
	datagram_send(q->update_port, str, strlen(str), address, q->catalog_port);

	// Clean up.
	free(str);
	jx_delete(j);
	last_update_time = time(0);
}

static void clean_task_state(struct work_queue_task *t) {

		t->total_bytes_transferred = 0;
		t->total_bytes_received = 0;
		t->total_bytes_sent = 0;
		t->total_transfer_time = 0;
		t->cmd_execution_time = 0;

		if (t->time_execute_cmd_start >= t->time_committed) {
			timestamp_t delta_time = timestamp_get() - t->time_execute_cmd_start;
			t->total_cmd_execution_time += delta_time;

			switch(t->result) {
				case WORK_QUEUE_RESULT_UNKNOWN:
				case WORK_QUEUE_RESULT_FORSAKEN:
					t->total_time_until_worker_failure += delta_time;
					break;
				default:
					break;
			}
		}

		t->time_execute_cmd_start = 0;

		if(t->output) {
			free(t->output);
			t->output = NULL;
		}

		if(t->hostname) {
			free(t->hostname);
			t->hostname = NULL;
		}
		if(t->host) {
			free(t->host);
			t->host = NULL;
		}

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
		clean_task_state(t);
		if(t->max_retries > 0 && (t->total_submissions >= t->max_retries)) {
			update_task_result(t, WORK_QUEUE_RESULT_MAX_RETRIES);
			reap_task_from_worker(q, w, t, WORK_QUEUE_TASK_RETRIEVED);
		} else {
			update_task_result(t, WORK_QUEUE_RESULT_UNKNOWN);
			reap_task_from_worker(q, w, t, WORK_QUEUE_TASK_READY);
		}

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

	accumulate_stat(qs, ws, total_workers_joined);
	accumulate_stat(qs, ws, total_workers_idled_out);
	accumulate_stat(qs, ws, total_workers_lost);
	accumulate_stat(qs, ws, total_workers_fast_aborted);
	accumulate_stat(qs, ws, total_send_time);
	accumulate_stat(qs, ws, total_receive_time);
	accumulate_stat(qs, ws, total_execute_time);
	accumulate_stat(qs, ws, total_bytes_sent);
	accumulate_stat(qs, ws, total_bytes_received);

	//Count all the workers joined as removed.
	qs->total_workers_removed = ws->total_workers_joined;
}

static void remove_worker(struct work_queue *q, struct work_queue_worker *w)
{
	if(!q || !w) return;

	debug(D_WQ, "worker %s (%s) removed", w->hostname, w->addrport);

	q->stats->total_workers_removed++;

	cleanup_worker(q, w);

	hash_table_remove(q->worker_table, w->hashkey);
	hash_table_remove(q->workers_with_available_results, w->hashkey);

	record_removed_worker_stats(q, w);
	log_worker_stats(q);

	if(w->link)
		link_close(w->link);

	itable_delete(w->current_tasks);
	itable_delete(w->current_tasks_boxes);
	hash_table_delete(w->current_files);
	work_queue_resources_delete(w->resources);
	free(w->stats);
	free(w->hostname);
	free(w->os);
	free(w->arch);
	free(w->version);
	free(w);

	/* update the largest worker seen */
	find_max_worker(q);

	debug(D_WQ, "%d workers are connected in total now", hash_table_size(q->worker_table));
}

static int release_worker(struct work_queue *q, struct work_queue_worker *w)
{
	if(!w) return 0;
	send_worker_msg(q,w,"release\n");
	remove_worker(q, w);
	return 1;
}

static void add_worker(struct work_queue *q)
{
	struct link *link;
	struct work_queue_worker *w;
	char addr[LINK_ADDRESS_MAX];
	int port;

	link = link_accept(q->master_link, time(0) + q->short_timeout);
	if(!link) return;

	link_keepalive(link, 1);
	link_tune(link, LINK_TUNE_INTERACTIVE);

	if(!link_address_remote(link, addr, &port)) {
		link_close(link);
		return;
	}

	debug(D_WQ,"worker %s:%d connected",addr,port);

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
	w->foreman = 0;
	w->link = link;
	w->current_files = hash_table_create(0, 0);
	w->current_tasks = itable_create(0);
	w->current_tasks_boxes = itable_create(0);
	w->finished_tasks = 0;
	w->start_time = timestamp_get();

	w->last_update_msg_time = w->start_time;

	struct work_queue_resources *r = work_queue_resources_create();

	r->cores.smallest = r->cores.largest = r->cores.total = -1;//default_resource_value;
	r->memory.smallest = r->memory.largest = r->memory.total = -1;//default_resource_value;
	r->disk.smallest = r->disk.largest = r->disk.total = -1;//default_resource_value;
	r->gpus.smallest = r->gpus.largest = r->gpus.total = -1;//default_resource_value;

	w->resources = r;

	w->stats     = calloc(1, sizeof(struct work_queue_stats));
	link_to_hash_key(link, w->hashkey);
	sprintf(w->addrport, "%s:%d", addr, port);
	hash_table_insert(q->worker_table, w->hashkey, w);
	log_worker_stats(q);
	q->stats->total_workers_joined++;

	debug(D_WQ, "%d workers are connected in total now", hash_table_size(q->worker_table));

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
			return APP_FAILURE;
		}
	}

	// Create the local file.
	debug(D_WQ, "Receiving file %s (size: %"PRId64" bytes) from %s (%s) ...", local_name, length, w->addrport, w->hostname);
	// Check if there is space for incoming file at master
	if(!check_disk_space_for_filesize(dirname, length, disk_avail_threshold)) {
		debug(D_WQ, "Could not recieve file %s, not enough disk space (%"PRId64" bytes needed)\n", local_name, length);
		return APP_FAILURE;
	}

	int fd = open(local_name, O_WRONLY | O_TRUNC | O_CREAT, 0777);
	if(fd < 0) {
		debug(D_NOTICE, "Cannot open file %s for writing: %s", local_name, strerror(errno));
		link_soak(w->link, length, stoptime);
		return APP_FAILURE;
	}

	// Write the data on the link to file.
	int64_t actual = link_stream_to_fd(w->link, fd, length, stoptime);

	close(fd);

	if(actual != length) {
		debug(D_WQ, "Received item size (%"PRId64") does not match the expected size - %"PRId64" bytes.", actual, length);
		unlink(local_name);
		return WORKER_FAILURE;
	}

	*total_bytes += length;

	// If the transfer was too fast, slow things down.
	timestamp_t current_time = timestamp_get();
	if(effective_stoptime && effective_stoptime > current_time) {
		usleep(effective_stoptime - current_time);
	}

	return SUCCESS;
}

/*
This function implements the recursive get protocol.
The master sents a single get message, then the worker
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

	work_queue_result_code_t result = SUCCESS; //return success unless something fails below

	// Process the recursive file/dir responses as they are sent.
	while(1) {
		char line[WORK_QUEUE_LINE_MAX];
		char tmp_remote_path[WORK_QUEUE_LINE_MAX];
		int64_t length;
		int errnum;

		if(recv_worker_msg_retry(q, w, line, sizeof(line)) == MSG_FAILURE) {
			result = WORKER_FAILURE;
			break;
		}

		if(sscanf(line,"dir %s", tmp_remote_path)==1) {
			char *tmp_local_name = string_format("%s%s",local_name,&tmp_remote_path[remote_name_len]);
			int result_dir = create_dir(tmp_local_name,0777);
			if(!result_dir) {
				debug(D_WQ, "Could not create directory - %s (%s)", tmp_local_name, strerror(errno));
				result = APP_FAILURE;
				free(tmp_local_name);
				break;
			}
			free(tmp_local_name);
		} else if(sscanf(line,"file %s %"SCNd64, tmp_remote_path, &length)==2) {
			char *tmp_local_name = string_format("%s%s",local_name,&tmp_remote_path[remote_name_len]);
			result = get_file(q,w,t,tmp_local_name,length,total_bytes);
			free(tmp_local_name);
			//Return if worker failure. Else wait for end message from worker.
			if(result == WORKER_FAILURE) break;
		} else if(sscanf(line,"missing %s %d",tmp_remote_path,&errnum)==2) {
			// If the output file is missing, we make a note of that in the task result,
			// but we continue and consider the transfer a 'success' so that other
			// outputs are transferred and the task is given back to the caller.
			debug(D_WQ, "%s (%s): could not access requested file %s (%s)",w->hostname,w->addrport,remote_name,strerror(errnum));
			update_task_result(t, WORK_QUEUE_RESULT_OUTPUT_MISSING);
		} else if(!strcmp(line,"end")) {
			// We have to return on receiving an end message.
			if (result == SUCCESS) {
				return result;
			} else {
				break;
			}
		} else {
			debug(D_WQ, "%s (%s): sent invalid response to get: %s",w->hostname,w->addrport,line);
			result = WORKER_FAILURE; //signal sys-level failure
			break;
		}
	}

	// If we failed to *transfer* the output file, then that is a hard
	// failure which causes this function to return failure and the task
	// to be returned to the queue to be attempted elsewhere.
	debug(D_WQ, "%s (%s) failed to return output %s to %s", w->addrport, w->hostname, remote_name, local_name);
	if(result == APP_FAILURE) {
		update_task_result(t, WORK_QUEUE_RESULT_OUTPUT_MISSING);;
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

char *make_cached_name( const struct work_queue_task *t, const struct work_queue_file *f )
{
	static int buffer_count = 0;

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

	/* 0 for cache files, taskid for non-cache files. With this, non-cache
	 * files cannot be shared among tasks, and can be safely deleted once a
	 * task finishes. */
	int cache_task_id = 0;
	if(t && !(f->flags | WORK_QUEUE_CACHE)) {
		cache_task_id = t->taskid;
	}

	switch(f->type) {
		case WORK_QUEUE_FILE:
		case WORK_QUEUE_DIRECTORY:
			return string_format("file-%d-%s-%s", cache_task_id, md5_string(digest), payload_enc);
			break;
		case WORK_QUEUE_FILE_PIECE:
			return string_format("piece-%d-%s-%s-%lld-%lld",cache_task_id, md5_string(digest),payload_enc,(long long)f->offset,(long long)f->piece_length);
			break;
		case WORK_QUEUE_REMOTECMD:
			return string_format("cmd-%d-%s", cache_task_id, md5_string(digest));
			break;
		case WORK_QUEUE_URL:
			return string_format("url-%d-%s", cache_task_id, md5_string(digest));
			break;
		case WORK_QUEUE_BUFFER:
		default:
			buffer_count++;
			return string_format("buffer-%d-%d-%s", cache_task_id, buffer_count, md5_string(digest));
			break;
	}
}

/*
This function stores an output file from the remote cache directory
to a third-party location, which can be either a remote filesystem
(WORK_QUEUE_FS_PATH) or a command to run (WORK_QUEUE_FS_CMD).
Returns 1 on success at worker and 0 on invalid message from worker.
*/
static int do_thirdput( struct work_queue *q, struct work_queue_worker *w,  const char *cached_name, const char *payload, int command )
{
	char line[WORK_QUEUE_LINE_MAX];
	int result;

	send_worker_msg(q,w,"thirdput %d %s %s\n",command,cached_name,payload);

	if(recv_worker_msg_retry(q, w, line, WORK_QUEUE_LINE_MAX) == MSG_FAILURE)
		return WORKER_FAILURE;

	if(sscanf(line, "thirdput-complete %d", &result)) {
		return result;
	} else {
		debug(D_WQ, "Error: invalid message received (%s)\n", line);
		return WORKER_FAILURE;
	}
}

/*
Get a single output file, located at the worker under 'cached_name'.
*/
static work_queue_result_code_t get_output_file( struct work_queue *q, struct work_queue_worker *w, struct work_queue_task *t, struct work_queue_file *f )
{
	int64_t total_bytes = 0;
	work_queue_result_code_t result = SUCCESS; //return success unless something fails below.

	timestamp_t open_time = timestamp_get();

	if(f->flags & WORK_QUEUE_THIRDPUT) {
		if(!strcmp(f->cached_name, f->payload)) {
			debug(D_WQ, "output file %s already on shared filesystem", f->cached_name);
			f->flags |= WORK_QUEUE_PREEXIST;
		} else {
			result = do_thirdput(q,w,f->cached_name,f->payload,WORK_QUEUE_FS_PATH);
		}
	} else if(f->type == WORK_QUEUE_REMOTECMD) {
		result = do_thirdput(q,w,f->cached_name,f->payload,WORK_QUEUE_FS_CMD);
	} else {
		result = get_file_or_directory(q, w, t, f->cached_name, f->payload, &total_bytes);
	}

	timestamp_t close_time = timestamp_get();
	timestamp_t sum_time = close_time - open_time;

	if(total_bytes>0) {
		q->stats->total_bytes_received += total_bytes;
		q->stats->total_receive_time += sum_time;
		t->total_bytes_received += total_bytes;
		t->total_bytes_transferred += total_bytes;
		t->total_transfer_time += sum_time;
		w->total_bytes_transferred += total_bytes;
		w->total_transfer_time += sum_time;
		debug(D_WQ, "%s (%s) sent %.2lf MB in %.02lfs (%.02lfs MB/s) average %.02lfs MB/s", w->hostname, w->addrport, total_bytes / 1000000.0, sum_time / 1000000.0, (double) total_bytes / sum_time, (double) w->total_bytes_transferred / w->total_transfer_time);
	}

	// If the transfer was successful, make a record of it in the cache.
	if(result == SUCCESS && f->flags & WORK_QUEUE_CACHE) {
		struct stat local_info;
		if (stat(f->payload,&local_info) == 0) {
			struct stat *remote_info = malloc(sizeof(*remote_info));
			if(!remote_info) {
				debug(D_NOTICE, "Cannot allocate memory for cache entry for output file %s at %s (%s)", f->payload, w->hostname, w->addrport);
				return APP_FAILURE;
			}
			memcpy(remote_info, &local_info, sizeof(local_info));
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
	work_queue_result_code_t result = SUCCESS;

	if(t->output_files) {
		list_first_item(t->output_files);
		while((f = list_next_item(t->output_files))) {
			result = get_output_file(q,w,t,f);
			//if success or app-level failure, continue to get other files.
			//if worker failure, return.
			if(result == WORKER_FAILURE) {
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
	work_queue_result_code_t result = SUCCESS;

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

	if(t->resources_measured)
		rmsummary_delete(t->resources_measured);

	t->resources_measured = rmsummary_parse_file_single(summary);

	if(t->resources_measured) {
		rmsummary_assign_char_field(t->resources_measured, "category", t->category);
		t->return_status = t->resources_measured->exit_status;
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

	if(q->monitor_mode == MON_FULL && q->monitor_output_directory)
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
		debug(D_NOTICE, "Could no succesfully compress '%s', and '%s'\n", series, debug_log);
	}

	free(series);
	free(debug_log);
	free(command);
}

static void fetch_output_from_worker(struct work_queue *q, struct work_queue_worker *w, int taskid)
{
	struct work_queue_task *t;
	work_queue_result_code_t result = SUCCESS;

	t = itable_lookup(w->current_tasks, taskid);
	if(!t) {
		debug(D_WQ, "Failed to find task %d at worker %s (%s).", taskid, w->hostname, w->addrport);
		handle_failure(q, w, t, WORKER_FAILURE);
		return;
	}

	// Receiving output ...
	t->time_receive_output_start = timestamp_get();

	if(t->result == WORK_QUEUE_RESULT_RESOURCE_EXHAUSTION) {
		result = get_monitor_output_file(q,w,t);
	} else {
		result = get_output_files(q,w,t);
	}

	if(result != SUCCESS) {
		debug(D_WQ, "Failed to receive output from worker %s (%s).", w->hostname, w->addrport);
		handle_failure(q, w, t, result);
	}

	if(result == WORKER_FAILURE)
		return;

	t->time_receive_output_finish = timestamp_get();

	delete_uncacheable_files(q,w,t);

	/* if q is monitoring, append the task summary to the single
	 * queue summary, update t->resources_used, and delete the task summary. */
	if(q->monitor_mode) {
		read_measured_resources(q, t);

		/* Further, if we got debug and series files, gzip them. */
		if(q->monitor_mode == MON_FULL)
			resource_monitor_compress_logs(q, t);
	}

	work_queue_category_accumulate_task(q, t);

	// At this point, a task is completed.
	reap_task_from_worker(q, w, t, WORK_QUEUE_TASK_RETRIEVED);

	w->finished_tasks--;
	t->time_task_finish = timestamp_get();

	// Update completed tasks and the total task execution time.
	q->stats->total_tasks_complete++;
	w->total_tasks_complete++;

	w->total_task_time += t->cmd_execution_time;

	if(t->result == WORK_QUEUE_RESULT_SUCCESS)
	{
		q->stats->total_good_execute_time  += t->cmd_execution_time;
		q->stats->total_good_transfer_time += t->total_transfer_time;
	}

	if(t->result == WORK_QUEUE_RESULT_RESOURCE_EXHAUSTION) {
		q->stats->total_exhausted_execute_time += t->cmd_execution_time;
		q->stats->total_exhausted_attempts++;

		t->total_cmd_exhausted_execute_time += t->cmd_execution_time;
		t->exhausted_attempts++;

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

		category_allocation_t next = category_next_label(q->categories, t->category, t->resource_request, /* resource overflow */ 1, t->resources_requested, t->resources_measured);

		if(next != CATEGORY_ALLOCATION_ERROR) {
			debug(D_WQ, "Task %d resubmitted using new resource allocation.\n", t->taskid);
			t->resource_request = next;
			change_task_state(q, t, WORK_QUEUE_TASK_READY);
			return;
		} else {
			debug(D_WQ, "Task %d failed given max resource exhaustion.\n", t->taskid);
		}
	}

	add_task_report(q, t);
	debug(D_WQ, "%s (%s) done in %.02lfs total tasks %lld average %.02lfs",
			w->hostname,
			w->addrport,
			(t->time_receive_output_finish - t->time_send_input_start) / 1000000.0,
			(long long) w->total_tasks_complete,
			w->total_task_time / w->total_tasks_complete / 1000000.0);

	return;
}

/*
Expire tasks in the ready list.
*/
static void expire_waiting_task(struct work_queue *q, struct work_queue_task *t)
{
	update_task_result(t, WORK_QUEUE_RESULT_TASK_TIMEOUT);

	//add the task to complete list so it is given back to the application.
	change_task_state(q, t, WORK_QUEUE_TASK_RETRIEVED);

	return;
}

static void expire_waiting_tasks(struct work_queue *q)
{
	struct work_queue_task *t;
	int count;

	timestamp_t current_time = timestamp_get();
	count = task_state_count(q, NULL, WORK_QUEUE_TASK_READY);

	while(count > 0)
	{
		count--;

		t = list_pop_head(q->ready_list);

		if(t->resources_requested->end > 0 && (uint64_t) t->resources_requested->end <= current_time)
		{
			expire_waiting_task(q, t);
		}
		else
		{
			list_push_tail(q->ready_list, t);
		}
	}
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
		if(t->time_execute_cmd_start > 0) {
			delete_task_output_files(q,w,t);
		}
	}

	return;
}

static void handle_worker_failure(struct work_queue *q, struct work_queue_worker *w)
{
	//WQ failures happen in the master-worker interactions. In this case, we
	//remove the worker and retry the tasks dispatched to it elsewhere.
	remove_worker(q, w);
	return;
}

static void handle_failure(struct work_queue *q, struct work_queue_worker *w, struct work_queue_task *t, work_queue_result_code_t fail_type)
{
	if(fail_type == APP_FAILURE) {
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
		debug(D_WQ|D_NOTICE,"worker (%s) is using work queue protocol %d, but I am using protocol %d",w->addrport,worker_protocol,WORK_QUEUE_PROTOCOL_VERSION);
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
		w->foreman = 1;
	}

	log_worker_stats(q);
	debug(D_WQ, "%s (%s) running CCTools version %s on %s (operating system) with architecture %s is ready", w->hostname, w->addrport, w->version, w->os, w->arch);

	if(cctools_version_cmp(CCTOOLS_VERSION, w->version) != 0) {
		debug(D_DEBUG, "Warning: potential worker version mismatch: worker %s (%s) is version %s, and master is version %s", w->hostname, w->addrport, w->version, CCTOOLS_VERSION);
	}

	return MSG_PROCESSED;
}

/*
If the master has requested that a file be watched with WORK_QUEUE_WATCH,
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
		return WORKER_FAILURE;
	}

	struct work_queue_task *t = itable_lookup(w->current_tasks,taskid);
	if(!t) {
		debug(D_WQ,"worker %s (%s) sent output for unassigned task %"PRId64, w->hostname, w->addrport, taskid);
		link_soak(w->link,length,time(0)+get_transfer_wait_time(q,w,0,length));
		return SUCCESS;
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
		return SUCCESS;
	}

	int fd = open(local_name,O_WRONLY|O_CREAT,0777);
	if(fd<0) {
		debug(D_WQ,"unable to update watched file %s: %s",local_name,strerror(errno));
		link_soak(w->link,length,stoptime);
		return SUCCESS;
	}

	lseek(fd,offset,SEEK_SET);
	link_stream_to_fd(w->link,fd,length,stoptime);
	ftruncate(fd,offset+length);
	close(fd);

	return SUCCESS;
}

/*
Failure to store result is treated as success so we continue to retrieve the
output files of the task.
*/
static work_queue_result_code_t get_result(struct work_queue *q, struct work_queue_worker *w, const char *line) {

	if(!q || !w || !line) 
		return WORKER_FAILURE;

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
	char items[4][WORK_QUEUE_PROTOCOL_FIELD_MAX];
	int n = sscanf(line, "result %s %s %s %s %" SCNd64, items[0], items[1], items[2], items[3], &taskid);

	if(n < 5) {
		debug(D_WQ, "Invalid message from worker %s (%s): %s", w->hostname, w->addrport, line);
		return WORKER_FAILURE;
	}

	task_status = atoi(items[0]);
	exit_status   = atoi(items[1]);
	output_length = atoll(items[2]);

	t = itable_lookup(w->current_tasks, taskid);
	if(!t) {
		debug(D_WQ, "Unknown task result from worker %s (%s): no task %" PRId64" assigned to worker.  Ignoring result.", w->hostname, w->addrport, taskid);
		stoptime = time(0) + get_transfer_wait_time(q, w, 0, output_length);
		link_soak(w->link, output_length, stoptime);
		return SUCCESS;
	}

	if(task_status == WORK_QUEUE_RESULT_FORSAKEN) {
		/* task will be resubmitted, so we do not update any of the execution stats */
		change_task_state(q, t, WORK_QUEUE_TASK_WAITING_RESUBMISSION);
		return SUCCESS;
	}

	t->time_receive_result_start = timestamp_get();
	observed_execution_time = timestamp_get() - t->time_execute_cmd_start;

	execution_time = atoll(items[3]);
	t->cmd_execution_time = observed_execution_time > execution_time ? execution_time : observed_execution_time;

	t->total_cmd_execution_time += t->cmd_execution_time;

	if(q->bandwidth) {
		effective_stoptime = (output_length/q->bandwidth)*1000000 + timestamp_get();
	}

	if(output_length <= MAX_TASK_STDOUT_STORAGE) {
		retrieved_output_length = output_length;
	} else {
		retrieved_output_length = MAX_TASK_STDOUT_STORAGE;
		fprintf(stderr, "warning: stdout of task %"PRId64" requires %2.2lf GB of storage. This exceeds maximum supported size of %d GB. Only %d GB will be retreived.\n", taskid, ((double) output_length)/MAX_TASK_STDOUT_STORAGE, MAX_TASK_STDOUT_STORAGE/GIGABYTE, MAX_TASK_STDOUT_STORAGE/GIGABYTE);
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
			return WORKER_FAILURE;
		}
		debug(D_WQ, "Retrieved %"PRId64" bytes from %s (%s)", actual, w->hostname, w->addrport);

		//Then read the bytes we need to throw away.
		if(output_length > retrieved_output_length) {
			debug(D_WQ, "Dropping the remaining %"PRId64" bytes of the stdout of task %"PRId64" since stdout length is limited to %d bytes.\n", (output_length-MAX_TASK_STDOUT_STORAGE), taskid, MAX_TASK_STDOUT_STORAGE);
			stoptime = time(0) + get_transfer_wait_time(q, w, t, (output_length-retrieved_output_length));
			link_soak(w->link, (output_length-retrieved_output_length), stoptime);

			//overwrite the last few bytes of buffer to signal truncated stdout.
			char *truncate_msg = string_format("\n>>>>>> WORK QUEUE HAS TRUNCATED THE STDOUT AFTER THIS POINT.\n>>>>>> MAXIMUM OF %d BYTES REACHED, %" PRId64 " BYTES TRUNCATED.", MAX_TASK_STDOUT_STORAGE, output_length - retrieved_output_length);
			strncpy(t->output+MAX_TASK_STDOUT_STORAGE-strlen(truncate_msg), truncate_msg, strlen(truncate_msg));
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

	t->time_receive_result_finish = timestamp_get();

	t->result        = task_status;
	t->return_status = exit_status;

	t->time_execute_cmd_finish = t->time_execute_cmd_start + t->cmd_execution_time;
	q->stats->total_execute_time += t->cmd_execution_time;

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

	return SUCCESS;
}

static work_queue_result_code_t get_available_results(struct work_queue *q, struct work_queue_worker *w)
{

	//max_count == -1, tells the worker to send all available results.
	send_worker_msg(q, w, "send_results %d\n", -1);
	debug(D_WQ, "Reading result(s) from %s (%s)", w->hostname, w->addrport);

	char line[WORK_QUEUE_LINE_MAX];
	int i = 0;

	work_queue_result_code_t result = SUCCESS; //return success unless something fails below.

	while(1) {
		if(recv_worker_msg_retry(q, w, line, sizeof(line)) == MSG_FAILURE) {
			result = WORKER_FAILURE;
			break;
		}

		if(string_prefix_is(line,"result")) {
			result = get_result(q, w, line);
			if(result != SUCCESS) break;
			i++;
		} else if(string_prefix_is(line,"update")) {
			result = get_update(q,w,line);
			if(result != SUCCESS) break;
		} else if(!strcmp(line,"end")) {
			//Only return success if last message is end.
			break;
		} else {
			debug(D_WQ, "%s (%s): sent invalid response to send_results: %s",w->hostname,w->addrport,line);
			result = WORKER_FAILURE;
			break;
		}
	}

	if(result != SUCCESS) {
		handle_worker_failure(q, w);
	}

	return result;
}

static int update_task_result(struct work_queue_task *t, work_queue_result_t new_result) {

	if(new_result & ~(0x7)) {
		/* Upper bits are set, so this is not related to old-style result for
		 * inputs, outputs, or stdout, so we simply make an update. */
		t->result = new_result;
	} else if(t->result & ~(0x7)) {

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

static char *blacklisted_to_string( struct work_queue  *q ) {
	if(hash_table_size(q->worker_blacklist) < 1) {
		return NULL;
	}

	buffer_t b;
	buffer_init(&b);

	char *hostname;
	struct blacklist_host_info *info;

	char *sep = "";

	hash_table_firstkey(q->worker_blacklist);
	while(hash_table_nextkey(q->worker_blacklist, &hostname, (void *) &info)) {
		if(info->blacklisted) {
			buffer_printf(&b, "%s%s", sep, hostname);
			sep = " ";
		}
	}

	char *result = xxstrdup(buffer_tostring(&b));
	buffer_free(&b);

	return result;
}

static struct rmsummary *largest_waiting_declared_resources(struct work_queue *q, const char *category) {
	struct rmsummary *max_resources_waiting = rmsummary_create(-1);
	struct work_queue_task *t;

	list_first_item(q->ready_list);
	while((t = list_next_item(q->ready_list))) {

		if(!category || (t->category && !strcmp(t->category, category))) {
			rmsummary_merge_max(max_resources_waiting, t->resources_requested);
		}
	}

	if(category) {
		struct category *c = work_queue_category_lookup_or_create(q, category);
		rmsummary_merge_max(max_resources_waiting, c->max_allocation);
	}

	return max_resources_waiting;
}

static struct rmsummary *largest_waiting_measured_resources(struct work_queue *q, const char *category) {
	struct rmsummary *max_resources_waiting = rmsummary_create(-1);
	struct work_queue_task *t;

	list_first_item(q->ready_list);
	while((t = list_next_item(q->ready_list))) {

		if(!category || (t->category && !strcmp(t->category, category))) {
			const struct rmsummary *r = task_min_resources(q, t);
			rmsummary_merge_max(max_resources_waiting, r);
		}
	}

	if(category) {
		struct category *c = work_queue_category_lookup_or_create(q, category);
		rmsummary_merge_max(max_resources_waiting, c->max_allocation);
	}

	return max_resources_waiting;
}

static int check_worker_fit(struct work_queue_worker *w, struct rmsummary *s) {

	if(w->resources->workers.total < 1)
		return 0;

	if(!s)
		return w->resources->workers.total;

	if(s->cores > w->resources->cores.total/w->resources->workers.total)
		return 0;
	if(s->memory > w->resources->memory.total/w->resources->workers.total)
		return 0;
	if(s->disk > w->resources->disk.total/w->resources->workers.total)
		return 0;
	if(s->gpus > w->resources->gpus.total/w->resources->workers.total)
		return 0;

	return w->resources->workers.total;
}

static int count_workers_for_waiting_tasks(struct work_queue *q, struct rmsummary *s) {

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

#define category_jx_insert_max(j, c, field)\
	{\
		struct rmsummary *largest = largest_waiting_declared_resources(q, c->name);\
		if(largest->field > -1){\
			char *max_str = string_format("%" PRId64, largest->field);\
			jx_insert_string(j, "max_" #field, max_str);\
			free(max_str);\
		} else if(c->max_resources_seen->limits_exceeded && c->max_resources_seen->limits_exceeded->field > -1) {\
			char *max_str = string_format(">%" PRId64, c->max_resources_seen->field - 1);\
			jx_insert_string(j, "max_" #field, max_str);\
			free(max_str);\
		} else if(c->max_resources_seen->field > -1) {\
			char *max_str = string_format("~%" PRId64, c->max_resources_seen->field);\
			jx_insert_string(j, "max_" #field, max_str);\
			free(max_str);\
		}\
		rmsummary_delete(largest);\
	}


static struct jx * category_to_jx(struct work_queue *q, const char *category) {
	struct category *c = work_queue_category_lookup_or_create(q, category);

	struct work_queue_stats s;
	work_queue_get_stats_category(q, category, &s);

	if(s.tasks_waiting + s.tasks_running + s.tasks_complete < 1)
		return 0;

	struct jx *j = jx_object(0);
	if(!j) return 0;

	jx_insert_string(j, "category", category);
	jx_insert_integer(j, "tasks_waiting", s.tasks_waiting);
	jx_insert_integer(j, "tasks_running", s.tasks_running + s.tasks_complete);
	jx_insert_integer(j, "total_tasks_dispatched", s.total_tasks_dispatched);
	jx_insert_integer(j, "total_tasks_complete", s.total_tasks_complete);
	jx_insert_integer(j, "total_tasks_failed", s.total_tasks_failed);
	jx_insert_integer(j, "total_tasks_cancelled", s.total_tasks_cancelled);
	jx_insert_integer(j, "workers_able", s.workers_able);

	category_jx_insert_max(j, c, cores);
	category_jx_insert_max(j, c, memory);
	category_jx_insert_max(j, c, disk);

	if(c->first_allocation) {
		if(c->first_allocation->cores > -1)
			jx_insert_integer(j, "first_cores", c->first_allocation->cores);
		if(c->first_allocation->memory > -1)
			jx_insert_integer(j, "first_memory", c->first_allocation->memory);
		if(c->first_allocation->disk > -1)
			jx_insert_integer(j, "first_disk", c->first_allocation->disk);
	}

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
an jx expression which can be sent to the catalog or directly to the
user that connects via work_queue_status.
*/

static struct jx * queue_to_jx( struct work_queue *q, struct link *foreman_uplink )
{
	struct jx *j = jx_object(0);
	if(!j) return 0;

	// Insert all properties from work_queue_stats

	struct work_queue_stats info;
	work_queue_get_stats(q,&info);

	jx_insert_integer(j,"port",info.port);
	jx_insert_integer(j,"priority",info.priority);
	jx_insert_integer(j,"tasks_left",q->num_tasks_left);

	//send info on workers
	jx_insert_integer(j,"workers",info.total_workers_connected);
	jx_insert_integer(j,"workers_init",info.workers_init);
	jx_insert_integer(j,"workers_idle",info.workers_idle);
	jx_insert_integer(j,"workers_busy",info.workers_busy);
	jx_insert_integer(j,"workers_able",info.workers_able);
	jx_insert_integer(j,"workers_ready",info.workers_ready); //workers_ready is deprecated
	jx_insert_integer(j,"total_workers_connected",info.total_workers_connected);
	jx_insert_integer(j,"total_workers_joined",info.total_workers_joined);
	jx_insert_integer(j,"total_workers_removed",info.total_workers_removed);
	jx_insert_integer(j,"total_workers_idled_out",info.total_workers_idled_out);
	jx_insert_integer(j,"total_workers_lost",info.total_workers_lost);
	jx_insert_integer(j,"total_workers_fast_aborted",info.total_workers_fast_aborted);

	//send info on tasks
	jx_insert_integer(j,"tasks_waiting",info.tasks_waiting);
	jx_insert_integer(j,"tasks_running",info.tasks_running);
	// KNOWN HACK: The following line is inconsistent but kept for compatibility reasons.
	// Everyone wants to know total_tasks_complete, but few are interested in tasks_complete.
	jx_insert_integer(j,"tasks_complete",info.total_tasks_complete);
	jx_insert_integer(j,"total_tasks_complete",info.total_tasks_complete);
	jx_insert_integer(j,"total_tasks_dispatched",info.total_tasks_dispatched);
	jx_insert_integer(j,"total_tasks_cancelled",info.total_tasks_cancelled);

	//send info on queue
	jx_insert_integer(j,"start_time",info.start_time);
	jx_insert_integer(j,"total_send_time",info.total_send_time);
	jx_insert_integer(j,"total_receive_time",info.total_receive_time);
	jx_insert_integer(j,"total_bytes_sent",info.total_bytes_sent);
	jx_insert_integer(j,"total_bytes_received",info.total_bytes_received);
	jx_insert_double(j,"efficiency",info.efficiency);
	jx_insert_double(j,"idle_percentage",info.idle_percentage);
	jx_insert_integer(j,"capacity",info.capacity);
	jx_insert_integer(j,"total_execute_time",info.total_execute_time);
	jx_insert_integer(j,"total_good_execute_time",info.total_good_execute_time);
	jx_insert_integer(j,"total_exhausted_execute_time",info.total_exhausted_execute_time);
	jx_insert_integer(j,"total_exhausted_retries",info.total_exhausted_attempts);
	jx_insert_string(j,"master_preferred_connection",q->master_preferred_connection);

	// Add the blacklisted workers
	char *blacklist = blacklisted_to_string(q);
	if(blacklist) {
		jx_insert_string(j,"workers-blacklisted", blacklist);
		free(blacklist);
	}

	// Add the resources computed from tributary workers.
	struct work_queue_resources r;
	aggregate_workers_resources(q,&r);
	work_queue_resources_add_to_jx(&r,j);

	char owner[USERNAME_MAX];
	username_get(owner);

	// Add special properties expected by the catalog server
	jx_insert_string(j,"type","wq_master");
	if(q->name) jx_insert_string(j,"project",q->name);
	jx_insert_integer(j,"starttime",(q->stats->start_time/1000000)); // catalog expects time_t not timestamp_t
	jx_insert_string(j,"working_dir",q->workingdir);
	jx_insert_string(j,"owner",owner);
	jx_insert_string(j,"version",CCTOOLS_VERSION);

	// If this is a foreman, add the master address and the disk resources
	if(foreman_uplink) {
		int port;
		char address[LINK_ADDRESS_MAX];
		char addrport[WORK_QUEUE_LINE_MAX];

		link_address_remote(foreman_uplink,address,&port);
		sprintf(addrport,"%s:%d",address,port);
		jx_insert_string(j,"my_master",addrport);

		// get foreman local resources and overwrite disk usage
		struct work_queue_resources local_resources;
		work_queue_resources_measure_locally(&local_resources,q->workingdir);
		r.disk.total = local_resources.disk.total;
		r.disk.inuse = local_resources.disk.inuse;
		work_queue_resources_add_to_jx(&r,j);
	}

	//add the stats per category
	jx_insert(j, jx_string("categories"), categories_to_jx(q));

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


struct jx * task_to_jx( struct work_queue_task *t, const char *state, const char *host )
{
	struct jx *j = jx_object(0);

	jx_insert_integer(j,"taskid",t->taskid);
	jx_insert_string(j,"state",state);
	if(t->tag) jx_insert_string(j,"tag",t->tag);
	if(t->category) jx_insert_string(j,"category",t->category);
	jx_insert_string(j,"command",t->command_line);
	if(host) jx_insert_string(j,"host",host);

	priority_add_to_jx(j, t->priority);


	return j;
}

static work_queue_msg_code_t process_queue_status( struct work_queue *q, struct work_queue_worker *target, const char *line, time_t stoptime )
{
	char request[WORK_QUEUE_LINE_MAX];
	struct link *l = target->link;

	struct jx *a = jx_array(NULL);

	free(target->hostname);
	target->hostname = xxstrdup("QUEUE_STATUS");

	//do not count a status connection as a worker
	q->stats->total_workers_joined--;
	q->stats->total_workers_removed--;

	if(sscanf(line, "%[^_]_status", request) != 1) {
		return MSG_FAILURE;
	}

	if(!strcmp(request, "queue")) {
		struct jx *j = queue_to_jx( q, 0 );
		if(j) {
			jx_array_insert(a, j);
		}
	} else if(!strcmp(request, "task")) {
		struct work_queue_task *t;
		struct work_queue_worker *w;
		struct jx *j;
		uint64_t taskid;

		itable_firstkey(q->tasks);
		while(itable_nextkey(q->tasks,&taskid,(void**)&t)) {
			w = itable_lookup(q->worker_task_map, taskid);
			if(w) {
				j = task_to_jx(t,"running",w->hostname);
				if(j) {
					// Include detailed information on where the task is running:
					// address and port, workspace
					jx_insert_string(j, "address_port", w->addrport);

					// Timestamps on running task related events
					jx_insert_integer(j, "submit_to_queue_time", t->time_task_submit);
					jx_insert_integer(j, "send_input_start_time", t->time_send_input_start);
					jx_insert_integer(j, "execute_cmd_start_time", t->time_execute_cmd_start);
					jx_insert_integer(j, "current_time", timestamp_get());

					jx_array_insert(a, j);
				}
			} else {
				work_queue_task_state_t state = (uintptr_t) itable_lookup(q->task_state_map, taskid);
				j = task_to_jx(t,task_state_str(state),0);
				if(j) {
					jx_array_insert(a, j);
				}
			}
		}
	} else if(!strcmp(request, "worker")) {
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
	} else if(!strcmp(request, "wable")) {
		jx_delete(a);
		a = categories_to_jx(q);
	}

	jx_print_link(a,l,stoptime);
	jx_delete(a);

	remove_worker(q, target);

	return MSG_PROCESSED;
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
		log_worker_stats(q);

	} else if(n == 4) {

		/* inuse is computed by the master, so we save it here */
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
	} else {
		return MSG_FAILURE;
	}

	return MSG_PROCESSED;
}

static void handle_worker(struct work_queue *q, struct link *l)
{
	char line[WORK_QUEUE_LINE_MAX];
	char key[WORK_QUEUE_LINE_MAX];
	struct work_queue_worker *w;

	link_to_hash_key(l, key);
	w = hash_table_lookup(q->worker_table, key);

	int worker_failure = 0;
	work_queue_msg_code_t result = recv_worker_msg(q, w, line, sizeof(line));

	//we only expect status messages from above. If the last message is left to be processed, we fail.
	if(result == MSG_NOT_PROCESSED) {
		debug(D_WQ, "Invalid message from worker %s (%s): %s", w->hostname, w->addrport, line);
		worker_failure = 1;
	} else if(result == MSG_FAILURE){
		debug(D_WQ, "Failed to read from worker %s (%s)", w->hostname, w->addrport);
		q->stats->total_workers_lost++;
		worker_failure = 1;
	} // otherwise do nothing..message was consumed and processed in recv_worker_msg()

	if(worker_failure) {
		handle_worker_failure(q, w);
	}
}

static int build_poll_table(struct work_queue *q, struct link *master)
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
			return 0;
		}
	}
	// The first item in the poll table is the master link, which accepts new connections.
	q->poll_table[0].link = q->master_link;
	q->poll_table[0].events = LINK_READ;
	q->poll_table[0].revents = 0;
	n = 1;

	if(master) {
		q->poll_table[n].link = master;
		q->poll_table[n].events = LINK_READ;
		q->poll_table[n].revents = 0;
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
				return 0;
			}
		}

		q->poll_table[n].link = w->link;
		q->poll_table[n].events = LINK_READ;
		q->poll_table[n].revents = 0;
		n++;
	}

	return n;
}

static int send_file( struct work_queue *q, struct work_queue_worker *w, struct work_queue_task *t, const char *localname, const char *remotename, off_t offset, int64_t length, int64_t *total_bytes, int flags)
{
	struct stat local_info;
	time_t stoptime;
	timestamp_t effective_stoptime = 0;
	int64_t actual = 0;

	if(stat(localname, &local_info) < 0) {
		debug(D_NOTICE, "Cannot stat file %s: %s", localname, strerror(errno));
		return APP_FAILURE;
	}

	/* normalize the mode so as not to set up invalid permissions */
	local_info.st_mode |= 0600;
	local_info.st_mode &= 0777;

	if(!length) {
		length = local_info.st_size;
	}

	debug(D_WQ, "%s (%s) needs file %s bytes %lld:%lld as '%s'", w->hostname, w->addrport, localname, (long long) offset, (long long) offset+length, remotename);
	int fd = open(localname, O_RDONLY, 0);
	if(fd < 0) {
		debug(D_NOTICE, "Cannot open file %s: %s", localname, strerror(errno));
		return APP_FAILURE;
	}

	//We want to send bytes starting from 'offset'. So seek to it first.
	if (offset >= 0 && (offset+length) <= local_info.st_size) {
		if(lseek(fd, offset, SEEK_SET) == -1) {
			debug(D_NOTICE, "Cannot seek file %s to offset %lld: %s", localname, (long long) offset, strerror(errno));
			close(fd);
			return APP_FAILURE;
		}
	} else {
		debug(D_NOTICE, "File specification %s (%lld:%lld) is invalid", localname, (long long) offset, (long long) offset+length);
		close(fd);
		return APP_FAILURE;
	}

	if(q->bandwidth) {
		effective_stoptime = (length/q->bandwidth)*1000000 + timestamp_get();
	}

	stoptime = time(0) + get_transfer_wait_time(q, w, t, length);
	send_worker_msg(q,w, "put %s %"PRId64" 0%o %d\n",remotename, length, local_info.st_mode, flags);
	actual = link_stream_from_fd(w->link, fd, length, stoptime);
	close(fd);

	*total_bytes += actual;

	if(actual != length)
		return WORKER_FAILURE;

	timestamp_t current_time = timestamp_get();
	if(effective_stoptime && effective_stoptime > current_time) {
		usleep(effective_stoptime - current_time);
	}

	return SUCCESS;
}

/*
Send a directory and all of its contentss.
*/
static work_queue_result_code_t send_directory( struct work_queue *q, struct work_queue_worker *w, struct work_queue_task *t, const char *dirname, const char *remotedirname, int64_t * total_bytes, int flags )
{
	DIR *dir = opendir(dirname);
	if(!dir) {
		debug(D_NOTICE, "Cannot open dir %s: %s", dirname, strerror(errno));
		return APP_FAILURE;
	}

	work_queue_result_code_t result = SUCCESS;

	// When putting a file its parent directories are automatically
	// created by the worker, so no need to manually create them.
	struct dirent *d;
	while((d = readdir(dir))) {
		if(!strcmp(d->d_name, ".") || !strcmp(d->d_name, "..")) continue;

		char *localpath = string_format("%s/%s",dirname,d->d_name);
		char *remotepath = string_format("%s/%s",remotedirname,d->d_name);

		struct stat local_info;
		if(stat(localpath, &local_info)>=0) {
			if(S_ISDIR(local_info.st_mode))  {
				result = send_directory( q, w, t, localpath, remotepath, total_bytes, flags );
			} else {
				result = send_file( q, w, t, localpath, remotepath, 0, 0, total_bytes, flags );
			}
		} else {
			debug(D_NOTICE, "Cannot stat file %s: %s", localpath, strerror(errno));
			result = APP_FAILURE;
		}

		free(localpath);
		free(remotepath);

		if(result != SUCCESS) break;
	}

	closedir(dir);
	return result;
}

/*
Send a file or directory to a remote worker, if it is not already cached.
The local file name should already have been expanded by the caller.
*/
static work_queue_result_code_t send_file_or_directory( struct work_queue *q, struct work_queue_worker *w, struct work_queue_task *t, struct work_queue_file *tf, const char *expanded_local_name, int64_t * total_bytes)
{
	struct stat local_info;
	struct stat *remote_info;

	if(stat(expanded_local_name, &local_info) < 0) {
		debug(D_NOTICE, "Cannot stat file %s: %s", expanded_local_name, strerror(errno));
		return APP_FAILURE;
	}

	work_queue_result_code_t result = SUCCESS;

	// Look in the current files hash to see if the file is already on the worker.
	remote_info = hash_table_lookup(w->current_files, tf->cached_name);

	/* If it is in the worker, but a new version is available, warn and return.
	   We do not want to rewrite the file while some other task may be using
	   it. */
	if(remote_info && (remote_info->st_mtime != local_info.st_mtime || remote_info->st_size != local_info.st_size)) {
		debug(D_NOTICE|D_WQ, "File %s changed locally. Task %d will be executed with an older version.", expanded_local_name, t->taskid);
	}
	else if(!remote_info) {
		/* If not on the worker, send it. */
		if(S_ISDIR(local_info.st_mode)) {
			result = send_directory(q, w, t, expanded_local_name, tf->cached_name, total_bytes, tf->flags);
		} else {
			result = send_file(q, w, t, expanded_local_name, tf->cached_name, tf->offset, tf->piece_length, total_bytes, tf->flags);
		}

		if(result == SUCCESS && tf->flags & WORK_QUEUE_CACHE) {
			remote_info = malloc(sizeof(*remote_info));
			if(remote_info) {
				memcpy(remote_info, &local_info, sizeof(local_info));
				hash_table_insert(w->current_files, tf->cached_name, remote_info);
			} else {
				debug(D_NOTICE, "Cannot allocate memory for cache entry for input file %s at %s (%s)", expanded_local_name, w->hostname, w->addrport);
			}
		}
	}
	else {
		/* Up-to-date file on the worker, we do nothing. */
	}

	return result;
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

static work_queue_result_code_t send_input_file(struct work_queue *q, struct work_queue_worker *w, struct work_queue_task *t, struct work_queue_file *f)
{

	int64_t total_bytes = 0;
	int64_t actual = 0;
	work_queue_result_code_t result = SUCCESS; //return success unless something fails below

	timestamp_t open_time = timestamp_get();

	switch (f->type) {

	case WORK_QUEUE_BUFFER:
		debug(D_WQ, "%s (%s) needs literal as %s", w->hostname, w->addrport, f->remote_name);
		time_t stoptime = time(0) + get_transfer_wait_time(q, w, t, f->length);
		send_worker_msg(q,w, "put %s %d %o %d\n",f->cached_name, f->length, 0777, f->flags);
		actual = link_putlstring(w->link, f->payload, f->length, stoptime);
		if(actual!=f->length) {
			result = WORKER_FAILURE;
		}
		total_bytes = actual;
		break;

	case WORK_QUEUE_REMOTECMD:
		debug(D_WQ, "%s (%s) needs %s from remote filesystem using %s", w->hostname, w->addrport, f->remote_name, f->payload);
		send_worker_msg(q,w, "thirdget %d %s %s\n",WORK_QUEUE_FS_CMD, f->cached_name, f->payload);
		break;

	case WORK_QUEUE_URL:
		debug(D_WQ, "%s (%s) needs %s from the url, %s %d", w->hostname, w->addrport, f->cached_name, f->payload, f->length);
		send_worker_msg(q,w, "url %s %d 0%o %d\n",f->cached_name, f->length, 0777, f->flags);
		link_putlstring(w->link, f->payload, f->length, time(0) + q->short_timeout);
		break;

	case WORK_QUEUE_DIRECTORY:
		// Do nothing.  Empty directories are handled by the task specification, while recursive directories are implemented as WORK_QUEUE_FILEs
		break;

	case WORK_QUEUE_FILE:
	case WORK_QUEUE_FILE_PIECE:
		if(f->flags & WORK_QUEUE_THIRDGET) {
			debug(D_WQ, "%s (%s) needs %s from shared filesystem as %s", w->hostname, w->addrport, f->payload, f->remote_name);

			if(!strcmp(f->remote_name, f->payload)) {
				f->flags |= WORK_QUEUE_PREEXIST;
			} else {
				if(f->flags & WORK_QUEUE_SYMLINK) {
					send_worker_msg(q,w, "thirdget %d %s %s\n", WORK_QUEUE_FS_SYMLINK, f->cached_name, f->payload);
				} else {
					send_worker_msg(q,w, "thirdget %d %s %s\n", WORK_QUEUE_FS_PATH, f->cached_name, f->payload);
				}
			}
		} else {
			char *expanded_payload = expand_envnames(w, f->payload);
			if(expanded_payload) {
				result = send_file_or_directory(q,w,t,f,expanded_payload,&total_bytes);
				free(expanded_payload);
			} else {
				result = APP_FAILURE; //signal app-level failure.
			}
		}
		break;
	}

	if(result == SUCCESS) {
		timestamp_t close_time = timestamp_get();
		timestamp_t elapsed_time = close_time-open_time;

		t->total_bytes_sent += total_bytes;
		t->total_bytes_transferred += total_bytes;
		t->total_transfer_time += elapsed_time;

		w->total_bytes_transferred += total_bytes;
		w->total_transfer_time += elapsed_time;

		q->stats->total_bytes_sent += total_bytes;
		q->stats->total_send_time += elapsed_time;

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

		if(result == APP_FAILURE) {
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
					return APP_FAILURE;
				}
				if(stat(expanded_payload, &s) != 0) {
					debug(D_WQ,"Could not stat %s: %s\n", expanded_payload, strerror(errno));
					free(expanded_payload);
					update_task_result(t, WORK_QUEUE_RESULT_INPUT_MISSING);
					return APP_FAILURE;
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
			if(result != SUCCESS) return result;
		}
	}

	return SUCCESS;
}

/* if max defined, use minimum of max or worker avg
 * else if min is less than avg, chose avg, otherwise 'infinity' */
#define task_worker_box_size_resource(w, min, max, avg, field)\
	( max->field  >  -1 ? max->field :\
	  min->field <= avg ? avg        : w->resources->field.total + 1 )

static struct rmsummary *task_worker_box_size(struct work_queue *q, struct work_queue_worker *w, struct work_queue_task *t) {

	const struct rmsummary *min = task_min_resources(q, t);
	const struct rmsummary *max = task_max_resources(q, t);

	struct rmsummary *limits = rmsummary_create(-1);

	limits->cores = task_worker_box_size_resource(w, min, max, w->resources->cores.total / w->resources->workers.total, cores);

	limits->memory = task_worker_box_size_resource(w, min, max, w->resources->memory.total / w->resources->workers.total, memory);

	limits->disk = task_worker_box_size_resource(w, min, max, w->resources->disk.total / w->resources->workers.total, disk);

	limits->gpus = task_worker_box_size_resource(w, min, max, w->resources->gpus.total / w->resources->workers.total, gpus);

	return limits;
}

static work_queue_result_code_t start_one_task(struct work_queue *q, struct work_queue_worker *w, struct work_queue_task *t)
{
	/* wrap command at the last minute, so that we have the updated information
	 * about resources. */
	struct rmsummary *limits    = task_worker_box_size(q, w, t);
	const struct rmsummary *max = task_max_resources(q, t);

	char *command_line;
	if(q->monitor_mode) {
		command_line = work_queue_monitor_wrap(q, w, t, limits);
	} else {
		command_line = xxstrdup(t->command_line);
	}

	t->time_send_input_start = timestamp_get();
	work_queue_result_code_t result = send_input_files(q, w, t);
	t->time_send_input_finish = timestamp_get(); //record end time in case we return prematurely below.

	if (result != SUCCESS) {
		free(command_line);
		return result;
	}

	t->time_execute_cmd_start = timestamp_get();

	send_worker_msg(q,w, "task %lld\n",  (long long) t->taskid);

	long long cmd_len = strlen(command_line);
	send_worker_msg(q,w, "cmd %lld\n", (long long) cmd_len);
	link_putlstring(w->link, command_line, cmd_len, /* stoptime */ time(0) + (w->foreman ? q->long_timeout : q->short_timeout));
	debug(D_WQ, "%s\n", command_line);
	free(command_line);

	send_worker_msg(q,w, "cores %"PRId64"\n",  max->cores);
	send_worker_msg(q,w, "memory %"PRId64"\n", max->memory);
	send_worker_msg(q,w, "disk %"PRId64"\n",   max->disk);
	send_worker_msg(q,w, "gpus %"PRId64"\n",   max->gpus);

	/* Do not specify end, wall_time if running the resource monitor. We let the monitor police these resources. */
	if(q->monitor_mode == MON_DISABLED) {
		send_worker_msg(q,w, "end_time %"PRIu64"\n",  max->end);
		send_worker_msg(q,w, "wall_time %"PRIu64"\n", max->wall_time);
	}

	itable_insert(w->current_tasks_boxes, t->taskid, limits);

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
	int result_msg = send_worker_msg(q,w, "end\n");
	t->time_send_input_finish = timestamp_get();

	if(result_msg > -1)
	{
		debug(D_WQ, "%s (%s) busy on '%s'", w->hostname, w->addrport, t->command_line);
		return SUCCESS;
	}
	else
	{
		return WORKER_FAILURE;
	}
}

/*
Store a report summarizing the performance of a completed task.
Keep a list of reports equal to the number of workers connected.
Used for computing queue capacity below.
*/

static void add_task_report( struct work_queue *q, struct work_queue_task *t )
{
	struct work_queue_task_report *tr;

	// Create a new report object and add it to the list.
	tr = malloc(sizeof(struct work_queue_task_report));
	if(!tr) return;
	tr->transfer_time = t->total_transfer_time;
	tr->exec_time     = t->cmd_execution_time;
	list_push_tail(q->task_reports,tr);

	// Trim the list to the current number of useful workers.
	int count = MAX(WORK_QUEUE_TASK_REPORT_MIN_SIZE, hash_table_size(q->worker_table) );
	while(list_size(q->task_reports) >= count) {
	  tr = list_pop_head(q->task_reports);
		free(tr);
	}

	resource_monitor_append_report(q, t);
}

/*
Compute queue capacity based on stored task reports
and the summary of master activity.
*/

static double compute_capacity( const struct work_queue *q )
{
	timestamp_t avg_transfer_time = 0;
	timestamp_t avg_exec_time = 0;
	int count = 0;

	struct list_node *n;

	// Sum up the task reports available.
	for(n=q->task_reports->head;n;n=n->next) {
		struct work_queue_task_report *tr = n->data;
		avg_transfer_time += tr->transfer_time;
		avg_exec_time += tr->exec_time;
		count++;
	}

	// Compute the average task properties.
	if(count==0) return WORK_QUEUE_DEFAULT_CAPACITY;
	avg_transfer_time /= count;
	avg_exec_time /= count;

	// Compute the average time spent outside of work_queue_wait
	if(q->stats->total_tasks_complete==0) return WORK_QUEUE_DEFAULT_CAPACITY;
	timestamp_t avg_app_time = q->total_app_time / q->stats->total_tasks_complete;

	// Capacity is the ratio of task execution time to time spent in the master doing other things.
	if(avg_transfer_time==0) return WORK_QUEUE_DEFAULT_CAPACITY;
	return (double) avg_exec_time / (avg_transfer_time + avg_app_time);
}

static int check_hand_against_task(struct work_queue *q, struct work_queue_worker *w, struct work_queue_task *t) {

	/* worker has no reported any resources yet */
	if(w->resources->tag < 0)
		return 0;

	if(w->resources->workers.total < 1) {
		return 0;
	}

	if(!w->foreman) {
		struct blacklist_host_info *info = hash_table_lookup(q->worker_blacklist, w->hostname);
		if (info && info->blacklisted) {
			return 0;
		}
	}

	struct rmsummary *limits = task_worker_box_size(q, w, t);

	int ok = 1;

	if(w->resources->cores.inuse + limits->cores > overcommitted_resource_total(q, w->resources->cores.total, 1)) {
		ok = 0;
	}

	if(w->resources->memory.inuse + limits->memory > overcommitted_resource_total(q, w->resources->memory.total, 0)) {
		ok = 0;
	}

	if(w->resources->disk.inuse + limits->disk > w->resources->disk.total) { /* No overcommit disk */
		ok = 0;
	}

	if(w->resources->gpus.inuse + limits->gpus > overcommitted_resource_total(q, w->resources->gpus.total, 0)) {
		ok = 0;
	}

	rmsummary_delete(limits);

	return ok;
}

static struct work_queue_worker *find_worker_by_files(struct work_queue *q, struct work_queue_task *t)
{
	char *key;
	struct work_queue_worker *w;
	struct work_queue_worker *best_worker = 0;
	int64_t most_task_cached_bytes = 0;
	int64_t task_cached_bytes;
	struct stat *remote_info;
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
						task_cached_bytes += remote_info->st_size;
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

	update_max_worker(q, w);

	if(w->resources->workers.total < 1)
	{
		return;
	}

	itable_firstkey(w->current_tasks_boxes);
	while(itable_nextkey(w->current_tasks_boxes, &taskid, (void **)& box)) {
		w->resources->cores.inuse     += box->cores;
		w->resources->memory.inuse    += box->memory;
		w->resources->disk.inuse      += box->disk;
		w->resources->gpus.inuse      += box->gpus;
	}
}

static void update_max_worker(struct work_queue *q, struct work_queue_worker *w) {
	if(!w)
		return;

	if(w->resources->workers.total < 1) {
		return;
	}

	int cores_avg = w->resources->cores.total  / w->resources->workers.total;
	int mem_avg   = w->resources->memory.total / w->resources->workers.total;
	int disk_avg  = w->resources->disk.total   / w->resources->workers.total;
	int gpus_avg  = w->resources->gpus.total   / w->resources->workers.total;

	if(q->current_max_worker->cores < cores_avg) {
		q->current_max_worker->cores = cores_avg;
	}

	if(q->current_max_worker->memory < mem_avg) {
		q->current_max_worker->memory = mem_avg;
	}

	if(q->current_max_worker->disk < disk_avg) {
		q->current_max_worker->disk = disk_avg;
	}

	if(q->current_max_worker->gpus < gpus_avg) {
		q->current_max_worker->gpus = gpus_avg;
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
	t->time_committed = timestamp_get();

	t->hostname = xxstrdup(w->hostname);
	t->host = xxstrdup(w->addrport);

	work_queue_result_code_t result = start_one_task(q, w, t);

	itable_insert(w->current_tasks, t->taskid, t);
	itable_insert(q->worker_task_map, t->taskid, w); //add worker as execution site for t.

	change_task_state(q, t, WORK_QUEUE_TASK_RUNNING);

	t->total_submissions += 1;

	count_worker_resources(q, w);

	if(result != SUCCESS) {
		debug(D_WQ, "Failed to send task %d to worker %s (%s).", t->taskid, w->hostname, w->addrport);
		handle_failure(q, w, t, result);
	}

	log_worker_stats(q);
}

static void reap_task_from_worker(struct work_queue *q, struct work_queue_worker *w, struct work_queue_task *t, work_queue_task_state_t new_state)
{
	struct work_queue_worker *wr = itable_lookup(q->worker_task_map, t->taskid);

	if(wr != w)
	{
		debug(D_WQ, "Cannot reap task %d from worker. It is not being run by %s (%s)\n", t->taskid, w->hostname, w->addrport);
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

	log_worker_stats(q);
}

static int send_one_task( struct work_queue *q )
{
	struct work_queue_task *t;
	struct work_queue_worker *w;

	// Consider each task in the order of priority:
	list_first_item(q->ready_list);
	while( (t = list_next_item(q->ready_list))) {

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
			return 1;
		}
	}

	return 0;
}

//Sends keepalives to check if connected workers are responsive, and ask for updates If not, removes those workers.
static void ask_for_workers_updates(struct work_queue *q) {
	struct work_queue_worker *w;
	char *key;
	int64_t last_recv_elapsed_time;
	timestamp_t current_time = timestamp_get();

	hash_table_firstkey(q->worker_table);
	while(hash_table_nextkey(q->worker_table, &key, (void **) &w)) {
		if(q->keepalive_interval > 0) {

			/* we have not received workqueue message from worker yet, so we
			 * simply check agains its start_time. */
			if(!strcmp(w->hostname, "unknown")){
				if ((int)((current_time - w->start_time)/1000000) >= q->keepalive_timeout) {
					debug(D_WQ, "Removing worker %s (%s): hasn't sent its initialization in more than %d s", w->hostname, w->addrport, q->keepalive_timeout);
					handle_worker_failure(q, w);
				}
				continue;
			}

			last_recv_elapsed_time = (int64_t)(current_time - w->last_update_msg_time)/1000000;

			// send new keepalive check only (1) if we received a response since last keepalive check AND
			// (2) we are past keepalive interval
			if(w->last_msg_recv_time >= w->last_update_msg_time) {
				if(last_recv_elapsed_time >= q->keepalive_interval) {
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

static void abort_slow_workers(struct work_queue *q)
{
	struct category *c;
	char *category_name;

	struct work_queue_worker *w;
	struct work_queue_task *t;
	uint64_t taskid;

	/* optimization. If no category has a fast abort multiplier, simply return. */
	int fast_abort_flag = 0;

	hash_table_firstkey(q->categories);
	while(hash_table_nextkey(q->categories, &category_name, (void **) &c)) {
		if(c->total_tasks < 10) {
			c->average_task_time = 0;
			continue;
		}

		struct work_queue_stats *stats = c->wq_stats;
		if(!stats) {
			/* no stats have been computed yet */
			continue;
		}

		c->average_task_time = (stats->total_good_execute_time + stats->total_good_transfer_time) / c->total_tasks;

		if(c->fast_abort > 0)
			fast_abort_flag = 1;
	}

	if(!fast_abort_flag)
		return;

	struct category *c_def = work_queue_category_lookup_or_create(q, "default");

	timestamp_t current = timestamp_get();

	itable_firstkey(q->tasks);
	while(itable_nextkey(q->tasks, &taskid, (void **) &t)) {

		c = work_queue_category_lookup_or_create(q, t->category);
		/* Fast abort deactivated for this category */
		if(c->fast_abort == 0)
			continue;

		timestamp_t runtime = current - t->time_send_input_start;
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
			/* Fast abort also deactivated for the defaut category. */
			continue;
		}

		if(runtime >= (average_task_time * multiplier)) {
			w = itable_lookup(q->worker_task_map, t->taskid);
			if(w && !w->foreman)
			{
				debug(D_WQ, "Removing worker %s (%s): takes too long to execute the current task - %.02lf s (average task execution time by other workers is %.02lf s)", w->hostname, w->addrport, runtime / 1000000.0, average_task_time / 1000000.0);
				work_queue_blacklist_add_with_timeout(q, w->hostname, wq_option_blacklist_slow_workers_timeout);
				remove_worker(q, w);
				q->stats->total_workers_fast_aborted++;
			}
		}
	}
}

static int shut_down_worker(struct work_queue *q, struct work_queue_worker *w)
{
	if(!w) return 0;
	send_worker_msg(q,w,"exit\n");
	remove_worker(q, w);
	return 1;
}

//comparator function for checking if a task matches given tag.
static int tasktag_comparator(void *t, const void *r) {

	struct work_queue_task *task_in_queue = t;
	const char *tasktag = r;

	if (!strcmp(task_in_queue->tag, tasktag)) {
		return 1;
	}
	return 0;
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

	t->category = xxstrdup("default");

	return t;
}

struct work_queue_task *work_queue_task_clone(const struct work_queue_task *task)
{
  struct work_queue_task *new = xxmalloc(sizeof(struct work_queue_task));
  memcpy(new, task, sizeof(*new));

  //allocate new memory so we don't segfault when original memory is freed.
  if(task->tag) {
	new->tag = xxstrdup(task->tag);
  }
  if(task->category) {
	new->category = xxstrdup(task->category);
  }

  if(task->command_line) {
	new->command_line = xxstrdup(task->command_line);
  }

  new->input_files  = work_queue_task_file_list_clone(task->input_files);
  new->output_files = work_queue_task_file_list_clone(task->output_files);
  new->env_list     = work_queue_task_env_list_clone(task->env_list);

  if(task->resources_requested) {
	  new->resources_requested = malloc(sizeof(struct rmsummary));
	  memcpy(new->resources_requested, task->resources_requested, sizeof(sizeof(struct rmsummary)));
  }

  if(task->resources_measured) {
	  new->resources_measured = malloc(sizeof(struct rmsummary));
	  memcpy(new->resources_measured, task->resources_measured, sizeof(sizeof(struct rmsummary)));
  }

  if(task->monitor_output_directory) {
	new->monitor_output_directory = xxstrdup(task->monitor_output_directory);
  }

  if(task->output) {
	new->output = xxstrdup(task->output);
  }

  if(task->host) {
	new->host = xxstrdup(task->host);
  }

  if(task->hostname) {
	new->hostname = xxstrdup(task->hostname);
  }

  return new;
}


void work_queue_task_specify_command( struct work_queue_task *t, const char *cmd )
{
	if(t->command_line) free(t->command_line);
	t->command_line = xxstrdup(cmd);
}

void work_queue_task_specify_enviroment_variable( struct work_queue_task *t, const char *name, const char *value )
{
	if(value) {
		list_push_tail(t->env_list,string_format("%s=%s",name,value));
	} else {
		/* Specifications without = indicate variables to me unset. */
		list_push_tail(t->env_list,string_format("%s",name));
	}
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
		t->resources_requested->end = useconds;
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
		t->resources_requested->wall_time = useconds;
	}
}

void work_queue_task_specify_resources(struct work_queue_task *t, const struct rmsummary *rm) {
	if(!rm)
		return;

	work_queue_task_specify_cores(t,        rm->cores);
	work_queue_task_specify_memory(t,       rm->memory);
	work_queue_task_specify_disk(t,         rm->disk);
	work_queue_task_specify_running_time(t, rm->wall_time);
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

struct work_queue_file *work_queue_file_create(const struct work_queue_task *t, const char *payload, const char *remote_name, work_queue_file_t type, work_queue_file_flags_t flags)
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

	f->cached_name = make_cached_name(t, f);

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
		fprintf(stderr, "Error: Remote name %s contains absolute path.\n", remote_name);
		return 0;
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
		files = t->output_files;

		//check if two different different remote names map to the same url for outputs.
		list_first_item(t->output_files);
		while((tf = (struct work_queue_file*)list_next_item(files))) {
			if(!strcmp(file_url, tf->payload) && strcmp(remote_name, tf->remote_name)) {
				fprintf(stderr, "Error: output url remote name %s conflicts with another output pointing to same url (%s).\n", remote_name, file_url);
				return 0;
			}
		}

		//check if there is an input file with the same remote name.
		list_first_item(t->input_files);
		while((tf = (struct work_queue_file*)list_next_item(t->input_files))) {
			if(!strcmp(remote_name, tf->remote_name)){
				fprintf(stderr, "Error: output url %s conflicts with an input pointing to same remote name (%s).\n", file_url, remote_name);
				return 0;
			}
		}
	}

	tf = work_queue_file_create(t, file_url, remote_name, WORK_QUEUE_URL, flags);
	if(!tf) return 0;

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
	// the Work Queue framework, workers are prohibitted from writing to paths
	// outside of their workspaces. When a task is specified, the workspace of
	// the worker(the worker on which the task will be executed) is unlikely to
	// be known. Thus @param remote_name should not be an absolute path.
	if(remote_name[0] == '/') {
		fprintf(stderr, "Error: Remote name %s contains absolute path.\n", remote_name);
		return 0;
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

	tf = work_queue_file_create(t, local_name, remote_name, WORK_QUEUE_FILE, flags);
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
	// the Work Queue framework, workers are prohibitted from writing to paths
	// outside of their workspaces. When a task is specified, the workspace of
	// the worker(the worker on which the task will be executed) is unlikely to
	// be known. Thus @param remote_name should not be an absolute path.
	if(remote_name[0] == '/') {
		fprintf(stderr, "Error: Remote name %s contains absolute path.\n", remote_name);
		return 0;
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

	tf = work_queue_file_create(t, payload, remote_name, WORK_QUEUE_DIRECTORY, flags);
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
		fprintf(stderr, "Error: Remote name %s contains absolute path.\n", remote_name);
		return 0;
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

	tf = work_queue_file_create(t, local_name, remote_name, WORK_QUEUE_FILE_PIECE, flags);
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
		fprintf(stderr, "Error: Remote name %s contains absolute path.\n", remote_name);
		return 0;
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

	tf = work_queue_file_create(t, NULL, remote_name, WORK_QUEUE_BUFFER, flags);
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

int work_queue_task_specify_file_command(struct work_queue_task *t, const char *remote_name, const char *cmd, work_queue_file_type_t type, work_queue_file_flags_t flags)
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
		fprintf(stderr, "Error: Remote name %s contains absolute path.\n", remote_name);
		return 0;
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
		files = t->output_files;

		//check if two different different remote names map to the same local name for outputs.
		list_first_item(files);
		while((tf = (struct work_queue_file*)list_next_item(files))) {
			if(!strcmp(cmd, tf->payload) && strcmp(remote_name, tf->remote_name)) {
				fprintf(stderr, "Error: output file command %s conflicts with another output pointing to same remote name (%s).\n", cmd, remote_name);
				return 0;
			}
		}

		//check if there is an input file with the same remote name.
		list_first_item(t->input_files);
		while((tf = (struct work_queue_file*)list_next_item(t->input_files))) {
			if(!strcmp(remote_name, tf->remote_name)){
				fprintf(stderr, "Error: output file command %s conflicts with an input pointing to same remote name (%s).\n", cmd, remote_name);
				return 0;
			}
		}
	}

	tf = work_queue_file_create(t, cmd, remote_name, WORK_QUEUE_REMOTECMD, flags);
	if(!tf) return 0;

	list_push_tail(files, tf);

	return 1;
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
	struct work_queue_file *f = work_queue_file_create(NULL, local_name, local_name, type, WORK_QUEUE_CACHE);

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

		if(w->foreman) {
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
		if(t->command_line)
			free(t->command_line);
		if(t->tag)
			free(t->tag);
		if(t->category)
			free(t->category);
		if(t->output)
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

		if(t->hostname)
			free(t->hostname);
		if(t->host)
			free(t->host);
		if(t->resources_requested)
			rmsummary_delete(t->resources_requested);
		if(t->resources_measured)
			rmsummary_delete(t->resources_measured);
		if(t->monitor_output_directory)
			free(t->monitor_output_directory);

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

struct work_queue *work_queue_create(int port)
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

	q->master_link = link_serve(port);

	if(!q->master_link) {
		debug(D_NOTICE, "Could not create work_queue on port %i.", port);
		free(q);
		return 0;
	} else {
		char address[LINK_ADDRESS_MAX];
		link_address_local(q->master_link, address, &q->port);
	}

	getcwd(q->workingdir,PATH_MAX);

	q->ready_list = list_create();

	q->tasks          = itable_create(0);

	q->task_state_map = itable_create(0);

	q->worker_table = hash_table_create(0, 0);
	q->worker_blacklist = hash_table_create(0, 0);
	q->worker_task_map = itable_create(0);

	q->measured_local_resources   = rmsummary_create(-1);
	q->current_max_worker         = rmsummary_create(-1);

	q->stats                      = calloc(1, sizeof(struct work_queue_stats));
	q->stats_disconnected_workers = calloc(1, sizeof(struct work_queue_stats));

	q->workers_with_available_results = hash_table_create(0, 0);

	// The poll table is initially null, and will be created
	// (and resized) as needed by build_poll_table.
	q->poll_table_size = 8;

	q->send_receive_ratio = wq_option_send_receive_ratio;

	q->worker_selection_algorithm = wq_option_scheduler;
	q->process_pending_check = 0;
	q->workers_to_wait = 0;

	q->short_timeout = 5;
	q->long_timeout = 3600;

	q->stats->start_time = timestamp_get();
	q->task_reports = list_create();

	q->catalog_host = 0;
	q->catalog_port = 0;

	q->keepalive_interval = WORK_QUEUE_DEFAULT_KEEPALIVE_INTERVAL;
	q->keepalive_timeout = WORK_QUEUE_DEFAULT_KEEPALIVE_TIMEOUT;

	q->monitor_mode = MON_DISABLED;

	q->allocation_default_mode = WORK_QUEUE_ALLOCATION_MODE_FIXED;
	q->categories = hash_table_create(0, 0);

	// The value -1 indicates that fast abort is inactive by default
	// fast abort depends on categories, thus set after them.
	work_queue_activate_fast_abort(q, -1);

	q->password = 0;

	q->asynchrony_multiplier = 1.0;
	q->asynchrony_modifier = 0;

	q->minimum_transfer_timeout = 10;
	q->foreman_transfer_timeout = 3600;
	q->transfer_outlier_factor = 10;
	q->default_transfer_rate = 1*MEGABYTE;

	q->master_preferred_connection = xxstrdup("by_ip");

	if( (envstring  = getenv("WORK_QUEUE_BANDWIDTH")) ) {
		q->bandwidth = string_metric_parse(envstring);
		if(q->bandwidth < 0) {
			q->bandwidth = 0;
		}
	}

	//Deprecated:
	q->task_ordering = WORK_QUEUE_TASK_ORDER_FIFO;
	//

	debug(D_WQ, "Work Queue is listening on port %d.", q->port);
	return q;
}

int work_queue_enable_monitoring(struct work_queue *q, char *monitor_output_directory)
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

	return 1;
}

int work_queue_enable_monitoring_full(struct work_queue *q, char *monitor_output_directory) {
	int status = work_queue_enable_monitoring(q, monitor_output_directory);

	if(status)
		q->monitor_mode = MON_FULL;

	return status;
}

int work_queue_send_receive_ratio(struct work_queue *q, double ratio)
{

	if(ratio > 1) {
		q->send_receive_ratio = 1;
		return 1;
	} else if(ratio < 0) {
		q->send_receive_ratio = 0;
		return 1;
	} else {
		q->send_receive_ratio = ratio;
		return 0;
	}

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

	if(link_address_local(q->master_link, addr, &port)) {
		return port;
	} else {
		return 0;
	}
}

void work_queue_activate_worker_waiting(struct work_queue *q, int value)
{
	q->workers_to_wait = value;
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

void work_queue_specify_master_mode(struct work_queue *q, int mode)
{
	// Deprecated: Report to the catalog iff a name is given.
}

void work_queue_specify_catalog_server(struct work_queue *q, const char *hostname, int port)
{
	if(hostname) {
		if(q->catalog_host) free(q->catalog_host);
		q->catalog_host = strdup(hostname);
		setenv("CATALOG_HOST", hostname, 1);
	}
	if(port > 0) {
		char portstr[DOMAIN_NAME_MAX];
		q->catalog_port = port;
		snprintf(portstr, DOMAIN_NAME_MAX, "%d", port);
		setenv("CATALOG_PORT", portstr, 1);
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
		if(q->name) {
			update_catalog(q, NULL, 1);
		}

		/* we call this function here before any of the structures are freed. */
		work_queue_disable_monitoring(q);

		if(q->catalog_host) free(q->catalog_host);
		if(q->update_port)  free(q->update_port);

		hash_table_delete(q->worker_table);
		hash_table_delete(q->worker_blacklist);
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

		list_free(q->task_reports);
		list_delete(q->task_reports);

		free(q->stats);
		free(q->stats_disconnected_workers);

		if(q->name)
			free(q->name);

		if(q->master_preferred_connection)
			free(q->master_preferred_connection);

		free(q->poll_table);
		link_close(q->master_link);
		if(q->logfile) {
			fclose(q->logfile);
		}

		if(q->transactions_logfile) {
			write_transaction(q, "MASTER END");
			fclose(q->transactions_logfile);
		}


		if(q->measured_local_resources)
			rmsummary_delete(q->measured_local_resources);

		if(q->current_max_worker)
			rmsummary_delete(q->current_max_worker);

		free(q);
	}
}

void update_resource_report(struct work_queue *q) {
	static time_t last_update_time = 0;

	// Only measure every few seconds.
	if((time(0) - last_update_time) < WORK_QUEUE_RESOURCE_MEASUREMENT_INTERVAL)
		return;

	rmonitor_measure_process_update_to_peak(q->measured_local_resources, getpid());

	last_update_time = time(0);
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

		struct jx *extra = jx_object(
				jx_pair(jx_string("type"), jx_string("work_queue"),
					jx_pair(jx_string("user"), jx_string(getlogin()),
						NULL)));

		if(q->name) {
			jx_insert_string(extra, "master_name", q->name);
		}

		rmsummary_print(final, q->measured_local_resources, /* pprint */ 0, extra);

		copy_fd_to_stream(summs_fd, final);

		jx_delete(extra);
		fclose(final);
		close(summs_fd);

		if(rename(template, q->monitor_summary_filename) < 0)
			warn(D_DEBUG, "Could not move monitor report to final destination file.");
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

	if(q->monitor_mode == MON_FULL && (q->monitor_output_directory || t->monitor_output_directory)) {
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
	char *extra_options;
	if(t->category) {
		extra_options = string_format("-V 'task_id: %d' -V 'category: %s'", t->taskid, t->category);
	} else {
		extra_options = string_format("-V 'task_id: %d'", t->taskid);
	}

	int extra_files = (q->monitor_mode == MON_FULL);

	char *monitor_cmd = resource_monitor_write_command("./" RESOURCE_MONITOR_REMOTE_NAME, RESOURCE_MONITOR_REMOTE_NAME, limits, extra_options, /* debug */ extra_files, /* series */ extra_files, /* inotify */ 0);
	char *wrap_cmd  = string_wrap_command(t->command_line, monitor_cmd);

	free(extra_options);
	free(monitor_cmd);

	return wrap_cmd;
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
		list_push_priority(q->ready_list,t,t->priority);
	} else {
		list_push_head(q->ready_list,t);
	}

	/* If the task has been used before, clear out accumulated state. */
	clean_task_state(t);
}


work_queue_task_state_t work_queue_task_state(struct work_queue *q, int taskid) {
	return (int)(uintptr_t)itable_lookup(q->task_state_map, taskid);
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
			push_task_to_ready_list(q, t);
			break;
		case WORK_QUEUE_TASK_DONE:
		case WORK_QUEUE_TASK_CANCELED:
			/* tasks are freed when returned to user, thus we remove them from our local record */
			itable_remove(q->tasks, t->taskid);
			break;
		default:
			/* do nothing */
			break;
	}

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
		case WORK_QUEUE_TASK_WAITING_RESUBMISSION:
			str = "WAITING_RESUBMISSION";
			break;
		case WORK_QUEUE_TASK_UNKNOWN:
		default:
			str = "UNKNOWN";
			break;
	}

	return str;
}

const char *task_result_str(work_queue_result_t result) {
	const char *str;

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
		case WORK_QUEUE_RESULT_FORSAKEN:
			str = "FORSAKEN";
			break;
		case WORK_QUEUE_RESULT_MAX_RETRIES:
			str = "MAX_RETRIES";
			break;
		case WORK_QUEUE_RESULT_TASK_MAX_RUN_TIME:
			str = "MAX_WALL_TIME";
			break;
		case WORK_QUEUE_RESULT_UNKNOWN:
		default:
			str = "UNKNOWN";
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

int work_queue_submit_internal(struct work_queue *q, struct work_queue_task *t)
{
	itable_insert(q->tasks, t->taskid, t);

	/* Ensure category structure is created. */
	work_queue_category_lookup_or_create(q, t->category);

	change_task_state(q, t, WORK_QUEUE_TASK_READY);

	t->time_task_submit = timestamp_get();
	q->stats->total_tasks_dispatched++;

	if(q->monitor_mode != MON_DISABLED)
		work_queue_monitor_add_files(q, t);

	return (t->taskid);
}

int work_queue_submit(struct work_queue *q, struct work_queue_task *t)
{
	static int next_taskid = 1;

	t->taskid = next_taskid;

	//Increment taskid. So we get a unique taskid for every submit.
	next_taskid++;

	return work_queue_submit_internal(q, t);
}

void work_queue_blacklist_add_with_timeout(struct work_queue *q, const char *hostname, time_t timeout)
{
	struct blacklist_host_info *info = hash_table_lookup(q->worker_blacklist, hostname);

	if(!info) {
		info = malloc(sizeof(struct blacklist_host_info));
		info->times_blacklisted = 0;
		info->blacklisted       = 0;
	}

	/* count the times the worker goes from active to blacklisted. */
	if(!info->blacklisted)
		info->times_blacklisted++;

	info->blacklisted = 1;

	if(timeout > 0) {
		debug(D_WQ, "Blacklisting host %s by %" PRIu64 " seconds (blacklisted %d times).\n", hostname, (uint64_t) timeout, info->times_blacklisted);
		info->release_at = time(0) + timeout;
	} else {
		debug(D_WQ, "Blacklisting host %s indefinitely.\n", hostname);
		info->release_at = -1;
	}

	hash_table_insert(q->worker_blacklist, hostname, (void *) info);
}

void work_queue_blacklist_add(struct work_queue *q, const char *hostname)
{
	work_queue_blacklist_add_with_timeout(q, hostname, -1);
}

void work_queue_blacklist_remove(struct work_queue *q, const char *hostname)
{
	struct blacklist_host_info *info = hash_table_remove(q->worker_blacklist, hostname);
	if(info) {
		info->blacklisted = 0;
		info->release_at  = 0;
	}
}

/* deadline < 1 means release all, regardless of release_at time. */
static void work_queue_blacklist_clear_by_time(struct work_queue *q, time_t deadline)
{
	char *hostname;
	struct blacklist_host_info *info;

	hash_table_firstkey(q->worker_blacklist);
	while(hash_table_nextkey(q->worker_blacklist, &hostname, (void *) &info)) {
		if(!info->blacklisted)
			continue;

		/* do not clear if blacklisted indefinitely, and we are not clearing the whole list. */
		if(info->release_at < 1 && deadline > 0)
			continue;

		/* do not clear if the time for this host has not meet the deadline. */
		if(deadline > 0 && info->release_at > deadline)
			continue;

		debug(D_WQ, "Clearing hostname %s from blacklist.\n", hostname);
		work_queue_blacklist_remove(q, hostname);
	}
}

void work_queue_blacklist_clear(struct work_queue *q)
{
	work_queue_blacklist_clear_by_time(q, -1);
}

static void print_password_warning( struct work_queue *q )
{
	static int did_password_warning = 0;

	if(did_password_warning) return;

		if(!q->password && q->name) {
			fprintf(stderr,"warning: this work queue master is visible to the public.\n");
			fprintf(stderr,"warning: you should set a password with the --password option.\n");
		did_password_warning = 1;
	}
}

struct work_queue_task *work_queue_wait(struct work_queue *q, int timeout)
{
	return work_queue_wait_internal(q, timeout, NULL, NULL);
}

static int wait_loop_poll_links(struct work_queue *q, int stoptime, struct link *foreman_uplink, int *foreman_uplink_active, int last_tasks_transfered)
{
	int n = build_poll_table(q, foreman_uplink);

	static int busy_waiting = 0;

	// Wait no longer than the caller's patience.
	int msec;
	if(stoptime) {
		msec = MAX(0, (stoptime - time(0)) * 1000);
	} else {
		msec = 5000;
	}

	// If workers are available and tasks waiting to be dispatched, don't wait
	// on a message. However, take care of not busy waiting, if no available
	// worker can execute a task in the ready list.

	if( (last_tasks_transfered || !busy_waiting) && available_workers(q) > 0 && (task_state_any(q, WORK_QUEUE_TASK_READY) || task_state_any(q, WORK_QUEUE_TASK_WAITING_RETRIEVAL)) ) {
		msec = 0;
		busy_waiting = 1;        //Mark that we may be busy waiting, so that if no task are transfered, we force a wait next cycle.
	}
	else {
		busy_waiting = 0;
	}

	// Poll all links for activity.
	timestamp_t link_poll_start = timestamp_get();
	int result = link_poll(q->poll_table, n, msec);
	q->link_poll_end = timestamp_get();
	q->total_idle_time += q->link_poll_end - link_poll_start;


	// If the master link was awake, then accept as many workers as possible.
	if(q->poll_table[0].revents) {
		do {
			add_worker(q);
		} while(link_usleep(q->master_link, 0, 1, 0) && (stoptime > time(0)));
	}

	int i, j = 1;

	// Consider the foreman_uplink passed into the function and disregard if inactive.
	if(foreman_uplink) {
		if(q->poll_table[1].revents) {
			*foreman_uplink_active = 1; //signal that the master link saw activity
		} else {
			*foreman_uplink_active = 0;
		}
		j++;
	}

	// Then consider all existing active workers
	for(i = j; i < n; i++) {
		if(q->poll_table[i].revents) {
			handle_worker(q, q->poll_table[i].link);
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

	work_queue_blacklist_clear_by_time(q, time(0));

	return result;
}

static int wait_loop_transfer_tasks(struct work_queue *q, time_t stoptime)
{
	int task_sent = 0, total_tasks_sent = 0;
	int task_received = 0, total_tasks_received = 0;

	do
	{
		task_sent     = 0;
		task_received = 0;

		double coin          = random() / ((double) RAND_MAX);
		int    send_decision = coin < q->send_receive_ratio;

		struct work_queue_task *t = task_state_any(q, WORK_QUEUE_TASK_WAITING_RETRIEVAL);

		if(task_state_any(q, WORK_QUEUE_TASK_READY) && (!t || send_decision))
		{
			task_sent         = send_one_task(q);
			total_tasks_sent += task_sent;
		}

		if(t && (!task_sent || !send_decision))
		{
			task_received         = receive_one_task(q);
			total_tasks_received += task_received;
		}

	} while((time(0) < stoptime) && (task_sent || task_received));

	return total_tasks_sent + total_tasks_received;
}

struct work_queue_task *work_queue_wait_internal(struct work_queue *q, int timeout, struct link *foreman_uplink, int *foreman_uplink_active)
/*
	  --------------------
	 |  compute stoptime  |
	  --------------------
			   |
			   v
		 --------------
+------>|  poll links  |
|        --------------
|              |
|              v
|        -------------
|       |transfer task|<----------------+
|        -------------                  |
|              |                    yes |
|              v                        |
|     ------------------  yes   -----------------
|    | tasks remaining? |----->| time remaining? |
|     ------------------        -----------------
|           no |                    no |
|              v                       |
|     ------------------               |
|    |fast abort workers|              |
|     ------------------               |
|              |                       |
|              v                       |
| yes  -----------------               |
+-----| time remaining? |              |
	   -----------------               |
			no |                       |
			   |-----------------------+
			   v
		   ----------
		  |  return  |
		   ----------
*/
{
	struct work_queue_task *t;
	time_t stoptime;
	int    tasks_transfered = 0;

	static timestamp_t last_left_time = 0;
	if(last_left_time!=0) {
		q->total_app_time += timestamp_get() - last_left_time;
	}

	print_password_warning(q);

	if(timeout == WORK_QUEUE_WAITFORTASK) {
		stoptime = 0;
	} else {
		stoptime = time(0) + timeout;
	}

	while(1) {
		if(q->name) {
			update_catalog(q, foreman_uplink, 0);
		}

		if(q->monitor_mode)
			update_resource_report(q);

		ask_for_workers_updates(q);

		t = task_state_any(q, WORK_QUEUE_TASK_RETRIEVED);
		if(t) {
			change_task_state(q, t, WORK_QUEUE_TASK_DONE);

			last_left_time = timestamp_get();

			if( t->result != WORK_QUEUE_RESULT_SUCCESS )
			{
				q->stats->total_tasks_failed++;
			}

			return t;

		}

		if( q->process_pending_check && process_pending() )
			break;

		if(!task_state_any(q, WORK_QUEUE_TASK_RUNNING) && !task_state_any(q, WORK_QUEUE_TASK_READY) && !task_state_any(q, WORK_QUEUE_TASK_WAITING_RETRIEVAL) && !(foreman_uplink))
			break;

		wait_loop_poll_links(q, stoptime, foreman_uplink, foreman_uplink_active, tasks_transfered);

		expire_waiting_tasks(q);

		//Re-enqueue the tasks that workers labeled for resubmission.
		while((t = task_state_any(q, WORK_QUEUE_TASK_WAITING_RESUBMISSION))) {
			change_task_state(q, t, WORK_QUEUE_TASK_READY);
		}

		//We have the resources we have been waiting for; start task transfers
		int known = known_workers(q);
		if(known > 0 && known >= q->workers_to_wait) {
			tasks_transfered = wait_loop_transfer_tasks(q, stoptime);
			q->workers_to_wait = 0; //disable it after we started dipatching tasks
		}

		// If fast abort is enabled, kill off slow workers.
		abort_slow_workers(q);

		// If the foreman_uplink is active then break so the caller can handle it.
		if(foreman_uplink) {
			break;
		}

		// If nothing was awake, restart the loop or return without a task.
		if(stoptime && time(0) >= stoptime) {
			break;
		} else {
			continue;
		}
	}

	last_left_time = timestamp_get();

	return 0;
}

int work_queue_hungry(struct work_queue *q)
{
	if(q->stats->total_tasks_dispatched < 100)
		return (100 - q->stats->total_tasks_dispatched);

	//BUG: fix this so that it actually looks at the number of cores available.

	int i, j;

	//i = 1.1 * number of current workers
	//j = # of queued tasks.
	//i-j = # of tasks to queue to re-reach the status quo.
	i = (1.1 * hash_table_size(q->worker_table));
	j = task_state_count(q, NULL, WORK_QUEUE_TASK_READY);
	return MAX(i - j, 0);
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

	q->stats->total_tasks_cancelled++;

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
			q->stats->total_tasks_cancelled++;
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

void work_queue_master_preferred_connection(struct work_queue *q, const char *preferred_connection)
{
	free(q->master_preferred_connection);
	q->master_preferred_connection = xxstrdup(preferred_connection);
}

int work_queue_tune(struct work_queue *q, const char *name, double value)
{

	if(!strcmp(name, "asynchrony-multiplier")) {
		q->asynchrony_multiplier = MAX(value, 1.0);

	} else if(!strcmp(name, "asynchrony-modifier")) {
		q->asynchrony_modifier = MAX(value, 0);

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

	} else if(!strcmp(name, "send-receive-ratio")) {
		work_queue_send_receive_ratio(q, value);

	} else if(!strcmp(name, "first-allocation-every-n-tasks")) {
		first_allocation_every_n_tasks = MAX(1, value);

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

void work_queue_get_stats(struct work_queue *q, struct work_queue_stats *s)
{
	struct work_queue_stats *qs;
	qs = q->stats;

	memset(s, 0, sizeof(*s));

	//info about workers
	s->total_workers_connected = hash_table_size(q->worker_table);
	s->workers_init = hash_table_size(q->worker_table) - known_workers(q);
	s->workers_idle = known_workers(q) - workers_with_tasks(q); //returns workers that are not running any tasks.
	s->workers_busy = workers_with_tasks(q);

	s->total_workers_joined       = qs->total_workers_joined;
	s->total_workers_removed      = qs->total_workers_removed;
	s->total_workers_idled_out    = qs->total_workers_idled_out;
	s->total_workers_lost         = qs->total_workers_lost;
	s->total_workers_fast_aborted = qs->total_workers_fast_aborted;

	//info about tasks
	s->tasks_waiting = task_state_count(q, NULL, WORK_QUEUE_TASK_READY);
	s->tasks_running = task_state_count(q, NULL, WORK_QUEUE_TASK_RUNNING) + task_state_count(q, NULL, WORK_QUEUE_TASK_WAITING_RETRIEVAL);
	s->tasks_complete = task_state_count(q, NULL, WORK_QUEUE_TASK_RETRIEVED);

	s->total_tasks_dispatched = qs->total_tasks_dispatched;
	s->total_tasks_complete = qs->total_tasks_complete;
	s->total_tasks_cancelled = qs->total_tasks_cancelled;

	//info about queue
	s->start_time = qs->start_time;
	s->total_send_time = qs->total_send_time;
	s->total_receive_time = qs->total_receive_time;
	s->total_bytes_sent = qs->total_bytes_sent;
	s->total_bytes_received = qs->total_bytes_received;
	s->total_execute_time = qs->total_execute_time;
	s->total_good_execute_time = qs->total_good_execute_time;
	s->total_exhausted_attempts = qs->total_exhausted_attempts;
	s->total_exhausted_execute_time = qs->total_exhausted_execute_time;

	timestamp_t wall_clock_time = timestamp_get() - qs->start_time;
	if(wall_clock_time>0 && s->total_workers_connected>0) {
		s->efficiency = (double) (qs->total_good_execute_time) / (wall_clock_time * s->total_workers_connected);
	}
	if(wall_clock_time>0) {
		s->idle_percentage = (double) q->total_idle_time / wall_clock_time;
	}
	s->capacity = compute_capacity(q);

	//info about resources
	s->bandwidth = work_queue_get_effective_bandwidth(q);
	struct work_queue_resources r;
	aggregate_workers_resources(q,&r);

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

	//deprecated fields
	s->port = q->port;
	s->priority = q->priority;
	s->workers_ready = s->workers_idle;
	s->workers_full = 0;
	s->total_worker_slots = s->tasks_running;
	s->avg_capacity = s->capacity;

	struct rmsummary *rmax = largest_waiting_measured_resources(q, NULL);
	char *key;
	struct category *c;
	hash_table_firstkey(q->categories);
	while(hash_table_nextkey(q->categories, &key, (void **) &c)) {
		rmsummary_merge_max(rmax, c->max_allocation);
	}

	s->workers_able = count_workers_for_waiting_tasks(q, rmax);
	rmsummary_delete(rmax);
}

void work_queue_get_stats_hierarchy(struct work_queue *q, struct work_queue_stats *s)
{
	work_queue_get_stats(q, s);

	char *key;
	struct work_queue_worker *w;

	/* Consider running only if reported by some hand. */
	s->tasks_running = 0;
	s->total_workers_connected = 0;

	hash_table_firstkey(q->worker_table);
	while(hash_table_nextkey(q->worker_table, &key, (void **) &w)) {
		if(w->foreman)
		{
			accumulate_stat(s, w->stats, total_workers_joined);
			accumulate_stat(s, w->stats, total_workers_removed);
			accumulate_stat(s, w->stats, total_workers_idled_out);
			accumulate_stat(s, w->stats, total_workers_lost);
			accumulate_stat(s, w->stats, total_workers_fast_aborted);
			accumulate_stat(s, w->stats, total_send_time);
			accumulate_stat(s, w->stats, total_receive_time);
			accumulate_stat(s, w->stats, total_execute_time);
			accumulate_stat(s, w->stats, total_bytes_sent);
			accumulate_stat(s, w->stats, total_bytes_received);
		}

		accumulate_stat(s, w->stats, tasks_waiting);
		accumulate_stat(s, w->stats, tasks_running);
	}

	/* Account also for workers connected directly to the master. */
	s->total_workers_connected = s->total_workers_joined - s->total_workers_removed;

	s->total_workers_joined  += q->stats_disconnected_workers->total_workers_joined;
	s->total_workers_removed += q->stats_disconnected_workers->total_workers_removed;
	s->total_workers_idled_out    += q->stats_disconnected_workers->total_workers_idled_out;
	s->total_workers_lost         += q->stats_disconnected_workers->total_workers_lost;
	s->total_workers_fast_aborted += q->stats_disconnected_workers->total_workers_fast_aborted;
	s->total_send_time       += q->stats_disconnected_workers->total_send_time;
	s->total_receive_time    += q->stats_disconnected_workers->total_receive_time;
	s->total_execute_time    += q->stats_disconnected_workers->total_execute_time;
	s->total_bytes_sent      += q->stats_disconnected_workers->total_bytes_sent;
	s->total_bytes_received  += q->stats_disconnected_workers->total_bytes_received;

	timestamp_t wall_clock_time = timestamp_get() - q->stats->start_time;
	if(wall_clock_time>0 && s->total_workers_connected>0) {
		s->efficiency = (double) (q->stats->total_good_execute_time) / (wall_clock_time * s->total_workers_connected);
	}
}

void work_queue_get_stats_category(struct work_queue *q, const char *category, struct work_queue_stats *s)
{
	struct category *c = work_queue_category_lookup_or_create(q, category);
	struct work_queue_stats *cs = c->wq_stats;
	memcpy(s, cs, sizeof(*s));

	//info about tasks
	s->tasks_waiting = task_state_count(q, category, WORK_QUEUE_TASK_READY);
	s->tasks_running = task_state_count(q, category, WORK_QUEUE_TASK_RUNNING) + task_state_count(q, category, WORK_QUEUE_TASK_WAITING_RETRIEVAL);
	s->tasks_complete = task_state_count(q, category, WORK_QUEUE_TASK_RETRIEVED);

	struct rmsummary *rmax = largest_waiting_measured_resources(q, c->name);

	s->workers_able  = count_workers_for_waiting_tasks(q, rmax);

	rmsummary_delete(rmax);
}

/*
This function is a little roundabout, because work_queue_resources_add
updates the min and max of each value as it goes.  So, we set total
to the value of the first item, then use work_queue_resources_add.
If there are no items, we must manually return zero.
*/

void aggregate_workers_resources( struct work_queue *q, struct work_queue_resources *total )
{
	struct work_queue_worker *w;
	char *key;

	bzero(total, sizeof(struct work_queue_resources));

	if(hash_table_size(q->worker_table)==0) {
		return;
	}

	hash_table_firstkey(q->worker_table);
	while(hash_table_nextkey(q->worker_table,&key,(void**)&w)) {
			work_queue_resources_add(total,w->resources);
	}
}

int work_queue_specify_log(struct work_queue *q, const char *logfile)
{
	q->logfile = fopen(logfile, "a");
	if(q->logfile) {
		setvbuf(q->logfile, NULL, _IOLBF, 1024); // line buffered, we don't want incomplete lines
		fprintf(q->logfile,
				// start with a comment
				"#"
			// time:
			"timestamp "
			//workers:
			"total_workers_connected workers_init workers_idle workers_busy total_workers_joined total_workers_removed "
			// tasks:
			"tasks_waiting tasks_running tasks_complete total_tasks_dispatched total_tasks_complete total_tasks_cancelled "
			// queue:
			"start_time total_send_time total_receive_time total_bytes_sent total_bytes_received efficiency idle_percentage capacity "
			// resource totals:
			"bandwidth total_cores total_memory total_disk total_gpus "
			//mins/maxs:
			"min_cores max_cores min_memory max_memory min_disk max_disk min_gpus max_gpus "
			//execute/good execute time
			"total_execute_time total_good_execute_time "
			//end with a newline
			"\n"
			);
		log_worker_stats(q);
		debug(D_WQ, "log enabled and is being written to %s\n", logfile);
		return 1;
	}
	else
	{
		debug(D_NOTICE | D_WQ, "couldn't open logfile %s: %s\n", logfile, strerror(errno));
		return 0;
	}
}

static void write_transaction(struct work_queue *q, const char *str) {
	if(!q->transactions_logfile)
		return;

	fprintf(q->transactions_logfile, "%" PRIu64, timestamp_get());
	fprintf(q->transactions_logfile, " %d", getpid());
	fprintf(q->transactions_logfile, " %s", str);
	fprintf(q->transactions_logfile, "\n");
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
	} else if(state == WORK_QUEUE_TASK_WAITING_RESUBMISSION) {
			/* do not add any info */
	} else if(state == WORK_QUEUE_TASK_DONE) {
		buffer_printf(&B, " %s ", task_result_str(t->result));
		rmsummary_print_buffer(&B, t->resources_measured, 1);
	} else if(state == WORK_QUEUE_TASK_RETRIEVED) {
		buffer_printf(&B, " %s ", task_result_str(t->result));
		if(t->result == WORK_QUEUE_RESULT_RESOURCE_EXHAUSTION) {
			if(t->resources_measured) {
				rmsummary_print_buffer(&B, t->resources_measured->limits_exceeded, 1);
			}
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
	rmsummary_print_buffer(&B, category_dynamic_task_max_resources(q->categories, c->name , NULL, CATEGORY_ALLOCATION_MAX), 1);
	write_transaction(q, buffer_tostring(&B));
	buffer_rewind(&B, 0);

	buffer_printf(&B, "CATEGORY %s MIN ", c->name);
	rmsummary_print_buffer(&B, category_dynamic_task_min_resources(q->categories, c->name , NULL, CATEGORY_ALLOCATION_MAX), 1);
	write_transaction(q, buffer_tostring(&B));
	buffer_rewind(&B, 0);

	const char *mode;

	switch(c->allocation_mode) {
		case WORK_QUEUE_ALLOCATION_MODE_MAX:
			mode = "MAX";
			break;
		case WORK_QUEUE_ALLOCATION_MODE_MIN_WASTE:
			mode = "MIN_WASTE";
			break;
		case WORK_QUEUE_ALLOCATION_MODE_MAX_THROUGHPUT:
			mode = "MAX_THROUGHPUT";
			break;
		case WORK_QUEUE_ALLOCATION_MODE_FIXED:
		default:
			mode = "FIXED";
			break;
	}

	buffer_printf(&B, "CATEGORY %s FIRST %s ", c->name, mode);
	rmsummary_print_buffer(&B, category_dynamic_task_max_resources(q->categories, c->name , NULL, CATEGORY_ALLOCATION_FIRST), 1);
	write_transaction(q, buffer_tostring(&B));
	buffer_rewind(&B, 0);

	buffer_free(&B);
}


int work_queue_specify_transactions_log(struct work_queue *q, const char *logfile) {
	q->transactions_logfile =fopen(logfile, "a");
	if(q->transactions_logfile) {
		setvbuf(q->transactions_logfile, NULL, _IOLBF, 1024); // line buffered, we don't want incomplete lines
		debug(D_WQ, "transactions log enabled and is being written to %s\n", logfile);

		fprintf(q->transactions_logfile, "# date time master-pid MASTER START|END\n");
		fprintf(q->transactions_logfile, "# date time master-pid CATEGORY name MAX resources-max-per-task\n");
		fprintf(q->transactions_logfile, "# date time master-pid CATEGORY name MIN resources-min-per-task-per-worker\n");
		fprintf(q->transactions_logfile, "# date time master-pid CATEGORY name FIRST FIXED|MAX|MIN_WASTE|MAX_THROUGHPUT resources-requested\n");
		fprintf(q->transactions_logfile, "# date time master-pid TASK taskid WAITING category-name FIRST_RESOURCES|MAX_RESOURCES resources-requested\n");
		fprintf(q->transactions_logfile, "# date time master-pid TASK taskid RUNNING worker-address FIRST_RESOURCES|MAX_RESOURCES resources-given\n");
		fprintf(q->transactions_logfile, "# date time master-pid TASK taskid WAITING_RETRIEVAL worker-address\n");
		fprintf(q->transactions_logfile, "# date time master-pid TASK taskid RETRIEVED|DONE task-result ...\n");
		fprintf(q->transactions_logfile, "# date time master-pid TASK taskid RETRIEVED SUCCESS|SIGNAL|END_TIME|FORSAKEN|MAX_RETRIES|MAX_WALLTIME|UNKNOWN|RESOURCE_EXHAUSTION [limits-exceeded]\n");
		fprintf(q->transactions_logfile, "# date time master-pid TASK taskid DONE SUCCESS|INPUT_MISS|OUTPUT_MISS|STDOUT_MISS|SIGNAL|END_TIME|MAX_RETRIES|MAX_WALLTIME|UNKNOWN|RESOURCE_EXHAUSTION [limits-exceeded]\n\n");

		write_transaction(q, "MASTER START");
		return 1;
	}
	else
	{
		debug(D_NOTICE | D_WQ, "couldn't open transactions logfile %s: %s\n", logfile, strerror(errno));
		return 0;
	}
}

void work_queue_category_accumulate_task(struct work_queue *q, struct work_queue_task *t) {
	const char *name              = t->category ? t->category : "default";
	struct category *c = work_queue_category_lookup_or_create(q, name);

	struct work_queue_stats *s = c->wq_stats;

	s->total_bytes_sent     += t->total_bytes_sent;
	s->total_bytes_received += t->total_bytes_received;
	s->total_execute_time   += t->cmd_execution_time;

	s->total_send_time      += (t->time_send_input_finish     - t->time_send_input_start);
	s->total_receive_time   += (t->time_receive_output_finish - t->time_receive_output_start);

	s->bandwidth = (1.0*MEGABYTE*(s->total_bytes_sent + s->total_bytes_received))/(s->total_send_time + s->total_receive_time + 1);

	category_accumulate_summary(q->categories, t->category, t->resources_measured);

	if(t->result == WORK_QUEUE_RESULT_SUCCESS)
	{
		c->total_tasks++;

		s->total_tasks_complete      = c->total_tasks;
		s->total_good_execute_time  += t->cmd_execution_time;
		s->total_good_transfer_time += t->total_transfer_time;
	} else {
		s->total_tasks_failed++;
	}

	if((c->total_tasks + s->total_tasks_failed) % first_allocation_every_n_tasks == 0 ||
			time(0) - c->first_allocation_time > first_allocation_every_n_seconds) {
		category_update_first_allocation(q->categories, q->current_max_worker, t->category);

		write_transaction_category(q, c);
	}
}

void work_queue_initialize_categories(struct work_queue *q, struct rmsummary *max, const char *summaries_file) {
	categories_initialize(q->categories, max, summaries_file);
}

void work_queue_specify_max_resources(struct work_queue *q,  const struct rmsummary *rm) {
	work_queue_specify_max_category_resources(q,  "default", rm);
}

void work_queue_specify_max_category_resources(struct work_queue *q,  const char *category, const struct rmsummary *rm) {
	struct category *c = work_queue_category_lookup_or_create(q, category);

	rmsummary_delete(c->max_allocation);
	c->max_allocation = rmsummary_create(-1);

	if(rm) {
		rmsummary_merge_max(c->max_allocation, rm);
	}

	write_transaction_category(q, c);
}

int work_queue_specify_category_mode(struct work_queue *q, const char *category, category_mode_t mode) {

	/* make sure category exists. */
	work_queue_category_lookup_or_create(q, category);

	switch(mode) {
		case WORK_QUEUE_ALLOCATION_MODE_FIXED:
		case WORK_QUEUE_ALLOCATION_MODE_MAX:
		case WORK_QUEUE_ALLOCATION_MODE_MIN_WASTE:
		case WORK_QUEUE_ALLOCATION_MODE_MAX_THROUGHPUT:
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
		category_specify_allocation_mode(q->categories, category, mode);
	}

	return 1;
}

int work_queue_enable_category_resource(struct work_queue *q, const char *category, const char *resource, int autolabel) {
	return category_enable_auto_resource(q->categories, category, resource, autolabel);
}

const struct rmsummary *task_max_declared_resources(struct work_queue *q, struct work_queue_task *t) {
	/* make sure category exists */
	work_queue_category_lookup_or_create(q, t->category);

	return category_dynamic_task_max_declared_resources(q->categories, t->category, t->resources_requested, t->resource_request);
}

const struct rmsummary *task_max_resources(struct work_queue *q, struct work_queue_task *t) {
	/* make sure category exists */
	work_queue_category_lookup_or_create(q, t->category);

	return category_dynamic_task_max_resources(q->categories, t->category, t->resources_requested, t->resource_request);
}

const struct rmsummary *task_min_resources(struct work_queue *q, struct work_queue_task *t) {
	/* make sure category exists */
	work_queue_category_lookup_or_create(q, t->category);

	return category_dynamic_task_min_resources(q->categories, t->category, t->resources_requested, t->resource_request);
}

struct category *work_queue_category_lookup_or_create(struct work_queue *q, const char *name) {
	struct category *c = category_lookup_or_create(q->categories, name);

	if(!c->wq_stats) {
		c->wq_stats = calloc(1, sizeof(struct work_queue_stats));
		category_specify_allocation_mode(q->categories, name, q->allocation_default_mode);
	}

	return c;
}

/* vim: set noexpandtab tabstop=4: */
