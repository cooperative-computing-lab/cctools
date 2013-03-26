/*
Copyright (C) 2008- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "work_queue.h"
#include "work_queue_protocol.h"
#include "work_queue_internal.h"
#include "work_queue_catalog.h"

#include "int_sizes.h"
#include "link.h"
#include "link_auth.h"
#include "debug.h"
#include "stringtools.h"
#include "catalog_query.h"
#include "catalog_server.h"
#include "datagram.h"
#include "domain_name_cache.h"
#include "hash_table.h"
#include "itable.h"
#include "list.h"
#include "macros.h"
#include "process.h"
#include "username.h"
#include "create_dir.h"
#include "xxmalloc.h"
#include "load_average.h"
#include "buffer.h"
#include "link_nvpair.h"
#include "get_canonical_path.h"
#include "rmonitor_hooks.h"
#include "copy_stream.h"
#include "random_init.h"

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
#include <stdlib.h>
#include <limits.h>

#ifdef CCTOOLS_OPSYS_SUNOS
extern int setenv(const char *name, const char *value, int overwrite);
#endif

#define WORKER_STATE_INIT  0
#define WORKER_STATE_READY 1
#define WORKER_STATE_BUSY  2
#define WORKER_STATE_FULL  3
#define WORKER_STATE_NONE  4
#define WORKER_STATE_MAX   (WORKER_STATE_NONE+1)

static const char *work_queue_state_names[] = {"init","ready","busy","full","none"};

// FIXME: These internal error flags should be clearly distinguished
// from the task result codes given by work_queue_wait.

#define WORK_QUEUE_RESULT_UNSET 0
#define WORK_QUEUE_RESULT_INPUT_FAIL 1
#define WORK_QUEUE_RESULT_INPUT_MISSING 2
#define WORK_QUEUE_RESULT_FUNCTION_FAIL 4
#define WORK_QUEUE_RESULT_OUTPUT_FAIL 8
#define WORK_QUEUE_RESULT_OUTPUT_MISSING 16
#define WORK_QUEUE_RESULT_LINK_FAIL 32

#define WORK_QUEUE_FILE 0
#define WORK_QUEUE_BUFFER 1
#define WORK_QUEUE_REMOTECMD 2
#define WORK_QUEUE_FILE_PIECE 3

#define MIN_TIME_LIST_SIZE 20

#define TIME_SLOT_TASK_TRANSFER 0
#define TIME_SLOT_TASK_EXECUTE 1
#define TIME_SLOT_MASTER_IDLE 2
#define TIME_SLOT_APPLICATION 3

#define POOL_DECISION_ENFORCEMENT_INTERVAL_DEFAULT 60

#define WORK_QUEUE_APP_TIME_OUTLIER_MULTIPLIER 10

// work_queue_worker struct related
#define WORKER_VERSION_NAME_MAX 128
#define WORKER_OS_NAME_MAX 65
#define WORKER_ARCH_NAME_MAX 65
#define WORKER_ADDRPORT_MAX 32
#define WORKER_HASHKEY_MAX 32

#define RESOURCE_MONITOR_TASK_SUMMARY_NAME "cctools-work-queue-%d-resource-monitor-task-%d"

double wq_option_fast_abort_multiplier = -1.0;
int wq_option_scheduler = WORK_QUEUE_SCHEDULE_TIME;
int wq_minimum_transfer_timeout = 3;

struct work_queue {
	char *name;
	int port;
	int master_mode;
	int priority;

	char workingdir[PATH_MAX];

	struct link *master_link;
	struct link_info *poll_table;
	int poll_table_size;

	struct list    *ready_list;      // ready to be sent to a worker
	struct itable  *running_tasks;   // running on a worker
	struct itable  *finished_tasks;  // have output waiting on a worker
	struct list    *complete_list;   // completed and awaiting return to the master process

	struct hash_table *worker_table;
	struct itable  *worker_task_map;

	int workers_in_state[WORKER_STATE_MAX];

	INT64_T total_tasks_submitted;
	INT64_T total_tasks_complete;
	INT64_T total_workers_joined;
	INT64_T total_workers_removed;
	INT64_T total_bytes_sent;
	INT64_T total_bytes_received;
	INT64_T total_workers_connected;

	timestamp_t start_time;
	timestamp_t total_send_time;
	timestamp_t total_receive_time;
	timestamp_t total_execute_time;

	double fast_abort_multiplier;
	int worker_selection_algorithm;
	int task_ordering;

	timestamp_t time_last_task_start;
	timestamp_t idle_time;
	timestamp_t accumulated_idle_time;
	timestamp_t app_time;

	struct list *idle_times;
	double idle_percentage;
	struct task_statistics *task_statistics;

	int estimate_capacity_on;
	int capacity;
	int avg_capacity;

	char catalog_host[DOMAIN_NAME_MAX];
	int catalog_port;
	struct hash_table *workers_by_pool;

	FILE *logfile;
	timestamp_t keepalive_interval;
	timestamp_t keepalive_timeout;

	int monitor_mode;
	int monitor_fd;
	char *monitor_exe;

	const char *password;
};

struct work_queue_worker {
	int state;
	int async_tasks;  // Can this worker support asynchronous tasks? (aka is it a new worker?)
	char hostname[DOMAIN_NAME_MAX];
	char version[WORKER_VERSION_NAME_MAX];
	char os[WORKER_OS_NAME_MAX];
	char arch[WORKER_ARCH_NAME_MAX];
	char addrport[WORKER_ADDRPORT_MAX];
	char hashkey[WORKER_HASHKEY_MAX];
	int ncpus;
	int nslots;
	INT64_T memory_avail;
	INT64_T memory_total;
	INT64_T disk_avail;
	INT64_T disk_total;
	struct hash_table *current_files;
	struct link *link;
	struct itable *current_tasks;
	INT64_T total_tasks_complete;
	INT64_T total_bytes_transferred;
	timestamp_t total_task_time;
	timestamp_t total_transfer_time;
	timestamp_t start_time;
	char pool_name[WORK_QUEUE_POOL_NAME_MAX];
	char workspace[WORKER_WORKSPACE_NAME_MAX];
	timestamp_t last_msg_sent_time;
	timestamp_t last_msg_recv_time;
	timestamp_t keepalive_check_sent_time;
};

struct time_slot {
	timestamp_t start;
	timestamp_t duration;
	int type;
};

struct task_statistics {
	struct list *reports;
	timestamp_t total_time_transfer_data;
	timestamp_t total_time_execute_cmd;
	INT64_T total_capacity;
	INT64_T total_busy_workers;
};

struct task_report {
	timestamp_t time_transfer_data;
	timestamp_t time_execute_cmd;
	int busy_workers;
	int capacity;
};

struct work_queue_file {
	int type;		// WORK_QUEUE_FILE, WORK_QUEUE_BUFFER, WORK_QUEUE_REMOTECMD, WORK_QUEUE_FILE_PIECE
	int flags;		// WORK_QUEUE_CACHE or others in the future.
	int length;		// length of payload
	off_t start_byte;	// start byte offset for WORK_QUEUE_FILE_PIECE
	off_t end_byte;		// end byte offset for WORK_QUEUE_FILE_PIECE
	void *payload;		// name on master machine or buffer of data.
	char *remote_name;	// name on remote machine.
};

static int start_task_on_worker(struct work_queue *q, struct work_queue_worker *w);

static struct task_statistics *task_statistics_init();
static void add_time_slot(struct work_queue *q, timestamp_t start, timestamp_t duration, int type, timestamp_t * accumulated_time, struct list *time_list);
static void add_task_report(struct work_queue *q, struct work_queue_task *t);
static void update_app_time(struct work_queue *q, timestamp_t last_left_time, int last_left_status);

static int process_ready(struct work_queue *q, struct work_queue_worker *w, const char *line);
static int process_result(struct work_queue *q, struct work_queue_worker *w, const char *line);
static int process_queue_status(struct work_queue *q, struct work_queue_worker *w, const char *line, time_t stoptime);
static int process_worker_update(struct work_queue *q, struct work_queue_worker *w, const char *line); 

static int short_timeout = 5;

static timestamp_t link_poll_end; //tracks when we poll link; used to timeout unacknowledged keepalive checks

static int tolerable_transfer_rate_denominator = 10;
static long double minimum_allowed_transfer_rate = 100000;	// 100 KB/s



/******************************************************/
/********** work_queue internal functions *************/
/******************************************************/

static void log_worker_states(struct work_queue *q)
{
	struct work_queue_stats s;
	work_queue_get_stats(q, &s);
	
	fprintf(q->logfile, "%16" PRIu64 " %25" PRIu64 " ", timestamp_get(), s.start_time); // time
	fprintf(q->logfile, "%25d %25d %25d %25d", s.workers_init, s.workers_ready, s.workers_busy + s.workers_full, 0);
	fprintf(q->logfile, "%25d %25d %25d ", s.tasks_waiting, s.tasks_running, s.tasks_complete);
	fprintf(q->logfile, "%25d %25d %25d %25d ", s.total_tasks_dispatched, s.total_tasks_complete, s.total_workers_joined, s.total_workers_connected);
	fprintf(q->logfile, "%25d %25" PRId64 " %25" PRId64 " ", s.total_workers_removed, s.total_bytes_sent, s.total_bytes_received); 
	fprintf(q->logfile, "%25" PRIu64 " %25" PRIu64 " ", s.total_send_time, s.total_receive_time);
	fprintf(q->logfile, "%25f %25f ", s.efficiency, s.idle_percentage);
	fprintf(q->logfile, "%25d %25d ", s.capacity, s.avg_capacity);
	fprintf(q->logfile, "%25d %25d ", s.port, s.priority);
	fprintf(q->logfile, "\n");
}

static void change_worker_state(struct work_queue *q, struct work_queue_worker *w, int state)
{
	q->workers_in_state[w->state]--;
	w->state = state;
	q->workers_in_state[state]++;
	debug(D_WQ, "workers status -- total: %d, init: %d, ready: %d, busy: %d, full: %d.",
		hash_table_size(q->worker_table),
		q->workers_in_state[WORKER_STATE_INIT],
		q->workers_in_state[WORKER_STATE_READY],
		q->workers_in_state[WORKER_STATE_BUSY],
		q->workers_in_state[WORKER_STATE_FULL]);
	if(q->logfile) {
		log_worker_states(q);
	}
}

static void link_to_hash_key(struct link *link, char *key)
{
	sprintf(key, "0x%p", link);
}

/**
 * This function sends a message to the worker and records the time the message is 
 * successfully sent. This timestamp is used to determine when to send keepalive checks.
 */
static int send_worker_msg(struct work_queue_worker *w, const char *fmt, time_t stoptime, ...) 
{
	va_list va;
		
	va_start(va, stoptime);
	//call link_putvfstring to send the message on the link	
	int result = link_putvfstring(w->link, fmt, stoptime, va);	
	if (result > 0) 
		w->last_msg_sent_time = timestamp_get();		
	va_end(va);

	return result;  
}

/**
 * This function receives a message from worker and records the time a message is successfully 
 * received. This timestamp is used in keepalive timeout computations. 
 * Its return value is:
 * -1 : failure to read from link
 *  0 : a keepalive message was received and message is processed 
 *  1 : a non-keepalive message was received but NOT processed 
 */
static int recv_worker_msg(struct work_queue *q, struct work_queue_worker *w, char *line, size_t length, time_t stoptime) 
{
	//call link_readline to recieve message from the link	
	int result = link_readline(w->link, line, length, stoptime);
	
	if (result <= 0) {
		return -1;
	}
	
	w->last_msg_recv_time = timestamp_get();

	debug(D_WQ, "Received message from %s (%s): %s", w->hostname, w->addrport, line);
	
	// Check for status updates that can be consumed here.
	if(string_prefix_is(line, "alive")) {
		debug(D_WQ, "Received keepalive response from %s (%s)", w->hostname, w->addrport);
	} else if(string_prefix_is(line, "ready")) {
		process_ready(q, w, line);
	} else if (string_prefix_is(line,"result")) {
		process_result(q, w, line);
	} else if (string_prefix_is(line,"worker_status") || string_prefix_is(line, "queue_status") || string_prefix_is(line, "task_status")) {
		process_queue_status(q, w, line, stoptime);
	} else if (string_prefix_is(line, "update")) {
		process_worker_update(q, w, line);
	} else {
		// Message is not a status update: return it to the user.
		return 1;
	}
	
	return 0;
}

static double get_idle_percentage(struct work_queue *q)
{
	// Calculate the master's idle percentage for since the most recent
	// finished Nth task where N equals the number of workers.
	struct time_slot *ts;
	timestamp_t accumulated_idle_start;

	ts = (struct time_slot *) list_peek_head(q->idle_times);
	if(ts) {
		accumulated_idle_start = ts->start;
	} else {
		accumulated_idle_start = q->start_time;
	}

	return (long double) (q->accumulated_idle_time + q->idle_time) / (timestamp_get() - accumulated_idle_start);
}

static timestamp_t get_transfer_wait_time(struct work_queue *q, struct work_queue_task *t, INT64_T length)
{
	timestamp_t timeout;
	struct work_queue_worker *w;
	long double avg_queue_transfer_rate, avg_worker_transfer_rate, retry_transfer_rate, tolerable_transfer_rate;
	INT64_T total_tasks_complete, total_tasks_running, total_tasks_waiting, num_of_free_workers;

	w = itable_lookup(q->worker_task_map, t->taskid);

	if(w->total_transfer_time) {
		avg_worker_transfer_rate = (long double) w->total_bytes_transferred / w->total_transfer_time * 1000000;
	} else {
		avg_worker_transfer_rate = 0;
	}

	retry_transfer_rate = 0;
	num_of_free_workers = q->workers_in_state[WORKER_STATE_INIT] + q->workers_in_state[WORKER_STATE_READY];
	total_tasks_complete = q->total_tasks_complete;
	total_tasks_running = itable_size(q->running_tasks) + itable_size(q->finished_tasks);
	total_tasks_waiting = list_size(q->ready_list);
	if(total_tasks_complete > total_tasks_running && num_of_free_workers > total_tasks_waiting) {
		// The master has already tried most of the workers connected and has free workers for retrying slow workers
		if(t->total_bytes_transferred) {
			avg_queue_transfer_rate = (long double) (q->total_bytes_sent + q->total_bytes_received) / (q->total_send_time + q->total_receive_time) * 1000000;
			retry_transfer_rate = (long double) length / t->total_bytes_transferred * avg_queue_transfer_rate;
		}
	}

	tolerable_transfer_rate = MAX(avg_worker_transfer_rate / tolerable_transfer_rate_denominator, retry_transfer_rate);
	tolerable_transfer_rate = MAX(minimum_allowed_transfer_rate, tolerable_transfer_rate);

	timeout = MAX(wq_minimum_transfer_timeout, length / tolerable_transfer_rate);	// try at least wq_minimum_transfer_timeout seconds

	debug(D_WQ, "%s (%s) will try up to %lld seconds for the transfer of this %.3Lf MB file.", w->hostname, w->addrport, timeout, (long double) length / 1000000);
	return timeout;
}

static void update_catalog(struct work_queue *q, int now)
{
	struct work_queue_stats s;
	work_queue_get_stats(q, &s);
	char * worker_summary = work_queue_get_worker_summary(q);
	advertise_master_to_catalog(q->catalog_host, q->catalog_port, q->name, &s, worker_summary, now);
	free(worker_summary);
}

static void cleanup_worker(struct work_queue *q, struct work_queue_worker *w)
{
	char *key, *value;
	struct work_queue_task *t;
	UINT64_T taskid;

	if(!q || !w) return;
	
	hash_table_firstkey(w->current_files);
	while(hash_table_nextkey(w->current_files, &key, (void **) &value)) {
		hash_table_remove(w->current_files, key);
		free(value);
	}
	hash_table_delete(w->current_files);

	itable_firstkey(w->current_tasks);
	while(itable_nextkey(w->current_tasks, &taskid, (void **)&t)) {
		if(t->result & WORK_QUEUE_RESULT_INPUT_MISSING || t->result & WORK_QUEUE_RESULT_OUTPUT_MISSING || t->result & WORK_QUEUE_RESULT_FUNCTION_FAIL) {
			list_push_head(q->complete_list, t);
		} else {
			t->result = WORK_QUEUE_RESULT_UNSET;
			t->total_bytes_transferred = 0;
			t->total_transfer_time = 0;
			t->cmd_execution_time = 0;
			list_push_head(q->ready_list, t);
		}
		itable_remove(q->running_tasks, t->taskid);
		itable_remove(q->finished_tasks, t->taskid);
		itable_remove(q->worker_task_map, t->taskid);
	}
	itable_clear(w->current_tasks);
}

static void remove_worker(struct work_queue *q, struct work_queue_worker *w)
{
	if(!q || !w) return;

	if((w->pool_name)[0]) {
		debug(D_WQ, "worker %s (%s) from pool \"%s\" removed", w->hostname, w->addrport, w->pool_name);
	} else {
		debug(D_WQ, "worker %s (%s) removed", w->hostname, w->addrport);
	}

	q->total_workers_removed++;

	cleanup_worker(q, w);

	hash_table_remove(q->worker_table, w->hashkey);
	
	if((w->pool_name)[0]) {
		struct pool_info *pi;
		pi = hash_table_lookup(q->workers_by_pool, w->pool_name);
		if(!pi) {
			debug(D_WQ, "Error: removing worker from pool \"%s\" but failed to find out how many workers are from that pool.", w->pool_name);
		} else {
			if(pi->count == 0) {
				debug(D_WQ, "Error: removing worker from pool \"%s\" but record indicates no workers from that pool are connected.", w->pool_name);
			} else {
				pi->count -= 1;
			}
		}
	}

	change_worker_state(q, w, WORKER_STATE_NONE);
	if(w->link)
		link_close(w->link);
	free(w);

	debug(D_WQ, "%d workers are connected in total now", hash_table_size(q->worker_table));
}

static int release_worker(struct work_queue *q, struct work_queue_worker *w)
{
	if(!w) return 0;

	send_worker_msg(w, "%s\n", time(0) + short_timeout, "release");
	remove_worker(q, w);
	return 1;
}

static int remove_workers_from_pool(struct work_queue *q, const char *pool_name, int workers_to_release) {
	struct work_queue_worker *w;
	char *key;
	int i = 0;

	if(!q)
		return -1;

	// send worker the "exit" msg
	hash_table_firstkey(q->worker_table);
	while(i < workers_to_release && hash_table_nextkey(q->worker_table, &key, (void **) &w)) {
		if(!strncmp(w->pool_name, pool_name, WORK_QUEUE_POOL_NAME_MAX)) {
			release_worker(q, w);
			i++;
		}
	}

	return i;
}

static void enforce_pool_decisions(struct work_queue *q) {
	struct list *decisions;

	debug(D_WQ, "Get pool decision from catalog server.\n");
	decisions = list_create();
	if(!decisions) {
		debug(D_WQ, "Failed to create list to store worker pool decisions!\n");
		return;
	}
	if(!get_pool_decisions_from_catalog(q->catalog_host, q->catalog_port, q->name, decisions)) {
		debug(D_WQ, "Failed to receive pool decisions from the catalog server(%s@%d)!\n", q->catalog_host, q->catalog_port);
		return;
	}

	if(!list_size(decisions)) {
		return;
	}

	struct pool_info *d;
	list_first_item(decisions);
	while((d = (struct pool_info *)list_next_item(decisions))) {
		struct pool_info *pi;
		pi = hash_table_lookup(q->workers_by_pool, d->name);
		if(pi) {
			debug(D_WQ, "Workers from pool %s: %d; Pool decison: %d\n", pi->name, pi->count, d->count);
			int workers_to_release = pi->count - d->count;
			if(workers_to_release > 0) {
				int k = remove_workers_from_pool(q, pi->name, workers_to_release);
				debug(D_WQ, "%d worker(s) has been rejected to enforce the pool decison.\n", k);
			}
		} 
	}

	list_free(decisions);
	list_delete(decisions);
}

static int add_worker(struct work_queue *q)
{
	struct link *link;
	struct work_queue_worker *w;
	char addr[LINK_ADDRESS_MAX];
	int port;

	link = link_accept(q->master_link, time(0) + short_timeout);
	if(!link) return 0;

	link_keepalive(link, 1);
	link_tune(link, LINK_TUNE_INTERACTIVE);

	if(!link_address_remote(link, addr, &port)) {
		link_close(link);
		return 0;
	}

	debug(D_WQ,"worker %s:%d connected",addr,port);

	if(q->password) {
		debug(D_WQ,"worker %s:%d authenticating",addr,port);
		if(!link_auth_password(link,q->password,time(0)+short_timeout)) {
			debug(D_WQ|D_NOTICE,"worker %s:%d presented the wrong password",addr,port);
			link_close(link);
			return 0;
		}
	}

	w = malloc(sizeof(*w));
	memset(w, 0, sizeof(*w));
	w->state = WORKER_STATE_NONE;
	w->link = link;
	w->current_files = hash_table_create(0, 0);
	w->current_tasks = itable_create(0);
	w->start_time = timestamp_get();
	link_to_hash_key(link, w->hashkey);
	sprintf(w->addrport, "%s:%d", addr, port);
	hash_table_insert(q->worker_table, w->hashkey, w);
	change_worker_state(q, w, WORKER_STATE_INIT);

	debug(D_WQ, "%d workers are connected in total now", hash_table_size(q->worker_table));

	q->total_workers_joined++;

	return 1;
}

/**
 * This function implements the "rget %s" protocol.
 * It reads a streamed item from a worker. For the stream format, please refer
 * to the stream_output_item function in worker.c
 */
static int get_output_item(char *remote_name, char *local_name, struct work_queue *q, struct work_queue_worker *w, struct work_queue_task *t, struct hash_table *received_items, INT64_T * total_bytes)
{
	char line[WORK_QUEUE_LINE_MAX];
	int fd;
	INT64_T actual, length;
	time_t stoptime;
	char type[256];
	char tmp_remote_name[WORK_QUEUE_LINE_MAX], tmp_local_name[WORK_QUEUE_LINE_MAX];
	char *cur_pos, *tmp_pos;
	int remote_name_len;
	int local_name_len;
	int recv_msg_result;

	if(hash_table_lookup(received_items, local_name))
		return 1;

	debug(D_WQ, "%s (%s) sending back %s to %s", w->hostname, w->addrport, remote_name, local_name);
	send_worker_msg(w, "rget %s\n", time(0) + short_timeout, remote_name);

	strcpy(tmp_local_name, local_name);
	remote_name_len = strlen(remote_name);
	local_name_len = strlen(local_name);

	while(1) {
		//call recv_worker_msg until it returns non-zero which indicates failure or a non-keepalive message is left to consume
		do { 			
			recv_msg_result = recv_worker_msg(q, w, line, sizeof(line), time(0) + short_timeout);
		} while (recv_msg_result == 0);
		if (recv_msg_result < 0) {
			goto link_failure;
		}
		
		if(sscanf(line, "%s %s %" SCNd64, type, tmp_remote_name, &length) == 3) {
			tmp_local_name[local_name_len] = '\0';
			strcat(tmp_local_name, &(tmp_remote_name[remote_name_len]));

			if(strncmp(type, "dir", 3) == 0) {
				debug(D_WQ, "%s (%s) dir %s", w->hostname, w->addrport, tmp_local_name);
				if(!create_dir(tmp_local_name, 0700)) {
					debug(D_WQ, "Cannot create directory - %s (%s)", tmp_local_name, strerror(errno));
					goto failure;
				}
				hash_table_insert(received_items, tmp_local_name, xxstrdup(tmp_local_name));
			} else if(strncmp(type, "file", 4) == 0) {
				// actually place the file
				if(length >= 0) {
					// create dirs in the filename path if needed
					cur_pos = tmp_local_name;
					if(!strncmp(cur_pos, "./", 2)) {
						cur_pos += 2;
					}

					tmp_pos = strrchr(cur_pos, '/');
					while(tmp_pos) {
						*tmp_pos = '\0';
						if(!create_dir(cur_pos, 0700)) {
							debug(D_WQ, "Could not create directory - %s (%s)", cur_pos, strerror(errno));
							goto failure;
						}
						*tmp_pos = '/';

						cur_pos = tmp_pos + 1;
						tmp_pos = strrchr(cur_pos, '/');
					}

					// get the remote file and place it
					debug(D_WQ, "Receiving file %s (size: %lld bytes) from %s (%s) ...", tmp_local_name, length, w->addrport, w->hostname);
					fd = open(tmp_local_name, O_WRONLY | O_TRUNC | O_CREAT, 0700);
					if(fd < 0) {
						debug(D_NOTICE, "Cannot open file %s for writing: %s", tmp_local_name, strerror(errno));
						goto failure;
					}
					stoptime = time(0) + get_transfer_wait_time(q, t, length);
					actual = link_stream_to_fd(w->link, fd, length, stoptime);
					close(fd);
					if(actual != length) {
						debug(D_WQ, "Received item size (%lld) does not match the expected size - %lld bytes.", actual, length);
						unlink(local_name);
						goto failure;
					}
					*total_bytes += length;

					hash_table_insert(received_items, tmp_local_name, xxstrdup(tmp_local_name));
				} else {
					debug(D_NOTICE, "%s on %s (%s) has invalid length: %lld", remote_name, w->addrport, w->hostname, length);
					goto failure;
				}
			} else if(strncmp(type, "missing", 7) == 0) {
				// now length holds the errno
				debug(D_WQ, "Failed to retrieve %s from %s (%s): %s", remote_name, w->addrport, w->hostname, strerror(length));
				t->result |= WORK_QUEUE_RESULT_OUTPUT_MISSING;
			} else {
				debug(D_WQ, "Invalid output item type - %s\n", type);
				goto failure;
			}
		} else if(sscanf(line, "%s", type) == 1) {
			if(strncmp(type, "end", 3) == 0) {
				break;
			} else {
				debug(D_WQ, "Invalid rget line - %s\n", line);
				goto failure;
			}
		} else {
			debug(D_WQ, "Invalid streaming output line - %s\n", line);
			goto failure;
		}
	}

	return 1;

      link_failure:
	debug(D_WQ, "Link to %s (%s) failed.\n", w->addrport, w->hostname);
	t->result |= WORK_QUEUE_RESULT_LINK_FAIL;

      failure:
	debug(D_WQ, "%s (%s) failed to return %s to %s", w->addrport, w->hostname, remote_name, local_name);
	t->result |= WORK_QUEUE_RESULT_OUTPUT_FAIL;
	return 0;
}

/**
 * Comparison function for sorting by file/dir names in the output files list
 * of a task
 */
static int filename_comparator(const void *a, const void *b)
{
	int rv;
	rv = strcmp(*(char *const *) a, *(char *const *) b);
	return rv > 0 ? -1 : 1;
}

static int get_output_files(struct work_queue_task *t, struct work_queue_worker *w, struct work_queue *q)
{
	struct work_queue_file *tf;

	// This may be where I can trigger output file cache
	// Added by Anthony Canino
	// Attempting to add output cacheing
	struct stat local_info;
	struct stat *remote_info;
	char *hash_name;
	char *key, *value;
	struct hash_table *received_items;
	INT64_T total_bytes = 0;
	int recv_msg_result = 0;

	timestamp_t open_time = 0;
	timestamp_t close_time = 0;
	timestamp_t sum_time;

	// Start transfer ...
	received_items = hash_table_create(0, 0);

	//  Sorting list will make sure that upper level dirs sit before their
	//  contents(files/dirs) in the output files list. So, when we emit rget
	//  command, we would first encounter top level dirs. Also, we would record
	//  every received files/dirs within those top level dirs. If any file/dir
	//  in those top level dirs appears later in the output files list, we
	//  won't transfer it again.
	list_sort(t->output_files, filename_comparator);

	if(t->output_files) {
		list_first_item(t->output_files);
		while((tf = list_next_item(t->output_files))) {
			if(tf->flags & WORK_QUEUE_THIRDPUT) {

				debug(D_WQ, "thirdputting %s as %s", tf->remote_name, tf->payload);

				if(!strcmp(tf->remote_name, tf->payload)) {
					debug(D_WQ, "output file %s already on shared filesystem", tf->remote_name);
					tf->flags |= WORK_QUEUE_PREEXIST;
				} else {
					char thirdput_result[WORK_QUEUE_LINE_MAX];
					debug(D_WQ, "putting %s from %s (%s) to shared filesystem from %s", tf->remote_name, w->hostname, w->addrport, tf->payload);
					open_time = timestamp_get();
					send_worker_msg(w, "thirdput %d %s %s\n", time(0) + short_timeout, WORK_QUEUE_FS_PATH, tf->remote_name, tf->payload);
					//call recv_worker_msg until it returns non-zero which indicates failure or a non-keepalive message is left to consume
					do { 
						recv_msg_result = recv_worker_msg(q, w, thirdput_result, WORK_QUEUE_LINE_MAX, time(0) + short_timeout);
					} while (recv_msg_result == 0);
					if (recv_msg_result < 0) {
						return 0;
					}
					close_time = timestamp_get();
					sum_time += (close_time - open_time);
				}
			} else if(tf->type == WORK_QUEUE_REMOTECMD) {
				char thirdput_result[WORK_QUEUE_LINE_MAX];
				debug(D_WQ, "putting %s from %s (%s) to remote filesystem using %s", tf->remote_name, w->hostname, w->addrport, tf->payload);
				open_time = timestamp_get();
				send_worker_msg(w, "thirdput %d %s %s\n", time(0) + short_timeout, WORK_QUEUE_FS_CMD, tf->remote_name, tf->payload);
				//call recv_worker_msg until it returns non-zero which indicates failure or a non-keepalive message is left to consume
				do { 
					recv_msg_result = recv_worker_msg(q, w, thirdput_result, WORK_QUEUE_LINE_MAX, time(0) + short_timeout);
				} while (recv_msg_result == 0);
				if (recv_msg_result < 0) {
					return 0;	
				}
				close_time = timestamp_get();
				sum_time += (close_time - open_time);
			} else {
				open_time = timestamp_get();
				get_output_item(tf->remote_name, tf->payload, q, w, t, received_items, &total_bytes);
				close_time = timestamp_get();
				if(t->result & WORK_QUEUE_RESULT_OUTPUT_FAIL) {
					return 0;
				}
				if(total_bytes) {
					sum_time = close_time - open_time;
					q->total_bytes_received += total_bytes;
					q->total_receive_time += sum_time;
					t->total_bytes_transferred += total_bytes;
					t->total_transfer_time += sum_time;
					w->total_bytes_transferred += total_bytes;
					w->total_transfer_time += sum_time;
					debug(D_WQ, "Got %d bytes from %s (%s) in %.03lfs (%.02lfs Mbps) average %.02lfs Mbps", total_bytes, w->hostname, w->addrport, sum_time / 1000000.0, ((8.0 * total_bytes) / sum_time),
					      (8.0 * w->total_bytes_transferred) / w->total_transfer_time);
				}
				total_bytes = 0;
			}

			// Add the output item to the hash table if its cacheable
			if(tf->flags & WORK_QUEUE_CACHE) {
				if(stat(tf->payload, &local_info) < 0) {
					unlink(tf->payload);
					if(t->result & WORK_QUEUE_RESULT_OUTPUT_MISSING)
						continue;
					return 0;
				}

				hash_name = (char *) malloc((strlen(tf->payload) + strlen(tf->remote_name) + 2) * sizeof(char));
				sprintf(hash_name, "%s-%s", (char *) tf->payload, tf->remote_name);

				remote_info = malloc(sizeof(*remote_info));
				memcpy(remote_info, &local_info, sizeof(local_info));
				hash_table_insert(w->current_files, hash_name, remote_info);
				free(hash_name);
			}

		}

	}
	// destroy received files hash table for this task
	hash_table_firstkey(received_items);
	while(hash_table_nextkey(received_items, &key, (void **) &value)) {
		free(value);
	}
	hash_table_delete(received_items);

	return 1;
}

// Sends "unlink file" for every file in the list except those that match one or more of the "except_flags"
static void delete_worker_files(struct work_queue_worker *w, struct list *files, int except_flags) {
	struct work_queue_file *tf;
	
	if(!files) return;

	list_first_item(files);
	while((tf = list_next_item(files))) {
		if(!(tf->flags & except_flags)) {
			debug(D_WQ, "%s (%s) unlink %s", w->hostname, w->addrport, tf->remote_name);
			send_worker_msg(w, "unlink %s\n", time(0) + short_timeout, tf->remote_name);
		}
	}
}


static void delete_uncacheable_files(struct work_queue_task *t, struct work_queue_worker *w)
{
	delete_worker_files(w, t->input_files, WORK_QUEUE_CACHE | WORK_QUEUE_PREEXIST);
	delete_worker_files(w, t->output_files, WORK_QUEUE_CACHE | WORK_QUEUE_PREEXIST);
}

void work_queue_monitor_append_report(struct work_queue *q, struct work_queue_task *t)
{
	struct flock lock;
	FILE        *fsummary;
	char        *summary = string_format(RESOURCE_MONITOR_TASK_SUMMARY_NAME, getpid(), t->taskid);
	char        *msg; 

	lock.l_type   = F_WRLCK;
	lock.l_start  = 0;
	lock.l_whence = SEEK_SET;
	lock.l_len    = 0;

	fcntl(q->monitor_fd, F_SETLKW, &lock);
	
	msg = string_format("Work Queue pid: %d Task: %d\n", getpid(), t->taskid);
	write(q->monitor_fd, msg, strlen(msg));
	free(msg);

	if( (fsummary = fopen(summary, "r")) == NULL )
	{
		msg = string_format("Summary for task %d:%d is not available.\n", getpid(), t->taskid);
		write(q->monitor_fd, msg, strlen(msg));
		free(msg);
	}
	else
	{
		copy_stream_to_fd(fsummary, q->monitor_fd);
		fclose(fsummary);	
	}

	write(q->monitor_fd, "\n\n", 2);

	lock.l_type   = F_ULOCK;
	fcntl(q->monitor_fd, F_SETLK, &lock);

	if(unlink(summary) != 0)
		debug(D_NOTICE, "Summary %s could not be removed.\n", summary);
}

static int fetch_output_from_worker(struct work_queue *q, struct work_queue_worker *w, int taskid)
{
	struct work_queue_task *t;

	t = itable_lookup(w->current_tasks, taskid);
	if(!t)
		goto failure;

	// Receiving output ...
	t->time_receive_output_start = timestamp_get();
	if(!get_output_files(t, w, q)) {
		free(t->output);
		t->output = 0;
		goto failure;
	}
	t->time_receive_output_finish = timestamp_get();

	delete_uncacheable_files(t, w);

	// At this point, a task is completed.

/*	if(itable_size(w->current_tasks) == w->nslots) {
		list_remove(q->busy_workers, w);
		list_push_tail(q->ready_workers, w);
	}
*/
	itable_remove(q->finished_tasks, t->taskid);
	list_push_head(q->complete_list, t);
	itable_remove(w->current_tasks, t->taskid);
	itable_remove(q->worker_task_map, t->taskid);
	t->time_task_finish = timestamp_get();

	/* if q is monitoring, append the task summary to the single
	 * queue summary, and delete the task summary. */
	if(q->monitor_mode)
		work_queue_monitor_append_report(q, t);


	// Record statistics information for capacity estimation
	if(q->estimate_capacity_on) {
		add_task_report(q, t);
	}
	
		
	// Change worker state and do some performance statistics
	if(!itable_size(w->current_tasks)) {
		change_worker_state(q, w, WORKER_STATE_READY);
	} else if(itable_size(w->current_tasks) < w->nslots) {
		change_worker_state(q, w, WORKER_STATE_BUSY);
	} else {
		change_worker_state(q, w, WORKER_STATE_FULL);
	}


	q->total_tasks_complete++;
	w->total_tasks_complete++;

	w->total_task_time += t->cmd_execution_time;

	debug(D_WQ, "%s (%s) done in %.02lfs total tasks %d average %.02lfs", w->hostname, w->addrport, (t->time_receive_output_finish - t->time_send_input_start) / 1000000.0, w->total_tasks_complete,
	      w->total_task_time / w->total_tasks_complete / 1000000.0);
	return 1;

      failure:
	debug(D_WQ, "Failed to receive output from worker %s (%s).", w->hostname, w->addrport);
	remove_worker(q, w);
	return 0;
}

static int field_set(const char *field) {
	if(strncmp(field, WORK_QUEUE_PROTOCOL_BLANK_FIELD, strlen(WORK_QUEUE_PROTOCOL_BLANK_FIELD) + 1) == 0) {
		return 0;
	}
	return 1;
}

static int process_ready(struct work_queue *q, struct work_queue_worker *w, const char *line) {
	if(!q || !w || !line) return 0;

	//Format: hostname, ncpus, memory_avail, memory_total, disk_avail, disk_total, proj_name, pool_name, os, arch, workspace, version
	char items[12][WORK_QUEUE_PROTOCOL_FIELD_MAX];
	int n = sscanf(line, "ready %s %s %s %s %s %s %s %s %s %s %s %s", items[0], items[1], items[2], items[3], items[4], items[5], items[6], items[7], items[8], items[9], items[10], items[11]);

	if(n < 6) {
		debug(D_WQ, "Invalid message from worker %s (%s): %s", w->hostname, w->addrport, line);
		return 0;
	}

	// Copy basic fields 
	strncpy(w->hostname, items[0], DOMAIN_NAME_MAX);
	w->ncpus = atoi(items[1]);
	w->nslots = 0;
	w->memory_avail = atoll(items[2]);
	w->memory_total = atoll(items[3]);
	w->disk_avail = atoll(items[4]);
	w->disk_total = atoll(items[5]);

	if(n >= 7 && field_set(items[6])) { // intended project name
		if(q->name) {
			if(strncmp(q->name, items[6], WORK_QUEUE_NAME_MAX) != 0) {
				goto reject;
			}
		} else {
			goto reject;
		}
	}

	if(n >= 8 && field_set(items[7])) { // worker pool name
		strncpy(w->pool_name, items[7], WORK_QUEUE_POOL_NAME_MAX);
	} else {
		strcpy(w->pool_name, "unmanaged");
	}

	struct pool_info *pi;
	pi = hash_table_lookup(q->workers_by_pool, w->pool_name);
	if(!pi) {
		pi = xxmalloc(sizeof(*pi));
		strncpy(pi->name, w->pool_name, WORK_QUEUE_POOL_NAME_MAX);
		pi->count = 1;
		hash_table_insert(q->workers_by_pool, w->pool_name, pi);
	} else {
		pi->count += 1;
	}

	if(n >= 9 && field_set(items[8])) { // operating system
		strncpy(w->os, items[8], WORKER_OS_NAME_MAX);
	} else {
		strcpy(w->os, "unknown");
	}

	if(n >= 10 && field_set(items[9])) { // architecture
		strncpy(w->arch, items[9], WORKER_ARCH_NAME_MAX);
	} else {
		strcpy(w->arch, "unknown");
	}

	if(n >= 11 && field_set(items[10])) { // workspace
		strncpy(w->workspace, items[10], WORKER_WORKSPACE_NAME_MAX);
	} else {
		strcpy(w->workspace, "unknown");
	}
	
	if(n >= 12 && field_set(items[11])) { // version
		strncpy(w->version, items[11], WORKER_VERSION_NAME_MAX);
		w->async_tasks = 1;
	} else {
		strncpy(w->version, "unknown", 8);
		w->async_tasks = 0;
		w->nslots = 1;
	}

	if(w->state == WORKER_STATE_INIT) {
		change_worker_state(q, w, WORKER_STATE_READY);
		//list_push_tail(q->ready_workers, w);
		q->total_workers_connected++;
		debug(D_WQ, "%s (%s) running CCTools version %s on %s (operating system) with architecture %s is ready", w->hostname, w->addrport, w->version, w->os, w->arch);
	}
	
	if(strcmp(CCTOOLS_VERSION, w->version)) {
		debug(D_DEBUG, "Warning: potential worker version mismatch: worker %s (%s) is version %s, and master is version %s", w->hostname, w->addrport, w->version, CCTOOLS_VERSION);
	}
	

	return 1;

reject:
	debug(D_NOTICE, "%s (%s) is rejected: the worker's intended project name (%s) does not match the master's (%s).", w->hostname, w->addrport, items[7], q->name);
	return 0;
}

static int process_result(struct work_queue *q, struct work_queue_worker *w, const char *line) {

	if(!q || !w || !line) return 0; 

	int result;
	UINT64_T taskid;
	INT64_T output_length;
	timestamp_t execution_time;
	struct work_queue_task *t;
	int actual;
	timestamp_t stoptime, observed_execution_time;

	//Format: result, output length, execution time, taskid
	char items[3][WORK_QUEUE_PROTOCOL_FIELD_MAX];
	int n = sscanf(line, "result %s %s %s %" SCNd64, items[0], items[1], items[2], &taskid);


	if(n < 2) {
		debug(D_WQ, "Invalid message from worker %s (%s): %s", w->hostname, w->addrport, line);
		return 0;
	}
	
	result = atoi(items[0]);
	output_length = atoll(items[1]);
	
	if(n < 4 && !w->async_tasks) {
		itable_firstkey(w->current_tasks);
		itable_nextkey(w->current_tasks, &taskid, (void **)&t);
	} else {
		t = itable_lookup(w->current_tasks, taskid);
	}
	
	observed_execution_time = timestamp_get() - t->time_execute_cmd_start;

	if(n >= 3) {
		execution_time = atoll(items[2]);
		t->cmd_execution_time = observed_execution_time > execution_time ? execution_time : observed_execution_time;
	} else {
		t->cmd_execution_time = observed_execution_time;
	}

	t->output = malloc(output_length + 1);
	if(output_length > 0) {
		debug(D_WQ, "Receiving stdout of task %d (size: %lld bytes) from %s (%s) ...", taskid, output_length, w->addrport, w->hostname);
		stoptime = time(0) + get_transfer_wait_time(q, t, (INT64_T) output_length);
		actual = link_read(w->link, t->output, output_length, stoptime);
		if(actual != output_length) {
			debug(D_WQ, "Failure: actual received stdout size (%lld bytes) is different from expected (%lld bytes).", actual, output_length);
			free(t->output);
			t->output = 0;
			return 0;
		}
		debug(D_WQ, "Got %d bytes from %s (%s)", actual, w->hostname, w->addrport);
	} else {
		actual = 0;
	}
	t->output[actual] = 0;

	t->return_status = result;
	if(t->return_status != 0)
		t->result |= WORK_QUEUE_RESULT_FUNCTION_FAIL;

	t->time_execute_cmd_finish = t->time_execute_cmd_start + t->cmd_execution_time;
	q->total_execute_time += t->cmd_execution_time;
	itable_remove(q->running_tasks, taskid);
	itable_insert(q->finished_tasks, taskid, (void*)t);
	
	return 1;
}

static struct nvpair * queue_to_nvpair( struct work_queue *q )
{
	struct nvpair *nv = nvpair_create();
	if(!nv) return 0;

	struct work_queue_stats info;
	work_queue_get_stats(q,&info);

	nvpair_insert_integer(nv,"port",info.port);
	if(q->name) nvpair_insert_string(nv,"project",q->name);
	nvpair_insert_string(nv,"working_dir",q->workingdir);
	nvpair_insert_integer(nv,"priority",info.priority);
	nvpair_insert_integer(nv,"workers",info.workers_ready+info.workers_busy+info.workers_full);
	nvpair_insert_integer(nv,"workers_init",info.workers_init);
	nvpair_insert_integer(nv,"workers_ready",info.workers_ready);
	nvpair_insert_integer(nv,"workers_busy",info.workers_busy);
	nvpair_insert_integer(nv,"workers_full",info.workers_full);
	nvpair_insert_integer(nv,"tasks_running",info.tasks_running);
	nvpair_insert_integer(nv,"tasks_waiting",info.tasks_waiting);
	nvpair_insert_integer(nv,"tasks_complete",info.total_tasks_complete);
	nvpair_insert_integer(nv,"total_tasks_dispatched",info.total_tasks_dispatched);
	nvpair_insert_integer(nv,"total_tasks_complete",info.total_tasks_complete);
	nvpair_insert_integer(nv,"total_workers_joined",info.total_workers_joined);
	nvpair_insert_integer(nv,"total_workers_removed",info.total_workers_removed);
	nvpair_insert_integer(nv,"total_bytes_sent",info.total_bytes_sent);
	nvpair_insert_integer(nv,"total_bytes_received",info.total_bytes_received);
	nvpair_insert_integer(nv,"start_time",info.start_time);
	nvpair_insert_integer(nv,"total_send_time",info.total_send_time);
	nvpair_insert_integer(nv,"total_receive_time",info.total_receive_time);

	return nv;
}

struct nvpair * worker_to_nvpair( struct work_queue_worker *w )
{
	struct nvpair *nv = nvpair_create();
	if(!nv) return 0;

	nvpair_insert_string(nv,"state",work_queue_state_names[w->state]);
	nvpair_insert_string(nv,"hostname",w->hostname);
	nvpair_insert_string(nv,"os",w->os);
	nvpair_insert_string(nv,"arch",w->arch);
	nvpair_insert_string(nv,"working_dir",w->workspace);
	nvpair_insert_string(nv,"address_port",w->addrport);
	nvpair_insert_integer(nv,"ncpus",w->ncpus);
	nvpair_insert_integer(nv,"memory_avail",w->memory_avail);
	nvpair_insert_integer(nv,"memory_total",w->memory_total);
	nvpair_insert_integer(nv,"disk_avail",w->disk_avail);
	nvpair_insert_integer(nv,"disk_total",w->disk_total);
	nvpair_insert_integer(nv,"total_tasks_complete",w->total_tasks_complete);
	nvpair_insert_integer(nv,"total_bytes_transferred",w->total_bytes_transferred);
	nvpair_insert_integer(nv,"total_transfer_time",w->total_transfer_time);

	nvpair_insert_integer(nv,"start_time",w->start_time);
	nvpair_insert_integer(nv,"current_time",timestamp_get()); 

	struct work_queue_task *t;
	UINT64_T taskid;
	int n = 0;

	itable_firstkey(w->current_tasks);
	while(itable_nextkey(w->current_tasks, &taskid, (void**)&t)) {
		char task_string[WORK_QUEUE_LINE_MAX];

		sprintf(task_string, "current_task_%03d_id", n);
		nvpair_insert_integer(nv,task_string,t->taskid);

		sprintf(task_string, "current_task_%03d_command", n);
		nvpair_insert_string(nv,task_string,t->command_line);
		n++;
	}

	return nv;
}

struct nvpair * task_to_nvpair( struct work_queue_task *t, const char *state, const char *host )
{
	struct nvpair *nv = nvpair_create();
	if(!nv) return 0;

	nvpair_insert_integer(nv,"taskid",t->taskid);
	nvpair_insert_string(nv,"state",state);
	if(t->tag) nvpair_insert_string(nv,"tag",t->tag);
	nvpair_insert_string(nv,"command",t->command_line);
	if(host) nvpair_insert_string(nv,"host",host);

	return nv;
}

static int process_queue_status( struct work_queue *q, struct work_queue_worker *target, const char *line, time_t stoptime )
{
	char request[WORK_QUEUE_LINE_MAX];
	struct link *l = target->link;
	
	if(!sscanf(line, "%[^_]_status", request) == 1) {
		return -1;
	}
	
	if(!strcmp(request, "queue")) {
		struct nvpair *nv = queue_to_nvpair( q );
		if(nv) {
			link_nvpair_write(l,nv,stoptime);
			nvpair_delete(nv);
		}
	} else if(!strcmp(request, "task")) {
		struct work_queue_task *t;
		struct work_queue_worker *w;
		struct nvpair *nv;
		UINT64_T key;

		itable_firstkey(q->running_tasks);
		while(itable_nextkey(q->running_tasks,&key,(void**)&t)) {
			w = itable_lookup(q->worker_task_map, t->taskid);
			if(w) {
				nv = task_to_nvpair(t,"running",w->hostname);
				if(nv) {
					// Include detailed information on where the task is running:
					// address and port, workspace
					nvpair_insert_string(nv, "address_port", w->addrport);
					nvpair_insert_string(nv, "working_dir", w->workspace);

					// Timestamps on running task related events 
					nvpair_insert_integer(nv, "submit_to_queue_time", t->time_task_submit); 
					nvpair_insert_integer(nv, "send_input_start_time", t->time_send_input_start);
					nvpair_insert_integer(nv, "execute_cmd_start_time", t->time_execute_cmd_start);
					nvpair_insert_integer(nv, "current_time", timestamp_get()); 

					link_nvpair_write(l,nv,stoptime);
					nvpair_delete(nv);
				}
			}
		}

		list_first_item(q->ready_list);
		while((t = list_next_item(q->ready_list))) {
			nv = task_to_nvpair(t,"waiting",0);
			if(nv) {
				link_nvpair_write(l,nv,stoptime);
				nvpair_delete(nv);
			}
		}

		list_first_item(q->complete_list);
		while((t = list_next_item(q->complete_list))) {
			nv = task_to_nvpair(t,"complete",0);
			if(nv) {
				link_nvpair_write(l,nv,stoptime);
				nvpair_delete(nv);
			}
		}
	} else if(!strcmp(request, "worker")) {
		struct work_queue_worker *w;
		struct nvpair *nv;
		char *key;

		hash_table_firstkey(q->worker_table);
		while(hash_table_nextkey(q->worker_table,&key,(void**)&w)) {
			if(w->state==WORKER_STATE_INIT) continue;
			nv = worker_to_nvpair(w);
			if(nv) {
				link_nvpair_write(l,nv,stoptime);
				nvpair_delete(nv);
			}
		}
	}

	return 0;
}

static int process_worker_update(struct work_queue *q, struct work_queue_worker *w, const char *line)
{
	char category[WORK_QUEUE_LINE_MAX];
	char arg[WORK_QUEUE_LINE_MAX];
	
	if(sscanf(line, "update %s %s", category, arg) != 2) {
		return 0;
	}
	
	if(!strcmp(category, "slots")) {
		
		w->nslots = atoi(arg);
		
		if(w->nslots > itable_size(w->current_tasks)) {
/*			if(list_remove(q->busy_workers, w)) {
				list_push_tail(q->ready_workers, w);
			}
*/			change_worker_state(q, w, itable_size(w->current_tasks)?WORKER_STATE_BUSY:WORKER_STATE_READY);
		} else if(w->nslots <= itable_size(w->current_tasks)) {
/*			if(list_remove(q->ready_workers, w)) {
				list_push_tail(q->busy_workers, w);
			}
*/			change_worker_state(q, w, WORKER_STATE_FULL);
		}
	} else if(!strcmp(category, "cpus")) {
		w->ncpus = atoi(arg);
	} else if(!strcmp(category, "disk")) {
	} else if(!strcmp(category, "memory")) {
	}
	
	return 1;
}


static void handle_worker(struct work_queue *q, struct link *l)
{
	char line[WORK_QUEUE_LINE_MAX];
	char key[WORK_QUEUE_LINE_MAX];
	struct work_queue_worker *w;

	link_to_hash_key(l, key);
	w = hash_table_lookup(q->worker_table, key);

	int keep_worker = 1;
	int result = recv_worker_msg(q, w, line, sizeof(line), time(0) + short_timeout);

	//if result > 0, it means a message is left to consume
	if(result > 0) {
		debug(D_WQ, "Invalid message from worker %s (%s): %s", w->hostname, w->addrport, line);
		keep_worker = 0;
	} else if(result < 0){
		debug(D_WQ, "Failed to read from worker %s (%s)", w->hostname, w->addrport);
		keep_worker = 0;
	} // otherwise do nothing..message was consumed and processed in recv_worker_msg()

	if(!keep_worker) {
		remove_worker(q, w);
	}
}

static int build_poll_table(struct work_queue *q, struct list *aux_links)
{
	int n = 0;
	char *key;
	struct work_queue_worker *w;

	if(aux_links) {
		if( list_size(aux_links) + 8 > q->poll_table_size )
		{	q->poll_table_size = list_size(aux_links)+8;	}
	}

	// Allocate a small table, if it hasn't been done yet.
	if(!q->poll_table) {
		q->poll_table = malloc(sizeof(*q->poll_table) * q->poll_table_size);
	}
	// The first item in the poll table is the master link, which accepts new connections.
	q->poll_table[0].link = q->master_link;
	q->poll_table[0].events = LINK_READ;
	q->poll_table[0].revents = 0;
	n = 1;

	if(aux_links) {
		struct link *lnk;
		list_first_item(aux_links);
		while((lnk = (struct link *)list_next_item(aux_links))) {
			q->poll_table[n].link = lnk;
			q->poll_table[n].events = LINK_READ;
			q->poll_table[n].revents = 0;
			n++;
		}
	}

	// For every worker in the hash table, add an item to the poll table
	hash_table_firstkey(q->worker_table);
	while(hash_table_nextkey(q->worker_table, &key, (void **) &w)) {

		// If poll table is not large enough, reallocate it
		if(n >= q->poll_table_size) {
			q->poll_table_size *= 2;
			q->poll_table = realloc(q->poll_table, sizeof(*q->poll_table) * q->poll_table_size);
		}

		q->poll_table[n].link = w->link;
		q->poll_table[n].events = LINK_READ;
		q->poll_table[n].revents = 0;
		n++;
	}

	return n;
}

static int put_file_whole(const char *localname, const char *remotename, struct work_queue *q, struct work_queue_worker *w, int taskid, INT64_T * total_bytes) {
	struct stat local_info;
	time_t stoptime;
	INT64_T actual = 0;
	INT64_T bytes_to_send = 0;

	if(stat(localname, &local_info) < 0)
		return 0;
	
	/* normalize the mode so as not to set up invalid permissions */
	local_info.st_mode |= 0600;
	local_info.st_mode &= 0777;

	debug(D_WQ, "%s (%s) needs file %s", w->hostname, w->addrport, localname);
	int fd = open(localname, O_RDONLY, 0);
	if(fd < 0)
		return 0;
	
	bytes_to_send = (INT64_T) local_info.st_size;
	
	struct work_queue_task *t = itable_lookup(q->running_tasks, taskid);
	stoptime = time(0) + get_transfer_wait_time(q, t, bytes_to_send);
	send_worker_msg(w, "put %s %lld 0%o %lld\n", time(0) + short_timeout, remotename, bytes_to_send, local_info.st_mode, taskid);
	actual = link_stream_from_fd(w->link, fd, bytes_to_send, stoptime);
	close(fd); 
	
	if(actual != bytes_to_send)
		return 0;
	
	*total_bytes += actual;
	return 1;
}

static int put_file_piece(const char *localname, const char *remotename, off_t startbyte, off_t endbyte, struct work_queue *q, struct work_queue_worker *w, int taskid, INT64_T *total_bytes){
	struct stat local_info;
	time_t stoptime;
	INT64_T actual = 0;
	INT64_T bytes_to_send = 0;
	
	if(stat(localname, &local_info) < 0)
		return 0;

	/* normalize the mode so as not to set up invalid permissions */
	local_info.st_mode |= 0600;
	local_info.st_mode &= 0777;
	
	debug(D_WQ, "%s (%s) needs file piece %s from byte offset %lld to %lld", w->hostname, w->addrport, localname, startbyte, endbyte);
	int fd = open(localname, O_RDONLY, 0);
	if(fd < 0)
		return 0;

	//We want to send bytes starting from startbyte. So seek to it first.
	if (startbyte >= 0 && endbyte < local_info.st_size && startbyte <= endbyte) {
		if(lseek(fd, startbyte, SEEK_SET) == -1) {	
			close(fd); 
			return 0;	
		}
	} else {
		debug(D_NOTICE, "File piece specification for %s is invalid", localname);
		close(fd); 
		return 0;	
	}
	bytes_to_send = (INT64_T) (endbyte - startbyte + 1);
		
	struct work_queue_task *t = itable_lookup(q->running_tasks, taskid);
	stoptime = time(0) + get_transfer_wait_time(q, t, bytes_to_send);
	send_worker_msg(w, "put %s %lld 0%o %lld\n", time(0) + short_timeout, remotename, bytes_to_send, local_info.st_mode, taskid);
	actual = link_stream_from_fd(w->link, fd, bytes_to_send, stoptime);
	close(fd); 
	
	if(actual != bytes_to_send)
		return 0;
	
	*total_bytes += actual;
	return 1;
}

static int put_directory(const char *dirname, const char *remotedirname, struct work_queue *q, struct work_queue_worker *w, int taskid, INT64_T * total_bytes) {
	DIR *dir = opendir(dirname);
	if(!dir)
		return 0;

	struct dirent *file;
	char buffer[WORK_QUEUE_LINE_MAX];
	char *localname, *remotename;
	int result;

	struct stat local_info;
	if(stat(dirname, &local_info) < 0)
		return 0;

	/* normalize the mode so as not to set up invalid permissions */
	local_info.st_mode |= 0700;
	local_info.st_mode &= 0777;

	// If mkdir fails, the future calls to put_file/directory() to place files
	// in that directory won't suceed. Such failure will eventually be captured
	// in 'start_tasks' function and in 'start_tasks' function the corresponding
	// worker will be removed.
	debug(D_WQ, "%s (%s) needs directory %s", w->hostname, w->addrport, dirname);
	send_worker_msg(w, "mkdir %s %o %lld\n", time(0) + short_timeout, remotedirname, local_info.st_mode, taskid);

	while((file = readdir(dir))) {
		char *filename = file->d_name;
		int len;

		if(!strcmp(filename, ".") || !strcmp(filename, "..")) {
			continue;
		}

		*buffer = '\0';
		len = sprintf(buffer, "%s/%s", dirname, filename);
		localname = xxstrdup(buffer);

		*buffer = '\0';
		len = sprintf(buffer, "%s/%s", remotedirname, filename);
		remotename = xxstrdup(buffer);
	
		if(stat(localname, &local_info) < 0) {
			closedir(dir);
			return 0;
		}	
		
		if(local_info.st_mode & S_IFDIR)  {
			result = put_directory(localname, remotename, q, w, taskid, total_bytes);	
		} else {
			result = put_file_whole(localname, remotename, q, w, taskid, total_bytes);	
		}	
		if(result == 0) {
			closedir(dir);
			return 0;
		}
		free(localname);
		free(remotename);
	}
	
	closedir(dir);
	return 1;
}

static int put_input_item(struct work_queue_file *tf, const char *expanded_payload, struct work_queue *q, struct work_queue_worker *w, int taskid, INT64_T * total_bytes) {
	struct stat local_info;
	struct stat *remote_info;
	char *hash_name;
	int dir = 0;
	char *payload;
	
	if(expanded_payload) {
		payload = xxstrdup(expanded_payload);
	} else {
		payload = xxstrdup(tf->payload);
	}

	if(stat(payload, &local_info) < 0)
		return 0;
	if(local_info.st_mode & S_IFDIR)
		dir = 1;
	
	hash_name = (char *) malloc((strlen(payload) + strlen(tf->remote_name) + 2) * sizeof(char));
	sprintf(hash_name, "%s-%s", payload, tf->remote_name);
	remote_info = hash_table_lookup(w->current_files, hash_name);

	if(!remote_info || remote_info->st_mtime != local_info.st_mtime || remote_info->st_size != local_info.st_size) {
		if(remote_info) {
			hash_table_remove(w->current_files, hash_name);
			free(remote_info);
		}

		if(dir) {
			if(!put_directory(payload, tf->remote_name, q, w, taskid, total_bytes))
				return 0;
		} else {
			if (tf->type == WORK_QUEUE_FILE_PIECE) {
				if(!put_file_piece(payload, tf->remote_name, tf->start_byte, tf->end_byte, q, w, taskid, total_bytes))
					return 0;
			} else {
				if(!put_file_whole(payload, tf->remote_name, q, w, taskid, total_bytes))
				return 0;
			}	
		}

		if(tf->flags & WORK_QUEUE_CACHE) {
			remote_info = malloc(sizeof(*remote_info));
			memcpy(remote_info, &local_info, sizeof(local_info));
			hash_table_insert(w->current_files, hash_name, remote_info);
		}
	} else {
		// TODO: Send message announcing what the job needs (put with 0 length?)
	}

	free(payload);
	free(hash_name);
	return 1;
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

	str = xxstrdup(payload);

	expanded_name = (char *) malloc(strlen(payload) + (50 * sizeof(char)));
	if(expanded_name == NULL) {
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
	return expanded_name;
}

static int send_output_files(struct work_queue_task *t, struct work_queue_worker *w, struct work_queue *q)
{
	struct work_queue_file *tf;
	
	if(!w->async_tasks) {
		// If the worker has not indicated that it is version 2.0 or higher, don't try sending "need" messages
		return 1;
	}

	if(t->output_files) {
		list_first_item(t->output_files);
		while((tf = list_next_item(t->output_files))) {
			send_worker_msg(w, "need %lld %s\n", time(0) + short_timeout, t->taskid, tf->remote_name);
		}
	}

	return 1;

}

static int send_input_files(struct work_queue_task *t, struct work_queue_worker *w, struct work_queue *q)
{
	struct work_queue_file *tf;
	int actual = 0;
	INT64_T total_bytes = 0;
	timestamp_t open_time = 0;
	timestamp_t close_time = 0;
	timestamp_t sum_time = 0;
	int fl;
	time_t stoptime;
	struct stat s;
	char *expanded_payload = NULL;

	// Check input existence
	if(t->input_files) {
		list_first_item(t->input_files);
		while((tf = list_next_item(t->input_files))) {
			if(tf->type == WORK_QUEUE_FILE || tf->type == WORK_QUEUE_FILE_PIECE) {
				if(strchr(tf->payload, '$')) {
					expanded_payload = expand_envnames(w, tf->payload);
					debug(D_WQ, "File name %s expanded to %s for %s (%s).", tf->payload, expanded_payload, w->hostname, w->addrport);
				} else {
					expanded_payload = xxstrdup(tf->payload);
				}
				if(stat(expanded_payload, &s) != 0) {
					debug(D_WQ,"Could not stat %s: %s\n", expanded_payload, strerror(errno));
					free(expanded_payload);
					goto failure;
				}
				free(expanded_payload);
			}
		}
	}
	// Start transfer ...
	if(t->input_files) {
		list_first_item(t->input_files);
		while((tf = list_next_item(t->input_files))) {
			if(tf->type == WORK_QUEUE_BUFFER) {
				debug(D_WQ, "%s (%s) needs literal as %s", w->hostname, w->addrport, tf->remote_name);
				fl = tf->length;

				stoptime = time(0) + get_transfer_wait_time(q, t, (INT64_T) fl);
				open_time = timestamp_get();
				send_worker_msg(w, "put %s %lld %o %lld\n", time(0) + short_timeout, tf->remote_name, (INT64_T) fl, 0777, t->taskid);
				actual = link_putlstring(w->link, tf->payload, fl, stoptime);
				close_time = timestamp_get();
				if(actual != (fl))
					goto failure;
				total_bytes += actual;
				sum_time += (close_time - open_time);
			} else if(tf->type == WORK_QUEUE_REMOTECMD) {
				debug(D_WQ, "%s (%s) needs %s from remote filesystem using %s", w->hostname, w->addrport, tf->remote_name, tf->payload);
				open_time = timestamp_get();
				send_worker_msg(w, "thirdget %d %s %s\n", time(0) + short_timeout, WORK_QUEUE_FS_CMD, tf->remote_name, tf->payload);
				close_time = timestamp_get();
				sum_time += (close_time - open_time);
			} else {
				if(tf->flags & WORK_QUEUE_THIRDGET) {
					debug(D_WQ, "%s (%s) needs %s from shared filesystem as %s", w->hostname, w->addrport, tf->payload, tf->remote_name);

					if(!strcmp(tf->remote_name, tf->payload)) {
						tf->flags |= WORK_QUEUE_PREEXIST;
					} else {
						open_time = timestamp_get();
						if(tf->flags & WORK_QUEUE_SYMLINK) {
							send_worker_msg(w, "thirdget %d %s %s\n", time(0) + short_timeout, WORK_QUEUE_FS_SYMLINK, tf->remote_name, tf->payload);
						} else {
							send_worker_msg(w, "thirdget %d %s %s\n", time(0) + short_timeout, WORK_QUEUE_FS_PATH, tf->remote_name, tf->payload);
						}
						close_time = timestamp_get();
						sum_time += (close_time - open_time);
					}
				} else {
					open_time = timestamp_get();
					if(strchr(tf->payload, '$')) {
						expanded_payload = expand_envnames(w, tf->payload);
					} else {
						expanded_payload = xxstrdup(tf->payload);
					}
					if(!put_input_item(tf, expanded_payload, q, w, t->taskid, &total_bytes)) {
						free(expanded_payload);
						goto failure;
					}
					free(expanded_payload);
					close_time = timestamp_get();
					sum_time += (close_time - open_time);
				}
			}
		}
		t->total_bytes_transferred += total_bytes;
		t->total_transfer_time += sum_time;
		w->total_bytes_transferred += total_bytes;
		w->total_transfer_time += sum_time;
		if(total_bytes > 0) {
			q->total_bytes_sent += (INT64_T) total_bytes;
			q->total_send_time += sum_time;
			debug(D_WQ, "%s (%s) got %d bytes in %.03lfs (%.02lfs Mbps) average %.02lfs Mbps", w->hostname, w->addrport, total_bytes, sum_time / 1000000.0, ((8.0 * total_bytes) / sum_time),
			      (8.0 * w->total_bytes_transferred) / w->total_transfer_time);
		}
	}

	return 1;

      failure:
	if(tf->type == WORK_QUEUE_FILE || tf->type == WORK_QUEUE_FILE_PIECE)
		debug(D_WQ, "%s (%s) failed to send %s (%i bytes received).", w->hostname, w->addrport, tf->payload, actual);
	else
		debug(D_WQ, "%s (%s) failed to send literal data (%i bytes received).", w->hostname, w->addrport, actual);
	t->result |= WORK_QUEUE_RESULT_INPUT_FAIL;
	return 0;
}

int start_one_task(struct work_queue *q, struct work_queue_worker *w, struct work_queue_task *t)
{
	add_time_slot(q, q->time_last_task_start, q->idle_time, TIME_SLOT_MASTER_IDLE, &(q->accumulated_idle_time), q->idle_times);
	q->idle_time = 0;

	t->time_send_input_start = q->time_last_task_start = timestamp_get();
	if(!send_input_files(t, w, q))
		return 0;
	if(!send_output_files(t, w, q))
		return 0;
	t->time_send_input_finish = timestamp_get();
	t->time_execute_cmd_start = timestamp_get();
	t->hostname = xxstrdup(w->hostname);
	t->host = xxstrdup(w->addrport);
	
	send_worker_msg(w, "work %zu %lld\n%s", time(0) + short_timeout, strlen(t->command_line), t->taskid, t->command_line);
	debug(D_WQ, "%s (%s) busy on '%s'", w->hostname, w->addrport, t->command_line);
	return 1;
}

static int get_num_of_effective_workers(struct work_queue *q)
{
	return q->workers_in_state[WORKER_STATE_BUSY] + q->workers_in_state[WORKER_STATE_READY] + q->workers_in_state[WORKER_STATE_FULL];
}

static struct task_statistics *task_statistics_init()
{
	struct task_statistics *ts;

	ts = (struct task_statistics *) malloc(sizeof(struct task_statistics));
	if(ts) {
		memset(ts, 0, sizeof(struct task_statistics));
	}
	return ts;
}

static void task_statistics_destroy(struct task_statistics *ts)
{
	if(!ts) return;
	if(ts->reports) {
		list_free(ts->reports);
		list_delete(ts->reports);
	}
	free(ts);
}

static void add_time_slot(struct work_queue *q, timestamp_t start, timestamp_t duration, int type, timestamp_t * accumulated_time, struct list *time_list)
{
	struct time_slot *ts;
	int count, effective_workers;

	if(!time_list)
		return;

	ts = (struct time_slot *) malloc(sizeof(struct time_slot));
	if(ts) {
		ts->start = start;
		ts->duration = duration;
		ts->type = type;
		*accumulated_time += ts->duration;
		list_push_tail(time_list, ts);
	} else {
		debug(D_WQ, "Failed to record time slot of type %d.", type);
	}

	// trim time list
	effective_workers = get_num_of_effective_workers(q);
	count = MAX(MIN_TIME_LIST_SIZE, effective_workers);
	while(list_size(time_list) > count) {
		ts = list_pop_head(time_list);
		*accumulated_time -= ts->duration;
		free(ts);
	}
}

static void add_task_report(struct work_queue *q, struct work_queue_task *t)
{
	struct list *reports;
	struct task_report *tr, *tmp_tr;
	struct task_statistics *ts;
	timestamp_t avg_task_execution_time;
	timestamp_t avg_task_transfer_time;
	timestamp_t avg_task_app_time;
	timestamp_t avg_task_time_at_master;
	int count, num_of_reports, effective_workers;


	if(!q || !t)
		return;

	// Record task statistics for master capacity estimation.
	tr = (struct task_report *) malloc(sizeof(struct task_report));
	if(!tr) {
		return;
	} else {
		tr->time_transfer_data = t->total_transfer_time;
		tr->time_execute_cmd = t->time_execute_cmd_finish - t->time_execute_cmd_start;
		tr->busy_workers = q->workers_in_state[WORKER_STATE_BUSY];
	}

	ts = q->task_statistics;
	if(!ts) {
		ts = task_statistics_init();
		if(!ts) {
			free(tr);
			return;
		}
		q->task_statistics = ts;
	}
	// Get task report list head
	reports = ts->reports;
	if(!reports) {
		reports = list_create();
		if(!reports) {
			free(tr);
			free(ts);
			return;
		}
		ts->reports = reports;
	}
	// Update sums 
	ts->total_time_transfer_data += tr->time_transfer_data;
	ts->total_time_execute_cmd += tr->time_execute_cmd;
	ts->total_busy_workers += tr->busy_workers;
	debug(D_WQ, "+%d busy workers. Total busy workers: %d\n", tr->busy_workers, ts->total_busy_workers);

	// Trim task report list size to N where N equals
	// MAX(MIN_TIME_LIST_SIZE, effective_workers).
	effective_workers = get_num_of_effective_workers(q);
	count = MAX(MIN_TIME_LIST_SIZE, effective_workers);
	while(list_size(reports) >= count) {
		tmp_tr = (struct task_report *) list_pop_head(reports);
		ts->total_time_transfer_data -= tmp_tr->time_transfer_data;
		ts->total_time_execute_cmd -= tmp_tr->time_execute_cmd;
		ts->total_busy_workers -= tmp_tr->busy_workers;
		debug(D_WQ, "-%d busy workers. Total busy workers: %d\n", tmp_tr->busy_workers, ts->total_busy_workers);
		ts->total_capacity -= tmp_tr->capacity;
		free(tmp_tr);
	}

	num_of_reports = list_size(reports) + 1;	// The number of task reports in the list (after the new entry has been added)

	avg_task_execution_time = ts->total_time_execute_cmd / num_of_reports;
	avg_task_transfer_time = ts->total_time_transfer_data / num_of_reports;
	avg_task_app_time = q->total_tasks_complete > 0 ? q->app_time / (q->total_tasks_complete + 1) : 0;
	debug(D_WQ, "Avg task execution time: %lld; Avg task tranfer time: %lld; Avg task app time: %lld\n", avg_task_execution_time, avg_task_transfer_time, avg_task_app_time);


	avg_task_time_at_master = avg_task_transfer_time + avg_task_app_time;
	// This is the Master Capacity Equation:
	if(avg_task_time_at_master > 0) {
		tr->capacity = avg_task_execution_time / avg_task_time_at_master + 1;
	} else {
		tr->capacity = INT_MAX;
	}

	// Record the recent capacities' sum for quick calculation of the avg
	ts->total_capacity += tr->capacity;

	// Appending the task report to the list
	list_push_tail(reports, tr);

	// Update the lastest capacity and avg capacity variables
	q->capacity = tr->capacity;
	q->avg_capacity = ts->total_capacity / num_of_reports;
	debug(D_WQ, "Latest master capacity: %d; Avg master capacity: %d\n", q->capacity, q->avg_capacity);
}

static struct work_queue_worker *find_worker_by_files(struct work_queue *q, struct work_queue_task *t)
{
	char *key;
	struct work_queue_worker *w;
	struct work_queue_worker *best_worker = 0;
	INT64_T most_task_cached_bytes = 0;
	INT64_T task_cached_bytes;
	struct stat *remote_info;
	struct work_queue_file *tf;
	char *hash_name;

	hash_table_firstkey(q->worker_table);
	while(hash_table_nextkey(q->worker_table, &key, (void **) &w)) {
		if(w->state == WORKER_STATE_READY) {
			task_cached_bytes = 0;
			list_first_item(t->input_files);
			while((tf = list_next_item(t->input_files))) {
				if((tf->type == WORK_QUEUE_FILE || tf->type == WORK_QUEUE_FILE_PIECE) && (tf->flags & WORK_QUEUE_CACHE)) {
					hash_name = malloc((strlen(tf->payload) + strlen(tf->remote_name) + 2) * sizeof(char));
					sprintf(hash_name, "%s-%s", (char *) tf->payload, tf->remote_name);
					remote_info = hash_table_lookup(w->current_files, hash_name);
					if(remote_info)
						task_cached_bytes += remote_info->st_size;
					free(hash_name);
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

static struct work_queue_worker *find_worker_by_fcfs(struct work_queue *q)
{
	char *key;
	struct work_queue_worker *w;
	hash_table_firstkey(q->worker_table);
	while(hash_table_nextkey(q->worker_table, &key, (void**)&w)) {
		if(w->state == WORKER_STATE_BUSY || w->state == WORKER_STATE_READY) {
			return w;
		}
	}
	return NULL;
}

static struct work_queue_worker *find_worker_by_random(struct work_queue *q)
{
	char *key;
	struct work_queue_worker *w;
	struct work_queue_worker *best_worker = 0;
	int num_workers_ready;
	int random_ready_worker, ready_worker_count = 1;

	num_workers_ready = q->workers_in_state[WORKER_STATE_READY];

	if(num_workers_ready > 0) {
		random_ready_worker = (rand() % num_workers_ready) + 1;
	} else {
		random_ready_worker = 0;
	}

	hash_table_firstkey(q->worker_table);
	while(hash_table_nextkey(q->worker_table, &key, (void **) &w)) {
		if(w->state == WORKER_STATE_READY && ready_worker_count == random_ready_worker) {
			return w;
		}
		if(w->state == WORKER_STATE_READY) {
			ready_worker_count++;
		}
	}

	return best_worker;
}

static struct work_queue_worker *find_worker_by_time(struct work_queue *q)
{
	char *key;
	struct work_queue_worker *w;
	struct work_queue_worker *best_worker = 0;
	double best_time = HUGE_VAL;

	hash_table_firstkey(q->worker_table);
	while(hash_table_nextkey(q->worker_table, &key, (void **) &w)) {
		if(w->state == WORKER_STATE_READY || w->state == WORKER_STATE_BUSY) {
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
		return find_worker_by_fcfs(q);
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
		debug(D_WQ, "Finding worker by Files");
		return find_worker_by_files(q, t);
	case WORK_QUEUE_SCHEDULE_TIME:
		debug(D_WQ, "Finding worker by Time");
		return find_worker_by_time(q);
	case WORK_QUEUE_SCHEDULE_RAND:
		debug(D_WQ, "Finding worker by Random");
		return find_worker_by_random(q);
	case WORK_QUEUE_SCHEDULE_FCFS:
	default:
		debug(D_WQ, "Finding worker by FCFS");
		return find_worker_by_fcfs(q);
	}
}

static int start_task_on_worker(struct work_queue *q, struct work_queue_worker *w)
{
	struct work_queue_task *t = list_pop_head(q->ready_list);
	if(!t)
		return 0;

	itable_insert(w->current_tasks, t->taskid, t);
	itable_insert(q->running_tasks, t->taskid, t); 
	itable_insert(q->worker_task_map, t->taskid, w); //add worker as execution site for t.

	if(start_one_task(q, w, t)) {
		
		if(w->nslots <= itable_size(w->current_tasks)) {
			change_worker_state(q, w, WORKER_STATE_FULL);
		} else {
			change_worker_state(q, w, WORKER_STATE_BUSY);
		}
		return 1;
	} else {
		debug(D_WQ, "Failed to send task to worker %s (%s).", w->hostname, w->addrport);
		remove_worker(q, w);	// puts tasks in w->current_tasks back into q->ready_list
		return 0;
	}
}

static void start_tasks(struct work_queue *q)
{				// try to start as many task as possible
	struct work_queue_task *t;
	struct work_queue_worker *w;

	while(list_size(q->ready_list) && (q->workers_in_state[WORKER_STATE_READY] || q->workers_in_state[WORKER_STATE_BUSY])) {
		t = list_peek_head(q->ready_list);
		debug(D_WQ, "finding worker for task %d", t->taskid);
		w = find_best_worker(q, t);
		if(w) {
			debug(D_WQ, "Worker %s (%s) found for task %d.", w->hostname, w->addrport, t->taskid);
		} else {
			debug(D_WQ, "No worker found for task %d.", t->taskid);
		}
		if(w) {
			start_task_on_worker(q, w);
		} else {
			break;
		}
	}
}

static void do_keepalive_checks(struct work_queue *q) {
	struct work_queue_worker *w;
	char *key;
	timestamp_t current = timestamp_get();
	
	hash_table_firstkey(q->worker_table);
	while(hash_table_nextkey(q->worker_table, &key, (void **) &w)) {
		if(w->state == WORKER_STATE_BUSY) {
			timestamp_t keepalive_elapsed_time = (current - w->last_msg_sent_time)/1000000;
			// send new keepalive check only (1) if we received a response since last keepalive check AND 
			// (2) we are past keepalive interval 
			if(w->last_msg_recv_time >= w->keepalive_check_sent_time) {	
				if(keepalive_elapsed_time >= q->keepalive_interval) {
					if (send_worker_msg(w, "%s\n", time(0) + short_timeout, "check") < 0) {
						debug(D_WQ, "Failed to send keepalive check to worker %s (%s).", w->hostname, w->addrport);
						remove_worker(q, w);
					} else {
						debug(D_WQ, "Sent keepalive check to worker %s (%s)", w->hostname, w->addrport);
						w->keepalive_check_sent_time = current;	
					}	
				}
			} else { 
				// Here because we haven't received a message from worker since its last keepalive check. Check if time 
				// since we last polled link for responses has exceeded keepalive timeout. If so, remove the worker.
				if (link_poll_end > w->keepalive_check_sent_time) {
					if (((link_poll_end - w->keepalive_check_sent_time)/1000000) >= q->keepalive_timeout) { 
						debug(D_WQ, "Removing worker %s (%s): hasn't responded to keepalive check for more than %d s", w->hostname, w->addrport, q->keepalive_timeout);
						remove_worker(q, w);
					}
				}	
			}
		}
	}
}

static void abort_slow_workers(struct work_queue *q)
{
	struct work_queue_worker *w;
	struct work_queue_task *t;
	UINT64_T key;
	const double multiplier = q->fast_abort_multiplier;

	if(q->total_tasks_complete < 10)
		return;

	timestamp_t average_task_time = (q->total_execute_time + q->total_send_time) / q->total_tasks_complete;
	timestamp_t current = timestamp_get();

	itable_firstkey(q->running_tasks);
	while(itable_nextkey(q->running_tasks, &key, (void **) &t)) {
		timestamp_t runtime = current - t->time_send_input_start;
		if(runtime > (average_task_time * multiplier)) {
			w = itable_lookup(q->worker_task_map, t->taskid);
			debug(D_WQ, "Removing worker %s (%s): takes too long to execute the current task - %.02lf s (average task execution time by other workers is %.02lf s)", w->hostname, w->addrport, runtime / 1000000.0, average_task_time / 1000000.0);
			remove_worker(q, w);
		}
	}
}

static void update_app_time(struct work_queue *q, timestamp_t last_left_time, int last_left_status)
{
	if(!q) return;

	timestamp_t t1, t2;

	if(last_left_time && last_left_status == 1) {
		t1 = timestamp_get() - last_left_time;

		if(q->total_tasks_complete > MIN_TIME_LIST_SIZE) {
			t2 = q->app_time / q->total_tasks_complete;
			// A simple way of discarding outliers that does not require much calculation.
			// Works for workloads that has stable app times.
			if(t1 > WORK_QUEUE_APP_TIME_OUTLIER_MULTIPLIER * t2) {
				debug(D_WQ, "Discarding outlier task app time: %lld\n", t1);
				q->app_time += t2;
			} else {
				q->app_time += t1;
			}
		} else {
			q->app_time += t1; 
		}
	}
}

static int shut_down_worker(struct work_queue *q, struct work_queue_worker *w)
{
	if(!w)
		return 0;

	send_worker_msg(w, "%s\n", time(0) + short_timeout, "exit");
	remove_worker(q, w);
	return 1;
}

//comparator function for checking if a task matches given taskid.
static int taskid_comparator(void *t, const void *r) {

	struct work_queue_task *task_in_queue = t;
	const int *taskid = r;

	if (task_in_queue->taskid == *taskid) {
		return 1;
	}	
	return 0;
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

static int cancel_running_task(struct work_queue *q, struct work_queue_task *t) {

	struct work_queue_worker *w;
	w = itable_lookup(q->worker_task_map, t->taskid);
	
	if (w) {
		//send message to worker asking to kill its task.
		send_worker_msg(w, "%s %d\n", time(0) + short_timeout, "kill", t->taskid);
		//update table.
		itable_remove(q->running_tasks, t->taskid);
		itable_remove(q->finished_tasks, t->taskid);
		itable_remove(q->worker_task_map, t->taskid);

		if (t->tag)
			debug(D_WQ, "Task with tag %s and id %d is aborted at worker %s (%s) and removed.", t->tag, t->taskid, w->hostname, w->addrport);
		else
			debug(D_WQ, "Task with id %d is aborted at worker %s (%s) and removed.", t->taskid, w->hostname, w->addrport);
			
		//Delete any input files that are not to be cached.
		delete_worker_files(w, t->input_files, WORK_QUEUE_CACHE | WORK_QUEUE_PREEXIST);
		//Delete all output files since they are not needed as the task was aborted.
		delete_worker_files(w, t->output_files, 0);
		
		change_worker_state(q, w, WORKER_STATE_READY);
		itable_remove(w->current_tasks, t->taskid);
		return 1;
	}
	
	return 0;
}

static struct work_queue_task *find_running_task_by_id(struct work_queue *q, int taskid) {
	
	struct work_queue_task *t;

	t=itable_lookup(q->running_tasks, taskid);
	if (t){
		return t;
	}
	
	t = itable_lookup(q->finished_tasks, taskid);
	if(t) {
		return t;
	}
	
	return NULL;
}

static struct work_queue_task *find_running_task_by_tag(struct work_queue *q, const char *tasktag) {
	
	struct work_queue_task *t;
	UINT64_T taskid;

	itable_firstkey(q->running_tasks);
	while(itable_nextkey(q->running_tasks, &taskid, (void**)&t)) {
		if (tasktag_comparator(t, tasktag)) {
			return t;
		}
	}

	itable_firstkey(q->finished_tasks);
	while(itable_nextkey(q->finished_tasks, &taskid, (void**)&t)) {
		if (tasktag_comparator(t, tasktag)) {
			return t;
		}
	}
	
	return NULL;
}

/******************************************************/
/********** work_queue_task public functions **********/
/******************************************************/

struct work_queue_task *work_queue_task_create(const char *command_line)
{
	struct work_queue_task *t = malloc(sizeof(*t));
	memset(t, 0, sizeof(*t));
	t->command_line = xxstrdup(command_line);
	t->worker_selection_algorithm = WORK_QUEUE_SCHEDULE_UNSET;
	t->input_files = list_create();
	t->output_files = list_create();
	t->return_status = -1;
	t->result = WORK_QUEUE_RESULT_UNSET;
	return t;
}

void work_queue_task_specify_tag(struct work_queue_task *t, const char *tag)
{
	if(t->tag)
		free(t->tag);
	t->tag = xxstrdup(tag);
}

struct work_queue_file * work_queue_file_create(const char * remote_name, int type, int flags)
{
	struct work_queue_file *f;
	
	f = malloc(sizeof(*f));
	if(!f) return NULL;
	
	memset(f, 0, sizeof(*f));
	
	f->remote_name = xxstrdup(remote_name);
	f->type = type;
	f->flags = flags;
	
	return f;
}

int work_queue_task_specify_file(struct work_queue_task *t, const char *local_name, const char *remote_name, int type, int flags)
{
	if(!t || !local_name || !remote_name) {
		return 0;
	}

	// @param remote_name is the path of the file as on the worker machine. In
	// the Work Queue framework, workers are prohibitted from writing to paths
	// outside of their workspaces. When a task is specified, the workspace of
	// the worker(the worker on which the task will be executed) is unlikely to
	// be known. Thus @param remote_name should not be an absolute path.
	if(remote_name[0] == '/') {
		return 0;
	}

	struct work_queue_file *tf = work_queue_file_create(remote_name, WORK_QUEUE_FILE, flags);

	tf->length = strlen(local_name);
	tf->payload = xxstrdup(local_name);

	if(type == WORK_QUEUE_INPUT) {
		list_push_tail(t->input_files, tf);
	} else {
		list_push_tail(t->output_files, tf);
	}
	return 1;
}

int work_queue_task_specify_file_piece(struct work_queue_task *t, const char *local_name, const char *remote_name, off_t start_byte, off_t end_byte, int type, int flags)
{
	if(!t || !local_name || !remote_name) {
		return 0;
	}

	// @param remote_name should not be an absolute path. @see
	// work_queue_task_specify_file
	if(remote_name[0] == '/') {
		return 0;
	}

	struct work_queue_file *tf = work_queue_file_create(remote_name, WORK_QUEUE_FILE_PIECE, flags);

	tf->length = strlen(local_name);
	tf->start_byte = start_byte;	
	tf->end_byte = end_byte;	
	tf->payload = xxstrdup(local_name);

	if(type == WORK_QUEUE_INPUT) {
		list_push_tail(t->input_files, tf);
	} else {
		list_push_tail(t->output_files, tf);
	}
	return 1;
}

int work_queue_task_specify_buffer(struct work_queue_task *t, const char *data, int length, const char *remote_name, int flags)
{
	if(!t || !remote_name) {
		return 0;
	}

	// @param remote_name should not be an absolute path. @see
	// work_queue_task_specify_file
	if(remote_name[0] == '/') {
		return 0;
	}

	struct work_queue_file *tf = work_queue_file_create(remote_name, WORK_QUEUE_BUFFER, flags);
	tf->length = length;
	tf->payload = malloc(length);
	memcpy(tf->payload, data, length);
	list_push_tail(t->input_files, tf);

	return 1;
}

int work_queue_task_specify_file_command(struct work_queue_task *t, const char *remote_name, const char *cmd, int type, int flags)
{
	if(!t || !remote_name || !cmd) {
		return 0;
	}

	// @param remote_name should not be an absolute path. @see
	// work_queue_task_specify_file
	if(remote_name[0] == '/') {
		return 0;
	}

	struct work_queue_file *tf = work_queue_file_create(remote_name, WORK_QUEUE_REMOTECMD, flags);
	tf->length = strlen(cmd);
	tf->payload = xxstrdup(cmd);

	if(type == WORK_QUEUE_INPUT) {
		list_push_tail(t->input_files, tf);
	} else {
		list_push_tail(t->output_files, tf);
	}
	return 1;
}

void work_queue_task_specify_algorithm(struct work_queue_task *t, int alg)
{
	t->worker_selection_algorithm = alg;
}

void work_queue_task_delete(struct work_queue_task *t)
{
	struct work_queue_file *tf;
	if(t) {
		if(t->command_line)
			free(t->command_line);
		if(t->tag)
			free(t->tag);
		if(t->output)
			free(t->output);
		if(t->input_files) {
			while((tf = list_pop_tail(t->input_files))) {
				if(tf->payload)
					free(tf->payload);
				if(tf->remote_name)
					free(tf->remote_name);
				free(tf);
			}
			list_delete(t->input_files);
		}
		if(t->output_files) {
			while((tf = list_pop_tail(t->output_files))) {
				if(tf->payload)
					free(tf->payload);
				if(tf->remote_name)
					free(tf->remote_name);
				free(tf);
			}
			list_delete(t->output_files);
		}
		if(t->hostname)
			free(t->hostname);
		if(t->host)
			free(t->host);
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
		goto failure;
	} else {
		char address[LINK_ADDRESS_MAX];
		link_address_local(q->master_link, address, &q->port);
	}

	get_canonical_path(".", q->workingdir, PATH_MAX);

	q->ready_list = list_create();
	q->running_tasks = itable_create(0);
	q->finished_tasks = itable_create(0);
	q->complete_list = list_create();

	q->worker_table = hash_table_create(0, 0);
	q->worker_task_map = itable_create(0);
	//q->busy_workers = list_create();
	//q->ready_workers = list_create();
	
	// The poll table is initially null, and will be created
	// (and resized) as needed by build_poll_table.
	q->poll_table_size = 8;

	int i;
	for(i = 0; i < WORKER_STATE_MAX; i++) {
		q->workers_in_state[i] = 0;
	}

	q->fast_abort_multiplier = wq_option_fast_abort_multiplier;
	q->worker_selection_algorithm = wq_option_scheduler;
	q->task_ordering = WORK_QUEUE_TASK_ORDER_FIFO;

	// Capacity estimation related
	q->start_time = timestamp_get();
	q->time_last_task_start = q->start_time;
	q->idle_times = list_create();
	q->task_statistics = task_statistics_init();

	q->workers_by_pool = hash_table_create(0,0);
	
	q->keepalive_interval = WORK_QUEUE_DEFAULT_KEEPALIVE_INTERVAL;
	q->keepalive_timeout = WORK_QUEUE_DEFAULT_KEEPALIVE_TIMEOUT; 

	q->monitor_mode   =  0;
	q->password = 0;
	
	debug(D_WQ, "Work Queue is listening on port %d.", q->port);
	return q;

failure:
	debug(D_NOTICE, "Could not create work_queue on port %i.", port);
	free(q);
	return 0;
}

struct work_queue *work_queue_create_monitoring(int port, char *monitor_summary_file)
{
	struct work_queue *q = work_queue_create(port);

	q->monitor_mode = 0;

	if(!q)
		return 0;

	q->monitor_exe = resource_monitor_copy_to_wd(NULL);
	if(!q->monitor_exe)
	{
		debug(D_NOTICE, "Could not find the resource monitor executable. Disabling monitor mode.\n");
		return q;
	}

	if(monitor_summary_file)
		monitor_summary_file = xxstrdup(monitor_summary_file);
	else
		monitor_summary_file = string_format("wq-%d-resource-usage", getpid());

	q->monitor_fd = open(monitor_summary_file, O_CREAT | O_WRONLY | O_APPEND, 00666);
	free(monitor_summary_file);

	if(q->monitor_fd < 0)
	{
		debug(D_NOTICE, "Could not open monitor log file. Disabling monitor mode.\n");
		return q;
	}

	q->monitor_mode = 1;
	return q;
}

int work_queue_activate_fast_abort(struct work_queue *q, double multiplier)
{
	if(multiplier >= 1) {
		q->fast_abort_multiplier = multiplier;
		return 0;
	} else if(multiplier < 0) {
		q->fast_abort_multiplier = multiplier;
		return 0;
	} else {
		q->fast_abort_multiplier = -1.0;
		return 1;
	}
}

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

void work_queue_specify_estimate_capacity_on(struct work_queue *q, int value)
{
	q->estimate_capacity_on = value;
}

void work_queue_specify_algorithm(struct work_queue *q, int alg)
{
	q->worker_selection_algorithm = alg;
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

void work_queue_specify_master_mode(struct work_queue *q, int mode)
{
	q->master_mode = mode;
	if(mode==WORK_QUEUE_MASTER_MODE_CATALOG) {
		strncpy(q->catalog_host, CATALOG_HOST, DOMAIN_NAME_MAX);
		q->catalog_port = CATALOG_PORT;
	}
}

void work_queue_specify_catalog_server(struct work_queue *q, const char *hostname, int port)
{
	if(hostname) {
		strncpy(q->catalog_host, hostname, DOMAIN_NAME_MAX);
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

void work_queue_delete(struct work_queue *q)
{
	if(q) {
		struct pool_info *pi;
		struct work_queue_worker *w;
		char *key;

		hash_table_firstkey(q->worker_table);
		while(hash_table_nextkey(q->worker_table, &key, (void **) &w)) {
			release_worker(q, w);
		}
		if(q->master_mode == WORK_QUEUE_MASTER_MODE_CATALOG) {
			update_catalog(q, 1);
		}
		hash_table_delete(q->worker_table);
		itable_delete(q->worker_task_map);
		
		list_delete(q->ready_list);
		itable_delete(q->running_tasks);
		itable_delete(q->finished_tasks);
		list_delete(q->complete_list);
		
		list_free(q->idle_times);
		list_delete(q->idle_times);
		task_statistics_destroy(q->task_statistics);
 
		hash_table_firstkey(q->workers_by_pool);
		while(hash_table_nextkey(q->workers_by_pool, &key, (void **) &pi)) {
			free(pi);
		}
		hash_table_delete(q->workers_by_pool);
		
		free(q->poll_table);
		link_close(q->master_link);
		if(q->logfile) {
			fclose(q->logfile);
		}
		free(q);
	}
}

int work_queue_monitor_wrap(struct work_queue *q, struct work_queue_task *t)
{
	char *wrap_cmd; 
	char *summary = string_format(RESOURCE_MONITOR_TASK_SUMMARY_NAME, getpid(), t->taskid);
	
	wrap_cmd = resource_monitor_rewrite_command(t->command_line, summary, RMONITOR_DONT_GENERATE, RMONITOR_DONT_GENERATE);

	//BUG: what if user changes current working directory?
	work_queue_task_specify_file(t, q->monitor_exe, q->monitor_exe, WORK_QUEUE_INPUT, WORK_QUEUE_CACHE);
	work_queue_task_specify_file(t, summary, summary, WORK_QUEUE_OUTPUT, WORK_QUEUE_NOCACHE);

	free(summary);
	free(t->command_line);

	t->command_line = wrap_cmd;

	return 0;
}

int work_queue_submit(struct work_queue *q, struct work_queue_task *t)
{
	static int next_taskid = 1;
	
	/* If the task has been used before, clear out accumlated state. */
	if(t->output) {
		free(t->output);
		t->output = 0;
	}
	if(t->hostname) {
		free(t->hostname);
		t->hostname = 0;
	}
	if(t->host) {
		free(t->host);
		t->host = 0;
	}
	t->total_transfer_time = 0;
	t->cmd_execution_time = 0;
	t->result = WORK_QUEUE_RESULT_UNSET;
	
	//Increment taskid. So we get a unique taskid for every submit.
	t->taskid = next_taskid++;

	if(q->monitor_mode)
		work_queue_monitor_wrap(q, t);

	/* Then, add it to the ready list and mark it as submitted. */
	if (q->task_ordering == WORK_QUEUE_TASK_ORDER_LIFO){
		list_push_head(q->ready_list, t);
	}	
	else {
		list_push_tail(q->ready_list, t);
	}	
	t->time_task_submit = timestamp_get();
	q->total_tasks_submitted++;

	return (t->taskid);
}

static void print_password_warning( struct work_queue *q )
{
	static int did_password_warning = 0;

	if(did_password_warning) return;

       	if(!q->password && q->master_mode==WORK_QUEUE_MASTER_MODE_CATALOG) {
       		fprintf(stderr,"warning: this work queue master is visible to the public.\n");
	       	fprintf(stderr,"warning: you should set a password with the --password option.\n");
		did_password_warning = 1;
	}
}

struct work_queue_task *work_queue_wait(struct work_queue *q, int timeout)
{
	return work_queue_wait_internal(q, timeout, NULL, NULL);
}

struct work_queue_task *work_queue_wait_internal(struct work_queue *q, int timeout, struct list *aux_links, struct list *active_aux_links)
{
	struct work_queue_task *t;
	time_t stoptime;

	static timestamp_t last_left_time = 0;
	static int last_left_status = 0;	// 0 -- did not return any done task; 1 -- returned done task 
	static time_t next_pool_decision_enforcement = 0;

	print_password_warning(q);

	update_app_time(q, last_left_time, last_left_status);

	if(q->master_mode == WORK_QUEUE_MASTER_MODE_CATALOG && next_pool_decision_enforcement < time(0)) {
		enforce_pool_decisions(q);
		next_pool_decision_enforcement = time(0) + POOL_DECISION_ENFORCEMENT_INTERVAL_DEFAULT;
	}

	if(timeout == WORK_QUEUE_WAITFORTASK) {
		stoptime = 0;
	} else {
		stoptime = time(0) + timeout;
	}

	while(1) {
		if(q->master_mode == WORK_QUEUE_MASTER_MODE_CATALOG) {
			update_catalog(q, 0);
		}
		
		if(q->keepalive_interval > 0) {
			do_keepalive_checks(q);
		}

		t = list_pop_head(q->complete_list);
		if(t) {
			last_left_time = timestamp_get();
			last_left_status = 1;
			return t;
		}

		debug(D_WQ, "workers_in_state[BUSY] = %d, workers_in_state[FULL] = %d, list_size(ready_list) = %d, aux_links = %x, list_size(aux_links) = %d", q->workers_in_state[WORKER_STATE_BUSY], q->workers_in_state[WORKER_STATE_FULL], list_size(q->ready_list), aux_links, aux_links?list_size(aux_links):0);

		if( (q->workers_in_state[WORKER_STATE_BUSY] + q->workers_in_state[WORKER_STATE_FULL]) == 0 && list_size(q->ready_list) == 0 && !(aux_links && list_size(aux_links)))
			break;

		debug(D_WQ, "starting tasks");
		start_tasks(q);

		int n = build_poll_table(q, aux_links);

		// Wait no longer than the caller's patience.
		int msec;
		if(stoptime) {
			msec = MAX(0, (stoptime - time(0)) * 1000);
		} else {
			msec = 5000;
		}

		// Poll all links for activity.
		timestamp_t link_poll_start = timestamp_get();
		int result = link_poll(q->poll_table, n, msec);
		link_poll_end = timestamp_get();	
		q->idle_time += link_poll_end - link_poll_start;

		// If nothing was awake, restart the loop or return without a task.
		if(result <= 0) {
			if(stoptime && time(0) >= stoptime) {
				break;
			} else {
				continue;
			}
		}

		// If the master link was awake, then accept as many workers as possible.
		if(q->poll_table[0].revents) {
			do {
				add_worker(q);
			} while(link_usleep(q->master_link, 0, 1, 0) && (stoptime > time(0)));
		}

		int i, j = 1;

		// Consider any auxiliary links passed into the function and remove from the list any which are not active.
		if(aux_links) {
			j = list_size(aux_links)+1;
			for(i = 1; i < j; i++) {
				if(q->poll_table[i].revents) {
					list_push_tail(active_aux_links, q->poll_table[i].link);
				}
			}
		}

		// Then consider all existing active workers and dispatch tasks.
		for(i = j; i < n; i++) {
			if(q->poll_table[i].revents) {
				handle_worker(q, q->poll_table[i].link);
			}
		}
		
		// If any worker has sent a results message, retrieve the output files.
		if(itable_size(q->finished_tasks)) {
			struct work_queue_worker *w;
			UINT64_T taskid;
			itable_firstkey(q->finished_tasks);
			while(itable_nextkey(q->finished_tasks, &taskid, (void **)&t)) {
				w = itable_lookup(q->worker_task_map, taskid);
				fetch_output_from_worker(q, w, taskid);
				itable_firstkey(q->finished_tasks);  // fetch_output removes the resolved task from the itable, thus potentially corrupting our current location.  This resets it to the top.
			}
		}

		// If fast abort is enabled, kill off slow workers.
		if(q->fast_abort_multiplier > 0) {
			abort_slow_workers(q);
		}

		// If any of the passed-in auxiliary links are active then break so the caller can handle them.
		if(aux_links && list_size(active_aux_links)) {
			break;
		}
	}

	last_left_time = timestamp_get();
	last_left_status = 0;
	return 0;
}

int work_queue_hungry(struct work_queue *q)
{
	if(q->total_tasks_submitted < 100)
		return (100 - q->total_tasks_submitted);

	// TODO: fix this so that it actually looks at the number of slots available.

	int i, j, workers_init, workers_ready, workers_busy, workers_full;
	workers_init = q->workers_in_state[WORKER_STATE_INIT];
	workers_ready = q->workers_in_state[WORKER_STATE_READY];
	workers_busy = q->workers_in_state[WORKER_STATE_BUSY];
	workers_full = q->workers_in_state[WORKER_STATE_FULL];

	//i = 1.1 * number of current workers
	//j = # of queued tasks.
	//i-j = # of tasks to queue to re-reach the status quo.
	i = (1.1 * (workers_init + workers_ready + workers_busy + workers_full));
	j = list_size(q->ready_list);
	return MAX(i - j, 0);
}

int work_queue_shut_down_workers(struct work_queue *q, int n)
{
	struct work_queue_worker *w;
	char *key;
	int i = 0;

	if(!q)
		return -1;

	// send worker the "exit" msg
	hash_table_firstkey(q->worker_table);
	while(i < n && hash_table_nextkey(q->worker_table, &key, (void **) &w)) {
		if(w->state == WORKER_STATE_READY) {
			shut_down_worker(q, w);
			i++;
		}
	}

	return i;
}

/**
 * Cancel submitted task as long as it has not been retrieved through wait().
 * This is non-blocking and has a worst-case running time of O(n) where n is 
 * number of submitted tasks.
 * This returns the work_queue_task struct corresponding to specified task and 
 * null if the task is not found.
 */
struct work_queue_task *work_queue_cancel_by_taskid(struct work_queue *q, int taskid) {

	struct work_queue_task *matched_task;

	if (taskid > 0){
		//see if task is executing at a worker (in running_tasks or finished_tasks).
		if ((matched_task = find_running_task_by_id(q, taskid))) {
			if (cancel_running_task(q, matched_task)) {
				return matched_task;
			}	
		} //if not, see if task is in ready list.
		else if ((matched_task = list_find(q->ready_list, taskid_comparator, &taskid))) {
			list_remove(q->ready_list, matched_task);
			debug(D_WQ, "Task with id %d is removed from ready list.", matched_task->taskid);
			return matched_task;
		} //if not, see if task is in complete list.
		else if ((matched_task = list_find(q->complete_list, taskid_comparator, &taskid))) {
			list_remove(q->complete_list, matched_task);
			debug(D_WQ, "Task with id %d is removed from complete list.", matched_task->taskid);
			return matched_task;
		} 
		else { 
			debug(D_WQ, "Task with id %d is not found in queue.", taskid);
		}	
	}
	
	return NULL;
}

struct work_queue_task *work_queue_cancel_by_tasktag(struct work_queue *q, const char* tasktag) {

	struct work_queue_task *matched_task;

	if (tasktag){
		//see if task is executing at a worker (in running_tasks or finished_tasks).
		if ((matched_task = find_running_task_by_tag(q, tasktag))) {
			if (cancel_running_task(q, matched_task)) {
				return matched_task;
			}
		} //if not, see if task is in ready list.
		else if ((matched_task = list_find(q->ready_list, tasktag_comparator, tasktag))) {
			list_remove(q->ready_list, matched_task);
			debug(D_WQ, "Task with tag %s and id %d is removed from ready list.", matched_task->tag, matched_task->taskid);
			return matched_task;
		} //if not, see if task is in complete list.
		else if ((matched_task = list_find(q->complete_list, tasktag_comparator, tasktag))) {
			list_remove(q->complete_list, matched_task);
			debug(D_WQ, "Task with tag %s and id %d is removed from complete list.", matched_task->tag, matched_task->taskid);
			return matched_task;
		} 
		else { 
			debug(D_WQ, "Task with tag %s is not found in queue.", tasktag);
		}
	}
	
	return NULL;
}

void work_queue_reset(struct work_queue *q, int flags) {
	struct work_queue_worker *w;
	struct work_queue_task *t;
	char *key;
	
	if(!q) return;

	hash_table_firstkey(q->worker_table);
	while(hash_table_nextkey(q->worker_table,&key,(void**)&w)) {
		if(w->async_tasks) {
			send_worker_msg(w, "reset", time(0)+short_timeout);
			cleanup_worker(q, w);
		} else {
			release_worker(q, w);
		}
	}
	
	if(flags & WORK_QUEUE_RESET_KEEP_TASKS) {
		return;
	}
	
	while((t = list_pop_head(q->ready_list))) {
		work_queue_task_delete(t);
	}
	
}

int work_queue_empty(struct work_queue *q)
{
	return ((list_size(q->ready_list) + itable_size(q->running_tasks) + itable_size(q->finished_tasks) + list_size(q->complete_list)) == 0);
}

void work_queue_specify_keepalive_interval(struct work_queue *q, int interval) 
{
	q->keepalive_interval = interval;
}

void work_queue_specify_keepalive_timeout(struct work_queue *q, int timeout) 
{
	q->keepalive_timeout = timeout;
}

char * work_queue_get_worker_summary( struct work_queue *q )
{
	char *key;
	struct pool_info *pi;

	struct buffer_t *b = buffer_create();

	hash_table_firstkey(q->workers_by_pool);
	while(hash_table_nextkey(q->workers_by_pool, &key, (void **) &pi)) {
		buffer_printf(b,"%s:%d ",pi->name,pi->count);
	}

	size_t length;
	char *result;
	const char * buffer_string = buffer_tostring(b,&length);
	if(buffer_string) {
		result = xxstrdup(buffer_string);
	} else {
		result = xxmalloc(4 * sizeof(char));
		strncpy(result, "n/a", 4);
	}

	buffer_delete(b);
	return result;
}

void work_queue_get_stats(struct work_queue *q, struct work_queue_stats *s)
{
	INT64_T effective_workers;
	timestamp_t wall_clock_time;

	memset(s, 0, sizeof(*s));
	s->port = q->port;
	s->priority = q->priority;
	s->workers_init = q->workers_in_state[WORKER_STATE_INIT];
	s->workers_ready = q->workers_in_state[WORKER_STATE_READY];
	s->workers_busy = q->workers_in_state[WORKER_STATE_BUSY];
	s->workers_full = q->workers_in_state[WORKER_STATE_FULL];

	s->tasks_waiting = list_size(q->ready_list);
	s->tasks_running = itable_size(q->running_tasks) + itable_size(q->finished_tasks);
	s->tasks_complete = list_size(q->complete_list);
	s->total_tasks_dispatched = q->total_tasks_submitted;
	s->total_tasks_complete = q->total_tasks_complete;
	s->total_workers_joined = q->total_workers_joined;
	s->total_workers_removed = q->total_workers_removed;
	s->total_bytes_sent = q->total_bytes_sent;
	s->total_bytes_received = q->total_bytes_received;
	s->total_send_time = q->total_send_time;
	s->total_receive_time = q->total_receive_time;
	effective_workers = get_num_of_effective_workers(q);
	s->start_time = q->start_time;
	wall_clock_time = timestamp_get() - q->start_time;
	s->efficiency = (long double) (q->total_execute_time) / (wall_clock_time * effective_workers);
	s->idle_percentage = get_idle_percentage(q);
	// Estimate master capacity, i.e. how many workers can this master handle
	s->capacity = q->capacity;
	s->avg_capacity = q->avg_capacity;
	s->total_workers_connected = q->total_workers_connected;
}

void work_queue_specify_log(struct work_queue *q, const char *logfile)
{
	q->logfile = fopen(logfile, "a");
	if(q->logfile) {
		setvbuf(q->logfile, NULL, _IOLBF, 1024); // line buffered, we don't want incomplete lines
		fprintf(q->logfile, "#%16s %25s %25s %25s %25s %25s %25s %25s %25s %25s %25s %25s %25s %25s %25s %25s %25s %25s %25s %25s %25s %25s %25s %25s\n", // header/column labels
			"timestamp", "start_time",
			"workers_init", "workers_ready", "workers_active", "workers_full", // workers
			"tasks_waiting", "tasks_running", "tasks_complete", // tasks
			"total_tasks_dispatched", "total_tasks_complete", "total_workers_joined", "total_workers_connected", // totals
			"total_workers_removed", "total_bytes_sent", "total_bytes_received", "total_send_time", "total_receive_time",
			"efficiency", "idle_percentage", "capacity", "avg_capacity", // other
			"port", "priority");
		log_worker_states(q);
	}
	debug(D_WQ, "log enabled and is being written to %s\n", logfile);
}
