/*
Copyright (C) 2008- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

/*
The following major problems must be fixed before this code
can be released:

- The capacity code assumes one task per worker.

- The log specification need to be updated.

- The details reported to the catalog should be examined.
*/

#include "work_queue.h"
#include "work_queue_protocol.h"
#include "work_queue_internal.h"
#include "work_queue_catalog.h"
#include "work_queue_resources.h"

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
#include "username.h"
#include "create_dir.h"
#include "xxmalloc.h"
#include "load_average.h"
#include "buffer.h"
#include "link_nvpair.h"
#include "rmonitor.h"
#include "copy_stream.h"
#include "random_init.h"
#include "process.h"

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

#define MIN_TIME_LIST_SIZE 20

#define TIME_SLOT_TASK_TRANSFER 0
#define TIME_SLOT_TASK_EXECUTE 1
#define TIME_SLOT_MASTER_IDLE 2
#define TIME_SLOT_APPLICATION 3

#define WORK_QUEUE_APP_TIME_OUTLIER_MULTIPLIER 10

#define WORKER_ADDRPORT_MAX 32
#define WORKER_HASHKEY_MAX 32

#define RESOURCE_MONITOR_TASK_SUMMARY_NAME "cctools-work-queue-%d-resource-monitor-task-%d"

double wq_option_fast_abort_multiplier = -1.0;
int wq_option_scheduler = WORK_QUEUE_SCHEDULE_TIME;
int wq_minimum_transfer_timeout = 3;
int wq_foreman_transfer_timeout = 3600;

struct work_queue {
	char *name;
	int port;
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
	int process_pending_check;

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
	
	double asynchrony_multiplier;
	int asynchrony_modifier;

	char *catalog_host;
	int catalog_port;

	FILE *logfile;
	int keepalive_interval;
	int keepalive_timeout;

	int monitor_mode;
	int monitor_fd;
	char *monitor_exe;

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
	struct work_queue_resources *resources;
	int cores_allocated;
	int memory_allocated;
	int disk_allocated;
	struct hash_table *current_files;
	struct link *link;
	struct itable *current_tasks;
	int finished_tasks;
	INT64_T total_tasks_complete;
	INT64_T total_bytes_transferred;
	timestamp_t total_task_time;
	timestamp_t total_transfer_time;
	timestamp_t start_time;
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

static int start_task_on_worker(struct work_queue *q, struct work_queue_worker *w);

static struct task_statistics *task_statistics_init();
static void add_time_slot(struct work_queue *q, timestamp_t start, timestamp_t duration, int type, timestamp_t * accumulated_time, struct list *time_list);
static void add_task_report(struct work_queue *q, struct work_queue_task *t);
static void update_app_time(struct work_queue *q, timestamp_t last_left_time, int last_left_status);

static int process_workqueue(struct work_queue *q, struct work_queue_worker *w, const char *line);
static int process_result(struct work_queue *q, struct work_queue_worker *w, const char *line, time_t stoptime);
static int process_queue_status(struct work_queue *q, struct work_queue_worker *w, const char *line, time_t stoptime);
static int process_resource(struct work_queue *q, struct work_queue_worker *w, const char *line); 

static int short_timeout = 5;

static timestamp_t link_poll_end; //tracks when we poll link; used to timeout unacknowledged keepalive checks

static int tolerable_transfer_rate_denominator = 10;
static long double minimum_allowed_transfer_rate = 100000;	// 100 KB/s



/******************************************************/
/********** work_queue internal functions *************/
/******************************************************/

static int get_worker_cores(struct work_queue *q, struct work_queue_worker *w) {
	if(w->resources->cores.total)
		return w->resources->cores.total * q->asynchrony_multiplier + q->asynchrony_modifier;
	else
		return 0;
}


static int get_worker_state(struct work_queue *q, struct work_queue_worker *w) {
	if(!strcmp(w->hostname, "unknown")) {
		return WORKER_STATE_INIT;
	} else if(get_worker_cores(q, w) && itable_size(w->current_tasks) == 0 ) {
		return WORKER_STATE_READY;
	} else if(get_worker_cores(q, w) && itable_size(w->current_tasks) > 0) {
		if(get_worker_cores(q, w) > w->cores_allocated || w->resources->disk.total > w->disk_allocated || w->resources->memory.total < w->memory_allocated) {
			return WORKER_STATE_BUSY;
		} else {
			return WORKER_STATE_FULL;
		}
	}
	return WORKER_STATE_NONE;
}

static void update_worker_states(struct work_queue *q) {
	struct work_queue_worker *w;
	char* id;
	
	memset(q->workers_in_state, 0, sizeof(q->workers_in_state));
	
	hash_table_firstkey(q->worker_table);
	while(hash_table_nextkey(q->worker_table, &id, (void**)&w)) {
		q->workers_in_state[get_worker_state(q, w)]++;
	}
}


static void log_worker_states(struct work_queue *q)
{
	struct work_queue_stats s;
	update_worker_states(q);
	
	debug(D_WQ, "workers status -- total: %d, init: %d, ready: %d, busy: %d, full: %d.",
		hash_table_size(q->worker_table),
		q->workers_in_state[WORKER_STATE_INIT],
		q->workers_in_state[WORKER_STATE_READY],
		q->workers_in_state[WORKER_STATE_BUSY],
		q->workers_in_state[WORKER_STATE_FULL]);
		
	if(!q->logfile) return;
	
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
	fprintf(q->logfile, "%25d ", s.total_worker_slots);
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
static int send_worker_msg(struct work_queue_worker *w, const char *fmt, time_t stoptime, ...) 
{
	char debug_msg[2*WORK_QUEUE_LINE_MAX];
	va_list va;
	va_list debug_va;
	
	va_start(va, stoptime);
	
	sprintf(debug_msg, "%s (%s) <-- ", w->hostname, w->addrport);
	strcat(debug_msg, fmt);
	va_copy(debug_va, va);
	vdebug(D_WQ, debug_msg, debug_va);
	
	int result = link_putvfstring(w->link, fmt, stoptime, va);	
	va_end(va);

	return result;  
}

/**
 * This function receives a message from worker and records the time a message is successfully 
 * received. This timestamp is used in keepalive timeout computations. 
 * Its return value is:
 *  0 : a message was received and processed 
 *  1 : a message was received but NOT processed 
 * -1 : failure to read from link or in processing received message
 */
static int recv_worker_msg(struct work_queue *q, struct work_queue_worker *w, char *line, size_t length, time_t stoptime) 
{
	//call link_readline to recieve message from the link	
	int result = link_readline(w->link, line, length, stoptime);
	
	if (result <= 0) {
		return -1;
	}
	
	w->last_msg_recv_time = timestamp_get();

	debug(D_WQ, "%s (%s) --> %s", w->hostname, w->addrport, line);
	
	// Check for status updates that can be consumed here.
	if(string_prefix_is(line, "alive")) {
		result = 0;	
	} else if(string_prefix_is(line, "workqueue")) {
		result = process_workqueue(q, w, line);
	} else if (string_prefix_is(line,"result")) {
		result = process_result(q, w, line, stoptime);
	} else if (string_prefix_is(line,"queue_status") || string_prefix_is(line, "worker_status") || string_prefix_is(line, "task_status")) {
		result = process_queue_status(q, w, line, stoptime);
	} else if (string_prefix_is(line, "resource")) {
		result = process_resource(q, w, line);
	} else if (string_prefix_is(line, "auth")) {
		debug(D_WQ|D_NOTICE,"worker (%s) is attempting to use a password, but I do not have one.",w->addrport);
		result = -1;
	} else if (string_prefix_is(line,"ready")) {
		debug(D_WQ|D_NOTICE,"worker (%s) is an older worker that is not compatible with this master.",w->addrport);
		result = -1;
	} else {
		// Message is not a status update: return it to the user.
		return 1;
	}

	return result; 
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

static timestamp_t get_transfer_wait_time(struct work_queue *q, struct work_queue_worker *w, int taskid, INT64_T length)
{
	timestamp_t timeout;
	long double avg_queue_transfer_rate, avg_worker_transfer_rate, retry_transfer_rate, tolerable_transfer_rate;
	INT64_T total_tasks_complete, total_tasks_running, total_tasks_waiting, num_of_free_workers;
	struct work_queue_task *t = NULL;
	
	t = itable_lookup(w->current_tasks, taskid);

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
		if(t && t->total_bytes_transferred) {
			avg_queue_transfer_rate = (long double) (q->total_bytes_sent + q->total_bytes_received) / (q->total_send_time + q->total_receive_time) * 1000000;
			retry_transfer_rate = (long double) length / t->total_bytes_transferred * avg_queue_transfer_rate;
		}
	}

	tolerable_transfer_rate = MAX(avg_worker_transfer_rate / tolerable_transfer_rate_denominator, retry_transfer_rate);
	tolerable_transfer_rate = MAX(minimum_allowed_transfer_rate, tolerable_transfer_rate);

	if(!strcmp(w->os, "foreman")) {
		timeout = MAX(wq_foreman_transfer_timeout, length / tolerable_transfer_rate);
	} else {
		timeout = MAX(wq_minimum_transfer_timeout, length / tolerable_transfer_rate);	// try at least wq_minimum_transfer_timeout seconds
	}

	debug(D_WQ, "%s (%s) will try up to %lld seconds for the transfer of this %.3Lf MB file.", w->hostname, w->addrport, (long long) timeout, (long double) length / 1000000);
	return timeout;
}

static void update_catalog(struct work_queue *q, struct link *master, int force_update )
{
	struct work_queue_stats s;
	char addrport[WORK_QUEUE_LINE_MAX];
	static time_t last_update_time = 0;

	if(!force_update) {
		if(time(0) - last_update_time < WORK_QUEUE_CATALOG_MASTER_UPDATE_INTERVAL) return;
	}

	if(!q->catalog_host) {
		q->catalog_host = strdup(CATALOG_HOST);
	}

	if(!q->catalog_port) {
		q->catalog_port = CATALOG_PORT;
	}
	work_queue_get_stats(q, &s);
	struct work_queue_resources r;
	memset(&r, 0, sizeof(r));
	work_queue_get_resources(q,&r);
	debug(D_WQ,"Updating catalog with resource information -- cores:%d memory:%d disk:%d\n", r.cores.total,r.memory.total,r.disk.total); //see if information is being passed correctly
	char * worker_summary = work_queue_get_worker_summary(q);

	if(master) {
		int port;
		link_address_remote(master, addrport, &port);
		sprintf(addrport, "%s:%d", addrport, port);
	} else {
		sprintf(addrport, "127.0.0.1:-1"); //this master has no master
	}

	advertise_master_to_catalog(q->catalog_host, q->catalog_port, q->name, addrport, &s, &r, worker_summary);
	free(worker_summary);

	last_update_time = time(0);
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
		hash_table_firstkey(w->current_files);
	}

	itable_firstkey(w->current_tasks);
	while(itable_nextkey(w->current_tasks, &taskid, (void **)&t)) {
		if(t->result & WORK_QUEUE_RESULT_INPUT_MISSING || t->result & WORK_QUEUE_RESULT_OUTPUT_MISSING || t->result & WORK_QUEUE_RESULT_FUNCTION_FAIL) {
			list_push_head(q->complete_list, t);
		} else {
			t->result = WORK_QUEUE_RESULT_UNSET;
			t->total_bytes_transferred = 0;
			t->total_transfer_time = 0;
			t->cmd_execution_time = 0;
			if(t->output) {
				free(t->output);
			}
			t->output = 0;
			if(t->unlabeled) {
				t->cores = t->memory = t->disk = -1;
			}
			list_push_head(q->ready_list, t);
		}
		itable_remove(q->running_tasks, t->taskid);
		itable_remove(q->finished_tasks, t->taskid);
		itable_remove(q->worker_task_map, t->taskid);
	}
	itable_clear(w->current_tasks);
	w->finished_tasks = 0;
}

static void remove_worker(struct work_queue *q, struct work_queue_worker *w)
{
	if(!q || !w) return;

	debug(D_WQ, "worker %s (%s) removed", w->hostname, w->addrport);

	q->total_workers_removed++;

	cleanup_worker(q, w);

	hash_table_remove(q->worker_table, w->hashkey);
	
	log_worker_states(q);
	
	if(w->link)
		link_close(w->link);

	itable_delete(w->current_tasks);
	hash_table_delete(w->current_files);
	work_queue_resources_delete(w->resources);
	free(w->hostname);
	free(w->os);
	free(w->arch);
	free(w->version);
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
	w->hostname = strdup("unknown");
	w->os = strdup("unknown");
	w->arch = strdup("unknown");
	w->version = strdup("unknown");
	w->link = link;
	w->current_files = hash_table_create(0, 0);
	w->current_tasks = itable_create(0);
	w->finished_tasks = 0;
	w->start_time = timestamp_get();
	w->resources = work_queue_resources_create();
	link_to_hash_key(link, w->hashkey);
	sprintf(w->addrport, "%s:%d", addr, port);
	hash_table_insert(q->worker_table, w->hashkey, w);
	log_worker_states(q);

	debug(D_WQ, "%d workers are connected in total now", hash_table_size(q->worker_table));

	q->total_workers_joined++;

	return 1;
}

/**
 * This function implements the "get %s" protocol.
 * It reads a streamed item from a worker. For the stream format, please refer
 * to the stream_output_item function in worker.c
 */
static int get_output_item(char *remote_name, int flags, char *local_name, struct work_queue *q, struct work_queue_worker *w, struct work_queue_task *t, struct hash_table *received_items, INT64_T * total_bytes)
{
	char line[WORK_QUEUE_LINE_MAX];
	int fd;
	INT64_T actual, length;
	time_t stoptime;
	timestamp_t effective_stoptime = 0;
	char type[256];
	char tmp_remote_name[WORK_QUEUE_LINE_MAX], tmp_local_name[WORK_QUEUE_LINE_MAX];
	char *cur_pos, *tmp_pos;
	int remote_name_len;
	int local_name_len;
	int recv_msg_result;

	if(hash_table_lookup(received_items, local_name))
		return 1;

	debug(D_WQ, "%s (%s) sending back %s to %s", w->hostname, w->addrport, remote_name, local_name);
	send_worker_msg(w, "get %s 1 %d\n", time(0) + short_timeout, remote_name, flags);

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
					debug(D_WQ, "Receiving file %s (size: %"PRId64" bytes) from %s (%s) ...", tmp_local_name, length, w->addrport, w->hostname);
					fd = open(tmp_local_name, O_WRONLY | O_TRUNC | O_CREAT, 0700);
					if(fd < 0) {
						debug(D_NOTICE, "Cannot open file %s for writing: %s", tmp_local_name, strerror(errno));
						goto failure;
					}
					
					if(q->bandwidth) {
						effective_stoptime = ((length * 8)/q->bandwidth)*1000000 + timestamp_get();
					}
					stoptime = time(0) + get_transfer_wait_time(q, w, t->taskid, length);
					actual = link_stream_to_fd(w->link, fd, length, stoptime);
					close(fd);
					if(actual != length) {
						debug(D_WQ, "Received item size (%"PRId64") does not match the expected size - %"PRId64" bytes.", actual, length);
						unlink(local_name);
						goto failure;
					}
					*total_bytes += length;
					timestamp_t current_time = timestamp_get();
					if(effective_stoptime && effective_stoptime > current_time) {
						usleep(effective_stoptime - current_time);
					}

					hash_table_insert(received_items, tmp_local_name, xxstrdup(tmp_local_name));
				} else {
					debug(D_NOTICE, "%s on %s (%s) has invalid length: %"PRId64, remote_name, w->addrport, w->hostname, length);
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
				debug(D_WQ, "Invalid get line - %s\n", line);
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
	//  contents(files/dirs) in the output files list. So, when we emit get
	//  command, we would first encounter top level dirs. Also, we would record
	//  every received files/dirs within those top level dirs. If any file/dir
	//  in those top level dirs appears later in the output files list, we
	//  won't transfer it again.
	list_sort(t->output_files, filename_comparator);

	if(t->output_files) {
		list_first_item(t->output_files);
		while((tf = list_next_item(t->output_files))) {
			
			char remote_name[WORK_QUEUE_LINE_MAX];
			
			if(!(tf->flags & WORK_QUEUE_CACHE)) {
				sprintf(remote_name, "%s.%d", tf->remote_name, t->taskid);
			} else {
				sprintf(remote_name, "%s", tf->remote_name);
			}

			
			if(tf->flags & WORK_QUEUE_THIRDPUT) {

				debug(D_WQ, "thirdputting %s as %s", tf->remote_name, tf->payload);

				if(!strcmp(tf->remote_name, tf->payload)) {
					debug(D_WQ, "output file %s already on shared filesystem", tf->remote_name);
					tf->flags |= WORK_QUEUE_PREEXIST;
				} else {
					char thirdput_result[WORK_QUEUE_LINE_MAX];
					debug(D_WQ, "putting %s from %s (%s) to shared filesystem from %s", tf->remote_name, w->hostname, w->addrport, tf->payload);
					open_time = timestamp_get();
					send_worker_msg(w, "thirdput %d %d %s %s\n", time(0) + short_timeout, WORK_QUEUE_FS_PATH, tf->flags, remote_name, tf->payload);
					//call recv_worker_msg until it returns non-zero which indicates failure or a non-keepalive message is left to consume
					do {
						recv_msg_result = recv_worker_msg(q, w, thirdput_result, WORK_QUEUE_LINE_MAX, time(0) + short_timeout);
						if(recv_msg_result < 0)
							return 0;
					} while (recv_msg_result == 0);
					
					if(sscanf(thirdput_result, "thirdput-complete %d", &recv_msg_result)) {
						if(!recv_msg_result) return 0;
					} else {
						debug(D_WQ, "Error: invalid message received (%s)\n", thirdput_result);
						return 0;
					}
					close_time = timestamp_get();
					sum_time += (close_time - open_time);
				}
			} else if(tf->type == WORK_QUEUE_REMOTECMD) {
				char thirdput_result[WORK_QUEUE_LINE_MAX];
				debug(D_WQ, "putting %s from %s (%s) to remote filesystem using %s", tf->remote_name, w->hostname, w->addrport, tf->payload);
				open_time = timestamp_get();
				send_worker_msg(w, "thirdput %d %d %s %s\n", time(0) + short_timeout, WORK_QUEUE_FS_CMD, tf->flags, remote_name, tf->payload);
				//call recv_worker_msg until it returns non-zero which indicates failure or a non-keepalive message is left to consume

				do {
					recv_msg_result = recv_worker_msg(q, w, thirdput_result, WORK_QUEUE_LINE_MAX, time(0) + short_timeout);
					if(recv_msg_result < 0)
						return 0;
				} while (recv_msg_result == 0);
				
				if(sscanf(thirdput_result, "thirdput-complete %d", &recv_msg_result)) {
					if(!recv_msg_result) return 0;
				} else {
					debug(D_WQ, "Error: invalid message received (%s)\n", thirdput_result);
					return 0;
				}
					
				close_time = timestamp_get();
				sum_time += (close_time - open_time);
			} else {
				
				open_time = timestamp_get();
				get_output_item(remote_name, tf->flags, tf->payload, q, w, t, received_items, &total_bytes);
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
					debug(D_WQ, "Got %"PRId64" bytes from %s (%s) in %.03lfs (%.02lfs Mbps) average %.02lfs Mbps", total_bytes, w->hostname, w->addrport, sum_time / 1000000.0, ((8.0 * total_bytes) / sum_time),
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
	
	// tell the worker you no longer need that task's output directory.
	send_worker_msg(w, "kill %d\n", time(0) + short_timeout, t->taskid);

	return 1;
}

// Sends "unlink file" for every file in the list except those that match one or more of the "except_flags"
static void delete_worker_files(struct work_queue_worker *w, struct list *files, int taskid, int except_flags) {
	struct work_queue_file *tf;
	
	if(!files) return;

	list_first_item(files);
	while((tf = list_next_item(files))) {
		if(!(tf->flags & except_flags)) {
			char remote_name[WORK_QUEUE_LINE_MAX];
			
			if(!(tf->flags & WORK_QUEUE_CACHE)) {
				sprintf(remote_name, "%s.%d", tf->remote_name, taskid);
			} else {
				sprintf(remote_name, "%s", tf->remote_name);
			}
			send_worker_msg(w, "unlink %s %d\n", time(0) + short_timeout, remote_name, tf->flags);
		}
	}
}


static void delete_uncacheable_files(struct work_queue_task *t, struct work_queue_worker *w)
{
	delete_worker_files(w, t->input_files, t->taskid, WORK_QUEUE_CACHE | WORK_QUEUE_PREEXIST);
	delete_worker_files(w, t->output_files, t->taskid, WORK_QUEUE_CACHE | WORK_QUEUE_PREEXIST);
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
	
	msg = string_format("# Work Queue pid: %d Task: %d\nsummary:", getpid(), t->taskid);
	write(q->monitor_fd, msg, strlen(msg));
	free(msg);

	if( (fsummary = fopen(summary, "r")) == NULL )
	{
		msg = string_format("# Summary for task %d:%d was not available.\n", getpid(), t->taskid);
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
		goto failure;
	}
	t->time_receive_output_finish = timestamp_get();

	delete_uncacheable_files(t, w);

	// At this point, a task is completed.
	itable_remove(w->current_tasks, taskid);
	itable_remove(q->finished_tasks, t->taskid);
	list_push_head(q->complete_list, t);
	itable_remove(q->worker_task_map, t->taskid);
	w->finished_tasks--;
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

static int process_workqueue(struct work_queue *q, struct work_queue_worker *w, const char *line)
{
	char items[4][WORK_QUEUE_LINE_MAX];
	int worker_protocol;

	int n = sscanf(line,"workqueue %d %s %s %s %s",&worker_protocol,items[0],items[1],items[2],items[3]);
	if(n!=5) return -1;

	if(worker_protocol!=WORK_QUEUE_PROTOCOL_VERSION) {
		debug(D_WQ|D_NOTICE,"worker (%s) is using work queue protocol %d, but I am using protocol %d",w->addrport,worker_protocol,WORK_QUEUE_PROTOCOL_VERSION);
		return -1;
	}

	w->hostname = strdup(items[0]);
	w->os       = strdup(items[1]);
	w->arch     = strdup(items[2]);
	w->version  = strdup(items[3]);

	log_worker_states(q);
	q->total_workers_connected++;
	debug(D_WQ, "%s (%s) running CCTools version %s on %s (operating system) with architecture %s is ready", w->hostname, w->addrport, w->version, w->os, w->arch);
	
	if(strcmp(CCTOOLS_VERSION, w->version)) {
		debug(D_DEBUG, "Warning: potential worker version mismatch: worker %s (%s) is version %s, and master is version %s", w->hostname, w->addrport, w->version, CCTOOLS_VERSION);
	}
	
	return 0;
}

static int process_result(struct work_queue *q, struct work_queue_worker *w, const char *line, time_t stoptime) {

	if(!q || !w || !line) return -1; 

	int result;
	UINT64_T taskid;
	INT64_T output_length;
	timestamp_t execution_time;
	struct work_queue_task *t;
	int actual;
	timestamp_t observed_execution_time;
	timestamp_t effective_stoptime = 0;

	//Format: result, output length, execution time, taskid
	char items[3][WORK_QUEUE_PROTOCOL_FIELD_MAX];
	int n = sscanf(line, "result %s %s %s %" SCNd64, items[0], items[1], items[2], &taskid);


	if(n < 4) {
		debug(D_WQ, "Invalid message from worker %s (%s): %s", w->hostname, w->addrport, line);
		return -1;
	}
	
	result = atoi(items[0]);
	output_length = atoll(items[1]);
	
	t = itable_lookup(w->current_tasks, taskid);
	if(!t) {
		debug(D_WQ, "Unknown task result from worker %s (%s): no task %d assigned to worker.  Ignoring result.", w->hostname, w->addrport, taskid);
		stoptime = time(0) + get_transfer_wait_time(q, w, -1, (INT64_T) output_length);
		link_soak(w->link, output_length, stoptime);
		return 0;
	}
	
	observed_execution_time = timestamp_get() - t->time_execute_cmd_start;
	
	if(q->bandwidth) {
		effective_stoptime = ((output_length * 8)/q->bandwidth)*1000000 + timestamp_get();
	}

	if(n >= 3) {
		execution_time = atoll(items[2]);
		t->cmd_execution_time = observed_execution_time > execution_time ? execution_time : observed_execution_time;
	} else {
		t->cmd_execution_time = observed_execution_time;
	}

	t->output = malloc(output_length + 1);
	if(output_length > 0) {
		debug(D_WQ, "Receiving stdout of task %ld (size: %"PRId64" bytes) from %s (%s) ...", (long int)taskid, output_length, w->addrport, w->hostname);
		stoptime = time(0) + get_transfer_wait_time(q, w, t->taskid, (INT64_T) output_length);
		actual = link_read(w->link, t->output, output_length, stoptime);
		if(actual != output_length) {
			debug(D_WQ, "Failure: actual received stdout size (%"PRId64" bytes) is different from expected (%"PRId64" bytes).", actual, output_length);
			t->output[actual] = '\0';
			return -1;
		}
		timestamp_t current_time = timestamp_get();
		if(effective_stoptime && effective_stoptime > current_time) {
			usleep(effective_stoptime - current_time);
		}
		debug(D_WQ, "Got %"PRId64" bytes from %s (%s)", actual, w->hostname, w->addrport);
		
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


	w->cores_allocated -= t->cores;
	w->memory_allocated -= t->memory;
	w->disk_allocated -= t->disk;
	
	if(t->unlabeled) {
		t->cores = t->memory = t->disk = -1;
	}

	w->finished_tasks++;

	log_worker_states(q);

	return 0;
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

	struct work_queue_resources r;
	work_queue_get_resources(q,&r);
	work_queue_resources_add_to_nvpair(&r,nv);

	return nv;
}



struct nvpair * worker_to_nvpair( struct work_queue *q, struct work_queue_worker *w )
{
	struct nvpair *nv = nvpair_create();
	if(!nv) return 0;

	nvpair_insert_string(nv,"state",work_queue_state_names[get_worker_state(q, w)]);
	nvpair_insert_string(nv,"hostname",w->hostname);
	nvpair_insert_string(nv,"os",w->os);
	nvpair_insert_string(nv,"arch",w->arch);
	nvpair_insert_string(nv,"address_port",w->addrport);
	nvpair_insert_integer(nv,"ncpus",w->resources->cores.total);
	nvpair_insert_integer(nv,"total_tasks_complete",w->total_tasks_complete);
	nvpair_insert_integer(nv,"total_bytes_transferred",w->total_bytes_transferred);
	nvpair_insert_integer(nv,"total_transfer_time",w->total_transfer_time);
	nvpair_insert_integer(nv,"start_time",w->start_time);
	nvpair_insert_integer(nv,"current_time",timestamp_get()); 

	work_queue_resources_add_to_nvpair(w->resources,nv);

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
			// If the worker has not been initializd, ignore it.
			if(!strcmp(w->hostname, "unknown")) continue;
			nv = worker_to_nvpair(q, w);
			if(nv) {
				link_nvpair_write(l,nv,stoptime);
				nvpair_delete(nv);
			}
		}
	}

	link_write(l, "\n", 1, stoptime);
	return 0;
}

static int process_resource( struct work_queue *q, struct work_queue_worker *w, const char *line )
{
	char category[WORK_QUEUE_LINE_MAX];
	struct work_queue_resource r;
	
	if(sscanf(line, "resource %s %d %d %d %d", category, &r.inuse,&r.total,&r.smallest,&r.largest)==5) {

		if(!strcmp(category,"cores")) {
			w->resources->cores = r;
		} else if(!strcmp(category,"memory")) {
			w->resources->memory = r;
		} else if(!strcmp(category,"disk")) {
			w->resources->disk = r;
		} else if(!strcmp(category,"workers")) {
			w->resources->workers = r;
		}

		if(w->cores_allocated) {
			log_worker_states(q);
		}
	}

	return 0;
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

static int build_poll_table(struct work_queue *q, struct link *master)
{
	int n = 0;
	char *key;
	struct work_queue_worker *w;

	// Allocate a small table, if it hasn't been done yet.
	if(!q->poll_table) {
		q->poll_table = malloc(sizeof(*q->poll_table) * q->poll_table_size);
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
		}

		q->poll_table[n].link = w->link;
		q->poll_table[n].events = LINK_READ;
		q->poll_table[n].revents = 0;
		n++;
	}

	return n;
}

static int put_file(const char *localname, const char *remotename, off_t offset, INT64_T length, struct work_queue *q, struct work_queue_worker *w, int taskid, INT64_T *total_bytes, int flags){
	struct stat local_info;
	time_t stoptime;
	timestamp_t effective_stoptime = 0;
	INT64_T actual = 0;
	
	if(stat(localname, &local_info) < 0)
		return 0;

	/* normalize the mode so as not to set up invalid permissions */
	local_info.st_mode |= 0600;
	local_info.st_mode &= 0777;
	
	if(!length) {
		length = local_info.st_size;
	}
	
	debug(D_WQ, "%s (%s) needs file %s bytes %lld:%lld as '%s'", w->hostname, w->addrport, localname, (long long) offset, (long long) offset+length, remotename);
	int fd = open(localname, O_RDONLY, 0);
	if(fd < 0)
		return 0;

	//We want to send bytes starting from 'offset'. So seek to it first.
	if (offset >= 0 && (offset+length) <= local_info.st_size) {
		if(lseek(fd, offset, SEEK_SET) == -1) {
			close(fd);
			return 0;
		}
	} else {
		debug(D_NOTICE, "File specification %s (%lld:%lld) is invalid", localname, (long long) offset, (long long) offset+length);
		close(fd);
		return 0;
	}
	
	struct work_queue_task *t = itable_lookup(q->running_tasks, taskid);
	
	if(q->bandwidth) {
		effective_stoptime = ((length * 8)/q->bandwidth)*1000000 + timestamp_get();
	}
	
	stoptime = time(0) + get_transfer_wait_time(q, w, t->taskid, length);
	send_worker_msg(w, "put %s %"PRId64" 0%o %d\n", time(0) + short_timeout, remotename, length, local_info.st_mode, flags);
	actual = link_stream_from_fd(w->link, fd, length, stoptime);
	close(fd);
	
	if(actual != length)
		return 0;
		
	timestamp_t current_time = timestamp_get();
	if(effective_stoptime && effective_stoptime > current_time) {
		usleep(effective_stoptime - current_time);
	}
	
	*total_bytes += actual;
	return 1;
}

static int put_directory(const char *dirname, const char *remotedirname, struct work_queue *q, struct work_queue_worker *w, int taskid, INT64_T * total_bytes, int flags) {
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

	// When putting a file its parent directories are automatically
	// created by the worker, so no need to manually create them.

	while((file = readdir(dir))) {
		char *filename = file->d_name;

		if(!strcmp(filename, ".") || !strcmp(filename, "..")) {
			continue;
		}

		*buffer = '\0';
		sprintf(buffer, "%s/%s", dirname, filename);
		localname = xxstrdup(buffer);

		*buffer = '\0';
		sprintf(buffer, "%s/%s", remotedirname, filename);
		remotename = xxstrdup(buffer);
	
		if(stat(localname, &local_info) < 0) {
			closedir(dir);
			return 0;
		}	
		
		if(local_info.st_mode & S_IFDIR)  {
			result = put_directory(localname, remotename, q, w, taskid, total_bytes, flags);	
		} else {
			result = put_file(localname, remotename, 0, 0, q, w, taskid, total_bytes, flags);
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
		char remote_name[WORK_QUEUE_LINE_MAX];
		
		if(remote_info) {
			hash_table_remove(w->current_files, hash_name);
			free(remote_info);
		}
		
		if(!(tf->flags & WORK_QUEUE_CACHE)) {
			sprintf(remote_name, "%s.%d", tf->remote_name, taskid);
		} else {
			sprintf(remote_name, "%s", tf->remote_name);
		}

		if(dir) {
			if(!put_directory(payload, remote_name, q, w, taskid, total_bytes, tf->flags))
				return 0;
		} else {
			if(!put_file(payload, remote_name, tf->offset, tf->piece_length, q, w, taskid, total_bytes, tf->flags))
				return 0;
		}
		
		if(tf->flags & WORK_QUEUE_CACHE) {
			remote_info = malloc(sizeof(*remote_info));
			memcpy(remote_info, &local_info, sizeof(local_info));
			hash_table_insert(w->current_files, hash_name, remote_info);
		}
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

static int send_input_files(struct work_queue_task *t, struct work_queue_worker *w, struct work_queue *q)
{
	struct work_queue_file *tf;
	INT64_T actual = 0;
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
			char remote_name[WORK_QUEUE_LINE_MAX];
			timestamp_t effective_stoptime;
			
			if(!(tf->flags & WORK_QUEUE_CACHE)) {
				sprintf(remote_name, "%s.%d", tf->remote_name, t->taskid);
			} else {
				sprintf(remote_name, "%s", tf->remote_name);
			}
			
			
			switch(tf->type) {
			
			case WORK_QUEUE_BUFFER:
				effective_stoptime = 0;
				debug(D_WQ, "%s (%s) needs literal as %s", w->hostname, w->addrport, tf->remote_name);
				fl = tf->length;

				if(q->bandwidth) {
					effective_stoptime = ((tf->length * 8)/q->bandwidth)*1000000 + timestamp_get();
				}
				
				stoptime = time(0) + get_transfer_wait_time(q, w, t->taskid, (INT64_T) fl);
				open_time = timestamp_get();
				send_worker_msg(w, "put %s %"PRId64" %o %d\n", time(0) + short_timeout, remote_name, (INT64_T) fl, 0777, tf->flags);
				actual = link_putlstring(w->link, tf->payload, fl, stoptime);
				timestamp_t current_time = timestamp_get();
				if(effective_stoptime && effective_stoptime > current_time) {
					usleep(effective_stoptime - current_time);
				}
				close_time = timestamp_get();
				if(actual != (fl))
					goto failure;
				total_bytes += actual;
				sum_time += (close_time - open_time);
			
				break;
			
			case WORK_QUEUE_REMOTECMD:
				debug(D_WQ, "%s (%s) needs %s from remote filesystem using %s", w->hostname, w->addrport, tf->remote_name, tf->payload);
				open_time = timestamp_get();
				send_worker_msg(w, "thirdget %d %d %s %s\n", time(0) + short_timeout, WORK_QUEUE_FS_CMD, tf->flags, remote_name, tf->payload);
				close_time = timestamp_get();
				sum_time += (close_time - open_time);
			
				break;

			case WORK_QUEUE_URL:
			{
				char remote_name[WORK_QUEUE_LINE_MAX];
				if(!(tf->flags & WORK_QUEUE_CACHE)) {
					sprintf(remote_name, "%s.%d", tf->remote_name, t->taskid);
				} else {
					sprintf(remote_name, "%s", tf->remote_name);
				}
				debug(D_WQ, "%s (%s) needs %s from the url, %s %d", w->hostname, w->addrport, remote_name, tf->payload, tf->length);
				open_time = timestamp_get();
				send_worker_msg(w, "url %s %lld 0%o %d\n",time(0) + short_timeout, remote_name, tf->length, 0777, tf->flags);
				link_putlstring(w->link, tf->payload, tf->length, stoptime);
				close_time = timestamp_get();
				sum_time += (close_time - open_time);

				break;
			}
			case WORK_QUEUE_DIRECTORY:
				// Do nothing.  Empty directories are handled by the task specification, while recursive directories are implemented as WORK_QUEUE_FILEs
				break;
			
			default:
				if(tf->flags & WORK_QUEUE_THIRDGET) {
					debug(D_WQ, "%s (%s) needs %s from shared filesystem as %s", w->hostname, w->addrport, tf->payload, tf->remote_name);

					if(!strcmp(tf->remote_name, tf->payload)) {
						tf->flags |= WORK_QUEUE_PREEXIST;
					} else {
						open_time = timestamp_get();
						if(tf->flags & WORK_QUEUE_SYMLINK) {
							send_worker_msg(w, "thirdget %d %d %s %s\n", time(0) + short_timeout, WORK_QUEUE_FS_SYMLINK, tf->flags, remote_name, tf->payload);
						} else {
							send_worker_msg(w, "thirdget %d %d %s %s\n", time(0) + short_timeout, WORK_QUEUE_FS_PATH, tf->flags, remote_name, tf->payload);
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
				break;
			}
		}
		t->total_bytes_transferred += total_bytes;
		t->total_transfer_time += sum_time;
		w->total_bytes_transferred += total_bytes;
		w->total_transfer_time += sum_time;
		if(total_bytes > 0) {
			q->total_bytes_sent += (INT64_T) total_bytes;
			q->total_send_time += sum_time;
			debug(D_WQ, "%s (%s) got %"PRId64" bytes in %.03lfs (%.02lfs Mbps) average %.02lfs Mbps", w->hostname, w->addrport, total_bytes, sum_time / 1000000.0, ((8.0 * total_bytes) / sum_time),
			      (8.0 * w->total_bytes_transferred) / w->total_transfer_time);
		}
	}

	return 1;

      failure:
	if(tf->type == WORK_QUEUE_FILE || tf->type == WORK_QUEUE_FILE_PIECE)
		debug(D_WQ, "%s (%s) failed to send %s (%"PRId64" bytes received).", w->hostname, w->addrport, tf->payload, actual);
	else
		debug(D_WQ, "%s (%s) failed to send literal data (%"PRId64" bytes received).", w->hostname, w->addrport, actual);
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
	t->time_send_input_finish = timestamp_get();
	t->time_execute_cmd_start = timestamp_get();
	t->hostname = xxstrdup(w->hostname);
	t->host = xxstrdup(w->addrport);
	
	send_worker_msg(w, "task %lld\n",  time(0) + short_timeout, (long long) t->taskid);
	send_worker_msg(w, "cmd %lld\n%s", time(0) + short_timeout, (long long) strlen(t->command_line), t->command_line);
	send_worker_msg(w, "cores %d\n",   time(0) + short_timeout, t->cores );
	send_worker_msg(w, "memory %d\n",  time(0) + short_timeout, t->memory );
	send_worker_msg(w, "disk %d\n",    time(0) + short_timeout, t->disk );

	if(t->input_files) {
		struct work_queue_file *tf;
		list_first_item(t->input_files);
		while((tf = list_next_item(t->input_files))) {
			char remote_name[WORK_QUEUE_LINE_MAX];
			
			if(tf->type == WORK_QUEUE_DIRECTORY) {
				send_worker_msg(w, "dir %s\n", time(0) + short_timeout, tf->remote_name);
				continue;
			}
			
			if(!(tf->flags & WORK_QUEUE_CACHE)) {
				sprintf(remote_name, "%s.%d", tf->remote_name, t->taskid);
			} else {
				sprintf(remote_name, "%s", tf->remote_name);
			}
			
			send_worker_msg(w, "infile %s %s %d\n", time(0) + short_timeout, remote_name, tf->remote_name, tf->flags);
		}
	}

	if(t->output_files) {
		struct work_queue_file *tf;
		list_first_item(t->output_files);
		while((tf = list_next_item(t->output_files))) {
			char remote_name[WORK_QUEUE_LINE_MAX];
			if(!(tf->flags & WORK_QUEUE_CACHE)) {
				sprintf(remote_name, "%s.%d", tf->remote_name, t->taskid);
			} else {
				sprintf(remote_name, "%s", tf->remote_name);
			}
			send_worker_msg(w, "outfile %s %s %d\n", time(0) + short_timeout, remote_name, tf->remote_name, tf->flags);
		}
	}

	send_worker_msg(w, "end\n", time(0) + short_timeout );

	debug(D_WQ, "%s (%s) busy on '%s'", w->hostname, w->addrport, t->command_line);
	return 1;
}

static int get_num_of_effective_workers(struct work_queue *q)
{
	update_worker_states(q);
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
	debug(D_WQ, "Avg task execution time: %lld; Avg task tranfer time: %lld; Avg task app time: %lld\n", (long long) avg_task_execution_time, (long long) avg_task_transfer_time, (long long) avg_task_app_time);


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

static int check_worker_against_task(struct work_queue *q, struct work_queue_worker *w, struct work_queue_task *t) {
	int cores_used, disk_used, mem_used, ok = 1;
	
	// If none of the resources used have not been specified, treat the task as consuming an entire "average" worker
	if(t->cores < 0 && t->memory < 0 && t->disk < 0) {
		cores_used = MAX((double)w->resources->cores.total/(double)w->resources->workers.total, 1);
		mem_used = MAX((double)w->resources->memory.total/(double)w->resources->workers.total, 0);
		disk_used = MAX((double)w->resources->disk.total/(double)w->resources->workers.total, 0);
	} else {
		// Otherwise use any values given, and assume the task will take "whatever it can get" for unlabled resources
		cores_used = MAX(t->cores, 0);
		mem_used = MAX(t->memory, 0);
		disk_used = MAX(t->disk, 0);
	}
	
	if(w->cores_allocated + cores_used > get_worker_cores(q, w)) {
		ok = 0;
	}
	
	if(w->memory_allocated + mem_used > w->resources->memory.total) {
		ok = 0;
	}
	
	if(w->disk_allocated + disk_used > w->resources->disk.total) {
		ok = 0;
	}
	
	return ok;
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
		if( check_worker_against_task(q, w, t) ) {
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

static struct work_queue_worker *find_worker_by_fcfs(struct work_queue *q, struct work_queue_task *t)
{
	char *key;
	struct work_queue_worker *w;
	hash_table_firstkey(q->worker_table);
	while(hash_table_nextkey(q->worker_table, &key, (void**)&w)) {
		if( check_worker_against_task(q, w, t) ) {
			return w;
		}
	}
	return NULL;
}

static struct work_queue_worker *find_worker_by_random(struct work_queue *q, struct work_queue_task *t)
{
	char *key;
	struct work_queue_worker *w = NULL;
	struct list *valid_workers = list_create();
	int random_worker;

	hash_table_firstkey(q->worker_table);
	while(hash_table_nextkey(q->worker_table, &key, (void**)&w)) {
		if(check_worker_against_task(q, w, t)) {
			list_push_tail(valid_workers, w);
		}
	}

	if(list_size(valid_workers) > 0) {
		random_worker = (rand() % list_size(valid_workers)) + 1;
	} else {
		list_delete(valid_workers);
		return NULL;
	}

	w = NULL;
	while(random_worker && list_size(valid_workers)) {
		w = list_pop_head(valid_workers);
		random_worker--;
	}
	list_delete(valid_workers);
	
	return w;
}

static struct work_queue_worker *find_worker_by_time(struct work_queue *q, struct work_queue_task *t)
{
	char *key;
	struct work_queue_worker *w;
	struct work_queue_worker *best_worker = 0;
	double best_time = HUGE_VAL;

	hash_table_firstkey(q->worker_table);
	while(hash_table_nextkey(q->worker_table, &key, (void **) &w)) {
		if(check_worker_against_task(q, w, t)) {
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
	case WORK_QUEUE_SCHEDULE_RAND:
		return find_worker_by_random(q, t);
	case WORK_QUEUE_SCHEDULE_FCFS:
	default:
		return find_worker_by_fcfs(q, t);
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
		
		//If everything is unspecified, set it to the value of an "average" worker.
		if(t->cores < 0 && t->memory < 0 && t->disk < 0) {
			t->cores = MAX((double)w->resources->cores.total/(double)w->resources->workers.total, 1);
			t->memory = MAX((double)w->resources->memory.total/(double)w->resources->workers.total, 0);
			t->disk = MAX((double)w->resources->disk.total/(double)w->resources->workers.total, 0);
		} else {
			// Otherwise use any values given, and assume the task will take "whatever it can get" for unlabled resources
			t->cores = MAX(t->cores, 0);
			t->memory = MAX(t->memory, 0);
			t->disk = MAX(t->disk, 0);
		}
		
		w->cores_allocated += t->cores;
		w->memory_allocated += t->memory;
		w->disk_allocated += t->disk;
		
		log_worker_states(q);
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

//Sends keepalives to check if connected workers are responsive. If not, removes those workers. 
static void remove_unresponsive_workers(struct work_queue *q) {
	struct work_queue_worker *w;
	char *key;
	int last_recv_elapsed_time;
	timestamp_t current_time = timestamp_get();
	
	hash_table_firstkey(q->worker_table);
	while(hash_table_nextkey(q->worker_table, &key, (void **) &w)) {
		if(q->keepalive_interval > 0) {
			if(!strcmp(w->hostname, "unknown")){ 
				last_recv_elapsed_time = (int)(current_time - w->start_time)/1000000;
			} else {
				last_recv_elapsed_time = (int)(current_time - w->last_msg_recv_time)/1000000;
			}
		
			// send new keepalive check only (1) if we received a response since last keepalive check AND 
			// (2) we are past keepalive interval 
			if(w->last_msg_recv_time >= w->keepalive_check_sent_time) {	
				if(last_recv_elapsed_time >= q->keepalive_interval) {
					if (send_worker_msg(w, "%s\n", time(0) + short_timeout, "check") < 0) {
						debug(D_WQ, "Failed to send keepalive check to worker %s (%s).", w->hostname, w->addrport);
						remove_worker(q, w);
					} else {
						debug(D_WQ, "Sent keepalive check to worker %s (%s)", w->hostname, w->addrport);
						w->keepalive_check_sent_time = current_time;	
					}	
				}
			} else { 
				// we haven't received a message from worker since its last keepalive check. Check if time 
				// since we last polled link for responses has exceeded keepalive timeout. If so, remove worker.
				if (link_poll_end > w->keepalive_check_sent_time) {
					if ((int)((link_poll_end - w->keepalive_check_sent_time)/1000000) >= q->keepalive_timeout) { 
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
				debug(D_WQ, "Discarding outlier task app time: %lld\n", (long long) t1);
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

	struct work_queue_worker *w = itable_lookup(q->worker_task_map, t->taskid);
	
	if (w) {
		//send message to worker asking to kill its task.
		send_worker_msg(w, "%s %d\n", time(0) + short_timeout, "kill", t->taskid);
		//update table.
		itable_remove(q->running_tasks, t->taskid);
		itable_remove(q->finished_tasks, t->taskid);
		itable_remove(q->worker_task_map, t->taskid);

		debug(D_WQ, "Task with id %d is aborted at worker %s (%s) and removed.", t->taskid, w->hostname, w->addrport);
			
		//Delete any input files that are not to be cached.
		delete_worker_files(w, t->input_files, t->taskid, WORK_QUEUE_CACHE | WORK_QUEUE_PREEXIST);

		//Delete all output files since they are not needed as the task was aborted.
		delete_worker_files(w, t->output_files, t->taskid, 0);
		
		w->cores_allocated -= t->cores;
		w->memory_allocated -= t->memory;
		w->disk_allocated -= t->disk;
		
		if(t->unlabeled) {
			t->cores = t->memory = t->disk = -1;
		}

		log_worker_states(q);
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

	if(command_line) t->command_line = xxstrdup(command_line);

	t->worker_selection_algorithm = WORK_QUEUE_SCHEDULE_UNSET;
	t->input_files = list_create();
	t->output_files = list_create();
	t->return_status = -1;
	t->result = WORK_QUEUE_RESULT_UNSET;

	/* In the absence of additional information, a task consumes an entire worker. */

	t->memory = -1;
	t->disk = -1;
	t->cores = -1;
	t->unlabeled = 1;

	return t;
}

void work_queue_task_specify_command( struct work_queue_task *t, const char *cmd )
{
	if(t->command_line) free(t->command_line);
	t->command_line = xxstrdup(cmd);
}

void work_queue_task_specify_memory( struct work_queue_task *t, int memory )
{
	t->memory = memory;
	t->unlabeled = 0;
}

void work_queue_task_specify_disk( struct work_queue_task *t, int disk )
{
	t->disk = disk;
	t->unlabeled = 0;
}

void work_queue_task_specify_cores( struct work_queue_task *t, int cores )
{
	t->cores = cores;
	t->unlabeled = 0;
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

int work_queue_task_specify_url(struct work_queue_task *t, const char *file_url, const char *remote_name, int type, int flags)
{
        debug(D_WQ, "work_queue_task_specify_url\n");
        struct list *files;
        struct work_queue_file *tf;

        if(!t || !file_url || !remote_name) {
                return 0;
        }
        if(remote_name[0] == '/') {
                return 0;
        }


        if(type == WORK_QUEUE_INPUT) {
                files = t->input_files;
        } else {
                files = t->output_files;
        }

        list_first_item(files);
        while((tf = (struct work_queue_file*)list_next_item(files))) {
                if(!strcmp(remote_name, tf->remote_name))
                {       return 0;       }
        }

        tf = work_queue_file_create(remote_name, WORK_QUEUE_URL, flags);
        tf->length = strlen(file_url);
        tf->payload = xxstrdup(file_url);

        list_push_tail(files, tf);

        return 1;
}

int work_queue_task_specify_file(struct work_queue_task *t, const char *local_name, const char *remote_name, int type, int flags)
{
	struct list *files;
	struct work_queue_file *tf;
	
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
	
	
	if(type == WORK_QUEUE_INPUT) {
		files = t->input_files;
	} else {
		files = t->output_files;
	}
	
	list_first_item(files);
	while((tf = (struct work_queue_file*)list_next_item(files))) {
		if(!strcmp(remote_name, tf->remote_name))
		{	return 0;	}
	}

	tf = work_queue_file_create(remote_name, WORK_QUEUE_FILE, flags);

	tf->length = strlen(local_name);
	tf->payload = xxstrdup(local_name);

	list_push_tail(files, tf);
	return 1;
}

int work_queue_task_specify_directory(struct work_queue_task *t, const char *local_name, const char *remote_name, int type, int flags, int recursive) {
	struct list *files;
	struct work_queue_file *tf;
	
	if(!t || !remote_name) {
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
	
	if(type == WORK_QUEUE_OUTPUT || recursive) {
		return work_queue_task_specify_file(t, local_name, remote_name, type, flags);
	}
	
	files = t->input_files;
	
	list_first_item(files);
	while((tf = (struct work_queue_file*)list_next_item(files))) {
		if(!strcmp(remote_name, tf->remote_name))
		{	return 0;	}
	}

	tf = work_queue_file_create(remote_name, WORK_QUEUE_DIRECTORY, flags);
	list_push_tail(files, tf);
	return 1;
	
}

int work_queue_task_specify_file_piece(struct work_queue_task *t, const char *local_name, const char *remote_name, off_t start_byte, off_t end_byte, int type, int flags)
{
	struct list *files;
	struct work_queue_file *tf;
	if(!t || !local_name || !remote_name) {
		return 0;
	}

	// @param remote_name should not be an absolute path. @see
	// work_queue_task_specify_file
	if(remote_name[0] == '/') {
		return 0;
	}

	if(end_byte < start_byte) {
		return 0;
	}

	if(type == WORK_QUEUE_INPUT) {
		files = t->input_files;
	} else {
		files = t->output_files;
	}
	
	list_first_item(files);
	while((tf = (struct work_queue_file*)list_next_item(files))) {
		if(!strcmp(remote_name, tf->remote_name))
		{	return 0;	}
	}
	
	tf = work_queue_file_create(remote_name, WORK_QUEUE_FILE_PIECE, flags);

	tf->length = strlen(local_name);
	tf->offset = start_byte;
	tf->piece_length = end_byte - start_byte + 1;
	tf->payload = xxstrdup(local_name);

	list_push_tail(files, tf);
	return 1;
}

int work_queue_task_specify_buffer(struct work_queue_task *t, const char *data, int length, const char *remote_name, int flags)
{
	struct work_queue_file *tf;
	if(!t || !remote_name) {
		return 0;
	}

	// @param remote_name should not be an absolute path. @see
	// work_queue_task_specify_file
	if(remote_name[0] == '/') {
		return 0;
	}

	list_first_item(t->input_files);
	while((tf = (struct work_queue_file*)list_next_item(t->input_files))) {
		if(!strcmp(remote_name, tf->remote_name))
		{	return 0;	}
	}
	
	tf = work_queue_file_create(remote_name, WORK_QUEUE_BUFFER, flags);
	tf->length = length;
	tf->payload = malloc(length);
	memcpy(tf->payload, data, length);
	list_push_tail(t->input_files, tf);

	return 1;
}

int work_queue_task_specify_file_command(struct work_queue_task *t, const char *remote_name, const char *cmd, int type, int flags)
{
	struct list *files;
	struct work_queue_file *tf;
	if(!t || !remote_name || !cmd) {
		return 0;
	}

	// @param remote_name should not be an absolute path. @see
	// work_queue_task_specify_file
	if(remote_name[0] == '/') {
		return 0;
	}

	if(type == WORK_QUEUE_INPUT) {
		files = t->input_files;
	} else {
		files = t->output_files;
	}
	
	list_first_item(files);
	while((tf = (struct work_queue_file*)list_next_item(files))) {
		if(!strcmp(remote_name, tf->remote_name))
		{	return 0;	}
	}
	
	tf = work_queue_file_create(remote_name, WORK_QUEUE_REMOTECMD, flags);
	tf->length = strlen(cmd);
	tf->payload = xxstrdup(cmd);

	list_push_tail(files, tf);
	
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

	getcwd(q->workingdir,PATH_MAX);

	q->ready_list = list_create();
	q->running_tasks = itable_create(0);
	q->finished_tasks = itable_create(0);
	q->complete_list = list_create();

	q->worker_table = hash_table_create(0, 0);
	q->worker_task_map = itable_create(0);
	
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
	q->process_pending_check = 0;

	// Capacity estimation related
	q->start_time = timestamp_get();
	q->time_last_task_start = q->start_time;
	q->idle_times = list_create();
	q->task_statistics = task_statistics_init();

	q->catalog_host = 0;
	q->catalog_port = 0;

	q->keepalive_interval = WORK_QUEUE_DEFAULT_KEEPALIVE_INTERVAL;
	q->keepalive_timeout = WORK_QUEUE_DEFAULT_KEEPALIVE_TIMEOUT; 

	q->monitor_mode   =  0;
	q->password = 0;
	
	q->asynchrony_multiplier = 1.0;
	q->asynchrony_modifier = 0;
	
	if( (envstring  = getenv("WORK_QUEUE_BANDWIDTH")) ) {
		q->bandwidth = string_metric_parse(envstring);
		if(q->bandwidth < 0) {
			q->bandwidth = 0;
		}
	}
	
	debug(D_WQ, "Work Queue is listening on port %d.", q->port);
	return q;

failure:
	debug(D_NOTICE, "Could not create work_queue on port %i.", port);
	free(q);
	return 0;
}

int work_queue_enable_monitoring(struct work_queue *q, char *monitor_summary_file)
{
  if(!q)
    return 0;

  if(q->monitor_mode)
  {
    debug(D_NOTICE, "Monitoring already enabled. Closing old logfile and opening (perhaps) new one.\n");
    if(close(q->monitor_fd))
      debug(D_NOTICE, "Error closing logfile: %s\n", strerror(errno));
  }

  q->monitor_mode = 0;

  q->monitor_exe = resource_monitor_copy_to_wd(NULL);
  if(!q->monitor_exe)
  {
    debug(D_NOTICE, "Could not find the resource monitor executable. Disabling monitor mode.\n");
    return 0;
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
    return 0;
  }

	q->monitor_mode = 1;

	return 1;
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
	return copy_file_to_buffer(file,&(q->password))>0;
}

void work_queue_delete(struct work_queue *q)
{
	if(q) {
		struct work_queue_worker *w;
		char *key;

		hash_table_firstkey(q->worker_table);
		while(hash_table_nextkey(q->worker_table, &key, (void **) &w)) {
			release_worker(q, w);
		}
		if(q->name) {
			update_catalog(q, NULL, 1);
		}
		if(q->catalog_host) free(q->catalog_host);
		hash_table_delete(q->worker_table);
		itable_delete(q->worker_task_map);
		
		list_delete(q->ready_list);
		itable_delete(q->running_tasks);
		itable_delete(q->finished_tasks);
		list_delete(q->complete_list);
		
		list_free(q->idle_times);
		list_delete(q->idle_times);
		task_statistics_destroy(q->task_statistics);
 
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
	
	wrap_cmd = resource_monitor_rewrite_command(t->command_line, summary, NULL, NULL, 1, 0, 0);

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

struct work_queue_task *work_queue_wait_internal(struct work_queue *q, int timeout, struct link *master_link, int *master_active)
{
	struct work_queue_task *t;
	time_t stoptime;

	static timestamp_t last_left_time = 0;
	static int last_left_status = 0;	// 0 -- did not return any done task; 1 -- returned done task 

	print_password_warning(q);

	update_app_time(q, last_left_time, last_left_status);

	if(timeout == WORK_QUEUE_WAITFORTASK) {
		stoptime = 0;
	} else {
		stoptime = time(0) + timeout;
	}

	while(1) {
		if(q->name) {
			update_catalog(q, master_link, 0);
		}
		
		remove_unresponsive_workers(q);	

		t = list_pop_head(q->complete_list);
		if(t) {
			last_left_time = timestamp_get();
			last_left_status = 1;
			return t;
		}
		
		if( q->process_pending_check && process_pending() ) {
			return NULL;
		}

//		debug(D_WQ, "workers_in_state[BUSY] = %d, workers_in_state[FULL] = %d, list_size(ready_list) = %d, aux_links = %x, list_size(aux_links) = %d", q->workers_in_state[WORKER_STATE_BUSY], q->workers_in_state[WORKER_STATE_FULL], list_size(q->ready_list), aux_links, aux_links?list_size(aux_links):0);

		update_worker_states(q);

		if( (q->workers_in_state[WORKER_STATE_BUSY] + q->workers_in_state[WORKER_STATE_FULL]) == 0 && list_size(q->ready_list) == 0 && !(master_link))
			break;

		int n = build_poll_table(q, master_link);

		// Wait no longer than the caller's patience.
		int msec;
		if(stoptime) {
			msec = MAX(0, (stoptime - time(0)) * 1000);
		} else {
			msec = 5000;
		}
		
		// If workers are available and tasks waiting to be dispatched, don't wait on a message.
		if( q->workers_in_state[WORKER_STATE_BUSY] + q->workers_in_state[WORKER_STATE_READY] > 0 && list_size(q->ready_list) > 0 ) {
			msec = 0;
		}

		// Poll all links for activity.
		timestamp_t link_poll_start = timestamp_get();
		int result = link_poll(q->poll_table, n, msec);
		link_poll_end = timestamp_get();	
		q->idle_time += link_poll_end - link_poll_start;


		// If the master link was awake, then accept as many workers as possible.
		if(q->poll_table[0].revents) {
			do {
				add_worker(q);
			} while(link_usleep(q->master_link, 0, 1, 0) && (stoptime > time(0)));
		}

		int i, j = 1;

		// Consider the master link passed into the function and disregard if inactive.
		if(master_link) {
			if(q->poll_table[1].revents) {
				*master_active = 1; //signal that the master link saw activity
			} else {
				*master_active = 0;
			}
			j++;
		}

		// Then consider all existing active workers and dispatch tasks.
		for(i = j; i < n; i++) {
			if(q->poll_table[i].revents) {
				handle_worker(q, q->poll_table[i].link);
			}
		}
		
		// Start tasks on ready workers
		start_tasks(q);
		
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
		
		// If the master link is active then break so the caller can handle it.
		if(master_link) {
			break;
		}
		
		// If nothing was awake, restart the loop or return without a task.
		if(result <= 0) {
			if(stoptime && time(0) >= stoptime) {
				break;
			} else {
				continue;
			}
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

	//BUG: fix this so that it actually looks at the number of cores available.

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
		if(itable_size(w->current_tasks) == 0) {
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

struct list * work_queue_cancel_all_tasks(struct work_queue *q) {
	struct list *l = list_create();
	struct work_queue_task *t;
	struct work_queue_worker *w;
	UINT64_T taskid;
	char *key;
	
	while( (t = list_pop_head(q->ready_list)) ) {
		list_push_tail(l, t);
	}
	while( (t = list_pop_head(q->complete_list)) ) {
		list_push_tail(l, t);
	}

	hash_table_firstkey(q->worker_table);
	while(hash_table_nextkey(q->worker_table, &key, (void**)&w)) {
		
		send_worker_msg(w, "%s %d\n", time(0) + short_timeout, "kill", -1);
		
		itable_firstkey(w->current_tasks);
		while(itable_nextkey(w->current_tasks, &taskid, (void**)&t)) {
			itable_remove(q->running_tasks, taskid);
			itable_remove(q->finished_tasks, taskid);
			itable_remove(q->worker_task_map, taskid);
			
			//Delete any input files that are not to be cached.
			delete_worker_files(w, t->input_files, t->taskid, WORK_QUEUE_CACHE | WORK_QUEUE_PREEXIST);

			//Delete all output files since they are not needed as the task was aborted.
			delete_worker_files(w, t->output_files, t->taskid, 0);
			
			w->cores_allocated -= t->cores;
			w->memory_allocated -= t->memory;
			w->disk_allocated -= t->disk;
			
			itable_remove(w->current_tasks, taskid);
			
			list_push_tail(l, t);
		}
	}
	return l;
}

void work_queue_reset(struct work_queue *q, int flags) {
	struct work_queue_worker *w;
	struct work_queue_task *t;
	char *key;
	
	if(!q) return;

	hash_table_firstkey(q->worker_table);
	while(hash_table_nextkey(q->worker_table,&key,(void**)&w)) {
		send_worker_msg(w, "reset\n", time(0)+short_timeout);
		cleanup_worker(q, w);
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

int work_queue_tune(struct work_queue *q, const char *name, double value)
{
	
	if(!strcmp(name, "asynchrony-multiplier")) {
		q->asynchrony_multiplier = MAX(value, 1.0);
		
	} else if(!strcmp(name, "asynchrony-modifier")) {
		q->asynchrony_modifier = MAX(value, 0);
		
	} else if(!strcmp(name, "min-transfer-timeout")) {
		wq_minimum_transfer_timeout = (int)value;
	
	} else if(!strcmp(name, "foreman-transfer-timeout")) {
		wq_foreman_transfer_timeout = (int)value;
		
	} else if(!strcmp(name, "fast-abort-multiplier")) {
		if(value >= 1) {
			q->fast_abort_multiplier = value;
		} else if(value< 0) {
			q->fast_abort_multiplier = value;
		} else {
			q->fast_abort_multiplier = -1.0;
		}
	} else if(!strcmp(name, "keepalive-interval")) {
		q->keepalive_interval = MAX(0, (int)value);
		
	} else if(!strcmp(name, "keepalive-timeout")) {
		q->keepalive_timeout = MAX(0, (int)value);

	} else if(!strcmp(name, "short-timeout")) {
		short_timeout = MAX(1, (int)value);
		
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

	if(effective_workers < 1 || wall_clock_time == 0)
		s->efficiency = 0;
	else
		s->efficiency = (long double) (q->total_execute_time) / (wall_clock_time * effective_workers);

	s->idle_percentage = get_idle_percentage(q);
	// Estimate master capacity, i.e. how many workers can this master handle
	s->capacity = q->capacity;
	s->avg_capacity = q->avg_capacity;
	s->total_workers_connected = q->total_workers_connected;
	// BUG: this should be the sum of the worker cpus
	s->total_worker_slots = s->total_workers_connected;
}

void work_queue_get_resources( struct work_queue *q, struct work_queue_resources *total )
{
	struct work_queue_worker *w;
	char *key;
	int first = 1;
	int wnum = 1;

	hash_table_firstkey(q->worker_table);
	while(hash_table_nextkey(q->worker_table,&key,(void**)&w)) {

		debug(D_WQ,"Worker #%d INFO - cores:%d memory:%d disk:%d\n", wnum,w->resources->cores.total,w->resources->memory.total,w->resources->disk.total); //see if information is being passed correctly

		if(first) {
			*total = *w->resources;
			first = 0;
		} else {
			work_queue_resources_add(total,w->resources);
		}
		wnum++;
	}
}

void work_queue_specify_log(struct work_queue *q, const char *logfile)
{
	q->logfile = fopen(logfile, "a");
	if(q->logfile) {
		setvbuf(q->logfile, NULL, _IOLBF, 1024); // line buffered, we don't want incomplete lines
		fprintf(q->logfile, "#%16s %25s %25s %25s %25s %25s %25s %25s %25s %25s %25s %25s %25s %25s %25s %25s %25s %25s %25s %25s %25s %25s %25s %25s %25s\n", // header/column labels
			"timestamp", "start_time",
			"workers_init", "workers_ready", "workers_active", "workers_full", // workers
			"tasks_waiting", "tasks_running", "tasks_complete", // tasks
			"total_tasks_dispatched", "total_tasks_complete", "total_workers_joined", "total_workers_connected", // totals
			"total_workers_removed", "total_bytes_sent", "total_bytes_received", "total_send_time", "total_receive_time",
			"efficiency", "idle_percentage", "capacity", "avg_capacity", // other
			"port", "priority", "total_worker_slots");
		log_worker_states(q);
		debug(D_WQ, "log enabled and is being written to %s\n", logfile);
	}
}


