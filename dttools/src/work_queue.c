/*
Copyright (C) 2008- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "work_queue.h"

#include "int_sizes.h"
#include "link.h"
#include "debug.h"
#include "stringtools.h"
#include "catalog_query.h"
#include "catalog_server.h"
#include "work_queue_catalog.h"
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
#define WORKER_STATE_NONE  3
#define WORKER_STATE_MAX   (WORKER_STATE_NONE+1)

#define WORK_QUEUE_FILE 0
#define WORK_QUEUE_BUFFER 1
#define WORK_QUEUE_REMOTECMD 2

#define TASK_STATUS_INITIALIZING 0
#define TASK_STATUS_SENDING_INPUT 1
#define TASK_STATUS_EXECUTING 2
#define TASK_STATUS_WAITING_FOR_OUTPUT 3
#define TASK_STATUS_RECEIVING_OUTPUT 4
#define TASK_STATUS_COMPLETE 4

#define MIN_TIME_LIST_SIZE 20

#define TIME_SLOT_TASK_TRANSFER 0
#define TIME_SLOT_TASK_EXECUTE 1
#define TIME_SLOT_MASTER_IDLE 2
#define TIME_SLOT_APPLICATION 3

#define POOL_DECISION_ENFORCEMENT_INTERVAL_DEFAULT 10

double wq_option_fast_abort_multiplier = -1.0;
int wq_option_scheduler = WORK_QUEUE_SCHEDULE_DEFAULT;
int wq_tolerable_transfer_time_multiplier = 10;
int wq_minimum_transfer_timeout = 3;

struct work_queue {
	char *name;
	int port;
	int master_mode;
	int worker_mode;
	int priority;
	struct link *master_link;
	struct list *ready_list;
	struct list *complete_list;
	struct list *receive_output_waiting_list;
	struct hash_table *worker_table;
	struct link_info *poll_table;
	int poll_table_size;
	int workers_in_state[WORKER_STATE_MAX];

	INT64_T total_tasks_submitted;
	INT64_T total_tasks_complete;
	INT64_T total_task_time;
	INT64_T total_wait_time;
	INT64_T total_workers_joined;
	INT64_T total_workers_removed;
	INT64_T total_bytes_sent;
	INT64_T total_bytes_received;

	timestamp_t start_time;
	timestamp_t total_send_time;
	timestamp_t total_receive_time;
	timestamp_t total_execute_time;
	double fast_abort_multiplier;

	int worker_selection_algorithm;		  /**< How to choose worker to run the task. */
	int task_ordering;

	timestamp_t time_last_task_start;
	timestamp_t idle_time;
	timestamp_t accumulated_idle_time;
	timestamp_t app_time;
	struct list *idle_times;
	double idle_percentage;
	struct task_statistics *task_statistics;

	int estimate_capacity_on;
	int auto_remove_workers_on;
	int capacity;
	int avg_capacity;
	int work_queue_wait_routine;
	INT64_T total_workers_connected;
	INT64_T excessive_workers_removed;
	int busy_workers_to_remove;
	int capacity_tolerance;
	int maximum_workers;
	char catalog_host[DOMAIN_NAME_MAX];
	int catalog_port;
	struct hash_table *workers_by_pool;

	int link_keepalive_on;
};


struct work_queue_worker {
	int state;
	char hostname[DOMAIN_NAME_MAX];
	char os[65];
	char arch[65];
	char addrport[32];
	char hashkey[32];
	int ncpus;
	INT64_T memory_avail;
	INT64_T memory_total;
	INT64_T disk_avail;
	INT64_T disk_total;
	struct hash_table *current_files;
	struct link *link;
	struct work_queue_task *current_task;
	INT64_T total_tasks_complete;
	INT64_T total_bytes_transferred;
	timestamp_t total_task_time;
	timestamp_t total_transfer_time;
	timestamp_t start_time;
	char pool_name[WORK_QUEUE_POOL_NAME_MAX];
};

struct pending_output {
	timestamp_t start;
	struct link *link;
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
	int type;		// WORK_QUEUE_FILE or WORK_QUEUE_BUFFER
	int flags;		// WORK_QUEUE_CACHE or others in the future.
	int length;		// length of payload
	void *payload;		// name on master machine or buffer of data.
	char *remote_name;	// name on remote machine.
};

static int start_one_task(struct work_queue *q, struct work_queue_worker *w, struct work_queue_task *t);
static int start_task_on_worker(struct work_queue *q, struct work_queue_worker *w);
static int start_n_tasks(struct work_queue *q, int n);

static int get_num_of_effective_workers(struct work_queue *q);
static timestamp_t get_transfer_wait_time(struct work_queue *q, struct work_queue_worker *w, INT64_T length);
static double get_idle_percentage(struct work_queue *q);

void receive_pending_output(struct work_queue *q, struct pending_output *p);
static int receive_output_from_worker(struct work_queue *q, struct work_queue_worker *w);

static struct task_statistics *task_statistics_init();
static void add_time_slot(struct work_queue *q, timestamp_t start, timestamp_t duration, int type, timestamp_t * accumulated_time, struct list *time_list);
static void add_task_report(struct work_queue *q, struct work_queue_task *t);

static int remove_workers_base_on_capacity(struct work_queue *q);
int work_queue_shut_down_workers(struct work_queue *q, int n);

int work_queue_specify_estimate_capacity_on(struct work_queue *q, int value);
int work_queue_specify_wait_routine(struct work_queue *q, int routine);
int work_queue_specify_capacity_tolerance(struct work_queue *q, int tolerance);
int work_queue_specify_auto_remove_workers_on(struct work_queue *q, int value);
int work_queue_specify_maximum_workers(struct work_queue *q, int max);

static void update_catalog(struct work_queue *q, int now);
static void enforce_pool_decisions(struct work_queue *q);

static int short_timeout = 5;
static int next_taskid = 1;

struct work_queue_task *work_queue_task_create(const char *command_line)
{
	struct work_queue_task *t = malloc(sizeof(*t));
	memset(t, 0, sizeof(*t));
	t->command_line = xxstrdup(command_line);
	t->tag = NULL;
	t->worker_selection_algorithm = WORK_QUEUE_SCHEDULE_UNSET;
	t->output = NULL;
	t->input_files = list_create();
	t->output_files = list_create();
	t->return_status = WORK_QUEUE_RETURN_STATUS_UNSET;
	t->result = WORK_QUEUE_RESULT_UNSET;

	t->time_task_submit = t->time_task_finish = 0;
	t->time_send_input_start = t->time_send_input_finish = 0;
	t->time_execute_cmd_start = t->time_execute_cmd_finish = 0;
	t->time_receive_output_start = t->time_receive_output_finish = 0;

	t->total_bytes_transferred = 0;
	t->cmd_execution_time = 0;
	t->status = TASK_STATUS_INITIALIZING;
	return t;
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
		if(t->host)
			free(t->host);
		free(t);
	}
}

static void change_worker_state(struct work_queue *q, struct work_queue_worker *w, int state)
{
	q->workers_in_state[w->state]--;
	w->state = state;
	q->workers_in_state[state]++;
	if(q->master_mode == WORK_QUEUE_MASTER_MODE_CATALOG) {
		update_catalog(q, 0);
	}
	debug(D_WQ, "Number of workers in state 'busy': %d;  'ready': %d", q->workers_in_state[WORKER_STATE_BUSY], q->workers_in_state[WORKER_STATE_READY]);
}

static void link_to_hash_key(struct link *link, char *key)
{
	sprintf(key, "0x%p", link);
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

	char *c = s->workers_by_pool;
	char *key;
	struct pool_info *pi;
	int x;
	hash_table_firstkey(q->workers_by_pool);
	while(hash_table_nextkey(q->workers_by_pool, &key, (void **) &pi)) {
		x = snprintf(c, WORK_QUEUE_CATALOG_LINE_MAX - (c - s->workers_by_pool), "%s:%d,", pi->name, pi->count);
		if(x <= 0) {
			fprintf(stderr, "failed to record worker_by_pool item: %s:%d\n", pi->name, pi->count);
			continue;
		}
		c += x;
	}
	if(c - s->workers_by_pool > 0) {
		*(c-1) = '\0';
	} else {
		strncpy(s->workers_by_pool, "n/a", 4); 
	}

	s->tasks_waiting = list_size(q->ready_list);
	s->tasks_running = q->workers_in_state[WORKER_STATE_BUSY];
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
	s->excessive_workers_removed = q->excessive_workers_removed;
}

static int add_worker(struct work_queue *q)
{
	struct link *link;
	struct work_queue_worker *w;
	char addr[LINK_ADDRESS_MAX];
	int port;

	link = link_accept(q->master_link, time(0) + short_timeout);
	if(link) {
		if(q->link_keepalive_on) {
			link_keepalive(link, 1);
		}
		link_tune(link, LINK_TUNE_INTERACTIVE);
		if(link_address_remote(link, addr, &port)) {
			w = malloc(sizeof(*w));
			memset(w, 0, sizeof(*w));
			w->state = WORKER_STATE_NONE;
			w->link = link;
			w->current_files = hash_table_create(0, 0);
			w->start_time = timestamp_get();
			link_to_hash_key(link, w->hashkey);
			sprintf(w->addrport, "%s:%d", addr, port);
			hash_table_insert(q->worker_table, w->hashkey, w);
			change_worker_state(q, w, WORKER_STATE_INIT);
			debug(D_WQ, "worker %s added", w->addrport);
			debug(D_WQ, "%d workers are connected in total now", hash_table_size(q->worker_table));
			q->total_workers_joined++;

			return 1;
		} else {
			link_close(link);
		}
	}

	return 0;
}

static void remove_worker(struct work_queue *q, struct work_queue_worker *w)
{
	char *key, *value;
	struct work_queue_task *t;

	if((w->pool_name)[0]) {
		debug(D_WQ, "worker %s from pool \"%s\" removed", w->addrport, w->pool_name);
	} else {
		debug(D_WQ, "worker %s removed", w->addrport);
	}

	q->total_workers_removed++;

	hash_table_firstkey(w->current_files);
	while(hash_table_nextkey(w->current_files, &key, (void **) &value)) {
		hash_table_remove(w->current_files, key);
		free(value);
	}
	hash_table_delete(w->current_files);

	hash_table_remove(q->worker_table, w->hashkey);
	t = w->current_task;
	if(t) {
		if(t->result & WORK_QUEUE_RESULT_INPUT_MISSING || t->result & WORK_QUEUE_RESULT_OUTPUT_MISSING || t->result & WORK_QUEUE_RESULT_FUNCTION_FAIL) {
			list_push_head(q->complete_list, w->current_task);
		} else {
			t->result = WORK_QUEUE_RESULT_UNSET;
			t->total_bytes_transferred = 0;
			t->total_transfer_time = 0;
			t->cmd_execution_time = 0;
			list_push_head(q->ready_list, w->current_task);
		}
		w->current_task = 0;
	}
	
	if((w->pool_name)[0]) {
		struct pool_info *pi;
		pi = hash_table_lookup(q->workers_by_pool, w->pool_name);
		if(!pi) {
			debug(D_NOTICE, "Error: removing worker from pool \"%s\" but failed to find out how many workers are from that pool.", w->pool_name);
		} else {
			if(pi->count == 0) {
				debug(D_NOTICE, "Error: removing worker from pool \"%s\" but record indicates no workers from that pool are connected.", w->pool_name);
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

/**
 * This function implements the "rget %s" protocol.
 * It reads a streamed item from a worker. For the stream format, please refer
 * to the stream_output_item function in worker.c
 */
static int get_output_item(char *remote_name, char *local_name, struct work_queue *q, struct work_queue_worker *w, struct hash_table *received_items, INT64_T * total_bytes)
{
	char line[WORK_QUEUE_CATALOG_LINE_MAX];
	int fd;
	INT64_T actual, length;
	time_t stoptime;
	char type[256];
	char tmp_remote_name[WORK_QUEUE_CATALOG_LINE_MAX], tmp_local_name[WORK_QUEUE_CATALOG_LINE_MAX];
	char *cur_pos, *tmp_pos;
	int remote_name_len;
	int local_name_len;

	if(hash_table_lookup(received_items, local_name))
		return 1;

	debug(D_WQ, "%s (%s) sending back %s to %s", w->hostname, w->addrport, remote_name, local_name);
	link_putfstring(w->link, "rget %s\n", time(0) + short_timeout, remote_name);

	strcpy(tmp_local_name, local_name);
	remote_name_len = strlen(remote_name);
	local_name_len = strlen(local_name);

	while(1) {
		if(!link_readline(w->link, line, sizeof(line), time(0) + short_timeout)) {
			goto link_failure;
		}

		if(sscanf(line, "%s %s %lld", type, tmp_remote_name, &length) == 3) {
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
					stoptime = time(0) + get_transfer_wait_time(q, w, length);
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
				debug(D_NOTICE, "Failed to retrieve %s from %s (%s): %s", remote_name, w->addrport, w->hostname, strerror(length));
				w->current_task->result |= WORK_QUEUE_RESULT_OUTPUT_MISSING;
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
	w->current_task->result |= WORK_QUEUE_RESULT_LINK_FAIL;

      failure:
	debug(D_WQ, "%s (%s) failed to return %s to %s", w->addrport, w->hostname, remote_name, local_name);
	w->current_task->result |= WORK_QUEUE_RESULT_OUTPUT_FAIL;
	return 0;
}

/**
 * Comparison function for sorting by file/dir names in the output files list
 * of a task
 */
int filename_comparator(const void *a, const void *b)
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

	timestamp_t open_time = 0;
	timestamp_t close_time = 0;
	timestamp_t sum_time;

	// Start transfer ...
	t->status = TASK_STATUS_RECEIVING_OUTPUT;
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
					link_putfstring(w->link, "thirdput %d %s %s\n", time(0) + short_timeout, WORK_QUEUE_FS_PATH, tf->remote_name, tf->payload);
					link_readline(w->link, thirdput_result, WORK_QUEUE_LINE_MAX, time(0) + short_timeout);
					close_time = timestamp_get();
					sum_time += (close_time - open_time);
				}
			} else if(tf->type == WORK_QUEUE_REMOTECMD) {
				char thirdput_result[WORK_QUEUE_LINE_MAX];
				debug(D_WQ, "putting %s from %s (%s) to remote filesystem using %s", tf->remote_name, w->hostname, w->addrport, tf->payload);
				open_time = timestamp_get();
				link_putfstring(w->link, "thirdput %d %s %s\n", time(0) + short_timeout, WORK_QUEUE_FS_CMD, tf->remote_name, tf->payload);
				link_readline(w->link, thirdput_result, WORK_QUEUE_LINE_MAX, time(0) + short_timeout);
				close_time = timestamp_get();
				sum_time += (close_time - open_time);
			} else {
				open_time = timestamp_get();
				get_output_item(tf->remote_name, (char *) tf->payload, q, w, received_items, &total_bytes);
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

static void delete_uncacheable_files(struct work_queue_task *t, struct work_queue_worker *w)
{
	struct work_queue_file *tf;

	if(t->input_files) {
		list_first_item(t->input_files);
		while((tf = list_next_item(t->input_files))) {
			if(!(tf->flags & WORK_QUEUE_CACHE) && !(tf->flags & WORK_QUEUE_PREEXIST)) {
				debug(D_WQ, "%s (%s) unlink %s", w->hostname, w->addrport, tf->remote_name);
				link_putfstring(w->link, "unlink %s\n", time(0) + short_timeout, tf->remote_name);
			}
		}
	}

	if(t->output_files) {
		list_first_item(t->output_files);
		while((tf = list_next_item(t->output_files))) {
			if(!(tf->flags & WORK_QUEUE_CACHE) && !(tf->flags & WORK_QUEUE_PREEXIST)) {
				debug(D_WQ, "%s (%s) unlink %s", w->hostname, w->addrport, tf->remote_name);
				link_putfstring(w->link, "unlink %s\n", time(0) + short_timeout, tf->remote_name);
			}
		}
	}
}

static int match_project_names(struct work_queue *q, const char *project_names)
{
	char *str;
	char *token = NULL;
	char *delims = " \t";

	if(!q || !project_names)
		return 0;

	str = xxstrdup(project_names);
	token = strtok(str, delims);
	while(token) {
		// token holds the preferred master name pattern
		debug(D_WQ, "Matching %s against %s", q->name, project_names);
		if(whole_string_match_regex(q->name, token)) {
			free(str);
			return 1;	// Match found!
		}

		token = strtok(NULL, delims);
	}
	free(str);
	return 0;
}

static int handle_worker(struct work_queue *q, struct link *l)
{
	char line[WORK_QUEUE_CATALOG_LINE_MAX];
	char key[WORK_QUEUE_CATALOG_LINE_MAX];
	struct work_queue_worker *w;
	int result;
	INT64_T output_length;
	time_t stoptime;
	timestamp_t execution_time;

	char project_names[WORK_QUEUE_LINE_MAX];

	link_to_hash_key(l, key);
	w = hash_table_lookup(q->worker_table, key);

	if(link_readline(l, line, sizeof(line), time(0) + short_timeout)) {
		debug(D_WQ, "msg from worker: %s", line);
		if(sscanf(line, "ready %s %d %lld %lld %lld %lld", w->hostname, &w->ncpus, &w->memory_avail, &w->memory_total, &w->disk_avail, &w->disk_total) == 6) {
			// More workers than needed are connected
			int workers_connected = hash_table_size(q->worker_table);
			int jobs_not_completed = list_size(q->ready_list) + q->workers_in_state[WORKER_STATE_BUSY];
			if(workers_connected > jobs_not_completed) {
				debug(D_WQ, "Jobs waiting + running: %d; Workers connected now: %d", jobs_not_completed, workers_connected);
				if(q->workers_in_state[WORKER_STATE_READY] >= list_size(q->ready_list)) {
					debug(D_NOTICE, "The number of remaining tasks is less than the number of ready workers.");
					goto reject;
				}
			}

			if(q->worker_mode == WORK_QUEUE_WORKER_MODE_EXCLUSIVE && q->name) {
				// For backward compatibility, we scan the line AGAIN to see if it contains extra fields
				if(sscanf(line, "ready %*s %*d %*d %*d %*d %*d \"%[^\"]\"", project_names) == 1) {
					if(!match_project_names(q, project_names)) {
						debug(D_NOTICE, "Preferred masters of %s (%s): %s", w->hostname, w->addrport, project_names);
						goto reject;
					}
				} else {
					// No extra fields, so this is a shared worker
					debug(D_NOTICE, "%s (%s) is a shared worker. But the master does not allow shared workers.", w->hostname, w->addrport);
					goto reject;
				}
			}

			//Re-scan to see if worker reports its os and arch.
			if(sscanf(line, "ready %*s %*d %*d %*d %*d %*d \"%[^\"]\"", project_names) == 1) {
				if(sscanf(line, "ready %*s %*d %*d %*d %*d %*d \"%*[^\"]\" %s %s", w->os, w->arch) != 2) {
					strcpy(w->os, "unknown");
					strcpy(w->arch, "unknown");
				}
			} else {
				//check in exclusive worker with no preferred project which it sends as a blank ""
				if(sscanf(line, "ready %*s %*d %*d %*d %*d %*d \"\" %s %s", w->os, w->arch) != 2) {
					//check in shared worker
					if(sscanf(line, "ready %*s %*d %*d %*d %*d %*d %s %s", w->os, w->arch) != 2) {
						strcpy(w->os, "unknown");
						strcpy(w->arch, "unknown");
					}
				}
			}

			char buffer[100]; //host name + pid
			buffer[0] = '\0';
			if(sscanf(line, "ready %*s %*d %*d %*d %*d %*d \"%*[^\"]\" %*s %*s %s", buffer) != 1) {
				if(sscanf(line, "ready %*s %*d %*d %*d %*d %*d \"\" %*s %*s %s", buffer) != 1) {
					if(sscanf(line, "ready %*s %*d %*d %*d %*d %*d %*s %*s %s", buffer) != 1) {
						(w->pool_name)[0] = 0;
					}
				}
			}
			if(buffer[0] != '\0') {
				struct pool_info *pi;
				strncpy(w->pool_name, buffer, WORK_QUEUE_POOL_NAME_MAX);
				pi = hash_table_lookup(q->workers_by_pool, w->pool_name);
				if(!pi) {
					pi = xxmalloc(sizeof(*pi));
					strncpy(pi->name, w->pool_name, WORK_QUEUE_POOL_NAME_MAX);
					pi->count = 1;
					hash_table_insert(q->workers_by_pool, w->pool_name, pi);
				} else {
					pi->count += 1;
				}
			}

			if(w->state == WORKER_STATE_INIT) {
				change_worker_state(q, w, WORKER_STATE_READY);
				q->total_workers_connected++;
				debug(D_WQ, "%s (%s) running %s on %s is ready", w->hostname, w->addrport, w->os, w->arch);
			}
		} else if(sscanf(line, "result %d %lld", &result, &output_length) == 2) {
			struct work_queue_task *t;
			int actual;
			timestamp_t observed_execution_time;

			t = w->current_task;

			observed_execution_time = timestamp_get() - t->time_execute_cmd_start;

			if(sscanf(line, "result %d %lld %llu", &result, &output_length, &execution_time) == 3) {
				t->cmd_execution_time = observed_execution_time > execution_time ? execution_time : observed_execution_time;
			} else {
				t->cmd_execution_time = observed_execution_time;
			}

			t->output = malloc(output_length + 1);
			if(output_length > 0) {
				//stoptime = time(0) + MAX(1.0,output_length/1250000.0);
				stoptime = time(0) + get_transfer_wait_time(q, w, (INT64_T) output_length);
				actual = link_read(l, t->output, output_length, stoptime);
				if(actual != output_length) {
					free(t->output);
					t->output = 0;
					goto failure;
				}
			} else {
				actual = 0;
			}
			t->output[actual] = 0;

			t->return_status = result;
			if(t->return_status != 0)
				t->result |= WORK_QUEUE_RESULT_FUNCTION_FAIL;

			t->time_execute_cmd_finish = t->time_execute_cmd_start + t->cmd_execution_time;
			q->total_execute_time += t->cmd_execution_time;

			t->status = TASK_STATUS_WAITING_FOR_OUTPUT;

			if(q->work_queue_wait_routine == WORK_QUEUE_WAIT_FAST_DISPATCH || q->work_queue_wait_routine == WORK_QUEUE_WAIT_ADAPTIVE) {
				// Receiving output delayed
				struct pending_output *p;
				p = (struct pending_output *) malloc(sizeof(struct pending_output));
				if(!p) {
					free(t->output);
					t->output = 0;
					goto failure;
				}
				p->start = timestamp_get();
				p->link = l;
				list_push_head(q->receive_output_waiting_list, p);
				debug(D_WQ, "%s (%s) has finished its task. Output transfer is postponed.", w->hostname, w->addrport);
			} else {
				//q->work_queue_wait_routine == WORK_QUEUE_WAIT_FCFS
				// Receive output immediately and start a new task on the this worker if available
				if(receive_output_from_worker(q, w)) {
					start_task_on_worker(q, w);
				}
			}
		} else {
			debug(D_WQ, "Invalid message from worker %s (%s): %s", w->hostname, w->addrport, line);
			goto failure;
		}
	} else {
		debug(D_WQ, "Failed to read from worker %s (%s)", w->hostname, w->addrport);
		goto failure;
	}

	return 1;

      reject:
	debug(D_NOTICE, "%s (%s) is rejected and removed.", w->hostname, w->addrport);
	remove_worker(q, w);
	return 0;

      failure:
	debug(D_NOTICE, "%s (%s) failed and removed.", w->hostname, w->addrport);
	remove_worker(q, w);
	return 0;
}

static int receive_output_from_worker(struct work_queue *q, struct work_queue_worker *w)
{
	struct work_queue_task *t;
	timestamp_t task_time;

	t = w->current_task;
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
	list_push_head(q->complete_list, w->current_task);
	w->current_task = 0;
	t->time_task_finish = timestamp_get();

	// Record statistics information for capacity estimation
	if(q->estimate_capacity_on) {
		add_task_report(q, t);
		if(q->auto_remove_workers_on) {
			remove_workers_base_on_capacity(q);
		}
	}
	// Change worker state and do some performance statistics
	change_worker_state(q, w, WORKER_STATE_READY);

	t->host = xxstrdup(w->addrport);
	task_time = t->total_transfer_time + t->cmd_execution_time;

	q->total_tasks_complete++;
	q->total_task_time += task_time;

	w->total_tasks_complete++;
	w->total_task_time += task_time;

	debug(D_WQ, "%s (%s) done in %.02lfs total tasks %d average %.02lfs", w->hostname, w->addrport, (t->time_receive_output_finish - t->time_send_input_start) / 1000000.0, w->total_tasks_complete,
	      w->total_task_time / w->total_tasks_complete / 1000000.0);
	return 1;

      failure:
	debug(D_NOTICE, "%s (%s) failed and removed because cannot receive output.", w->hostname, w->addrport);
	remove_worker(q, w);
	return 0;
}

static int build_poll_table(struct work_queue *q)
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

static int put_file(struct work_queue_file *, const char *, struct work_queue *, struct work_queue_worker *, INT64_T *);

static int put_directory(const char *dirname, const char *remote_name, struct work_queue *q, struct work_queue_worker *w, INT64_T * total_bytes)
{
	DIR *dir = opendir(dirname);
	if(!dir)
		return 0;

	struct dirent *file;
	struct work_queue_file tf;

	char buffer[WORK_QUEUE_CATALOG_LINE_MAX];

	while((file = readdir(dir))) {
		char *filename = file->d_name;
		int len;

		if(!strcmp(filename, ".") || !strcmp(filename, "..")) {
			continue;
		}

		tf.type = WORK_QUEUE_FILE;
		tf.flags = WORK_QUEUE_CACHE;

		*buffer = '\0';
		len = sprintf(buffer, "%s/%s", dirname, filename);
		tf.length = len;
		tf.payload = xxstrdup(buffer);

		*buffer = '\0';
		len = sprintf(buffer, "%s/%s", remote_name, filename);
		tf.remote_name = xxstrdup(buffer);

		if(!put_file(&tf, NULL, q, w, total_bytes)) {
			closedir(dir);
			return 0;
		}
	}

	closedir(dir);
	return 1;
}

static int put_file(struct work_queue_file *tf, const char *expanded_payload, struct work_queue *q, struct work_queue_worker *w, INT64_T * total_bytes)
{
	struct stat local_info;
	struct stat *remote_info;
	char *hash_name;
	time_t stoptime;
	INT64_T actual = 0;
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
	/* normalize the mode so as not to set up invalid permissions */
	if(dir) {
		local_info.st_mode |= 0700;
	} else {
		local_info.st_mode |= 0600;
	}
	local_info.st_mode &= 0777;

	hash_name = (char *) malloc((strlen(payload) + strlen(tf->remote_name) + 2) * sizeof(char));
	sprintf(hash_name, "%s-%s", payload, tf->remote_name);

	remote_info = hash_table_lookup(w->current_files, hash_name);

	if(!remote_info || remote_info->st_mtime != local_info.st_mtime || remote_info->st_size != local_info.st_size) {
		if(remote_info) {
			hash_table_remove(w->current_files, hash_name);
			free(remote_info);
		}

		debug(D_WQ, "%s (%s) needs file %s", w->hostname, w->addrport, payload);
		if(dir) {
			// If mkdir fails, the future calls to 'put_file' function to place
			// files in that directory won't suceed. Such failure would
			// eventually be captured in 'start_tasks' function and in the
			// 'start_tasks' function the corresponding worker would be removed.
			link_putfstring(w->link, "mkdir %s %o\n", time(0) + short_timeout, tf->remote_name, local_info.st_mode);

			if(!put_directory(payload, tf->remote_name, q, w, total_bytes))
				return 0;

			return 1;
		}

		int fd = open(payload, O_RDONLY, 0);
		if(fd < 0)
			return 0;

		stoptime = time(0) + get_transfer_wait_time(q, w, (INT64_T) local_info.st_size);
		link_putfstring(w->link, "put %s %lld 0%o\n", time(0) + short_timeout, tf->remote_name, (INT64_T) local_info.st_size, local_info.st_mode);
		actual = link_stream_from_fd(w->link, fd, local_info.st_size, stoptime);
		close(fd);

		if(actual != local_info.st_size)
			return 0;

		if(tf->flags & WORK_QUEUE_CACHE) {
			remote_info = malloc(sizeof(*remote_info));
			memcpy(remote_info, &local_info, sizeof(local_info));
			hash_table_insert(w->current_files, hash_name, remote_info);
		}

		*total_bytes += actual;
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
char *expand_envnames(struct work_queue_worker *w, const char *payload)
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
			//Put back '$' only if it does not appear at start of the string.
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
	int actual = 0;
	INT64_T total_bytes = 0;
	timestamp_t open_time = 0;
	timestamp_t close_time = 0;
	timestamp_t sum_time = 0;
	int fl;
	time_t stoptime;
	struct stat s;
	char *expanded_payload = NULL;
	t->status = TASK_STATUS_SENDING_INPUT;

	// Check input existence
	if(t->input_files) {
		list_first_item(t->input_files);
		while((tf = list_next_item(t->input_files))) {
			if(tf->type == WORK_QUEUE_FILE) {
				if(strchr(tf->payload, '$')) {
					expanded_payload = expand_envnames(w, tf->payload);
					debug(D_WQ, "File name %s expanded to %s for %s (%s).", tf->payload, expanded_payload, w->hostname, w->addrport);
				} else {
					expanded_payload = xxstrdup(tf->payload);
				}
				if(stat(expanded_payload, &s) != 0) {
					fprintf(stderr, "Could not stat %s. (%s)\n", expanded_payload, strerror(errno));
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

				stoptime = time(0) + get_transfer_wait_time(q, w, (INT64_T) fl);
				open_time = timestamp_get();
				link_putfstring(w->link, "put %s %lld %o\n", time(0) + short_timeout, tf->remote_name, (INT64_T) fl, 0777);
				actual = link_putlstring(w->link, tf->payload, fl, stoptime);
				close_time = timestamp_get();
				if(actual != (fl))
					goto failure;
				total_bytes += actual;
				sum_time += (close_time - open_time);
			} else if(tf->type == WORK_QUEUE_REMOTECMD) {
				debug(D_WQ, "%s (%s) needs %s from remote filesystem using %s", w->hostname, w->addrport, tf->remote_name, tf->payload);
				open_time = timestamp_get();
				link_putfstring(w->link, "thirdget %d %s %s\n", time(0) + short_timeout, WORK_QUEUE_FS_CMD, tf->remote_name, tf->payload);
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
							link_putfstring(w->link, "thirdget %d %s %s\n", time(0) + short_timeout, WORK_QUEUE_FS_SYMLINK, tf->remote_name, tf->payload);
						} else {
							link_putfstring(w->link, "thirdget %d %s %s\n", time(0) + short_timeout, WORK_QUEUE_FS_PATH, tf->remote_name, tf->payload);
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
					if(!put_file(tf, expanded_payload, q, w, &total_bytes)) {
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
	if(tf->type == WORK_QUEUE_FILE)
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
	t->time_send_input_finish = timestamp_get();

	t->time_execute_cmd_start = timestamp_get();
	//Old work command (worker does not fork to execute a task, thus can't
	//communicate with the master during task execution):
	//link_putfstring(w->link, "work %zu\n%s", time(0) + short_timeout, strlen(t->command_line), t->command_line);
	link_putfstring(w->link, "work %zu fork\n%s", time(0) + short_timeout, strlen(t->command_line), t->command_line);
	t->status = TASK_STATUS_EXECUTING;
	debug(D_WQ, "%s (%s) busy on '%s'", w->hostname, w->addrport, t->command_line);
	return 1;
}

static int get_num_of_effective_workers(struct work_queue *q)
{
	return q->workers_in_state[WORKER_STATE_BUSY] + q->workers_in_state[WORKER_STATE_READY];
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
		debug(D_NOTICE, "Failed to record time slot of type %d.", type);
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
		fprintf(stderr, "Failed to allocate memory for task report.\n");
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
	debug(D_WQ, "Lastest master capacity: %d; Avg master capacity: %d\n", q->capacity, q->avg_capacity);
}

static int remove_workers_base_on_capacity(struct work_queue *q)
{
	// Right now this function would completely shut down the remote worker (by
	// sending the "exit" msg to the worker).
	struct list *reports;
	struct task_report *tr;
	struct task_statistics *ts;
	int num_of_reports;
	int connected_workers, busy_workers, excessive_workers, workers_removed;
	int idle_percentage;
	double avg_capacity, capacity_stddev, avg_busy_workers, busy_workers_stddev;

	if(!q)
		return 0;
	if(q->work_queue_wait_routine != WORK_QUEUE_WAIT_ADAPTIVE)
		return 0;

	ts = q->task_statistics;
	if(!ts) {
		return 0;
	}

	reports = ts->reports;
	if(!reports) {
		return 0;
	}

	num_of_reports = list_size(reports);
	if(!num_of_reports)
		return 0;

	idle_percentage = get_idle_percentage(q) * 100;
	debug(D_WQ, "Current master idle percentage: %d%%\n", idle_percentage);

	// Do not remove workers if ...
	if(idle_percentage > 0 || num_of_reports < MIN_TIME_LIST_SIZE)
		return 0;

	// Calculate the standard deviation of the master capacities
	avg_capacity = (double) ts->total_capacity / num_of_reports;
	capacity_stddev = 0;
	list_first_item(reports);
	while((tr = (struct task_report *) list_next_item(reports))) {
		capacity_stddev += ((double) tr->capacity - avg_capacity) * ((double) tr->capacity - avg_capacity);
	}
	capacity_stddev = sqrt(capacity_stddev / (num_of_reports - 1));	// sample standard deviation
	debug(D_WQ, "Avg capacity: %.2f over last %d finished tasks; stddev: %.2f\n", avg_capacity, num_of_reports, capacity_stddev);

	// Calculate the standard deviation of the number of busy workers
	avg_busy_workers = (double) ts->total_busy_workers / num_of_reports;
	busy_workers_stddev = 0;
	list_first_item(reports);
	while((tr = (struct task_report *) list_next_item(reports))) {
		busy_workers_stddev += ((double) tr->busy_workers - avg_busy_workers) * ((double) tr->busy_workers - avg_busy_workers);
	}
	busy_workers_stddev = sqrt(busy_workers_stddev / (num_of_reports - 1));	// sample standard deviation
	debug(D_WQ, "Avg busy workers: %.2f over last %d finished tasks; stddev: %.2f\n", avg_busy_workers, num_of_reports, busy_workers_stddev);

	connected_workers = hash_table_size(q->worker_table);
	busy_workers = q->workers_in_state[WORKER_STATE_BUSY];

	workers_removed = 0;
	if(busy_workers_stddev < 1) {
		if(connected_workers == busy_workers && connected_workers > avg_capacity + q->capacity_tolerance) {
			if(capacity_stddev < 1) {
				excessive_workers = busy_workers - (avg_capacity + q->capacity_tolerance) + 1;	// "+ 1" act as the C math library's ceil function
			} else {
				excessive_workers = busy_workers - (avg_capacity + capacity_stddev * (double) (q->capacity_tolerance)) + 1;
			}
			q->busy_workers_to_remove = MAX(0, excessive_workers);
			debug(D_WQ, "Plan to remove %d busy workers. Waiting for them to return ongoing tasks.\n", excessive_workers);
		} else if(connected_workers > busy_workers) {
			excessive_workers = connected_workers - MAX(busy_workers, avg_capacity + q->capacity_tolerance + 1);
			workers_removed = work_queue_shut_down_workers(q, excessive_workers);
			q->excessive_workers_removed += workers_removed;
			debug(D_WQ, "%d excessive workers has just been removed.\n", workers_removed);
		}
	}

	return workers_removed;
}

struct work_queue_worker *find_worker_by_files(struct work_queue *q, struct work_queue_task *t)
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
				if(tf->type == WORK_QUEUE_FILE && (tf->flags & WORK_QUEUE_CACHE)) {
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

struct work_queue_worker *find_worker_by_fcfs(struct work_queue *q)
{
	char *key;
	struct work_queue_worker *w;
	struct work_queue_worker *best_worker = 0;

	hash_table_firstkey(q->worker_table);
	while(hash_table_nextkey(q->worker_table, &key, (void **) &w)) {
		if(w->state == WORKER_STATE_READY)
			return w;
	}

	return best_worker;
}

struct work_queue_worker *find_worker_by_random(struct work_queue *q)
{
	char *key;
	struct work_queue_worker *w;
	struct work_queue_worker *best_worker = 0;
	struct work_queue_stats qs;
	int num_workers_ready;
	int random_ready_worker, ready_worker_count = 1;

	srand(time(0));

	work_queue_get_stats(q, &qs);
	num_workers_ready = qs.workers_ready;

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

struct work_queue_worker *find_worker_by_time(struct work_queue *q)
{
	char *key;
	struct work_queue_worker *w;
	struct work_queue_worker *best_worker = 0;
	double best_time = HUGE_VAL;

	hash_table_firstkey(q->worker_table);
	while(hash_table_nextkey(q->worker_table, &key, (void **) &w)) {
		if(w->state == WORKER_STATE_READY) {
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
struct work_queue_worker *find_best_worker(struct work_queue *q, struct work_queue_task *t)
{
	int a = t->worker_selection_algorithm;

	if(a == WORK_QUEUE_SCHEDULE_UNSET) {
		a = q->worker_selection_algorithm;
	}

	switch (a) {
	case WORK_QUEUE_SCHEDULE_FILES:
		return find_worker_by_files(q, t);
	case WORK_QUEUE_SCHEDULE_TIME:
		return find_worker_by_time(q);
	case WORK_QUEUE_SCHEDULE_RAND:
		return find_worker_by_random(q);
	case WORK_QUEUE_SCHEDULE_FCFS:
	default:
		return find_worker_by_fcfs(q);
	}
}

static int start_task_on_worker(struct work_queue *q, struct work_queue_worker *w)
{
	struct work_queue_task *t = list_pop_head(q->ready_list);
	if(!t)
		return 0;

	w->current_task = t;

	if(start_one_task(q, w, t)) {
		change_worker_state(q, w, WORKER_STATE_BUSY);
		return 1;
	} else {
		debug(D_NOTICE, "%s (%s) removed because couldn't send task.", w->hostname, w->addrport);
		remove_worker(q, w);	// puts w->current_task back into q->ready_list
		return 0;
	}
}

static void start_tasks(struct work_queue *q)
{				// try to start as many task as possible
	struct work_queue_task *t;
	struct work_queue_worker *w;

	while(list_size(q->ready_list) && q->workers_in_state[WORKER_STATE_READY]) {
		t = list_peek_head(q->ready_list);
		w = find_best_worker(q, t);
		if(w) {
			start_task_on_worker(q, w);
		} else {
			break;
		}
	}
}

static int start_n_tasks(struct work_queue *q, int n)
{				// start at most "n" tasks
	struct work_queue_task *t;
	struct work_queue_worker *w;
	int count = 0;

	if(!q || n <= 0)
		return 0;

	while(list_size(q->ready_list) && q->workers_in_state[WORKER_STATE_READY]) {
		t = list_peek_head(q->ready_list);
		w = find_best_worker(q, t);
		if(w) {
			if(start_task_on_worker(q, w)) {	// successfully started one task on the worker
				count++;
				if(count == n)
					break;
			}
		} else {
			break;
		}
	}

	return count;
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

void abort_slow_workers(struct work_queue *q)
{
	struct work_queue_worker *w;
	char *key;
	const double multiplier = q->fast_abort_multiplier;

	if(q->total_tasks_complete < 10)
		return;

	timestamp_t average_task_time = q->total_task_time / q->total_tasks_complete;
	timestamp_t current = timestamp_get();

	hash_table_firstkey(q->worker_table);
	while(hash_table_nextkey(q->worker_table, &key, (void **) &w)) {
		if(w->state == WORKER_STATE_BUSY) {
			timestamp_t runtime = current - w->current_task->time_send_input_start;
			if(runtime > (average_task_time * multiplier)) {
				debug(D_NOTICE, "%s (%s) has run too long: %.02lf s (average is %.02lf s)", w->hostname, w->addrport, runtime / 1000000.0, average_task_time / 1000000.0);
				remove_worker(q, w);
			}
		}
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


struct work_queue *work_queue_create(int port)
{
	struct work_queue *q = malloc(sizeof(*q));
	char *envstring;

	memset(q, 0, sizeof(*q));

	if(port == 0) {
		envstring = getenv("WORK_QUEUE_PORT");
		if(envstring) {
			port = atoi(envstring);
		} else {
			// indicate using a random available port
			port = 0;
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
		q->port = port;
	}

	q->ready_list = list_create();
	q->complete_list = list_create();

	q->receive_output_waiting_list = list_create();
	q->worker_table = hash_table_create(0, 0);

	// The poll table is initially null, and will be created
	// (and resized) as needed by build_poll_table.
	q->poll_table_size = 8;
	q->poll_table = 0;

	int i;
	for(i = 0; i < WORKER_STATE_MAX; i++) {
		q->workers_in_state[i] = 0;
	}

	q->fast_abort_multiplier = wq_option_fast_abort_multiplier;
	q->worker_selection_algorithm = wq_option_scheduler;
	q->task_ordering = WORK_QUEUE_TASK_ORDER_DEFAULT;

	envstring = getenv("WORK_QUEUE_NAME");
	if(envstring)
		work_queue_specify_name(q, envstring);

	envstring = getenv("WORK_QUEUE_MASTER_MODE");
	if(envstring) {
		work_queue_specify_master_mode(q, atoi(envstring));
	} else {
		q->master_mode = WORK_QUEUE_MASTER_MODE_STANDALONE;
	}

	envstring = getenv("WORK_QUEUE_PRIORITY");
	if(envstring) {
		work_queue_specify_priority(q, atoi(envstring));
	} else {
		q->priority = WORK_QUEUE_MASTER_PRIORITY_DEFAULT;
	}

	envstring = getenv("WORK_QUEUE_WORKER_MODE");
	if(envstring) {
		work_queue_specify_worker_mode(q, atoi(envstring));
	} else {
		q->worker_mode = WORK_QUEUE_WORKER_MODE_SHARED;
	}

	q->estimate_capacity_on = WORK_QUEUE_SWITCH_UNSPECIFIED;
	q->auto_remove_workers_on = WORK_QUEUE_SWITCH_UNSPECIFIED;
	q->work_queue_wait_routine = WORK_QUEUE_WAIT_UNSPECIFIED;

	envstring = getenv("WORK_QUEUE_AUTO_REMOVE_WORKERS_ON");
	if(envstring) {
		work_queue_specify_auto_remove_workers_on(q, atoi(envstring));
	}

	envstring = getenv("WORK_QUEUE_ESTIMATE_CAPACITY_ON");
	if(envstring) {
		work_queue_specify_estimate_capacity_on(q, atoi(envstring));
	}
	// Normal user should not set this!
	envstring = getenv("WORK_QUEUE_WAIT_ROUTINE");
	if(envstring) {
		work_queue_specify_wait_routine(q, atoi(envstring));
	}

	if(q->auto_remove_workers_on == WORK_QUEUE_SWITCH_ON) {
		if(q->estimate_capacity_on == WORK_QUEUE_SWITCH_UNSPECIFIED) {
			q->estimate_capacity_on = WORK_QUEUE_SWITCH_ON;
		}
		if(q->work_queue_wait_routine == WORK_QUEUE_WAIT_UNSPECIFIED) {
			q->work_queue_wait_routine = WORK_QUEUE_WAIT_ADAPTIVE;
		}
		if(q->work_queue_wait_routine != WORK_QUEUE_WAIT_ADAPTIVE || !q->estimate_capacity_on) {
			fprintf(stderr, "Auto remove workers has been turned off!\n");
			fprintf(stderr, "In order to use auto remove workers, please do not turn \"estimate capacity\" option off or\n");
			fprintf(stderr, "set \"work_queue_wait routine\" option to ones other than \"adaptive\".\n");
			q->auto_remove_workers_on = WORK_QUEUE_SWITCH_OFF;
		}
	} else {
		if(q->estimate_capacity_on == WORK_QUEUE_SWITCH_UNSPECIFIED) {
			q->estimate_capacity_on = WORK_QUEUE_SWITCH_OFF;
		}
		if(q->work_queue_wait_routine == WORK_QUEUE_WAIT_UNSPECIFIED) {
			q->work_queue_wait_routine = WORK_QUEUE_WAIT_FCFS;
		}
	}

	envstring = getenv("WORK_QUEUE_CAPACITY_TOLERANCE");
	if(envstring) {
		work_queue_specify_capacity_tolerance(q, atoi(envstring));
	} else {
		q->capacity_tolerance = WORK_QUEUE_CAPACITY_TOLERANCE_DEFAULT;
	}

	envstring = getenv("WORK_QUEUE_MAXIMUM_WORKERS");
	if(envstring) {
		work_queue_specify_maximum_workers(q, atoi(envstring));
	} else {
		q->maximum_workers = WORK_QUEUE_WORKERS_NO_LIMIT;
	}

	q->total_send_time = 0;
	q->total_execute_time = 0;
	q->total_receive_time = 0;

	q->start_time = timestamp_get();
	q->time_last_task_start = q->start_time;

	q->idle_time = 0;
	q->idle_times = list_create();
	q->accumulated_idle_time = 0;

	q->app_time = 0;
	q->capacity = 0;
	q->avg_capacity = 0;
	q->busy_workers_to_remove = 0;

	q->task_statistics = task_statistics_init();

	q->link_keepalive_on = 1;
	q->workers_by_pool = hash_table_create(0,0);

	debug(D_WQ, "Work Queue is listening on port %d.", port);
	return q;

      failure:
	debug(D_NOTICE, "Could not create work_queue on port %i.", port);
	free(q);
	return 0;
}

int work_queue_specify_maximum_workers(struct work_queue *q, int max)
{
	if(max > 0) {
		q->maximum_workers = max;
	} else {
		q->maximum_workers = WORK_QUEUE_WORKERS_NO_LIMIT;
	}
	return q->maximum_workers;
}


int work_queue_specify_estimate_capacity_on(struct work_queue *q, int value)
{
	if(value == WORK_QUEUE_SWITCH_ON) {
		q->estimate_capacity_on = WORK_QUEUE_SWITCH_ON;
	} else if(value == WORK_QUEUE_SWITCH_OFF) {
		q->estimate_capacity_on = WORK_QUEUE_SWITCH_OFF;
	} else {
		q->estimate_capacity_on = WORK_QUEUE_SWITCH_UNSPECIFIED;
	}
	return q->estimate_capacity_on;
}


int work_queue_specify_auto_remove_workers_on(struct work_queue *q, int value)
{
	if(value == WORK_QUEUE_SWITCH_ON) {
		q->auto_remove_workers_on = WORK_QUEUE_SWITCH_ON;
	} else if(value == WORK_QUEUE_SWITCH_OFF) {
		q->auto_remove_workers_on = WORK_QUEUE_SWITCH_OFF;
	} else {
		q->auto_remove_workers_on = WORK_QUEUE_SWITCH_UNSPECIFIED;
	}
	return q->auto_remove_workers_on;
}

int work_queue_specify_wait_routine(struct work_queue *q, int routine)
{
	switch (routine) {
	case WORK_QUEUE_WAIT_FCFS:
	case WORK_QUEUE_WAIT_FAST_DISPATCH:
	case WORK_QUEUE_WAIT_ADAPTIVE:
		q->work_queue_wait_routine = routine;
		break;
	default:
		q->work_queue_wait_routine = WORK_QUEUE_WAIT_UNSPECIFIED;
	}
	return q->work_queue_wait_routine;
}

int work_queue_specify_capacity_tolerance(struct work_queue *q, int tolerance)
{
	if(tolerance > 0 && tolerance <= WORK_QUEUE_CAPACITY_TOLERANCE_MAX) {
		q->capacity_tolerance = tolerance;
	} else {
		q->capacity_tolerance = WORK_QUEUE_CAPACITY_TOLERANCE_DEFAULT;
	}
	return q->capacity_tolerance;
}

int work_queue_specify_name(struct work_queue *q, const char *name)
{
	if(q && name) {
		if(q->name)
			free(q->name);
		q->name = xxstrdup(name);
		setenv("WORK_QUEUE_NAME", q->name, 1);
	}
	return 0;
}

const char *work_queue_name(struct work_queue *q)
{
	return q->name;
}

int work_queue_specify_priority(struct work_queue *q, int priority)
{
	if(priority > 0 && priority <= WORK_QUEUE_MASTER_PRIORITY_MAX) {
		q->priority = priority;
	} else {
		q->priority = WORK_QUEUE_MASTER_PRIORITY_DEFAULT;
	}
	return q->priority;
}

int work_queue_specify_master_mode(struct work_queue *q, int mode)
{
	switch (mode) {
	case WORK_QUEUE_MASTER_MODE_CATALOG:
		/* Set the catalog host and port */
		strncpy(q->catalog_host, CATALOG_HOST, DOMAIN_NAME_MAX);
		q->catalog_port = CATALOG_PORT;
		/* Fall through to set mode */
	case WORK_QUEUE_MASTER_MODE_STANDALONE:
		q->master_mode = mode;
		break;
	default:
		q->master_mode = WORK_QUEUE_MASTER_MODE_STANDALONE;
	}

	return q->master_mode;
}

int work_queue_specify_worker_mode(struct work_queue *q, int mode)
{
	switch (mode) {
	case WORK_QUEUE_WORKER_MODE_EXCLUSIVE:
	case WORK_QUEUE_WORKER_MODE_SHARED:
		q->worker_mode = mode;
		break;
	default:
		q->worker_mode = WORK_QUEUE_WORKER_MODE_SHARED;
	}
	return q->worker_mode;
}

void work_queue_delete(struct work_queue *q)
{
	if(q) {
		struct work_queue_worker *w;
		char *key;

		hash_table_firstkey(q->worker_table);
		while(hash_table_nextkey(q->worker_table, &key, (void **) &w)) {
			remove_worker(q, w);
		}
		if(q->master_mode == WORK_QUEUE_MASTER_MODE_CATALOG) {
			update_catalog(q, 1);
		}
		hash_table_delete(q->worker_table);
		list_delete(q->ready_list);
		list_delete(q->complete_list);
		free(q->poll_table);
		link_close(q->master_link);
		free(q);
	}
}

int work_queue_submit(struct work_queue *q, struct work_queue_task *t)
{
	/* If the task has been used before, clear out accumlated state. */
	if(t->output) {
		free(t->output);
		t->output = 0;
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

static int add_more_workers(struct work_queue *q, time_t stoptime)
{
	int count;

	count = 0;
	do {
		if(add_worker(q))
			count++;
	} while(link_usleep(q->master_link, 0, 1, 0) && stoptime > time(0));
	return count;
}

int master_poll(struct work_queue *q, struct link_info *links, int nlinks, int msec)
{
	timestamp_t idle_start;
	int result;

	idle_start = timestamp_get();
	result = link_poll(links, nlinks, msec);
	q->idle_time += timestamp_get() - idle_start;

	return result;
}

struct work_queue_task *work_queue_wait_fcfs(struct work_queue *q, int timeout)
{
	struct work_queue_task *t;
	int i, n, msec, result;
	time_t stoptime;

	if(timeout == WORK_QUEUE_WAITFORTASK) {
		stoptime = 0;
	} else {
		stoptime = time(0) + timeout;
	}

	while(1) {
		if(q->master_mode == WORK_QUEUE_MASTER_MODE_CATALOG) {
			update_catalog(q, 0);
		}

		t = list_pop_head(q->complete_list);
		if(t) {
			return t;
		}

		if(q->workers_in_state[WORKER_STATE_BUSY] == 0 && list_size(q->ready_list) == 0)
			break;

		start_tasks(q);

		n = build_poll_table(q);

		// Wait no longer than the caller's patience.
		if(stoptime) {
			msec = MAX(0, (stoptime - time(0)) * 1000);
		} else {
			msec = 5000;
		}

		result = master_poll(q, q->poll_table, n, msec);

		// If a process is waiting to complete, return without a task.
		if(process_pending()) {
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
		// If the master link was awake, then accept as many workers as possible.
		if(q->poll_table[0].revents) {
			add_more_workers(q, stoptime);
		}
		// Then consider all existing active workers and dispatch tasks.
		for(i = 1; i < n; i++) {
			if(q->poll_table[i].revents) {
				handle_worker(q, q->poll_table[i].link);
			}
		}

		// If fast abort is enabled, kill off slow workers.
		if(q->fast_abort_multiplier > 0) {
			abort_slow_workers(q);
		}
	}

	return 0;
}

/**
 * This function is for research use only.
 */
struct work_queue_task *work_queue_wait_fast_dispatch(struct work_queue *q, int timeout)
{
	struct work_queue_task *t;
	struct pending_output *po;
	int i, n, msec, result;
	time_t stoptime;
	static int added_workers = 0;

	if(timeout == WORK_QUEUE_WAITFORTASK) {
		stoptime = 0;
	} else {
		stoptime = time(0) + timeout;
	}

	while(1) {
		if(q->master_mode == WORK_QUEUE_MASTER_MODE_CATALOG) {
			update_catalog(q, 0);
		}

		t = list_pop_head(q->complete_list);
		if(t) {
			return t;
		}

		if(q->workers_in_state[WORKER_STATE_BUSY] == 0 && list_size(q->ready_list) == 0)
			break;

		// Upper level application may have just submitted some new tasks. Try to dispatch them to ready workers.
		start_tasks(q);

		if(added_workers == 0) {
			if((po = list_pop_head(q->receive_output_waiting_list))) {
				receive_pending_output(q, po);
				free(po);
			}
		}
		added_workers = 0;

		n = build_poll_table(q);

		// Wait no longer than the caller's patience.
		if(stoptime) {
			msec = MAX(0, (stoptime - time(0)) * 1000);
		} else {
			msec = 5000;
		}

		// There are output waiting for being tranferred back. So, do not waste time on polling.
		if(list_size(q->receive_output_waiting_list) != 0) {
			msec = 0;
		}

		result = master_poll(q, q->poll_table, n, msec);

		// If a process is waiting to complete, return without a task.
		if(process_pending()) {
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
		// If the master link was awake, then accept as many workers as possible.
		if(q->poll_table[0].revents) {
			added_workers = add_more_workers(q, stoptime);
		}
		// Then consider all existing active workers and dispatch tasks.
		for(i = 1; i < n; i++) {
			if(q->poll_table[i].revents) {
				handle_worker(q, q->poll_table[i].link);
			}
		}

		// This is after polling because new added workers must be in READY state in order to receive tasks.
		start_tasks(q);

		// If fast abort is enabled, kill off slow workers.
		if(q->fast_abort_multiplier > 0) {
			abort_slow_workers(q);
		}
	}

	return 0;
}

// make a smarter decision on whether to start new task on a new worker or an old worker.
struct work_queue_task *work_queue_wait_adaptive(struct work_queue *q, int timeout)
{
	struct work_queue_task *t;
	struct pending_output *po;
	int i, n, msec, result, percentage;
	int ready_workers;
	time_t stoptime;

	if(timeout == WORK_QUEUE_WAITFORTASK) {
		stoptime = 0;
	} else {
		stoptime = time(0) + timeout;
	}

	while(1) {
		if(q->master_mode == WORK_QUEUE_MASTER_MODE_CATALOG) {
			update_catalog(q, 0);
		}

		t = list_pop_head(q->complete_list);
		if(t) {
			return t;
		}

		if(q->workers_in_state[WORKER_STATE_BUSY] == 0 && list_size(q->ready_list) == 0)
			break;

		if((po = list_pop_head(q->receive_output_waiting_list))) {
			receive_pending_output(q, po);
			free(po);
			continue;
		}

		percentage = get_idle_percentage(q) * 100;
		ready_workers = q->workers_in_state[WORKER_STATE_READY];
		if(ready_workers > 0 && percentage > 0) {
			// start new tasks conservatively, one at a time, otherwise we
			// might use too many workers than we actually need. 
			start_n_tasks(q, 1);
			//work_queue_shut_down_workers (q, 1000);
		}

		n = build_poll_table(q);

		// Wait no longer than the caller's patience.
		if(stoptime) {
			msec = MAX(0, (stoptime - time(0)) * 1000);
		} else {
			msec = 5000;
		}

		// there are ready tasks and ready workers
		if(list_size(q->ready_list) && q->workers_in_state[WORKER_STATE_READY]) {
			msec = 0;
		}
		// There are output waiting for being tranferred back. So, do not waste
		// time on polling.
		if(list_size(q->receive_output_waiting_list) != 0) {
			msec = 0;
		}

		result = master_poll(q, q->poll_table, n, msec);

		// If a process is waiting to complete, return without a task.
		if(process_pending()) {
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
		// If the master link was awake, then accept as many workers as possible.
		if(q->poll_table[0].revents) {
			add_more_workers(q, stoptime);
		}
		// Then consider all existing active workers and dispatch tasks.
		for(i = 1; i < n; i++) {
			if(q->poll_table[i].revents) {
				handle_worker(q, q->poll_table[i].link);
			}
		}

		// If fast abort is enabled, kill off slow workers.
		if(q->fast_abort_multiplier > 0) {
			abort_slow_workers(q);
		}
	}

	return 0;
}

struct work_queue_task *work_queue_wait(struct work_queue *q, int timeout)
{
	struct work_queue_task *result;
	static timestamp_t last_left_time = 0;
	static int last_left_status = 0;	// 0 -- did not return any done task; 1 -- returned done task 
	static time_t next_pool_decision_enforcement = 0;

	if(last_left_time && last_left_status == 1) {
		if(q->total_tasks_complete > MIN_TIME_LIST_SIZE) {
			timestamp_t tmp_time = timestamp_get() - last_left_time;
			// A simple way of discarding outliers that does not require much calculation.
			// Works for workloads that has stable app times.
			if(tmp_time > WORK_QUEUE_APP_TIME_OUTLIER_MULTIPLIER * (q->app_time / q->total_tasks_complete)) {
				debug(D_WQ, "Discarding outlier task app time: %lld\n", tmp_time);
			} else {
				q->app_time += tmp_time;
			}
		} else {
			q->app_time += timestamp_get() - last_left_time;
		}
	}

	if(q->master_mode == WORK_QUEUE_MASTER_MODE_CATALOG && next_pool_decision_enforcement < time(0)) {
		enforce_pool_decisions(q);
		next_pool_decision_enforcement = time(0) + POOL_DECISION_ENFORCEMENT_INTERVAL_DEFAULT;
	}

	switch (q->work_queue_wait_routine) {
	case WORK_QUEUE_WAIT_FCFS:
		result = work_queue_wait_fcfs(q, timeout);
		break;
	case WORK_QUEUE_WAIT_FAST_DISPATCH:
		result = work_queue_wait_fast_dispatch(q, timeout);
		break;
	case WORK_QUEUE_WAIT_ADAPTIVE:
		result = work_queue_wait_adaptive(q, timeout);
		break;
	default:
		result = work_queue_wait_fcfs(q, timeout);
	}

	last_left_time = timestamp_get();
	last_left_status = result ? 1 : 0;

	return result;
}

void receive_pending_output(struct work_queue *q, struct pending_output *p)
{
	struct work_queue_worker *w;
	char key[WORK_QUEUE_CATALOG_LINE_MAX];

	if(!p)
		return;

	link_to_hash_key(p->link, key);
	w = hash_table_lookup(q->worker_table, key);
	if(!w)
		return;

	// make sure it's not a new worker (orginal worker might be disconnected and replaced)
	if(p->start > w->start_time) {
		if(receive_output_from_worker(q, w)) {
			if(q->auto_remove_workers_on && q->busy_workers_to_remove > 0) {
				q->busy_workers_to_remove--;
			} else {
				start_task_on_worker(q, w);
			}
		}
	}
}

int work_queue_hungry(struct work_queue *q)
{
	struct work_queue_stats qs;
	int i, j;
	work_queue_get_stats(q, &qs);

	if(qs.total_tasks_dispatched < 100)
		return (100 - qs.total_tasks_dispatched);

	//i = 1.1 * number of current workers
	//j = # of queued tasks.
	//i-j = # of tasks to queue to re-reach the status quo.
	i = (1.1 * (qs.workers_init + qs.workers_ready + qs.workers_busy));
	j = (qs.tasks_waiting);
	return MAX(i - j, 0);
}


void work_queue_task_specify_tag(struct work_queue_task *t, const char *tag)
{
	if(t->tag)
		free(t->tag);
	t->tag = xxstrdup(tag);
}

void work_queue_task_specify_preferred_host(struct work_queue_task *t, const char *hostname)
{
	if(t->preferred_host)
		free(t->preferred_host);
	t->preferred_host = xxstrdup(hostname);
}

void work_queue_task_specify_file(struct work_queue_task *t, const char *local_name, const char *remote_name, int type, int flags)
{
	struct work_queue_file *tf = malloc(sizeof(struct work_queue_file));

	tf->type = WORK_QUEUE_FILE;
	tf->flags = flags;
	tf->length = strlen(local_name);
	tf->payload = xxstrdup(local_name);
	tf->remote_name = xxstrdup(remote_name);

	if(type == WORK_QUEUE_INPUT) {
		list_push_tail(t->input_files, tf);
	} else {
		list_push_tail(t->output_files, tf);
	}
}

void work_queue_task_specify_buffer(struct work_queue_task *t, const char *data, int length, const char *remote_name, int flags)
{
	struct work_queue_file *tf = malloc(sizeof(struct work_queue_file));
	tf->type = WORK_QUEUE_BUFFER;
	tf->flags = flags;
	tf->length = length;
	tf->payload = malloc(length);
	memcpy(tf->payload, data, length);
	tf->remote_name = xxstrdup(remote_name);
	list_push_tail(t->input_files, tf);
}

void work_queue_task_specify_file_command(struct work_queue_task *t, const char *remote_name, const char *cmd, int type, int flags)
{
	struct work_queue_file *tf = malloc(sizeof(struct work_queue_file));
	tf->type = WORK_QUEUE_REMOTECMD;
	tf->flags = flags;
	tf->length = strlen(cmd);
	tf->payload = xxstrdup(cmd);
	tf->remote_name = xxstrdup(remote_name);

	if(type == WORK_QUEUE_INPUT) {
		list_push_tail(t->input_files, tf);
	} else {
		list_push_tail(t->output_files, tf);
	}
}

void work_queue_task_specify_output_file(struct work_queue_task *t, const char *rname, const char *fname)
{
	return work_queue_task_specify_file(t, fname, rname, WORK_QUEUE_OUTPUT, WORK_QUEUE_CACHE);
}

void work_queue_task_specify_output_file_do_not_cache(struct work_queue_task *t, const char *rname, const char *fname)
{
	return work_queue_task_specify_file(t, fname, rname, WORK_QUEUE_OUTPUT, WORK_QUEUE_NOCACHE);
}

void work_queue_task_specify_input_buf(struct work_queue_task *t, const char *buf, int length, const char *rname)
{
	return work_queue_task_specify_buffer(t, buf, length, rname, WORK_QUEUE_NOCACHE);
}

void work_queue_task_specify_input_file(struct work_queue_task *t, const char *fname, const char *rname)
{
	return work_queue_task_specify_file(t, fname, rname, WORK_QUEUE_INPUT, WORK_QUEUE_CACHE);
}

void work_queue_task_specify_input_file_do_not_cache(struct work_queue_task *t, const char *fname, const char *rname)
{
	return work_queue_task_specify_file(t, fname, rname, WORK_QUEUE_INPUT, WORK_QUEUE_NOCACHE);
}

int work_queue_task_specify_algorithm(struct work_queue_task *t, int alg)
{
	if(t && alg >= WORK_QUEUE_SCHEDULE_UNSET && alg <= WORK_QUEUE_SCHEDULE_MAX) {
		t->worker_selection_algorithm = alg;
		return 0;
	} else {
		return 1;
	}
}

int work_queue_specify_algorithm(struct work_queue *q, int alg)
{
	if(q && alg > WORK_QUEUE_SCHEDULE_UNSET && alg <= WORK_QUEUE_SCHEDULE_MAX) {
		q->worker_selection_algorithm = alg;
		return 0;
	} else {
		return 1;
	}
}

int work_queue_specify_task_order(struct work_queue *q, int order)
{
	if(q && order >= WORK_QUEUE_TASK_ORDER_FIFO && order <= WORK_QUEUE_TASK_ORDER_LIFO) {
		q->task_ordering = order;
		return 0;
	} else {
		return 1;
	}
}

static int shut_down_worker(struct work_queue *q, struct work_queue_worker *w)
{
	if(!w)
		return 0;

	link_putliteral(w->link, "exit\n", time(0) + short_timeout);
	remove_worker(q, w);
	return 1;
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

int taskid_comparator(void *t, const void *r) {

	struct work_queue_task *task_in_queue = t;
	const struct work_queue_task *task_to_remove = r;

	if (task_in_queue->taskid == task_to_remove->taskid) {
		return 1;
	}	
	return 0;
}

/**
 * Note this is different from work_queue_task_delete(). This simply removes the
 * task from ready list and returns without cleaning up the task. This means the 
 * task can be resubmitted to queue again from userland. It is still up to the 
 * user to call work_queue_task_delete() when the task is no longer required.
 */
int work_queue_task_remove(struct work_queue *q, struct work_queue_task *t) {

	struct work_queue_task *matched_task;

	if (t->taskid > 0){
		matched_task = list_find(q->ready_list, taskid_comparator, t);
		if (matched_task) {	
			list_remove(q->ready_list,matched_task);
			debug(D_WQ, "Task with tag %s is removed.", matched_task->tag);
			return 0;
		}
	}
	return 1;
}

int work_queue_empty(struct work_queue *q)
{
	return ((list_size(q->ready_list) + list_size(q->complete_list) + q->workers_in_state[WORKER_STATE_BUSY]) == 0);
}

static int tolerable_transfer_rate_denominator = 10;
static long double minimum_allowed_transfer_rate = 100000;	// 100 KB/s
static timestamp_t get_transfer_wait_time(struct work_queue *q, struct work_queue_worker *w, INT64_T length)
{
	timestamp_t timeout;
	struct work_queue_task *t;
	long double avg_queue_transfer_rate, avg_worker_transfer_rate, retry_transfer_rate, tolerable_transfer_rate;
	INT64_T total_tasks_complete, total_tasks_running, total_tasks_waiting, num_of_free_workers;

	t = w->current_task;

	if(w->total_transfer_time) {
		avg_worker_transfer_rate = (long double) w->total_bytes_transferred / w->total_transfer_time * 1000000;
	} else {
		avg_worker_transfer_rate = 0;
	}

	retry_transfer_rate = 0;
	num_of_free_workers = q->workers_in_state[WORKER_STATE_INIT] + q->workers_in_state[WORKER_STATE_READY];
	total_tasks_complete = q->total_tasks_complete;
	total_tasks_running = q->workers_in_state[WORKER_STATE_BUSY];
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

	timeout = MAX(3, length / tolerable_transfer_rate);	// try at least 3 seconds

	debug(D_WQ, "%s (%s) will try up to %lld seconds for the transfer of this %.3Lf MB file.", w->hostname, w->addrport, timeout, (long double) length / 1000000);
	return timeout;
}

static void update_catalog(struct work_queue *q, int now)
{
	struct work_queue_stats s;
	work_queue_get_stats(q, &s);
	if(!advertise_master_to_catalog(q->catalog_host, q->catalog_port, q->name, &s, now)) {
		fprintf(stderr, "Reporting master status to the catalog server (%s@%d) failed!\n", q->catalog_host, q->catalog_port);
	}
}

static int release_worker(struct work_queue *q, struct work_queue_worker *w)
{
	if(!w)
		return 0;

	link_putliteral(w->link, "release\n", time(0) + short_timeout);
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
		debug(D_NOTICE, "Failed to create list to store worker pool decisions!\n");
		return;
	}
	if(!get_pool_decisions_from_catalog(q->catalog_host, q->catalog_port, q->name, decisions)) {
		fprintf(stderr, "Failed to receive pool decisions from the catalog server(%s@%d)!\n", q->catalog_host, q->catalog_port);
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
				printf("%d worker(s) has been rejected to enforce the pool decison.\n", k);
			}
		} 
	}

	list_free(decisions);
	list_delete(decisions);
}
